use pgrx::prelude::*;

pg_module_magic!();

mod introspect;
mod reader;
mod sweep;
mod types;

use types::{DeleteMode, MergeMode, PlanRow};

/// Native Rust implementation of the temporal_merge planner.
/// Drop-in replacement for sql_saga.temporal_merge_plan() — produces the same
/// SETOF sql_saga.temporal_merge_plan output via direct INSERT into pg_temp.temporal_merge_plan.
///
/// This function reads source + target rows via 2 bulk SPI scans, then performs
/// all planning (atomic segmentation, payload resolution, coalescing, diff,
/// operation classification, statement sequencing) in memory using a sweep-line
/// algorithm.
#[pg_extern]
fn temporal_merge_plan_native(
    target_table: pg_sys::Oid,
    source_table: pg_sys::Oid,
    mode: &str,
    era_name: &str,
    identity_columns: Option<Vec<String>>,
    row_id_column: default!(&str, "'row_id'"),
    founding_id_column: Option<&str>,
    delete_mode: default!(&str, "'NONE'"),
    lookup_keys: Option<pgrx::JsonB>,
    ephemeral_columns: Option<Vec<String>>,
    p_log_trace: default!(bool, false),
    _p_log_sql: default!(bool, false),
) -> i64 {
    let mode = MergeMode::from_str(mode)
        .unwrap_or_else(|| pgrx::error!("Invalid merge mode: {}", mode));
    let delete_mode = DeleteMode::from_str(delete_mode)
        .unwrap_or_else(|| pgrx::error!("Invalid delete mode: {}", delete_mode));

    // Parse lookup_keys JSONB into Vec<Vec<String>>
    let all_lookup_cols = parse_lookup_keys(lookup_keys);

    // Phase 1: Introspect era metadata
    let era = introspect::introspect_era(target_table, era_name)
        .unwrap_or_else(|e| pgrx::error!("{}", e));

    // Introspect PK columns
    let mut temporal_cols = vec![era.range_col.clone(), era.valid_from_col.clone()];
    if let Some(ref vt) = era.valid_to_col {
        temporal_cols.push(vt.clone());
    }
    temporal_cols.push(era.valid_until_col.clone());
    let pk_cols = introspect::introspect_pk_cols(target_table, &temporal_cols);

    // Build planner context
    let ctx = introspect::build_planner_context(
        mode,
        delete_mode,
        era,
        identity_columns,
        all_lookup_cols,
        pk_cols,
        ephemeral_columns.unwrap_or_default(),
        founding_id_column.map(|s| s.to_string()),
        row_id_column.to_string(),
        p_log_trace,
    );

    // Phase 2: Bulk SPI reads
    let source_rows = reader::read_source_rows(source_table, &ctx)
        .unwrap_or_else(|e| pgrx::error!("Failed to read source rows: {}", e));
    let target_rows = reader::read_target_rows(target_table, &ctx)
        .unwrap_or_else(|e| pgrx::error!("Failed to read target rows: {}", e));

    // Phase 3: Sweep-line planning
    let plan_rows = sweep::sweep_line_plan(source_rows, target_rows, &ctx);

    // Phase 4: Insert into pg_temp.temporal_merge_plan
    let count = emit_plan_rows(&plan_rows);

    count
}

/// Parse lookup_keys JSONB (array of arrays) into a flat, distinct list of column names.
fn parse_lookup_keys(lookup_keys: Option<pgrx::JsonB>) -> Option<Vec<String>> {
    let pgrx::JsonB(val) = lookup_keys?;
    let arr = val.as_array()?;
    let mut cols = Vec::new();
    for key_array in arr {
        if let Some(inner) = key_array.as_array() {
            for col in inner {
                if let Some(s) = col.as_str() {
                    if !cols.contains(&s.to_string()) {
                        cols.push(s.to_string());
                    }
                }
            }
        }
    }
    if cols.is_empty() {
        None
    } else {
        cols.sort();
        Some(cols)
    }
}

/// Insert plan rows into pg_temp.temporal_merge_plan via SPI.
fn emit_plan_rows(plan_rows: &[PlanRow]) -> i64 {
    if plan_rows.is_empty() {
        return 0;
    }

    // Build a batch INSERT statement
    let mut values_parts: Vec<String> = Vec::with_capacity(plan_rows.len());

    for row in plan_rows {
        let row_ids_str = format!(
            "ARRAY[{}]::bigint[]",
            row.row_ids
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );

        let operation = format!("'{}'::sql_saga.temporal_merge_plan_action", row.operation.as_str());
        let update_effect = row
            .update_effect
            .map(|e| format!("'{}'::sql_saga.temporal_merge_update_effect", e.as_str()))
            .unwrap_or_else(|| "NULL".to_string());

        let causal_id = sql_text_or_null(&row.causal_id);
        let is_new_entity = row.is_new_entity.to_string();
        let entity_keys = sql_jsonb_or_null(&row.entity_keys);
        let identity_keys = sql_jsonb_or_null(&row.identity_keys);
        let lookup_keys = sql_jsonb_or_null(&row.lookup_keys);

        let s_t_relation = row
            .s_t_relation
            .map(|r| format!("'{}'::sql_saga.allen_interval_relation", r.as_str()))
            .unwrap_or_else(|| "NULL".to_string());
        let b_a_relation = row
            .b_a_relation
            .map(|r| format!("'{}'::sql_saga.allen_interval_relation", r.as_str()))
            .unwrap_or_else(|| "NULL".to_string());

        let old_valid_from = sql_text_or_null(&row.old_valid_from);
        let old_valid_until = sql_text_or_null(&row.old_valid_until);
        let new_valid_from = sql_text_or_null(&row.new_valid_from);
        let new_valid_until = sql_text_or_null(&row.new_valid_until);
        let old_valid_range = sql_text_or_null(&row.old_valid_range);
        let new_valid_range = sql_text_or_null(&row.new_valid_range);
        let data = sql_jsonb_or_null(&row.data);
        let feedback = sql_jsonb_or_null(&row.feedback);
        let trace = sql_jsonb_or_null(&row.trace);
        let grouping_key = sql_escape_text(&row.grouping_key);

        values_parts.push(format!(
            "({plan_op_seq}, {stmt_seq}, {row_ids}, {op}, {ue}, {causal}, {is_new}, {ek}, {ik}, {lk}, {st}, {ba}, {ovf}, {ovu}, {nvf}, {nvu}, {ovr}, {nvr}, {data}, {fb}, {trace}, {gk})",
            plan_op_seq = row.plan_op_seq,
            stmt_seq = row.statement_seq,
            row_ids = row_ids_str,
            op = operation,
            ue = update_effect,
            causal = causal_id,
            is_new = is_new_entity,
            ek = entity_keys,
            ik = identity_keys,
            lk = lookup_keys,
            st = s_t_relation,
            ba = b_a_relation,
            ovf = old_valid_from,
            ovu = old_valid_until,
            nvf = new_valid_from,
            nvu = new_valid_until,
            ovr = old_valid_range,
            nvr = new_valid_range,
            data = data,
            fb = feedback,
            trace = trace,
            gk = grouping_key,
        ));
    }

    // Batch insert in chunks to avoid SQL statement size limits
    let chunk_size = 500;
    let mut total = 0i64;

    Spi::connect_mut(|client| {
        for chunk in values_parts.chunks(chunk_size) {
            let sql = format!(
                "INSERT INTO pg_temp.temporal_merge_plan (plan_op_seq, statement_seq, row_ids, operation, update_effect, causal_id, is_new_entity, entity_keys, identity_keys, lookup_keys, s_t_relation, b_a_relation, old_valid_from, old_valid_until, new_valid_from, new_valid_until, old_valid_range, new_valid_range, data, feedback, trace, grouping_key) VALUES {}",
                chunk.join(", ")
            );
            client.update(&sql, None, &[])
                .unwrap_or_else(|e| pgrx::error!("Failed to insert plan rows: {}", e));
            total += chunk.len() as i64;
        }
    });

    total
}

fn sql_text_or_null(val: &Option<String>) -> String {
    match val {
        Some(s) => format!("'{}'", s.replace('\'', "''")),
        None => "NULL".to_string(),
    }
}

fn sql_escape_text(val: &str) -> String {
    format!("'{}'", val.replace('\'', "''"))
}

fn sql_jsonb_or_null(val: &Option<serde_json::Value>) -> String {
    match val {
        Some(v) => format!(
            "'{}'::jsonb",
            serde_json::to_string(v)
                .unwrap_or_else(|_| "{}".to_string())
                .replace('\'', "''")
        ),
        None => "NULL".to_string(),
    }
}

// ── Tests ──

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_extension_loads() {
        let result = Spi::get_one::<bool>("SELECT true");
        assert_eq!(result, Ok(Some(true)));
    }

    #[pg_test]
    fn test_allen_relation_equals() {
        use crate::types::AllenRelation;
        let r = AllenRelation::compute("2024-01-01", "2024-02-01", "2024-01-01", "2024-02-01");
        assert_eq!(r, Some(AllenRelation::Equals));
    }

    #[pg_test]
    fn test_allen_relation_precedes() {
        use crate::types::AllenRelation;
        let r = AllenRelation::compute("2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01");
        assert_eq!(r, Some(AllenRelation::Precedes));
    }

    #[pg_test]
    fn test_allen_relation_meets() {
        use crate::types::AllenRelation;
        let r = AllenRelation::compute("2024-01-01", "2024-02-01", "2024-02-01", "2024-03-01");
        assert_eq!(r, Some(AllenRelation::Meets));
    }

    #[pg_test]
    fn test_allen_relation_overlaps() {
        use crate::types::AllenRelation;
        let r = AllenRelation::compute("2024-01-01", "2024-03-01", "2024-02-01", "2024-04-01");
        assert_eq!(r, Some(AllenRelation::Overlaps));
    }

    #[pg_test]
    fn test_strip_nulls() {
        use crate::sweep::strip_nulls;
        let mut map = serde_json::Map::new();
        map.insert("a".to_string(), serde_json::Value::Number(1.into()));
        map.insert("b".to_string(), serde_json::Value::Null);
        map.insert("c".to_string(), serde_json::Value::String("x".to_string()));
        let stripped = strip_nulls(&map);
        assert_eq!(stripped.len(), 2);
        assert!(stripped.contains_key("a"));
        assert!(stripped.contains_key("c"));
        assert!(!stripped.contains_key("b"));
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
