use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Instant;

use pgrx::prelude::*;

pg_module_magic!();

mod introspect;
mod reader;
mod sweep;
mod types;

use types::{CachedState, DeleteMode, MergeMode, PlanRow};

thread_local! {
    static PLANNER_CACHE: RefCell<Option<CachedState>> = RefCell::new(None);
    static EMIT_STMT: RefCell<Option<pgrx::spi::OwnedPreparedStatement>> = RefCell::new(None);
}

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

    // Compute cache key (excludes source_table OID — it changes per batch)
    let cache_key = compute_cache_key(
        target_table,
        mode,
        era_name,
        &identity_columns,
        &all_lookup_cols,
        &ephemeral_columns,
        founding_id_column,
        row_id_column,
        delete_mode,
        p_log_trace,
    );

    // Resolve source_ident (changes per batch, always needed)
    let source_ident = reader::resolve_table_name(source_table)
        .unwrap_or_else(|e| pgrx::error!("Failed to resolve source table: {}", e));

    // Check cache and get or build CachedState
    let cache_hit = PLANNER_CACHE.with(|c| {
        c.borrow().as_ref().map_or(false, |s| s.cache_key == cache_key)
    });

    let state = if cache_hit {
        PLANNER_CACHE.with(|c| c.borrow().as_ref().unwrap().clone())
    } else {
        // Cache miss: clear cached prepared statements
        EMIT_STMT.with(|cell| {
            *cell.borrow_mut() = None;
        });
        reader::clear_target_read_cache();
        reader::clear_source_read_cache();

        // Single SPI connection for all introspection
        let result = introspect::introspect_all(target_table, source_table, era_name)
            .unwrap_or_else(|e| pgrx::error!("{}", e));

        let ctx = introspect::build_planner_context(
            mode,
            delete_mode,
            result.era,
            identity_columns,
            all_lookup_cols,
            result.pk_cols,
            ephemeral_columns.unwrap_or_default(),
            founding_id_column.map(|s| s.to_string()),
            row_id_column.to_string(),
            p_log_trace,
        );

        // Build SQL templates from pre-fetched column data (no SPI calls)
        let templates = reader::build_sql_templates_from_cols(
            &result.source_cols,
            &result.target_cols,
            &result.target_col_types,
            &result.target_ident,
            &ctx,
        )
        .unwrap_or_else(|e| pgrx::error!("Failed to build SQL templates: {}", e));

        let new_state = CachedState {
            cache_key,
            ctx,
            target_ident: templates.target_ident,
            source_sql_template: templates.source_sql_template,
            target_sql_template: templates.target_sql_template,
            source_data_cols: templates.source_data_cols,
            target_data_cols: templates.target_data_cols,
            eph_in_source: templates.eph_in_source,
            eph_in_target: templates.eph_in_target,
            target_filter_params: templates.target_filter_params,
        };
        PLANNER_CACHE.with(|c| {
            *c.borrow_mut() = Some(new_state.clone());
        });

        new_state
    };

    let t_start = Instant::now();

    // Phase 2a: Read source rows (cached prepared statement, keyed by source_ident)
    let source_rows = reader::read_source_rows_cached(&source_ident, &state)
        .unwrap_or_else(|e| pgrx::error!("Failed to read source rows: {}", e));
    let t_source = Instant::now();

    // Phase 2b: Read target rows — parameterized (cached stmt) or dynamic SQL
    let target_rows = if let Some(ref filter_params) = state.target_filter_params {
        if filter_params.is_empty() {
            // Full scan or no filter: static SQL, no parameters
            reader::read_target_rows_parameterized(&state, &[])
                .unwrap_or_else(|e| pgrx::error!("Failed to read target rows: {}", e))
        } else {
            // Parameterized filter: extract identity values from source rows
            let param_values = reader::extract_filter_values(&source_rows, filter_params);
            reader::read_target_rows_parameterized(&state, &param_values)
                .unwrap_or_else(|e| pgrx::error!("Failed to read target rows: {}", e))
        }
    } else {
        // Dynamic SQL fallback (multi-column key sets)
        let target_sql = state
            .target_sql_template
            .replace("__SOURCE_IDENT__", &source_ident);
        reader::read_target_rows_with_sql(&target_sql, &state)
            .unwrap_or_else(|e| pgrx::error!("Failed to read target rows: {}", e))
    };
    let t_target = Instant::now();

    // Phase 3: Sweep-line planning
    let plan_rows = sweep::sweep_line_plan(source_rows, target_rows, &state.ctx);
    let t_sweep = Instant::now();

    // Phase 4: Insert into pg_temp.temporal_merge_plan
    let count = emit_plan_rows(&plan_rows);
    let t_emit = Instant::now();

    if p_log_trace {
        let n_src = plan_rows.len(); // plan_rows count as proxy
        pgrx::notice!(
            "native planner timing: cache={}, source_read={:.1}ms, target_read={:.1}ms, sweep={:.1}ms ({}plan_rows), emit={:.1}ms, total={:.1}ms",
            if cache_hit { "HIT" } else { "MISS" },
            t_source.duration_since(t_start).as_secs_f64() * 1000.0,
            t_target.duration_since(t_source).as_secs_f64() * 1000.0,
            t_sweep.duration_since(t_target).as_secs_f64() * 1000.0,
            n_src,
            t_emit.duration_since(t_sweep).as_secs_f64() * 1000.0,
            t_emit.duration_since(t_start).as_secs_f64() * 1000.0,
        );
    }

    count
}

/// Compute a cache key from all parameters that affect SQL template construction.
/// Source table OID is excluded because it changes per batch.
fn compute_cache_key(
    target_table: pg_sys::Oid,
    mode: MergeMode,
    era_name: &str,
    identity_columns: &Option<Vec<String>>,
    all_lookup_cols: &Option<Vec<String>>,
    ephemeral_columns: &Option<Vec<String>>,
    founding_id_column: Option<&str>,
    row_id_column: &str,
    delete_mode: DeleteMode,
    log_trace: bool,
) -> u64 {
    let mut hasher = DefaultHasher::new();
    u32::from(target_table).hash(&mut hasher);
    mode.hash(&mut hasher);
    era_name.hash(&mut hasher);
    identity_columns.hash(&mut hasher);
    all_lookup_cols.hash(&mut hasher);
    ephemeral_columns.hash(&mut hasher);
    founding_id_column.hash(&mut hasher);
    row_id_column.hash(&mut hasher);
    delete_mode.hash(&mut hasher);
    log_trace.hash(&mut hasher);
    hasher.finish()
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

/// Insert plan rows into pg_temp.temporal_merge_plan via a single bulk
/// INSERT ... SELECT * FROM json_populate_recordset(...).
/// Serializes all rows into one JSON array, sends as one SPI call.
fn emit_plan_rows(plan_rows: &[PlanRow]) -> i64 {
    use pgrx::datum::DatumWithOid;
    use std::fmt::Write;

    if plan_rows.is_empty() {
        return 0;
    }

    // Serialize all plan rows into a single JSON array string
    // Use a pre-allocated buffer to avoid intermediate allocations
    let estimated_size = plan_rows.len() * 300; // ~300 bytes per row average
    let mut buf = String::with_capacity(estimated_size);
    buf.push('[');

    for (i, row) in plan_rows.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        buf.push('{');
        // plan_op_seq, statement_seq
        write!(buf, "\"plan_op_seq\":{},\"statement_seq\":{}", row.plan_op_seq, row.statement_seq).unwrap();
        // row_ids as JSON array
        buf.push_str(",\"row_ids\":[");
        for (j, id) in row.row_ids.iter().enumerate() {
            if j > 0 { buf.push(','); }
            write!(buf, "{}", id).unwrap();
        }
        buf.push(']');
        // operation (always present)
        write!(buf, ",\"operation\":\"{}\"", row.operation.as_str()).unwrap();
        // update_effect (nullable)
        if let Some(ue) = row.update_effect {
            write!(buf, ",\"update_effect\":\"{}\"", ue.as_str()).unwrap();
        } else {
            buf.push_str(",\"update_effect\":null");
        }
        // causal_id (nullable)
        json_text_field(&mut buf, "causal_id", &row.causal_id);
        // is_new_entity
        write!(buf, ",\"is_new_entity\":{}", row.is_new_entity).unwrap();
        // JSONB fields
        json_value_field(&mut buf, "entity_keys", &row.entity_keys);
        json_value_field(&mut buf, "identity_keys", &row.identity_keys);
        json_value_field(&mut buf, "lookup_keys", &row.lookup_keys);
        // Allen relations (nullable enums)
        if let Some(r) = row.s_t_relation {
            write!(buf, ",\"s_t_relation\":\"{}\"", r.as_str()).unwrap();
        } else {
            buf.push_str(",\"s_t_relation\":null");
        }
        if let Some(r) = row.b_a_relation {
            write!(buf, ",\"b_a_relation\":\"{}\"", r.as_str()).unwrap();
        } else {
            buf.push_str(",\"b_a_relation\":null");
        }
        // Text fields (all nullable)
        json_text_field(&mut buf, "old_valid_from", &row.old_valid_from);
        json_text_field(&mut buf, "old_valid_until", &row.old_valid_until);
        json_text_field(&mut buf, "new_valid_from", &row.new_valid_from);
        json_text_field(&mut buf, "new_valid_until", &row.new_valid_until);
        json_text_field(&mut buf, "old_valid_range", &row.old_valid_range);
        json_text_field(&mut buf, "new_valid_range", &row.new_valid_range);
        // JSONB fields
        json_value_field(&mut buf, "data", &row.data);
        json_value_field(&mut buf, "feedback", &row.feedback);
        json_value_field(&mut buf, "trace", &row.trace);
        // grouping_key (always present)
        write!(buf, ",\"grouping_key\":\"{}\"", json_escape(&row.grouping_key)).unwrap();
        buf.push('}');
    }
    buf.push(']');

    let count = plan_rows.len() as i64;

    // Single SPI call: bulk INSERT via json_populate_recordset
    // Uses SPI_keepplan to cache the prepared statement across batches.
    // If the temp table is recreated, PostgreSQL auto-replans (skips parsing).
    Spi::connect_mut(|client| {
        // Check if we have a cached prepared statement
        let has_stmt = EMIT_STMT.with(|cell| cell.borrow().is_some());

        if !has_stmt {
            // First call: prepare and keep
            let stmt = client
                .prepare_mut(
                    "INSERT INTO pg_temp.temporal_merge_plan \
                     SELECT * FROM json_populate_recordset(null::pg_temp.temporal_merge_plan, $1::json)",
                    &[pgrx::PgOid::from(pg_sys::TEXTOID)],
                )
                .unwrap_or_else(|e| pgrx::error!("Failed to prepare bulk insert: {}", e));
            let owned = stmt.keep();
            EMIT_STMT.with(|cell| {
                *cell.borrow_mut() = Some(owned);
            });
        }

        // Execute using cached prepared statement
        let args = [DatumWithOid::from(buf)];
        EMIT_STMT.with(|cell| {
            let borrow = cell.borrow();
            let stmt_ref = borrow.as_ref().unwrap();
            client
                .update(stmt_ref, None, &args)
                .unwrap_or_else(|e| pgrx::error!("Failed to bulk insert plan rows: {}", e));
        });
    });

    count
}

/// Write a nullable text field to the JSON buffer.
fn json_text_field(buf: &mut String, key: &str, val: &Option<String>) {
    use std::fmt::Write;
    match val {
        Some(s) => write!(buf, ",\"{}\":\"{}\"", key, json_escape(s)).unwrap(),
        None => write!(buf, ",\"{}\":null", key).unwrap(),
    }
}

/// Write a nullable serde_json::Value field to the JSON buffer.
fn json_value_field(buf: &mut String, key: &str, val: &Option<serde_json::Value>) {
    use std::fmt::Write;
    match val {
        Some(v) => write!(buf, ",\"{}\":{}", key, v).unwrap(), // serde_json::Value Display is JSON
        None => write!(buf, ",\"{}\":null", key).unwrap(),
    }
}

/// Escape a string for JSON output (handles \, ", and control chars).
fn json_escape(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            c if c < '\x20' => {
                use std::fmt::Write;
                write!(result, "\\u{:04x}", c as u32).unwrap();
            }
            c => result.push(c),
        }
    }
    result
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
