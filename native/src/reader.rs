use std::cell::RefCell;

use pgrx::prelude::*;

use crate::types::{CachedState, FilterParam, PlannerContext, SourceRow, TargetRow};

thread_local! {
    static TARGET_READ_STMT: RefCell<Option<pgrx::spi::OwnedPreparedStatement>> = RefCell::new(None);
}

// ── SQL template building (called once on cache miss) ──

pub struct SqlTemplates {
    pub source_sql_template: String,
    pub target_sql_template: String,
    pub target_ident: String,
    pub source_data_cols: Vec<String>,
    pub target_data_cols: Vec<String>,
    pub eph_in_source: Vec<String>,
    pub eph_in_target: Vec<String>,
    /// If Some, target SQL uses parameters instead of __SOURCE_IDENT__ subquery.
    pub target_filter_params: Option<Vec<FilterParam>>,
}

/// Build SQL templates from pre-fetched column data (no SPI calls).
/// Called once on cache miss after introspect_all() provides column lists.
pub fn build_sql_templates_from_cols(
    source_cols: &[String],
    target_cols: &[String],
    target_col_types: &std::collections::HashMap<String, String>,
    target_ident: &str,
    ctx: &PlannerContext,
) -> Result<SqlTemplates, String> {
    // Build source SQL template
    let source_sql_template = build_source_sql_template(source_cols, ctx)?;

    // Pre-compute column classifications for row_to_json splitting
    let exclude_source: Vec<&str> = ctx
        .original_entity_segment_key_cols
        .iter()
        .chain(ctx.temporal_cols.iter())
        .chain(ctx.ephemeral_columns.iter())
        .chain(std::iter::once(&ctx.row_id_column))
        .map(|s| s.as_str())
        .collect();
    let source_data_cols: Vec<String> = source_cols
        .iter()
        .filter(|c| !exclude_source.contains(&c.as_str()) && *c != "era_id" && *c != "era_name")
        .cloned()
        .collect();

    let exclude_target: Vec<&str> = ctx
        .original_entity_segment_key_cols
        .iter()
        .chain(ctx.temporal_cols.iter())
        .chain(ctx.ephemeral_columns.iter())
        .map(|s| s.as_str())
        .collect();
    let target_data_cols: Vec<String> = target_cols
        .iter()
        .filter(|c| !exclude_target.contains(&c.as_str()) && *c != "era_id" && *c != "era_name")
        .cloned()
        .collect();

    let eph_in_source: Vec<String> = ctx
        .ephemeral_columns
        .iter()
        .filter(|c| source_cols.contains(c))
        .cloned()
        .collect();
    let eph_in_target: Vec<String> = ctx
        .ephemeral_columns
        .iter()
        .filter(|c| target_cols.contains(c))
        .cloned()
        .collect();

    // Build target SQL template (try parameterized first, fall back to dynamic)
    let (target_sql_template, target_filter_params) = build_target_sql_template(
        target_ident,
        source_cols,
        target_col_types,
        ctx,
    );

    Ok(SqlTemplates {
        source_sql_template,
        target_sql_template,
        target_ident: target_ident.to_string(),
        source_data_cols,
        target_data_cols,
        eph_in_source,
        eph_in_target,
        target_filter_params,
    })
}

/// Build the source SQL template with __SOURCE_IDENT__ placeholder.
/// Uses row_to_json(s) instead of multiple jsonb_build_object() calls.
fn build_source_sql_template(
    source_cols: &[String],
    ctx: &PlannerContext,
) -> Result<String, String> {
    let has_range = source_cols.contains(&ctx.era.range_col);
    let has_from = source_cols.contains(&ctx.era.valid_from_col);
    let has_until = source_cols.contains(&ctx.era.valid_until_col);
    let has_to = ctx
        .era
        .valid_to_col
        .as_ref()
        .map(|c| source_cols.contains(c))
        .unwrap_or(false);

    if !has_from && !has_range {
        return Err(format!(
            "Source table must have either \"{}\" or \"{}\"",
            ctx.era.range_col, ctx.era.valid_from_col
        ));
    }

    let interval_expr = match ctx.era.range_subtype_category {
        'D' => "'1 day'::interval",
        'N' => "1",
        _ => {
            return Err(format!(
                "Unsupported range subtype category: {}",
                ctx.era.range_subtype_category
            ))
        }
    };

    let from_expr = if has_range {
        format!(
            "COALESCE(lower(s.{rc}), {fb})",
            rc = qi(&ctx.era.range_col),
            fb = if has_from {
                format!("s.{}", qi(&ctx.era.valid_from_col))
            } else {
                "NULL".into()
            },
        )
    } else {
        format!("s.{}", qi(&ctx.era.valid_from_col))
    };

    let until_expr = build_until_expr("s", has_range, has_until, has_to, ctx, interval_expr);

    let causal_expr = if ctx.is_founding_mode() {
        format!(
            "COALESCE(s.{}::text, s.{}::text)",
            qi(ctx.founding_id_column.as_ref().unwrap()),
            qi(&ctx.row_id_column)
        )
    } else {
        format!("s.{}::text", qi(&ctx.row_id_column))
    };

    // Single row_to_json replaces 5 separate jsonb_build_object calls
    Ok(format!(
        "SELECT s.{rid}::bigint, ({causal}), ({from_e})::text, ({until_e})::text, \
         row_to_json(s) \
         FROM __SOURCE_IDENT__ AS s",
        rid = qi(&ctx.row_id_column),
        causal = causal_expr,
        from_e = from_expr,
        until_e = until_expr,
    ))
}

/// Build the target SQL template. Tries parameterized WHERE first (static SQL),
/// falls back to __SOURCE_IDENT__ subquery (dynamic SQL) for multi-column keys.
/// Returns (sql_template, Option<filter_params>).
fn build_target_sql_template(
    target_ident: &str,
    source_cols: &[String],
    target_col_types: &std::collections::HashMap<String, String>,
    ctx: &PlannerContext,
) -> (String, Option<Vec<FilterParam>>) {
    // Try parameterized filter first (enables prepared statement caching)
    if let Some((where_clause, params)) =
        try_build_parameterized_filter(target_col_types, source_cols, ctx)
    {
        let sql = format!(
            "SELECT lower(t.{rc})::text, upper(t.{rc})::text, \
             row_to_json(t) \
             FROM {tgt} AS t{where_c}",
            rc = qi(&ctx.era.range_col),
            tgt = target_ident,
            where_c = where_clause,
        );
        return (sql, Some(params));
    }

    // Fall back to dynamic SQL with __SOURCE_IDENT__ subquery
    let where_clause = build_target_filter("__SOURCE_IDENT__", source_cols, ctx);
    let sql = format!(
        "SELECT lower(t.{rc})::text, upper(t.{rc})::text, \
         row_to_json(t) \
         FROM {tgt} AS t{where_c}",
        rc = qi(&ctx.era.range_col),
        tgt = target_ident,
        where_c = where_clause,
    );
    (sql, None)
}

// ── SQL execution (called every batch with pre-built SQL) ──

/// Read source rows by executing a pre-built SQL template.
/// Parses row_to_json output and splits into column categories using CachedState.
pub fn read_source_rows_with_sql(
    sql: &str,
    state: &CachedState,
) -> Result<Vec<SourceRow>, String> {
    Spi::connect(|client| {
        let table = client
            .select(sql, None, &[])
            .map_err(|e| format!("SPI error reading source rows: {e}"))?;

        let mut rows = Vec::with_capacity(table.len());
        for row in table {
            let row_id: i64 = row.get::<i64>(1).unwrap_or(Some(0)).unwrap_or(0);
            let causal_id: String = row
                .get::<String>(2)
                .unwrap_or(Some(String::new()))
                .unwrap_or_default();
            let valid_from: String = row
                .get::<String>(3)
                .unwrap_or(Some(String::new()))
                .unwrap_or_default();
            let valid_until: String = row
                .get::<String>(4)
                .unwrap_or(Some(String::new()))
                .unwrap_or_default();

            // Single row_to_json → split into category maps
            let full_json = get_json_map(&row, 5);
            let (identity_keys, lookup_keys, data_payload, ephemeral_payload, stable_pk_payload) =
                split_source_json(&full_json, state);

            // Derive is_identifiable and lookup_cols_are_null from JSON
            let is_identifiable = state.ctx.identity_columns.is_empty()
                || state
                    .ctx
                    .identity_columns
                    .iter()
                    .any(|c| full_json.get(c).map_or(false, |v| !v.is_null()));

            let lookup_cols_are_null = state.ctx.all_lookup_cols.is_empty()
                || state
                    .ctx
                    .all_lookup_cols
                    .iter()
                    .all(|c| full_json.get(c).map_or(true, |v| v.is_null()));

            rows.push(SourceRow {
                row_id,
                causal_id,
                valid_from,
                valid_until,
                identity_keys,
                lookup_keys,
                data_payload,
                ephemeral_payload,
                stable_pk_payload,
                is_identifiable,
                lookup_cols_are_null,
            });
        }
        Ok(rows)
    })
}

/// Read target rows by executing a pre-built SQL template.
/// Parses row_to_json output and splits into column categories using CachedState.
pub fn read_target_rows_with_sql(
    sql: &str,
    state: &CachedState,
) -> Result<Vec<TargetRow>, String> {
    Spi::connect(|client| {
        let table = client
            .select(sql, None, &[])
            .map_err(|e| format!("SPI error reading target rows: {e}"))?;

        let mut rows = Vec::with_capacity(table.len());
        for row in table {
            let valid_from: String = row
                .get::<String>(1)
                .unwrap_or(Some(String::new()))
                .unwrap_or_default();
            let valid_until: String = row
                .get::<String>(2)
                .unwrap_or(Some(String::new()))
                .unwrap_or_default();

            // Single row_to_json → split into category maps
            let full_json = get_json_map(&row, 3);
            let (identity_keys, lookup_keys, data_payload, ephemeral_payload) =
                split_target_json(&full_json, state);

            rows.push(TargetRow {
                valid_from,
                valid_until,
                identity_keys,
                lookup_keys,
                data_payload,
                ephemeral_payload,
            });
        }
        Ok(rows)
    })
}

// ── JSON splitting (single-pass column extraction from row_to_json) ──

/// Split a source row's row_to_json output into category maps.
fn split_source_json(
    full_json: &serde_json::Map<String, serde_json::Value>,
    state: &CachedState,
) -> (
    serde_json::Map<String, serde_json::Value>, // identity_keys
    serde_json::Map<String, serde_json::Value>, // lookup_keys
    serde_json::Map<String, serde_json::Value>, // data_payload
    serde_json::Map<String, serde_json::Value>, // ephemeral_payload
    serde_json::Map<String, serde_json::Value>, // stable_pk_payload
) {
    let mut identity = serde_json::Map::with_capacity(state.ctx.identity_columns.len());
    let mut stable_pk = serde_json::Map::with_capacity(state.ctx.identity_columns.len());
    let mut lookup = serde_json::Map::with_capacity(state.ctx.all_lookup_cols.len());
    let mut data = serde_json::Map::with_capacity(state.source_data_cols.len());
    let mut ephemeral = serde_json::Map::with_capacity(state.eph_in_source.len());

    for col in &state.ctx.identity_columns {
        if let Some(val) = full_json.get(col) {
            identity.insert(col.clone(), val.clone());
            stable_pk.insert(col.clone(), val.clone());
        } else {
            stable_pk.insert(col.clone(), serde_json::Value::Null);
        }
    }

    for col in &state.ctx.all_lookup_cols {
        if let Some(val) = full_json.get(col) {
            lookup.insert(col.clone(), val.clone());
        }
    }

    for col in &state.source_data_cols {
        if let Some(val) = full_json.get(col) {
            data.insert(col.clone(), val.clone());
        }
    }

    for col in &state.eph_in_source {
        if let Some(val) = full_json.get(col) {
            ephemeral.insert(col.clone(), val.clone());
        }
    }

    (identity, lookup, data, ephemeral, stable_pk)
}

/// Split a target row's row_to_json output into category maps.
fn split_target_json(
    full_json: &serde_json::Map<String, serde_json::Value>,
    state: &CachedState,
) -> (
    serde_json::Map<String, serde_json::Value>, // identity_keys
    serde_json::Map<String, serde_json::Value>, // lookup_keys
    serde_json::Map<String, serde_json::Value>, // data_payload
    serde_json::Map<String, serde_json::Value>, // ephemeral_payload
) {
    let mut identity = serde_json::Map::with_capacity(state.ctx.identity_columns.len());
    let mut lookup = serde_json::Map::with_capacity(state.ctx.all_lookup_cols.len());
    let mut data = serde_json::Map::with_capacity(state.target_data_cols.len());
    let mut ephemeral = serde_json::Map::with_capacity(state.eph_in_target.len());

    for col in &state.ctx.identity_columns {
        if let Some(val) = full_json.get(col) {
            identity.insert(col.clone(), val.clone());
        }
    }

    for col in &state.ctx.all_lookup_cols {
        if let Some(val) = full_json.get(col) {
            lookup.insert(col.clone(), val.clone());
        }
    }

    for col in &state.target_data_cols {
        if let Some(val) = full_json.get(col) {
            data.insert(col.clone(), val.clone());
        }
    }

    for col in &state.eph_in_target {
        if let Some(val) = full_json.get(col) {
            ephemeral.insert(col.clone(), val.clone());
        }
    }

    (identity, lookup, data, ephemeral)
}

// ── Parameterized target read (cached prepared statement) ──

/// Clear the cached target read prepared statement (call on cache miss).
pub fn clear_target_read_cache() {
    TARGET_READ_STMT.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

/// Try to build a parameterized WHERE clause using = ANY($N::text::type[]).
/// Returns None if any filter key set has multiple columns (can't parameterize).
/// Returns Some(("", [])) for full-scan modes (no WHERE clause needed).
fn try_build_parameterized_filter(
    target_col_types: &std::collections::HashMap<String, String>,
    source_cols: &[String],
    ctx: &PlannerContext,
) -> Option<(String, Vec<FilterParam>)> {
    use crate::types::{DeleteMode, MergeMode};

    // Full scan modes: no WHERE clause needed, SQL is already static
    let needs_full_scan = matches!(
        (&ctx.mode, &ctx.delete_mode),
        (
            MergeMode::MergeEntityPatch | MergeMode::MergeEntityReplace,
            DeleteMode::DeleteMissingEntities | DeleteMode::DeleteMissingTimelineAndEntities
        )
    );
    if needs_full_scan {
        return Some((String::new(), vec![]));
    }

    // Collect filter key sets (same logic as build_target_filter)
    let mut filter_key_sets: Vec<(Vec<String>, bool)> = Vec::new(); // (cols, is_identity)

    if !ctx.all_lookup_cols.is_empty() {
        filter_key_sets.push((ctx.all_lookup_cols.clone(), false));
    }

    if !ctx.identity_columns.is_empty() {
        let id_cols_in_source: Vec<String> = ctx
            .identity_columns
            .iter()
            .filter(|c| source_cols.contains(c))
            .cloned()
            .collect();
        if !id_cols_in_source.is_empty() {
            let already_present = filter_key_sets
                .iter()
                .any(|(cols, _)| *cols == id_cols_in_source);
            if !already_present {
                filter_key_sets.push((id_cols_in_source, true));
            }
        }
    }

    if filter_key_sets.is_empty() {
        return Some((String::new(), vec![])); // No filter needed
    }

    // All key sets must be single-column and have known types
    let mut params = Vec::new();
    let mut where_parts = Vec::new();
    let mut param_index = 1usize;

    for (cols, is_identity) in &filter_key_sets {
        if cols.len() != 1 {
            return None; // Multi-column key set: fall back to dynamic SQL
        }
        let col = &cols[0];
        let pg_type = target_col_types.get(col)?; // Must know the type

        params.push(FilterParam {
            col_name: col.clone(),
            pg_type: pg_type.clone(),
            param_index,
            is_identity: *is_identity,
        });
        where_parts.push(format!(
            "t.{col} = ANY(${idx}::text::{typ}[])",
            col = qi(col),
            idx = param_index,
            typ = pg_type,
        ));
        param_index += 1;
    }

    let where_clause = format!(" WHERE {}", where_parts.join(" OR "));
    Some((where_clause, params))
}

/// Extract distinct filter values from source rows for parameterized target read.
/// Returns one PostgreSQL array literal string per FilterParam.
pub fn extract_filter_values(
    source_rows: &[SourceRow],
    filter_params: &[FilterParam],
) -> Vec<String> {
    filter_params
        .iter()
        .map(|param| {
            let mut values: Vec<String> = Vec::new();
            let mut seen = std::collections::HashSet::new();

            for row in source_rows {
                let map = if param.is_identity {
                    &row.identity_keys
                } else {
                    &row.lookup_keys
                };

                if let Some(val) = map.get(&param.col_name) {
                    if !val.is_null() {
                        let text_val = match val {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Number(n) => n.to_string(),
                            serde_json::Value::Bool(b) => b.to_string(),
                            _ => val.to_string(),
                        };
                        if seen.insert(text_val.clone()) {
                            values.push(text_val);
                        }
                    }
                }
            }

            format_pg_array_literal(&values)
        })
        .collect()
}

/// Read target rows using a cached prepared statement with parameters.
/// The target SQL in state.target_sql_template must use $N parameters.
pub fn read_target_rows_parameterized(
    state: &CachedState,
    param_values: &[String],
) -> Result<Vec<TargetRow>, String> {
    Spi::connect_mut(|client| {
        // Prepare on first call, cache with SPI_keepplan
        let has_stmt = TARGET_READ_STMT.with(|cell| cell.borrow().is_some());
        if !has_stmt {
            let param_types: Vec<pgrx::PgOid> = (0..param_values.len())
                .map(|_| pgrx::PgOid::from(pg_sys::TEXTOID))
                .collect();
            let stmt = client
                .prepare_mut(&state.target_sql_template, &param_types)
                .map_err(|e| format!("Failed to prepare target read: {}", e))?;
            let owned = stmt.keep();
            TARGET_READ_STMT.with(|cell| {
                *cell.borrow_mut() = Some(owned);
            });
        }

        // Execute with parameters and parse rows
        use pgrx::datum::DatumWithOid;
        let args: Vec<DatumWithOid> = param_values
            .iter()
            .map(|v| DatumWithOid::from(v.clone()))
            .collect();

        TARGET_READ_STMT.with(|cell| {
            let borrow = cell.borrow();
            let stmt_ref = borrow.as_ref().unwrap();
            let table = client
                .update(stmt_ref, None, &args)
                .map_err(|e| format!("SPI error reading target rows: {e}"))?;

            let mut rows = Vec::with_capacity(table.len());
            for row in table {
                let valid_from: String = row
                    .get::<String>(1)
                    .unwrap_or(Some(String::new()))
                    .unwrap_or_default();
                let valid_until: String = row
                    .get::<String>(2)
                    .unwrap_or(Some(String::new()))
                    .unwrap_or_default();

                let full_json = get_json_map(&row, 3);
                let (identity_keys, lookup_keys, data_payload, ephemeral_payload) =
                    split_target_json(&full_json, state);

                rows.push(TargetRow {
                    valid_from,
                    valid_until,
                    identity_keys,
                    lookup_keys,
                    data_payload,
                    ephemeral_payload,
                });
            }
            Ok(rows)
        })
    })
}

/// Format a list of string values as a PostgreSQL array literal.
/// E.g., ["a", "b with,comma"] → {"a","b with,comma"}
fn format_pg_array_literal(values: &[String]) -> String {
    let mut buf = String::with_capacity(values.len() * 10 + 2);
    buf.push('{');
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        buf.push('"');
        for c in v.chars() {
            match c {
                '"' => buf.push_str("\"\""),
                '\\' => buf.push_str("\\\\"),
                _ => buf.push(c),
            }
        }
        buf.push('"');
    }
    buf.push('}');
    buf
}

// ── Target filter (O(1) optimization — dynamic SQL fallback) ──

/// Build a WHERE clause that filters the target table to only entities present
/// in the source batch. Mirrors the PL/pgSQL planner's `v_target_rows_filter`.
fn build_target_filter(
    source_ident: &str,
    source_cols: &[String],
    ctx: &PlannerContext,
) -> String {
    use crate::types::{DeleteMode, MergeMode};

    let needs_full_scan = matches!(
        (&ctx.mode, &ctx.delete_mode),
        (
            MergeMode::MergeEntityPatch | MergeMode::MergeEntityReplace,
            DeleteMode::DeleteMissingEntities | DeleteMode::DeleteMissingTimelineAndEntities
        )
    );
    if needs_full_scan {
        return String::new();
    }

    let mut filter_key_sets: Vec<Vec<String>> = Vec::new();

    if !ctx.all_lookup_cols.is_empty() {
        filter_key_sets.push(ctx.all_lookup_cols.clone());
    }

    if !ctx.identity_columns.is_empty() {
        let id_cols_in_source: Vec<String> = ctx
            .identity_columns
            .iter()
            .filter(|c| source_cols.contains(c))
            .cloned()
            .collect();
        if !id_cols_in_source.is_empty() && !filter_key_sets.contains(&id_cols_in_source) {
            filter_key_sets.push(id_cols_in_source);
        }
    }

    if filter_key_sets.is_empty() {
        return String::new();
    }

    let union_parts: Vec<String> = filter_key_sets
        .iter()
        .filter_map(|key_cols| {
            let all_in_source = key_cols.iter().all(|c| source_cols.contains(c));
            if !all_in_source {
                return None;
            }

            let t_cols = key_cols
                .iter()
                .map(|c| format!("t.{}", qi(c)))
                .collect::<Vec<_>>()
                .join(", ");
            let s_cols = key_cols
                .iter()
                .map(|c| format!("s.{}", qi(c)))
                .collect::<Vec<_>>()
                .join(", ");
            let not_null = key_cols
                .iter()
                .map(|c| format!("s.{} IS NOT NULL", qi(c)))
                .collect::<Vec<_>>()
                .join(" OR ");

            Some(format!(
                "({t_cols}) IN (SELECT DISTINCT {s_cols} FROM {src} AS s WHERE {not_null})",
                t_cols = t_cols,
                s_cols = s_cols,
                src = source_ident,
                not_null = not_null,
            ))
        })
        .collect();

    if union_parts.is_empty() {
        return String::new();
    }

    format!(" WHERE {}", union_parts.join(" OR "))
}

// ── Helpers ──

fn qi(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn build_until_expr(
    alias: &str,
    has_range: bool,
    has_until: bool,
    has_to: bool,
    ctx: &PlannerContext,
    interval_expr: &str,
) -> String {
    let to_expr = |a: &str| -> String {
        format!(
            "({a}.{vt} + {iv})::{st}",
            a = a,
            vt = qi(ctx.era.valid_to_col.as_ref().unwrap()),
            iv = interval_expr,
            st = ctx.era.range_subtype,
        )
    };

    if has_range {
        let fallback = if has_until && has_to {
            format!(
                "COALESCE({a}.{vu}, {to})",
                a = alias,
                vu = qi(&ctx.era.valid_until_col),
                to = to_expr(alias)
            )
        } else if has_until {
            format!("{}.{}", alias, qi(&ctx.era.valid_until_col))
        } else if has_to {
            to_expr(alias)
        } else {
            "NULL".into()
        };
        format!(
            "COALESCE(upper({a}.{rc}), {fb})",
            a = alias,
            rc = qi(&ctx.era.range_col),
            fb = fallback
        )
    } else if has_until && has_to {
        format!(
            "COALESCE({a}.{vu}, {to})",
            a = alias,
            vu = qi(&ctx.era.valid_until_col),
            to = to_expr(alias)
        )
    } else if has_until {
        format!("{}.{}", alias, qi(&ctx.era.valid_until_col))
    } else if has_to {
        to_expr(alias)
    } else {
        "NULL".into()
    }
}

/// Resolve a table OID to its schema-qualified name.
pub fn resolve_table_name(table_oid: pg_sys::Oid) -> Result<String, String> {
    let sql = format!("SELECT {}::regclass::text", u32::from(table_oid));
    Spi::connect(|client| {
        client
            .select(&sql, None, &[])
            .map_err(|e| format!("SPI error: {e}"))?
            .first()
            .get_one::<String>()
            .map_err(|e| format!("SPI error: {e}"))
    })?
    .ok_or_else(|| "Could not resolve table name".to_string())
}

/// Read a JSON (not JSONB) column from an SPI row as a serde_json::Map.
fn get_json_map(
    row: &pgrx::spi::SpiHeapTupleData,
    ordinal: usize,
) -> serde_json::Map<String, serde_json::Value> {
    match row.get::<pgrx::Json>(ordinal) {
        Ok(Some(pgrx::Json(serde_json::Value::Object(map)))) => map,
        _ => serde_json::Map::new(),
    }
}
