use std::cell::RefCell;
use std::collections::HashMap;

use pgrx::prelude::*;

use crate::types::{CachedState, ColCategory, ColMapping, FilterParam, PlannerContext, SourceRow, TargetRow};

thread_local! {
    /// Multi-entry cache keyed by target SQL template (one per target table config).
    static TARGET_READ_STMTS: RefCell<HashMap<String, pgrx::spi::OwnedPreparedStatement>> = RefCell::new(HashMap::new());
    /// Multi-entry cache keyed by source_ident (one per source table).
    /// PostgreSQL auto-replans when the underlying temp view is recreated via CREATE OR REPLACE.
    static SOURCE_READ_STMTS: RefCell<HashMap<String, pgrx::spi::OwnedPreparedStatement>> = RefCell::new(HashMap::new());
}

/// Return the number of cached target read prepared statements.
pub fn target_read_stmt_count() -> usize {
    TARGET_READ_STMTS.with(|c| c.borrow().len())
}

/// Return the number of cached source read prepared statements.
pub fn source_read_stmt_count() -> usize {
    SOURCE_READ_STMTS.with(|c| c.borrow().len())
}

/// Clear all cached read prepared statements.
pub fn clear_read_stmts() {
    TARGET_READ_STMTS.with(|c| c.borrow_mut().clear());
    SOURCE_READ_STMTS.with(|c| c.borrow_mut().clear());
}

// ── SQL template building (called once on cache miss) ──

pub struct SqlTemplates {
    pub source_sql_template: String,
    pub target_sql_template: String,
    pub target_ident: String,
    pub source_col_layout: Vec<ColMapping>,
    pub target_col_layout: Vec<ColMapping>,
    /// If Some, target SQL uses parameters instead of __SOURCE_IDENT__ subquery.
    pub target_filter_params: Option<Vec<FilterParam>>,
}

/// Build SQL templates from pre-fetched column data (no SPI calls).
/// Called once on cache miss after introspect_all() provides column lists.
///
/// Instead of row_to_json, SELECTs individual columns with ::text casts.
/// Returns column layouts that map ordinal positions to category maps.
pub fn build_sql_templates_from_cols(
    source_cols: &[String],
    target_cols: &[String],
    target_col_types: &std::collections::HashMap<String, String>,
    source_col_types: &std::collections::HashMap<String, String>,
    target_ident: &str,
    ctx: &PlannerContext,
    source_table_name: &str,
) -> Result<SqlTemplates, String> {
    // Classify source columns into categories (restrict Data to target-intersecting columns)
    let source_col_layout = build_column_layout(source_cols, source_col_types, ctx, true, Some(target_cols));

    // Build source SQL template with individual columns
    let source_sql_template = build_source_sql_template(source_cols, &source_col_layout, ctx, source_table_name)?;

    // Classify target columns into categories
    let target_col_layout = build_column_layout(target_cols, target_col_types, ctx, false, None);

    // Build target SQL template (try parameterized first, fall back to dynamic)
    let (target_sql_template, target_filter_params) = build_target_sql_template(
        target_ident,
        source_cols,
        target_col_types,
        &target_col_layout,
        ctx,
    );

    Ok(SqlTemplates {
        source_sql_template,
        target_sql_template,
        target_ident: target_ident.to_string(),
        source_col_layout,
        target_col_layout,
        target_filter_params,
    })
}

/// Build the column layout: classify each column into a category.
/// Only includes columns that belong to identity/lookup/data/ephemeral categories.
/// For source layouts, `other_table_cols` should be the target columns to restrict
/// Data category to columns that exist on both tables (matching PL/pgSQL behavior).
fn build_column_layout(
    table_cols: &[String],
    col_types: &std::collections::HashMap<String, String>,
    ctx: &PlannerContext,
    is_source: bool,
    other_table_cols: Option<&[String]>,
) -> Vec<ColMapping> {
    // Build exclusion set for "data" category
    let mut excluded_from_data: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for c in &ctx.original_entity_segment_key_cols {
        excluded_from_data.insert(c.as_str());
    }
    for c in &ctx.temporal_cols {
        excluded_from_data.insert(c.as_str());
    }
    for c in &ctx.ephemeral_columns {
        excluded_from_data.insert(c.as_str());
    }
    if is_source {
        excluded_from_data.insert(ctx.row_id_column.as_str());
    }
    excluded_from_data.insert("era_id");
    excluded_from_data.insert("era_name");
    excluded_from_data.insert("merge_status");
    excluded_from_data.insert("merge_statuses");
    excluded_from_data.insert("merge_errors");

    let identity_set: std::collections::HashSet<&str> =
        ctx.identity_columns.iter().map(|s| s.as_str()).collect();
    let lookup_set: std::collections::HashSet<&str> =
        ctx.all_lookup_cols.iter().map(|s| s.as_str()).collect();
    let ephemeral_set: std::collections::HashSet<&str> =
        ctx.ephemeral_columns.iter().map(|s| s.as_str()).collect();
    let temporal_set: std::collections::HashSet<&str> =
        ctx.temporal_cols.iter().map(|s| s.as_str()).collect();

    let mut layout = Vec::new();

    for col in table_cols {
        let col_str = col.as_str();

        // Skip columns that don't belong to any payload category
        if temporal_set.contains(col_str) {
            continue;
        }
        if is_source && col_str == ctx.row_id_column.as_str() {
            continue;
        }
        if col_str == "era_id" || col_str == "era_name" || col_str == "merge_status"
            || col_str == "merge_statuses" || col_str == "merge_errors" {
            continue;
        }
        // Exclude founding_id column from source data payload (internal correlation column)
        if is_source {
            if let Some(ref fid) = ctx.founding_id_column {
                if col_str == fid.as_str() {
                    continue;
                }
            }
        }

        let category = if identity_set.contains(col_str) {
            ColCategory::Identity
        } else if lookup_set.contains(col_str) {
            ColCategory::Lookup
        } else if ephemeral_set.contains(col_str) && table_cols.contains(col) {
            ColCategory::Ephemeral
        } else if !excluded_from_data.contains(col_str) {
            // For source Data columns, only include if the column also exists on the target.
            // PL/pgSQL payload_columns is the intersection of source and target columns.
            if let Some(other_cols) = other_table_cols {
                if !other_cols.contains(col) {
                    continue;
                }
            }
            ColCategory::Data
        } else if ctx.pk_cols.contains(col) {
            ColCategory::StablePk
        } else {
            continue; // other segment key cols (e.g. temporal) already filtered above
        };

        let pg_type = col_types
            .get(col)
            .cloned()
            .unwrap_or_else(|| "text".to_string());

        layout.push(ColMapping {
            col_name: col.clone(),
            category,
            pg_type,
        });
    }

    layout
}

/// Build the source SQL template with __SOURCE_IDENT__ placeholder.
/// SELECTs individual columns with ::text casts (no JSON construction).
fn build_source_sql_template(
    source_cols: &[String],
    col_layout: &[ColMapping],
    ctx: &PlannerContext,
    source_table_name: &str,
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
        let vf_str = if ctx.era.valid_from_col.is_empty() { "<NULL>" } else { &ctx.era.valid_from_col };
        return Err(format!(
            "Source table \"{}\" must have either the range column \"{}\" or the component column \"{}\".",
            source_table_name, ctx.era.range_col, vf_str
        ));
    }

    // Validate upper bound: need range, valid_until, or valid_to (matches PL/pgSQL validation)
    if !has_range && !has_until && !has_to {
        let vto_str = ctx.era.valid_to_col.as_deref().unwrap_or("<NULL>");
        return Err(format!(
            "Source table \"{}\" must have a \"{}\", \"{}\", or \"{}\" column.",
            source_table_name, ctx.era.range_col, ctx.era.valid_until_col, vto_str
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

    // Individual columns with ::text casts — no JSON construction/parsing
    let col_selects: Vec<String> = col_layout
        .iter()
        .map(|cm| format!("s.{}::text", qi(&cm.col_name)))
        .collect();

    let col_list = if col_selects.is_empty() {
        String::new()
    } else {
        format!(", {}", col_selects.join(", "))
    };

    Ok(format!(
        "SELECT s.{rid}::bigint, ({causal}), ({from_e})::text, ({until_e})::text{cols} \
         FROM __SOURCE_IDENT__ AS s",
        rid = qi(&ctx.row_id_column),
        causal = causal_expr,
        from_e = from_expr,
        until_e = until_expr,
        cols = col_list,
    ))
}

/// Build the target SQL template. Tries parameterized WHERE first (static SQL),
/// falls back to __SOURCE_IDENT__ subquery (dynamic SQL) for multi-column keys.
/// Returns (sql_template, Option<filter_params>).
fn build_target_sql_template(
    target_ident: &str,
    source_cols: &[String],
    target_col_types: &std::collections::HashMap<String, String>,
    col_layout: &[ColMapping],
    ctx: &PlannerContext,
) -> (String, Option<Vec<FilterParam>>) {
    // Individual columns with ::text casts — no JSON construction/parsing
    let col_selects: Vec<String> = col_layout
        .iter()
        .map(|cm| format!("t.{}::text", qi(&cm.col_name)))
        .collect();
    let col_list = if col_selects.is_empty() {
        String::new()
    } else {
        format!(", {}", col_selects.join(", "))
    };

    // Try parameterized filter first (enables prepared statement caching)
    if let Some((where_clause, params)) =
        try_build_parameterized_filter(target_col_types, source_cols, ctx)
    {
        let sql = format!(
            "SELECT lower(t.{rc})::text, upper(t.{rc})::text{cols} \
             FROM {tgt} AS t{where_c}",
            rc = qi(&ctx.era.range_col),
            cols = col_list,
            tgt = target_ident,
            where_c = where_clause,
        );
        return (sql, Some(params));
    }

    // Fall back to dynamic SQL with __SOURCE_IDENT__ subquery
    let where_clause = build_target_filter("__SOURCE_IDENT__", source_cols, ctx);
    let sql = format!(
        "SELECT lower(t.{rc})::text, upper(t.{rc})::text{cols} \
         FROM {tgt} AS t{where_c}",
        rc = qi(&ctx.era.range_col),
        cols = col_list,
        tgt = target_ident,
        where_c = where_clause,
    );
    (sql, None)
}

// ── SQL execution (called every batch with pre-built SQL) ──

/// Read target rows by executing a pre-built SQL template.
/// Reads individual columns by ordinal — no JSON construction or parsing.
pub fn read_target_rows_with_sql(
    sql: &str,
    state: &CachedState,
) -> Result<Vec<TargetRow>, String> {
    Spi::connect(|client| {
        let table = client
            .select(sql, None, &[])
            .map_err(|e| format!("SPI error reading target rows: {e}"))?;

        let layout = &state.target_col_layout;
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

            let (identity_keys, lookup_keys, data_payload, ephemeral_payload, pk_payload) =
                read_target_ordinals(&row, layout);

            rows.push(TargetRow {
                valid_from,
                valid_until,
                identity_keys,
                lookup_keys,
                data_payload,
                ephemeral_payload,
                pk_payload,
            });
        }
        Ok(rows)
    })
}

// ── Parameterized target read (cached prepared statement) ──

// Note: No clear functions needed — multi-entry caches grow organically.
// Stale entries (old SQL templates from changed schemas) become unreachable
// but harmless. The number of entries is bounded by the number of distinct
// target tables and source views used in the session.

/// Read source rows using a cached prepared statement (0 params, keyed by source_ident).
/// The source table name stays the same across batches (CREATE OR REPLACE TEMP VIEW),
/// so PostgreSQL auto-replans via relcache invalidation when the view is recreated.
///
/// Reads individual columns by ordinal — no JSON construction or parsing.
pub fn read_source_rows_cached(
    source_ident: &str,
    state: &CachedState,
) -> Result<Vec<SourceRow>, String> {
    let source_sql = state.source_sql_template.replace("__SOURCE_IDENT__", source_ident);

    Spi::connect_mut(|client| {
        // Key by full SQL (not just source_ident) because the same source table
        // can be read with different templates (e.g., with/without founding_id_column)
        let cache_key = source_sql.clone();
        let needs_prepare = SOURCE_READ_STMTS.with(|cell| {
            !cell.borrow().contains_key(&cache_key)
        });

        if needs_prepare {
            let stmt = client
                .prepare_mut(&source_sql, &[])
                .map_err(|e| format!("Failed to prepare source read: {e}"))?;
            let owned = stmt.keep();
            SOURCE_READ_STMTS.with(|cell| {
                cell.borrow_mut().insert(cache_key.clone(), owned);
            });
        }

        // Execute using cached prepared statement
        SOURCE_READ_STMTS.with(|cell| {
            let borrow = cell.borrow();
            let stmt_ref = borrow.get(&cache_key).unwrap();
            let table = client
                .update(stmt_ref, None, &[])
                .map_err(|e| format!("SPI error reading source rows: {e}"))?;

            let layout = &state.source_col_layout;
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

                // Read individual columns by ordinal — no JSON parsing
                let (identity_keys, lookup_keys, data_payload, ephemeral_payload,
                     stable_pk_payload, is_identifiable, lookup_cols_are_null) =
                    read_source_ordinals(&row, layout, &state.ctx);

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
    })
}

/// Read source row columns by ordinal and classify into category maps.
/// Columns start at ordinal 5 (after row_id, causal_id, valid_from, valid_until).
fn read_source_ordinals(
    row: &pgrx::spi::SpiHeapTupleData,
    layout: &[ColMapping],
    ctx: &PlannerContext,
) -> (
    serde_json::Map<String, serde_json::Value>, // identity_keys
    serde_json::Map<String, serde_json::Value>, // lookup_keys
    serde_json::Map<String, serde_json::Value>, // data_payload
    serde_json::Map<String, serde_json::Value>, // ephemeral_payload
    serde_json::Map<String, serde_json::Value>, // stable_pk_payload
    bool,                                        // is_identifiable
    bool,                                        // lookup_cols_are_null
) {
    let mut identity = serde_json::Map::new();
    let mut lookup = serde_json::Map::new();
    let mut data = serde_json::Map::new();
    let mut ephemeral = serde_json::Map::new();

    for (i, cm) in layout.iter().enumerate() {
        let ordinal = 5 + i; // 1-based, first 4 are fixed
        let val = match row.get::<String>(ordinal) {
            Ok(Some(s)) => parse_typed_value(s, &cm.pg_type),
            _ => serde_json::Value::Null,
        };
        match cm.category {
            ColCategory::Identity => {
                identity.insert(cm.col_name.clone(), val);
            }
            ColCategory::Lookup => {
                lookup.insert(cm.col_name.clone(), val);
            }
            ColCategory::Data => {
                data.insert(cm.col_name.clone(), val);
            }
            ColCategory::Ephemeral => {
                ephemeral.insert(cm.col_name.clone(), val);
            }
            ColCategory::StablePk => {
                // PK-only columns: not included in source stable_pk (source may not have them)
            }
        }
    }

    // stable_pk_payload: all identity columns, Null for missing
    let mut stable_pk = serde_json::Map::with_capacity(ctx.identity_columns.len());
    for col in &ctx.identity_columns {
        let val = identity
            .get(col)
            .cloned()
            .unwrap_or(serde_json::Value::Null);
        stable_pk.insert(col.clone(), val);
    }

    let is_identifiable = ctx.identity_columns.is_empty()
        || identity.values().any(|v| !v.is_null());

    let lookup_cols_are_null = ctx.all_lookup_cols.is_empty()
        || lookup.values().all(|v| v.is_null());

    (identity, lookup, data, ephemeral, stable_pk, is_identifiable, lookup_cols_are_null)
}

/// Try to build a parameterized WHERE clause for the target read query.
/// Single-column key sets: WHERE t."col" = ANY($N::text::type[])
/// Multi-column key sets: WHERE (t."c1", t."c2") IN (SELECT c1, c2 FROM unnest($N1::text::type1[], $N2::text::type2[]) AS u(c1, c2))
/// Returns None only if any column type is unknown.
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

    // Collect filter key sets — use individual lookup_key_sets (OR logic), not flat all_lookup_cols.
    // PL/pgSQL uses each natural key set independently in its join expression.
    let mut filter_key_sets: Vec<(Vec<String>, bool)> = Vec::new(); // (cols, is_identity)

    for key_set in &ctx.lookup_key_sets {
        if !key_set.is_empty() {
            let already_present = filter_key_sets
                .iter()
                .any(|(cols, _)| cols == key_set);
            if !already_present {
                filter_key_sets.push((key_set.clone(), false));
            }
        }
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

    // Build parameterized WHERE clause — handles both single and multi-column key sets
    let mut params = Vec::new();
    let mut where_parts = Vec::new();
    let mut param_index = 1usize;

    for (key_set_id, (cols, is_identity)) in filter_key_sets.iter().enumerate() {
        // Verify all columns have known types
        for col in cols {
            if !target_col_types.contains_key(col) {
                return None; // Unknown column type: can't parameterize
            }
        }

        if cols.len() == 1 {
            // Single-column: EXISTS (SELECT 1 FROM unnest($N) AS _u(v) WHERE t."col" IS NOT DISTINCT FROM _u.v)
            // Uses IS NOT DISTINCT FROM to correctly match NULL values.
            let col = &cols[0];
            let pg_type = target_col_types.get(col).unwrap();
            params.push(FilterParam {
                col_name: col.clone(),
                pg_type: pg_type.clone(),
                param_index,
                is_identity: *is_identity,
                key_set_id,
            });
            where_parts.push(format!(
                "EXISTS (SELECT 1 FROM unnest(${idx}::text::{typ}[]) AS _u(v) WHERE t.{col} IS NOT DISTINCT FROM _u.v)",
                col = qi(col),
                idx = param_index,
                typ = pg_type,
            ));
            param_index += 1;
        } else {
            // Multi-column: EXISTS with IS NOT DISTINCT FROM for NULL-safe matching.
            // PostgreSQL forbids column definition lists on multi-arg UNNEST, so we use
            // ROWS FROM() and alias the composite result with column names.
            let mut unnest_calls = Vec::new();
            let mut u_col_names = Vec::new();
            let mut conditions = Vec::new();

            for (ci, col) in cols.iter().enumerate() {
                let pg_type = target_col_types.get(col).unwrap();
                params.push(FilterParam {
                    col_name: col.clone(),
                    pg_type: pg_type.clone(),
                    param_index,
                    is_identity: *is_identity,
                    key_set_id,
                });
                let u_alias = format!("_c{}", ci);
                unnest_calls.push(format!(
                    "unnest(${idx}::text::{typ}[])",
                    idx = param_index,
                    typ = pg_type,
                ));
                u_col_names.push(u_alias.clone());
                conditions.push(format!(
                    "t.{col} IS NOT DISTINCT FROM _u.{u_alias}",
                    col = qi(col),
                    u_alias = u_alias,
                ));
                param_index += 1;
            }

            where_parts.push(format!(
                "EXISTS (SELECT 1 FROM ROWS FROM({fns}) AS _u({cols}) WHERE {conds})",
                fns = unnest_calls.join(", "),
                cols = u_col_names.join(", "),
                conds = conditions.join(" AND "),
            ));
        }
    }

    let where_clause = format!(" WHERE {}", where_parts.join(" OR "));
    Some((where_clause, params))
}

/// Extract distinct filter values from source rows for parameterized target read.
/// Returns one PostgreSQL array literal string per FilterParam.
///
/// For single-column key sets: extracts distinct values for that column.
/// For multi-column key sets: extracts distinct tuples as parallel arrays,
/// ensuring the arrays stay aligned (same index = same tuple).
pub fn extract_filter_values(
    source_rows: &[SourceRow],
    filter_params: &[FilterParam],
) -> Vec<String> {
    // Group params by key_set_id to handle multi-column correctly
    let mut key_set_ids: Vec<usize> = filter_params.iter().map(|p| p.key_set_id).collect();
    key_set_ids.sort_unstable();
    key_set_ids.dedup();

    // For each key_set, extract distinct tuples (Option<String> to support NULLs)
    let mut key_set_values: std::collections::HashMap<usize, Vec<Vec<Option<String>>>> =
        std::collections::HashMap::new();

    for &ks_id in &key_set_ids {
        let ks_params: Vec<&FilterParam> = filter_params
            .iter()
            .filter(|p| p.key_set_id == ks_id)
            .collect();

        let mut seen = std::collections::HashSet::new();
        let n_cols = ks_params.len();
        let mut columns: Vec<Vec<Option<String>>> = vec![Vec::new(); n_cols];

        for row in source_rows {
            let mut tuple: Vec<Option<String>> = Vec::with_capacity(n_cols);

            for param in &ks_params {
                // Check both maps: a column might be in identity_keys or lookup_keys
                // depending on how PL/pgSQL wrapper classified it. For filter purposes,
                // we just need the value from whichever map has it.
                let val_opt = row
                    .identity_keys
                    .get(&param.col_name)
                    .or_else(|| row.lookup_keys.get(&param.col_name));

                if let Some(val) = val_opt {
                    if val.is_null() {
                        tuple.push(None); // NULL — included for IS NOT DISTINCT FROM matching
                    } else {
                        let text_val = match val {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Number(n) => n.to_string(),
                            serde_json::Value::Bool(b) => b.to_string(),
                            _ => val.to_string(),
                        };
                        tuple.push(Some(text_val));
                    }
                } else {
                    tuple.push(None); // Missing column treated as NULL
                }
            }

            // Deduplicate by tuple (represent NULL as sentinel for dedup key)
            let tuple_key: Vec<String> = tuple.iter().map(|v| match v {
                Some(s) => s.clone(),
                None => "\x00NULL\x00".to_string(),
            }).collect();
            if seen.insert(tuple_key) {
                for (i, val) in tuple.into_iter().enumerate() {
                    columns[i].push(val);
                }
            }
        }

        key_set_values.insert(ks_id, columns);
    }

    // Map back to per-FilterParam array literals (in order)
    filter_params
        .iter()
        .map(|param| {
            let columns = key_set_values.get(&param.key_set_id).unwrap();
            let ks_params: Vec<&FilterParam> = filter_params
                .iter()
                .filter(|p| p.key_set_id == param.key_set_id)
                .collect();
            let col_idx = ks_params
                .iter()
                .position(|p| p.param_index == param.param_index)
                .unwrap();
            format_pg_array_literal(&columns[col_idx])
        })
        .collect()
}

/// Read target rows using a cached prepared statement with parameters.
/// Reads individual columns by ordinal — no JSON construction or parsing.
pub fn read_target_rows_parameterized(
    state: &CachedState,
    param_values: &[String],
) -> Result<Vec<TargetRow>, String> {
    Spi::connect_mut(|client| {
        // Prepare on first call per target SQL template, cache with SPI_keepplan
        let cache_key = state.target_sql_template.clone();
        let has_stmt = TARGET_READ_STMTS.with(|cell| cell.borrow().contains_key(&cache_key));
        if !has_stmt {
            let param_types: Vec<pgrx::PgOid> = (0..param_values.len())
                .map(|_| pgrx::PgOid::from(pg_sys::TEXTOID))
                .collect();
            let stmt = client
                .prepare_mut(&state.target_sql_template, &param_types)
                .map_err(|e| format!("Failed to prepare target read: {}", e))?;
            let owned = stmt.keep();
            TARGET_READ_STMTS.with(|cell| {
                cell.borrow_mut().insert(cache_key.clone(), owned);
            });
        }

        // Execute with parameters and parse rows
        use pgrx::datum::DatumWithOid;
        let args: Vec<DatumWithOid> = param_values
            .iter()
            .map(|v| DatumWithOid::from(v.clone()))
            .collect();

        TARGET_READ_STMTS.with(|cell| {
            let borrow = cell.borrow();
            let stmt_ref = borrow.get(&cache_key).unwrap();
            let table = client
                .update(stmt_ref, None, &args)
                .map_err(|e| format!("SPI error reading target rows: {e}"))?;

            let layout = &state.target_col_layout;
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

                let (identity_keys, lookup_keys, data_payload, ephemeral_payload, pk_payload) =
                    read_target_ordinals(&row, layout);

                rows.push(TargetRow {
                    valid_from,
                    valid_until,
                    identity_keys,
                    lookup_keys,
                    data_payload,
                    ephemeral_payload,
                    pk_payload,
                });
            }
            Ok(rows)
        })
    })
}

/// Read target row columns by ordinal and classify into category maps.
/// Columns start at ordinal 3 (after valid_from, valid_until).
fn read_target_ordinals(
    row: &pgrx::spi::SpiHeapTupleData,
    layout: &[ColMapping],
) -> (
    serde_json::Map<String, serde_json::Value>, // identity_keys
    serde_json::Map<String, serde_json::Value>, // lookup_keys
    serde_json::Map<String, serde_json::Value>, // data_payload
    serde_json::Map<String, serde_json::Value>, // ephemeral_payload
    serde_json::Map<String, serde_json::Value>, // pk_payload (PK-only columns)
) {
    let mut identity = serde_json::Map::new();
    let mut lookup = serde_json::Map::new();
    let mut data = serde_json::Map::new();
    let mut ephemeral = serde_json::Map::new();
    let mut pk = serde_json::Map::new();

    for (i, cm) in layout.iter().enumerate() {
        let ordinal = 3 + i; // 1-based, first 2 are valid_from/valid_until
        let val = match row.get::<String>(ordinal) {
            Ok(Some(s)) => parse_typed_value(s, &cm.pg_type),
            _ => serde_json::Value::Null,
        };
        match cm.category {
            ColCategory::Identity => {
                identity.insert(cm.col_name.clone(), val);
            }
            ColCategory::Lookup => {
                lookup.insert(cm.col_name.clone(), val);
            }
            ColCategory::Data => {
                data.insert(cm.col_name.clone(), val);
            }
            ColCategory::Ephemeral => {
                ephemeral.insert(cm.col_name.clone(), val);
            }
            ColCategory::StablePk => {
                pk.insert(cm.col_name.clone(), val);
            }
        }
    }

    (identity, lookup, data, ephemeral, pk)
}

/// Parse a text value from PostgreSQL into the correct JSON type based on pg_type.
fn parse_typed_value(text: String, pg_type: &str) -> serde_json::Value {
    match pg_type {
        "integer" | "bigint" | "smallint" | "serial" | "bigserial" | "smallserial"
        | "int2" | "int4" | "int8" | "oid" => {
            if let Ok(n) = text.parse::<i64>() {
                serde_json::Value::Number(n.into())
            } else {
                serde_json::Value::String(text)
            }
        }
        "numeric" | "real" | "double precision" | "decimal"
        | "float4" | "float8" | "money" => {
            // Try integer first (preserves "1250" as 1250, not 1250.0)
            if let Ok(n) = text.parse::<i64>() {
                serde_json::Value::Number(n.into())
            } else if let Ok(n) = text.parse::<f64>() {
                serde_json::Number::from_f64(n)
                    .map(serde_json::Value::Number)
                    .unwrap_or_else(|| serde_json::Value::String(text))
            } else {
                serde_json::Value::String(text)
            }
        }
        "boolean" | "bool" => match text.as_str() {
            "t" | "true" => serde_json::Value::Bool(true),
            "f" | "false" => serde_json::Value::Bool(false),
            _ => serde_json::Value::String(text),
        },
        _ => serde_json::Value::String(text),
    }
}

/// Format a list of string values as a PostgreSQL array literal.
/// E.g., ["a", "b with,comma"] → {"a","b with,comma"}
fn format_pg_array_literal(values: &[Option<String>]) -> String {
    let mut buf = String::with_capacity(values.len() * 10 + 2);
    buf.push('{');
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        match v {
            None => buf.push_str("NULL"),
            Some(s) => {
                buf.push('"');
                for c in s.chars() {
                    match c {
                        '"' => buf.push_str("\"\""),
                        '\\' => buf.push_str("\\\\"),
                        _ => buf.push(c),
                    }
                }
                buf.push('"');
            }
        }
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

