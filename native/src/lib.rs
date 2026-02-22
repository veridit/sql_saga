use std::cell::{Cell, RefCell};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Instant;

use pgrx::prelude::*;

pg_module_magic!();

mod executor_cache;
mod introspect;
mod reader;
mod sweep;
mod types;

use types::{CachedState, DeleteMode, MergeMode, PlanRow};

thread_local! {
    /// Multi-entry cache keyed by config cache_key (target_table + mode + columns).
    /// Holds one CachedState per distinct temporal_merge configuration, enabling
    /// cache hits when alternating between target tables (e.g., StatBus pattern
    /// of 4 target tables per import cycle).
    static PLANNER_CACHE: RefCell<HashMap<u64, CachedState>> = RefCell::new(HashMap::new());
    static EMIT_STMT: RefCell<Option<pgrx::spi::OwnedPreparedStatement>> = RefCell::new(None);
    static CACHE_HITS: Cell<u64> = Cell::new(0);
    static CACHE_MISSES: Cell<u64> = Cell::new(0);
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
    // Clear the emit prepared statement — the target temp table (pg_temp.temporal_merge_plan)
    // is dropped and recreated by the PL/pgSQL wrapper on every call, so any cached plan
    // referencing the old table OID is stale.
    EMIT_STMT.with(|cell| { *cell.borrow_mut() = None; });

    let mode = MergeMode::from_str(mode)
        .unwrap_or_else(|| pgrx::error!("Invalid merge mode: {}", mode));
    let delete_mode = DeleteMode::from_str(delete_mode)
        .unwrap_or_else(|| pgrx::error!("Invalid delete mode: {}", delete_mode));

    // Parse lookup_keys JSONB into flat list + individual key sets
    let (all_lookup_cols, lookup_key_sets) = parse_lookup_keys(lookup_keys);

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

    // Quick introspection of source column names + types to detect structure changes.
    // Including types ensures (id int) vs (id bigint) are distinguished.
    let source_cols_hash = {
        let source_oid = u32::from(source_table);
        let cols_query = format!(
            "SELECT array_agg(attname || '::' || format_type(atttypid, atttypmod) ORDER BY attnum)::text \
             FROM pg_attribute \
             WHERE attrelid = {}::oid AND attnum > 0 AND NOT attisdropped",
            source_oid
        );
        let cols_str: String = Spi::connect(|client| {
            client
                .select(&cols_query, None, &[])
                .ok()
                .and_then(|t| t.first().get_one::<String>().ok().flatten())
                .unwrap_or_default()
        });
        let mut h = DefaultHasher::new();
        cols_str.hash(&mut h);
        h.finish()
    };

    // Check cache: config cache_key + source columns hash must match.
    // Source OID is intentionally excluded — different temp tables with identical
    // column structure should be cache hits (StatBus pattern).
    let cache_hit = PLANNER_CACHE.with(|c| {
        c.borrow().get(&cache_key).map_or(false, |s| {
            s.source_cols_hash == source_cols_hash
        })
    });

    let state = if cache_hit {
        CACHE_HITS.with(|c| c.set(c.get() + 1));
        PLANNER_CACHE.with(|c| c.borrow().get(&cache_key).unwrap().clone())
    } else {
        CACHE_MISSES.with(|c| c.set(c.get() + 1));
        // Cache miss for this config — evict stale entry if exists
        PLANNER_CACHE.with(|c| { c.borrow_mut().remove(&cache_key); });

        // Single SPI connection for all introspection
        let result = introspect::introspect_all(target_table, source_table, era_name)
            .unwrap_or_else(|e| pgrx::error!("{}", e));

        let ephemeral_columns = ephemeral_columns.unwrap_or_default();

        // Validate ephemeral columns don't include temporal boundary columns (matches PL/pgSQL).
        {
            let era = &result.era;
            if ephemeral_columns.contains(&era.valid_from_col) || ephemeral_columns.contains(&era.valid_until_col) {
                pgrx::error!(
                    "Temporal boundary columns (\"{}\", \"{}\") cannot be specified in ephemeral_columns.",
                    era.valid_from_col, era.valid_until_col
                );
            }
            if let Some(ref vt) = era.valid_to_col {
                if ephemeral_columns.contains(vt) {
                    pgrx::error!(
                        "Synchronized column \"{}\" is automatically handled and should not be specified in ephemeral_columns.",
                        vt
                    );
                }
            }
            if ephemeral_columns.contains(&era.range_col) {
                pgrx::error!(
                    "Synchronized column \"{}\" is automatically handled and should not be specified in ephemeral_columns.",
                    era.range_col
                );
            }
        }

        // Validate column existence (matches PL/pgSQL consolidated column validation)
        {
            let src = &result.source_cols;
            let tgt = &result.target_cols;
            if !src.contains(&row_id_column.to_string()) {
                pgrx::error!(
                    "row_id_column \"{}\" does not exist in source table {}",
                    row_id_column, source_ident
                );
            }
            if let Some(ref fid) = founding_id_column {
                if !src.contains(&fid.to_string()) {
                    pgrx::error!(
                        "founding_id_column \"{}\" does not exist in source table {}",
                        fid, source_ident
                    );
                }
            }
            if let Some(ref id_cols) = identity_columns {
                for col in id_cols {
                    if !tgt.contains(col) {
                        pgrx::error!(
                            "identity_column {} does not exist in target table {}",
                            col, result.target_ident
                        );
                    }
                }
            }
            if let Some(ref lk_cols) = all_lookup_cols {
                for col in lk_cols {
                    if !tgt.contains(col) {
                        pgrx::error!(
                            "lookup_column {} does not exist in target table {}",
                            col, result.target_ident
                        );
                    }
                }
            }
        }

        let ctx = introspect::build_planner_context(
            mode,
            delete_mode,
            result.era,
            identity_columns,
            all_lookup_cols,
            lookup_key_sets,
            result.pk_cols,
            ephemeral_columns,
            founding_id_column.map(|s| s.to_string()),
            row_id_column.to_string(),
            p_log_trace,
            result.exclude_if_null_columns,
        );

        // Build SQL templates from pre-fetched column data (no SPI calls)
        let templates = reader::build_sql_templates_from_cols(
            &result.source_cols,
            &result.target_cols,
            &result.target_col_types,
            &result.source_col_types,
            &result.target_col_notnull,
            &result.target_ident,
            &ctx,
            &source_ident,
        )
        .unwrap_or_else(|e| pgrx::error!("{}", e));

        let new_state = CachedState {
            cache_key,
            ctx,
            target_ident: templates.target_ident,
            source_sql_template: templates.source_sql_template,
            target_sql_template: templates.target_sql_template,
            source_col_layout: templates.source_col_layout,
            target_col_layout: templates.target_col_layout,
            target_filter_params: templates.target_filter_params,
            source_cols_hash,
        };
        PLANNER_CACHE.with(|c| {
            c.borrow_mut().insert(cache_key, new_state.clone());
        });

        new_state
    };

    let t_start = Instant::now();

    // Phase 2a: Read source rows (cached prepared statement, keyed by source_ident)
    let source_rows = reader::read_source_rows_cached(&source_ident, &state)
        .unwrap_or_else(|e| pgrx::error!("Failed to read source rows: {}", e));
    let t_source = Instant::now();

    if p_log_trace {
        pgrx::notice!("native planner: read {} source rows", source_rows.len());
        for sr in &source_rows {
            pgrx::notice!(
                "  src row_id={} identity_keys={:?} lookup_keys={:?} valid=[{},{})",
                sr.row_id,
                sr.identity_keys,
                sr.lookup_keys,
                sr.valid_from,
                sr.valid_until,
            );
        }
    }

    // Phase 2b: Read target rows — parameterized (cached stmt) or dynamic SQL
    let target_rows = if let Some(ref filter_params) = state.target_filter_params {
        if filter_params.is_empty() {
            // Full scan or no filter: static SQL, no parameters
            reader::read_target_rows_parameterized(&state, &[])
                .unwrap_or_else(|e| pgrx::error!("Failed to read target rows: {}", e))
        } else {
            // Parameterized filter: extract identity values from source rows
            let param_values = reader::extract_filter_values(&source_rows, filter_params);
            if p_log_trace {
                pgrx::notice!("native planner: target filter params={:?}", param_values);
                pgrx::notice!("native planner: target SQL template={}", state.target_sql_template);
            }
            reader::read_target_rows_parameterized(&state, &param_values)
                .unwrap_or_else(|e| pgrx::error!("Failed to read target rows: {}", e))
        }
    } else {
        // Dynamic SQL fallback (multi-column key sets)
        let target_sql = state
            .target_sql_template
            .replace("__SOURCE_IDENT__", &source_ident);
        if p_log_trace {
            pgrx::notice!("native planner: target SQL (dynamic)={}", target_sql);
        }
        reader::read_target_rows_with_sql(&target_sql, &state)
            .unwrap_or_else(|e| pgrx::error!("Failed to read target rows: {}", e))
    };
    let t_target = Instant::now();

    if p_log_trace {
        pgrx::notice!("native planner: read {} target rows", target_rows.len());
        for tr in &target_rows {
            pgrx::notice!(
                "  tgt identity_keys={:?} lookup_keys={:?} valid=[{},{})",
                tr.identity_keys,
                tr.lookup_keys,
                tr.valid_from,
                tr.valid_until,
            );
        }
    }

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

/// Return per-connection cache statistics for observability.
#[pg_extern]
fn temporal_merge_native_cache_stats() -> TableIterator<
    'static,
    (
        name!(stat_name, String),
        name!(stat_value, i64),
    ),
> {
    let planner_entries = PLANNER_CACHE.with(|c| c.borrow().len()) as i64;
    let target_stmts = reader::target_read_stmt_count() as i64;
    let source_stmts = reader::source_read_stmt_count() as i64;
    let hits = CACHE_HITS.with(|c| c.get()) as i64;
    let misses = CACHE_MISSES.with(|c| c.get()) as i64;
    let executor_entries = executor_cache::EXECUTOR_CACHE.with(|c| c.borrow().len()) as i64;
    let executor_hits = executor_cache::EXECUTOR_CACHE_HITS.with(|c| c.get()) as i64;
    let executor_misses = executor_cache::EXECUTOR_CACHE_MISSES.with(|c| c.get()) as i64;

    TableIterator::new(vec![
        ("planner_cache_entries".to_string(), planner_entries),
        ("target_read_stmts".to_string(), target_stmts),
        ("source_read_stmts".to_string(), source_stmts),
        ("cache_hits".to_string(), hits),
        ("cache_misses".to_string(), misses),
        ("executor_cache_entries".to_string(), executor_entries),
        ("executor_cache_hits".to_string(), executor_hits),
        ("executor_cache_misses".to_string(), executor_misses),
    ])
}

/// Reset all per-connection caches and counters.
#[pg_extern]
fn temporal_merge_native_cache_reset() {
    PLANNER_CACHE.with(|c| c.borrow_mut().clear());
    EMIT_STMT.with(|c| { *c.borrow_mut() = None; });
    CACHE_HITS.with(|c| c.set(0));
    CACHE_MISSES.with(|c| c.set(0));
    reader::clear_read_stmts();
    executor_cache::EXECUTOR_CACHE.with(|c| c.borrow_mut().clear());
    executor_cache::EXECUTOR_CACHE_HITS.with(|c| c.set(0));
    executor_cache::EXECUTOR_CACHE_MISSES.with(|c| c.set(0));
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
/// Parse lookup_keys JSONB `[["id"], ["legal_unit_id"]]` into flat column list
/// and individual key sets.
fn parse_lookup_keys(lookup_keys: Option<pgrx::JsonB>) -> (Option<Vec<String>>, Vec<Vec<String>>) {
    let Some(pgrx::JsonB(val)) = lookup_keys else {
        return (None, Vec::new());
    };
    let Some(arr) = val.as_array() else {
        return (None, Vec::new());
    };
    let mut cols = Vec::new();
    let mut key_sets: Vec<Vec<String>> = Vec::new();
    for key_array in arr {
        if let Some(inner) = key_array.as_array() {
            let mut set = Vec::new();
            for col in inner {
                if let Some(s) = col.as_str() {
                    if !cols.contains(&s.to_string()) {
                        cols.push(s.to_string());
                    }
                    set.push(s.to_string());
                }
            }
            if !set.is_empty() {
                key_sets.push(set);
            }
        }
    }
    if cols.is_empty() {
        (None, Vec::new())
    } else {
        cols.sort();
        (Some(cols), key_sets)
    }
}

/// Insert plan rows into pg_temp.temporal_merge_plan via a single bulk
/// INSERT ... SELECT * FROM unnest($1::text[], ..., $22::text[]) with casts.
/// Each column is a parallel text[] array; no JSON serialization needed.
fn emit_plan_rows(plan_rows: &[PlanRow]) -> i64 {
    use pgrx::datum::DatumWithOid;
    use std::fmt::Write;

    if plan_rows.is_empty() {
        return 0;
    }

    let n = plan_rows.len();
    // Build 22 parallel arrays (one per column), each as a PG text[] literal
    let mut plan_op_seq = Vec::with_capacity(n);
    let mut statement_seq = Vec::with_capacity(n);
    let mut row_ids: Vec<Option<String>> = Vec::with_capacity(n);
    let mut operation = Vec::with_capacity(n);
    let mut update_effect = Vec::with_capacity(n);
    let mut causal_id = Vec::with_capacity(n);
    let mut is_new_entity = Vec::with_capacity(n);
    let mut entity_keys = Vec::with_capacity(n);
    let mut identity_keys = Vec::with_capacity(n);
    let mut lookup_keys = Vec::with_capacity(n);
    let mut s_t_relation = Vec::with_capacity(n);
    let mut b_a_relation = Vec::with_capacity(n);
    let mut old_valid_from = Vec::with_capacity(n);
    let mut old_valid_until = Vec::with_capacity(n);
    let mut new_valid_from = Vec::with_capacity(n);
    let mut new_valid_until = Vec::with_capacity(n);
    let mut old_valid_range = Vec::with_capacity(n);
    let mut new_valid_range = Vec::with_capacity(n);
    let mut data = Vec::with_capacity(n);
    let mut feedback = Vec::with_capacity(n);
    let mut trace = Vec::with_capacity(n);
    let mut grouping_key = Vec::with_capacity(n);

    for row in plan_rows {
        plan_op_seq.push(row.plan_op_seq.to_string());
        statement_seq.push(row.statement_seq.to_string());

        // row_ids: bigint[] → text representation "{1,2,3}" or NULL for empty
        // PL/pgSQL produces NULL row_ids for DELETE rows (no source contributes)
        if row.row_ids.is_empty() {
            row_ids.push(None);
        } else {
            let mut ids_buf = String::with_capacity(row.row_ids.len() * 8);
            ids_buf.push('{');
            for (j, id) in row.row_ids.iter().enumerate() {
                if j > 0 { ids_buf.push(','); }
                write!(ids_buf, "{}", id).unwrap();
            }
            ids_buf.push('}');
            row_ids.push(Some(ids_buf));
        }

        operation.push(row.operation.as_str().to_string());
        update_effect.push(opt_str(row.update_effect.map(|u| u.as_str())));
        causal_id.push(opt_owned(&row.causal_id));
        is_new_entity.push(row.is_new_entity.to_string());
        entity_keys.push(opt_json(&row.entity_keys));
        identity_keys.push(opt_json(&row.identity_keys));
        lookup_keys.push(opt_json(&row.lookup_keys));
        s_t_relation.push(opt_str(row.s_t_relation.map(|r| r.as_str())));
        b_a_relation.push(opt_str(row.b_a_relation.map(|r| r.as_str())));
        old_valid_from.push(opt_owned(&row.old_valid_from));
        old_valid_until.push(opt_owned(&row.old_valid_until));
        new_valid_from.push(opt_owned(&row.new_valid_from));
        new_valid_until.push(opt_owned(&row.new_valid_until));
        old_valid_range.push(opt_owned(&row.old_valid_range));
        new_valid_range.push(opt_owned(&row.new_valid_range));
        data.push(opt_json(&row.data));
        feedback.push(opt_json(&row.feedback));
        trace.push(opt_json(&row.trace));
        grouping_key.push(row.grouping_key.clone());
    }

    let count = n as i64;

    // Convert each Vec<String> to a PG text[] literal, handling NULLs
    let arrays: Vec<String> = vec![
        pg_text_array(&plan_op_seq),
        pg_text_array(&statement_seq),
        pg_nullable_text_array(&row_ids),
        pg_text_array(&operation),
        pg_nullable_text_array(&update_effect),
        pg_nullable_text_array(&causal_id),
        pg_text_array(&is_new_entity),
        pg_nullable_text_array(&entity_keys),
        pg_nullable_text_array(&identity_keys),
        pg_nullable_text_array(&lookup_keys),
        pg_nullable_text_array(&s_t_relation),
        pg_nullable_text_array(&b_a_relation),
        pg_nullable_text_array(&old_valid_from),
        pg_nullable_text_array(&old_valid_until),
        pg_nullable_text_array(&new_valid_from),
        pg_nullable_text_array(&new_valid_until),
        pg_nullable_text_array(&old_valid_range),
        pg_nullable_text_array(&new_valid_range),
        pg_nullable_text_array(&data),
        pg_nullable_text_array(&feedback),
        pg_nullable_text_array(&trace),
        pg_text_array(&grouping_key),
    ];

    Spi::connect_mut(|client| {
        let has_stmt = EMIT_STMT.with(|cell| cell.borrow().is_some());

        if !has_stmt {
            let param_types: Vec<pgrx::PgOid> = (0..22)
                .map(|_| pgrx::PgOid::from(pg_sys::TEXTOID))
                .collect();
            let stmt = client
                .prepare_mut(
                    "INSERT INTO pg_temp.temporal_merge_plan (\
                     plan_op_seq, statement_seq, row_ids, operation, update_effect, \
                     causal_id, is_new_entity, entity_keys, identity_keys, lookup_keys, \
                     s_t_relation, b_a_relation, old_valid_from, old_valid_until, \
                     new_valid_from, new_valid_until, old_valid_range, new_valid_range, \
                     data, feedback, trace, grouping_key) \
                     SELECT \
                     a1::bigint, a2::int, a3::bigint[], \
                     a4::sql_saga.temporal_merge_plan_action, \
                     a5::sql_saga.temporal_merge_update_effect, \
                     a6, a7::boolean, a8::jsonb, a9::jsonb, a10::jsonb, \
                     a11::sql_saga.allen_interval_relation, \
                     a12::sql_saga.allen_interval_relation, \
                     a13, a14, a15, a16, a17, a18, \
                     a19::jsonb, a20::jsonb, a21::jsonb, a22 \
                     FROM unnest(\
                     $1::text[], $2::text[], $3::text[], $4::text[], $5::text[], \
                     $6::text[], $7::text[], $8::text[], $9::text[], $10::text[], \
                     $11::text[], $12::text[], $13::text[], $14::text[], $15::text[], \
                     $16::text[], $17::text[], $18::text[], $19::text[], $20::text[], \
                     $21::text[], $22::text[]) \
                     AS t(a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, \
                     a11, a12, a13, a14, a15, a16, a17, a18, a19, a20, a21, a22)",
                    &param_types,
                )
                .unwrap_or_else(|e| pgrx::error!("Failed to prepare bulk insert: {}", e));
            let owned = stmt.keep();
            EMIT_STMT.with(|cell| {
                *cell.borrow_mut() = Some(owned);
            });
        }

        let args: Vec<DatumWithOid> = arrays
            .into_iter()
            .map(DatumWithOid::from)
            .collect();
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

/// Format a non-nullable text[] array literal: {"val1","val2",...}
fn pg_text_array(values: &[String]) -> String {
    let mut buf = String::with_capacity(values.len() * 20 + 2);
    buf.push('{');
    for (i, v) in values.iter().enumerate() {
        if i > 0 { buf.push(','); }
        buf.push('"');
        for c in v.chars() {
            match c {
                '"' => buf.push_str("\\\""),
                '\\' => buf.push_str("\\\\"),
                _ => buf.push(c),
            }
        }
        buf.push('"');
    }
    buf.push('}');
    buf
}

/// Format a nullable text[] array literal: {"val1",NULL,"val3",...}
/// Values of None::String are represented as NULL (unquoted).
/// Uses backslash escaping (PG array_in format): \" for double quotes, \\ for backslashes.
fn pg_nullable_text_array(values: &[Option<String>]) -> String {
    let mut buf = String::with_capacity(values.len() * 20 + 2);
    buf.push('{');
    for (i, v) in values.iter().enumerate() {
        if i > 0 { buf.push(','); }
        match v {
            Some(s) => {
                buf.push('"');
                for c in s.chars() {
                    match c {
                        '"' => buf.push_str("\\\""),
                        '\\' => buf.push_str("\\\\"),
                        _ => buf.push(c),
                    }
                }
                buf.push('"');
            }
            None => buf.push_str("NULL"),
        }
    }
    buf.push('}');
    buf
}

/// Convert Option<&str> to Option<String> for nullable columns.
fn opt_str(v: Option<&str>) -> Option<String> {
    v.map(|s| s.to_string())
}

/// Convert Option<String> ref to Option<String> for nullable text columns.
fn opt_owned(v: &Option<String>) -> Option<String> {
    v.clone()
}

/// Convert Option<serde_json::Value> to Option<String> (JSON text) for nullable jsonb columns.
fn opt_json(v: &Option<serde_json::Value>) -> Option<String> {
    v.as_ref().map(|j| j.to_string())
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
        let r = AllenRelation::compute("2024-01-01", "2024-02-01", "2024-01-01", "2024-02-01", false);
        assert_eq!(r, Some(AllenRelation::Equals));
    }

    #[pg_test]
    fn test_allen_relation_precedes() {
        use crate::types::AllenRelation;
        let r = AllenRelation::compute("2024-01-01", "2024-02-01", "2024-03-01", "2024-04-01", false);
        assert_eq!(r, Some(AllenRelation::Precedes));
    }

    #[pg_test]
    fn test_allen_relation_meets() {
        use crate::types::AllenRelation;
        let r = AllenRelation::compute("2024-01-01", "2024-02-01", "2024-02-01", "2024-03-01", false);
        assert_eq!(r, Some(AllenRelation::Meets));
    }

    #[pg_test]
    fn test_allen_relation_overlaps() {
        use crate::types::AllenRelation;
        let r = AllenRelation::compute("2024-01-01", "2024-03-01", "2024-02-01", "2024-04-01", false);
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
