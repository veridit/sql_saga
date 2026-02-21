use std::cell::{Cell, RefCell};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use pgrx::prelude::*;

/// Cached executor introspection state.
/// Contains all metadata and SQL fragments needed by the PL/pgSQL executor,
/// replacing ~570 lines of per-call introspection + CTE logic.
#[derive(Debug, Clone)]
pub struct ExecutorCachedState {
    // Era metadata
    pub range_col: String,
    pub range_constructor: String,
    pub range_subtype: String,
    pub valid_from_col: String,
    pub valid_until_col: String,
    pub valid_to_col: Option<String>,
    pub valid_from_col_type: String,
    pub valid_until_col_type: String,
    // Column info
    pub pk_cols: Vec<String>,
    pub not_null_defaulted_cols: Vec<String>,
    pub insert_defaulted_columns: Vec<String>,
    pub founding_defaulted_columns: Vec<String>,
    pub source_col_names: Vec<String>,
    // SQL fragments for DML
    pub update_set_clause: Option<String>,
    pub all_cols_ident: Option<String>,
    pub all_cols_select: Option<String>,
    pub all_cols_from_jsonb: Option<String>,
    pub founding_all_cols_ident: Option<String>,
    pub founding_all_cols_from_jsonb: Option<String>,
    pub entity_key_join_clause: String,
    pub entity_key_select_list: String,
    // Cache validation
    pub source_cols_hash: u64,
}

thread_local! {
    /// Multi-entry cache keyed by config hash.
    pub static EXECUTOR_CACHE: RefCell<HashMap<u64, ExecutorCachedState>> = RefCell::new(HashMap::new());
    pub static EXECUTOR_CACHE_HITS: Cell<u64> = Cell::new(0);
    pub static EXECUTOR_CACHE_MISSES: Cell<u64> = Cell::new(0);
}

/// Compute a cache key from all executor-relevant parameters.
fn compute_executor_cache_key(
    target_table: pg_sys::Oid,
    source_cols_hash: u64,
    identity_columns: &[String],
    lookup_columns: &[String],
    era_name: &str,
    ephemeral_columns: &[String],
    founding_id_column: Option<&str>,
    row_id_column: &str,
) -> u64 {
    let mut hasher = DefaultHasher::new();
    u32::from(target_table).hash(&mut hasher);
    source_cols_hash.hash(&mut hasher);
    identity_columns.hash(&mut hasher);
    lookup_columns.hash(&mut hasher);
    era_name.hash(&mut hasher);
    ephemeral_columns.hash(&mut hasher);
    founding_id_column.hash(&mut hasher);
    row_id_column.hash(&mut hasher);
    hasher.finish()
}

/// Hash source column names for cache validation.
fn hash_source_cols(client: &pgrx::spi::SpiClient, source_oid: u32) -> u64 {
    let cols_query = format!(
        "SELECT array_agg(attname::text ORDER BY attnum)::text \
         FROM pg_attribute \
         WHERE attrelid = {}::oid AND attnum > 0 AND NOT attisdropped",
        source_oid
    );
    let cols_str: String = client
        .select(&cols_query, None, &[])
        .ok()
        .and_then(|t| t.first().get_one::<String>().ok().flatten())
        .unwrap_or_default();
    let mut h = DefaultHasher::new();
    cols_str.hash(&mut h);
    h.finish()
}

/// Helper: quote identifier (double-quote, escaping inner double-quotes).
fn qi(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Perform all executor introspection in a single SPI connection and return
/// the cached state. On cache hit, returns immediately with zero SPI calls.
#[pg_extern]
fn temporal_merge_executor_introspect(
    target_table: pg_sys::Oid,
    source_table: pg_sys::Oid,
    identity_columns: Option<Vec<String>>,
    lookup_columns: Option<Vec<String>>,
    era_name: default!(&str, "'valid'"),
    ephemeral_columns: Option<Vec<String>>,
    founding_id_column: Option<&str>,
    row_id_column: default!(&str, "'row_id'"),
) -> pgrx::composite_type!('static, "sql_saga.temporal_merge_executor_cache") {
    let identity_columns = identity_columns.unwrap_or_default();
    let lookup_columns_resolved = lookup_columns.unwrap_or_else(|| identity_columns.clone());
    let ephemeral_columns = ephemeral_columns.unwrap_or_default();

    let target_oid = u32::from(target_table);
    let source_oid = u32::from(source_table);

    // Compute source_cols_hash for cache validation (quick SPI call)
    let source_cols_hash = Spi::connect(|client| {
        hash_source_cols(&client, source_oid)
    });

    let cache_key = compute_executor_cache_key(
        target_table,
        source_cols_hash,
        &identity_columns,
        &lookup_columns_resolved,
        era_name,
        &ephemeral_columns,
        founding_id_column,
        row_id_column,
    );

    // Check cache
    let cache_hit = EXECUTOR_CACHE.with(|c| {
        c.borrow().get(&cache_key).map_or(false, |s| {
            s.source_cols_hash == source_cols_hash
        })
    });

    let state = if cache_hit {
        EXECUTOR_CACHE_HITS.with(|c| c.set(c.get() + 1));
        EXECUTOR_CACHE.with(|c| c.borrow().get(&cache_key).unwrap().clone())
    } else {
        EXECUTOR_CACHE_MISSES.with(|c| c.set(c.get() + 1));
        EXECUTOR_CACHE.with(|c| { c.borrow_mut().remove(&cache_key); });

        let new_state = run_executor_introspection(
            target_oid,
            source_oid,
            &identity_columns,
            &lookup_columns_resolved,
            era_name,
            &ephemeral_columns,
            founding_id_column,
            row_id_column,
            source_cols_hash,
        );

        EXECUTOR_CACHE.with(|c| {
            c.borrow_mut().insert(cache_key, new_state.clone());
        });
        new_state
    };

    // Build composite type result
    let mut result = PgHeapTuple::new_composite_type("sql_saga.temporal_merge_executor_cache")
        .unwrap_or_else(|e| pgrx::error!("Failed to create composite type: {}", e));

    result.set_by_name("range_col", state.range_col.clone())
        .unwrap_or_else(|e| pgrx::error!("set range_col: {}", e));
    result.set_by_name("range_constructor", state.range_constructor.clone())
        .unwrap_or_else(|e| pgrx::error!("set range_constructor: {}", e));
    result.set_by_name("range_subtype", state.range_subtype.clone())
        .unwrap_or_else(|e| pgrx::error!("set range_subtype: {}", e));
    result.set_by_name("valid_from_col", state.valid_from_col.clone())
        .unwrap_or_else(|e| pgrx::error!("set valid_from_col: {}", e));
    result.set_by_name("valid_until_col", state.valid_until_col.clone())
        .unwrap_or_else(|e| pgrx::error!("set valid_until_col: {}", e));
    result.set_by_name::<Option<String>>("valid_to_col", state.valid_to_col.clone())
        .unwrap_or_else(|e| pgrx::error!("set valid_to_col: {}", e));
    result.set_by_name("valid_from_col_type", state.valid_from_col_type.clone())
        .unwrap_or_else(|e| pgrx::error!("set valid_from_col_type: {}", e));
    result.set_by_name("valid_until_col_type", state.valid_until_col_type.clone())
        .unwrap_or_else(|e| pgrx::error!("set valid_until_col_type: {}", e));
    result.set_by_name("pk_cols", state.pk_cols.clone())
        .unwrap_or_else(|e| pgrx::error!("set pk_cols: {}", e));
    result.set_by_name("not_null_defaulted_cols", state.not_null_defaulted_cols.clone())
        .unwrap_or_else(|e| pgrx::error!("set not_null_defaulted_cols: {}", e));
    result.set_by_name("insert_defaulted_columns", state.insert_defaulted_columns.clone())
        .unwrap_or_else(|e| pgrx::error!("set insert_defaulted_columns: {}", e));
    result.set_by_name("founding_defaulted_columns", state.founding_defaulted_columns.clone())
        .unwrap_or_else(|e| pgrx::error!("set founding_defaulted_columns: {}", e));
    result.set_by_name("source_col_names", state.source_col_names.clone())
        .unwrap_or_else(|e| pgrx::error!("set source_col_names: {}", e));
    result.set_by_name::<Option<String>>("update_set_clause", state.update_set_clause.clone())
        .unwrap_or_else(|e| pgrx::error!("set update_set_clause: {}", e));
    result.set_by_name::<Option<String>>("all_cols_ident", state.all_cols_ident.clone())
        .unwrap_or_else(|e| pgrx::error!("set all_cols_ident: {}", e));
    result.set_by_name::<Option<String>>("all_cols_select", state.all_cols_select.clone())
        .unwrap_or_else(|e| pgrx::error!("set all_cols_select: {}", e));
    result.set_by_name::<Option<String>>("all_cols_from_jsonb", state.all_cols_from_jsonb.clone())
        .unwrap_or_else(|e| pgrx::error!("set all_cols_from_jsonb: {}", e));
    result.set_by_name::<Option<String>>("founding_all_cols_ident", state.founding_all_cols_ident.clone())
        .unwrap_or_else(|e| pgrx::error!("set founding_all_cols_ident: {}", e));
    result.set_by_name::<Option<String>>("founding_all_cols_from_jsonb", state.founding_all_cols_from_jsonb.clone())
        .unwrap_or_else(|e| pgrx::error!("set founding_all_cols_from_jsonb: {}", e));
    result.set_by_name("entity_key_join_clause", state.entity_key_join_clause.clone())
        .unwrap_or_else(|e| pgrx::error!("set entity_key_join_clause: {}", e));
    result.set_by_name("entity_key_select_list", state.entity_key_select_list.clone())
        .unwrap_or_else(|e| pgrx::error!("set entity_key_select_list: {}", e));

    result
}

/// Run all executor introspection queries in a single SPI connection.
/// This is the cache-miss path that replaces ~570 lines of PL/pgSQL.
fn run_executor_introspection(
    target_oid: u32,
    source_oid: u32,
    identity_columns: &[String],
    lookup_columns: &[String],
    era_name: &str,
    _ephemeral_columns: &[String],
    _founding_id_column: Option<&str>,
    _row_id_column: &str,
    source_cols_hash: u64,
) -> ExecutorCachedState {
    let era_escaped = era_name.replace('\'', "''");

    Spi::connect(|client| {
        // 1. Source column names
        let src_cols_query = format!(
            "SELECT COALESCE(array_agg(attname::text), '{{}}') \
             FROM pg_attribute \
             WHERE attrelid = {}::oid AND attnum > 0 AND NOT attisdropped",
            source_oid
        );
        let source_col_names: Vec<String> = client
            .select(&src_cols_query, None, &[])
            .ok()
            .and_then(|t| t.first().get_one::<Vec<String>>().ok().flatten())
            .unwrap_or_default();

        // 2. NOT NULL defaulted cols
        let nn_def_query = format!(
            "SELECT COALESCE(array_agg(a.attname::text), '{{}}') \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = {}::oid AND a.attnum > 0 AND NOT a.attisdropped \
             AND a.atthasdef AND a.attnotnull",
            target_oid
        );
        let not_null_defaulted_cols: Vec<String> = client
            .select(&nn_def_query, None, &[])
            .ok()
            .and_then(|t| t.first().get_one::<Vec<String>>().ok().flatten())
            .unwrap_or_default();

        // 3. Era metadata
        let era_query = format!(
            "SELECT e.range_column_name::text, \
                    e.valid_from_column_name::text, \
                    e.valid_until_column_name::text, \
                    e.valid_to_column_name::text, \
                    e.range_type::text, \
                    e.range_subtype::text \
             FROM sql_saga.era e \
             JOIN pg_class c ON c.relname = e.table_name \
             JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = e.table_schema \
             WHERE c.oid = {oid}::oid AND e.era_name = '{era}'",
            oid = target_oid,
            era = era_escaped,
        );
        let era_row = client
            .select(&era_query, Some(1), &[])
            .unwrap_or_else(|e| pgrx::error!("SPI error introspecting era: {}", e))
            .first();

        let range_col: String = era_row.get::<String>(1)
            .unwrap_or(None)
            .unwrap_or_else(|| pgrx::error!("No era named \"{}\" found for table", era_name));
        let valid_from_col: String = era_row.get::<String>(2).unwrap_or(None).unwrap_or_default();
        let valid_until_col: String = era_row.get::<String>(3).unwrap_or(None).unwrap_or_default();
        let valid_to_col: Option<String> = era_row.get::<String>(4).unwrap_or(None);
        let range_constructor: String = era_row.get::<String>(5).unwrap_or(None).unwrap_or_default();
        let range_subtype: String = era_row.get::<String>(6).unwrap_or(None).unwrap_or_default();

        // 4. PK columns (excluding temporal)
        let pk_query = format!(
            "SELECT COALESCE(array_agg(a.attname::text), '{{}}') \
             FROM pg_constraint c \
             JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey) \
             WHERE c.conrelid = {}::oid AND c.contype = 'p' \
             AND a.attname NOT IN ('{}', '{}', '{}')",
            target_oid,
            range_col.replace('\'', "''"),
            valid_from_col.replace('\'', "''"),
            valid_until_col.replace('\'', "''"),
        );
        let pk_cols: Vec<String> = client
            .select(&pk_query, None, &[])
            .ok()
            .and_then(|t| t.first().get_one::<Vec<String>>().ok().flatten())
            .unwrap_or_default();

        // 5. Insert defaulted columns (GENERATED ALWAYS)
        let insert_def_query = format!(
            "SELECT COALESCE(array_agg(a.attname::text), '{{}}') \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = {}::oid AND a.attnum > 0 AND NOT a.attisdropped \
             AND (a.attidentity = 'a' OR a.attgenerated <> '')",
            target_oid
        );
        let mut insert_defaulted_columns: Vec<String> = client
            .select(&insert_def_query, None, &[])
            .ok()
            .and_then(|t| t.first().get_one::<Vec<String>>().ok().flatten())
            .unwrap_or_default();

        // 6. Founding defaulted columns (GENERATED ALWAYS + IDENTITY BY DEFAULT)
        let founding_def_query = format!(
            "SELECT COALESCE(array_agg(a.attname::text), '{{}}') \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = {}::oid AND a.attnum > 0 AND NOT a.attisdropped \
             AND (a.attidentity IN ('a', 'd') OR a.attgenerated <> '')",
            target_oid
        );
        let founding_defaulted_columns: Vec<String> = client
            .select(&founding_def_query, None, &[])
            .ok()
            .and_then(|t| t.first().get_one::<Vec<String>>().ok().flatten())
            .unwrap_or_default();

        // Also exclude synchronized columns from insert_defaulted
        if !valid_from_col.is_empty() {
            insert_defaulted_columns.push(valid_from_col.clone());
        }
        if !valid_until_col.is_empty() {
            insert_defaulted_columns.push(valid_until_col.clone());
        }
        if let Some(ref vt) = valid_to_col {
            insert_defaulted_columns.push(vt.clone());
        }
        if !range_col.is_empty() {
            insert_defaulted_columns.push(range_col.clone());
        }

        // 7. Column type lookups for valid_from/valid_until
        let valid_from_col_type = if !valid_from_col.is_empty() {
            let q = format!(
                "SELECT atttypid::regtype::text FROM pg_attribute \
                 WHERE attrelid = {}::oid AND attname = '{}'",
                target_oid,
                valid_from_col.replace('\'', "''"),
            );
            client.select(&q, None, &[]).ok()
                .and_then(|t| t.first().get_one::<String>().ok().flatten())
                .unwrap_or_else(|| range_subtype.clone())
        } else {
            range_subtype.clone()
        };

        let valid_until_col_type = if !valid_until_col.is_empty() {
            let q = format!(
                "SELECT atttypid::regtype::text FROM pg_attribute \
                 WHERE attrelid = {}::oid AND attname = '{}'",
                target_oid,
                valid_until_col.replace('\'', "''"),
            );
            client.select(&q, None, &[]).ok()
                .and_then(|t| t.first().get_one::<String>().ok().flatten())
                .unwrap_or_else(|| range_subtype.clone())
        } else {
            range_subtype.clone()
        };

        // 8. Entity key join/select clause
        // Build using a single query to get identity column types
        let (entity_key_select_list, entity_key_join_clause) = if identity_columns.is_empty() {
            (String::new(), "true".to_string())
        } else {
            let cols_list = identity_columns.iter()
                .map(|c| format!("'{}'", c.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(",");
            let ek_query = format!(
                "SELECT a.attname::text, format_type(a.atttypid, -1) \
                 FROM pg_attribute a \
                 WHERE a.attrelid = {}::oid AND a.attname IN ({}) AND NOT a.attisdropped \
                 ORDER BY array_position(ARRAY[{}]::text[], a.attname::text)",
                target_oid, cols_list, cols_list
            );
            let table = client.select(&ek_query, None, &[])
                .unwrap_or_else(|e| pgrx::error!("SPI error getting identity col types: {}", e));

            let mut select_parts = Vec::new();
            let mut join_parts = Vec::new();
            for row in table {
                let col: String = row.get::<String>(1).unwrap_or(None).unwrap_or_default();
                let col_type: String = row.get::<String>(2).unwrap_or(None).unwrap_or_default();
                let ek_alias = format!("{}_ek", col);
                select_parts.push(format!(
                    "(entity_keys->>'{col}')::{typ} AS {alias}",
                    col = col.replace('\'', "''"),
                    typ = col_type,
                    alias = qi(&ek_alias),
                ));
                join_parts.push(format!(
                    "t.{col} = p.{alias}",
                    col = qi(&col),
                    alias = qi(&ek_alias),
                ));
            }

            if select_parts.is_empty() {
                (String::new(), "true".to_string())
            } else {
                (select_parts.join(", "), join_parts.join(" AND "))
            }
        };

        // 9. The big column list CTE — run as a single SQL query
        // This is the most complex part: it mirrors lines 513-640 of the PL/pgSQL executor.
        let cte_query = build_column_list_cte_query(
            target_oid,
            &identity_columns,
            &lookup_columns,
            &pk_cols,
            &insert_defaulted_columns,
            &founding_defaulted_columns,
            &not_null_defaulted_cols,
            &range_col,
            &valid_from_col,
            &valid_until_col,
            &valid_to_col,
        );

        let cte_row = client
            .select(&cte_query, Some(1), &[])
            .unwrap_or_else(|e| pgrx::error!("SPI error in column list CTE: {}", e))
            .first();

        let update_set_clause: Option<String> = cte_row.get::<String>(1).unwrap_or(None);
        let all_cols_ident: Option<String> = cte_row.get::<String>(2).unwrap_or(None);
        let all_cols_select: Option<String> = cte_row.get::<String>(3).unwrap_or(None);
        let all_cols_from_jsonb: Option<String> = cte_row.get::<String>(4).unwrap_or(None);
        let founding_all_cols_ident: Option<String> = cte_row.get::<String>(5).unwrap_or(None);
        let founding_all_cols_from_jsonb: Option<String> = cte_row.get::<String>(6).unwrap_or(None);

        ExecutorCachedState {
            range_col,
            range_constructor,
            range_subtype,
            valid_from_col,
            valid_until_col,
            valid_to_col,
            valid_from_col_type,
            valid_until_col_type,
            pk_cols,
            not_null_defaulted_cols,
            insert_defaulted_columns,
            founding_defaulted_columns,
            source_col_names,
            update_set_clause,
            all_cols_ident,
            all_cols_select,
            all_cols_from_jsonb,
            founding_all_cols_ident,
            founding_all_cols_from_jsonb,
            entity_key_join_clause,
            entity_key_select_list,
            source_cols_hash,
        }
    })
}

/// Build the SQL query that replaces the column list CTE (lines 513-640).
/// This is executed once on cache miss; the results are cached.
fn build_column_list_cte_query(
    target_oid: u32,
    identity_columns: &[String],
    lookup_columns: &[String],
    pk_cols: &[String],
    insert_defaulted_columns: &[String],
    founding_defaulted_columns: &[String],
    not_null_defaulted_cols: &[String],
    range_col: &str,
    valid_from_col: &str,
    valid_until_col: &str,
    valid_to_col: &Option<String>,
) -> String {
    // Build SQL array literals for the various column lists
    let identity_arr = pg_text_array_literal(identity_columns);
    let lookup_arr = pg_text_array_literal(lookup_columns);
    let pk_arr = pg_text_array_literal(pk_cols);
    let insert_def_arr = pg_text_array_literal(insert_defaulted_columns);
    let founding_def_arr = pg_text_array_literal(founding_defaulted_columns);
    let nn_def_arr = pg_text_array_literal(not_null_defaulted_cols);

    let range_col_escaped = range_col.replace('\'', "''");
    let vf_escaped = valid_from_col.replace('\'', "''");
    let vu_escaped = valid_until_col.replace('\'', "''");
    let vt_escaped = valid_to_col.as_deref().unwrap_or("").replace('\'', "''");

    // Build the range_col filter, carefully handling IS DISTINCT FROM for NULL valid_to
    let range_filter = format!("t.attname <> '{}'", range_col_escaped);
    let vf_filter = if !valid_from_col.is_empty() {
        format!(" AND t.attname <> '{}'", vf_escaped)
    } else {
        String::new()
    };
    let vu_filter = if !valid_until_col.is_empty() {
        format!(" AND t.attname <> '{}'", vu_escaped)
    } else {
        String::new()
    };
    let vt_filter = if valid_to_col.is_some() {
        format!(" AND t.attname <> '{}'", vt_escaped)
    } else {
        String::new()
    };

    // The valid_from/valid_until exclusion for the column ident/select lists
    let vf_is_distinct = if !valid_from_col.is_empty() {
        format!("cfi.attname IS DISTINCT FROM '{}'", vf_escaped)
    } else {
        "true".to_string()
    };
    let vu_is_distinct = if !valid_until_col.is_empty() {
        format!("cfi.attname IS DISTINCT FROM '{}'", vu_escaped)
    } else {
        "true".to_string()
    };
    let cffi_vf_is_distinct = if !valid_from_col.is_empty() {
        format!("cffi.attname IS DISTINCT FROM '{}'", vf_escaped)
    } else {
        "true".to_string()
    };
    let cffi_vu_is_distinct = if !valid_until_col.is_empty() {
        format!("cffi.attname IS DISTINCT FROM '{}'", vu_escaped)
    } else {
        "true".to_string()
    };

    format!(
        r#"WITH target_cols AS (
            SELECT pa.attname::text, pa.atttypid, pa.attgenerated, pa.attidentity, pa.attnum, pa.atthasdef, pa.attnotnull
            FROM pg_catalog.pg_attribute pa
            WHERE pa.attrelid = {target_oid}::oid AND pa.attnum > 0 AND NOT pa.attisdropped
        ),
        common_data_cols AS (
            SELECT t.attname, t.atttypid, t.attnotnull, t.atthasdef, t.attnum
            FROM target_cols t
            LEFT JOIN pg_attrdef ad ON ad.adrelid = {target_oid}::oid AND ad.adnum = t.attnum
            WHERE {range_filter}{vf_filter}{vu_filter}{vt_filter}
              AND t.attname <> ALL({identity_arr}::text[])
              AND t.attname <> ALL({lookup_arr}::text[])
              AND t.attname <> ALL({pk_arr}::text[])
              AND t.attidentity <> 'a'
              AND t.attgenerated = ''
              AND COALESCE(pg_get_expr(ad.adbin, {target_oid}::oid), '') NOT ILIKE 'nextval(%'
        ),
        all_available_cols AS (
            SELECT c.attname, c.atttypid, c.attnotnull, c.atthasdef, c.attnum FROM common_data_cols c
            UNION
            SELECT u.attname, t.atttypid, t.attnotnull, t.atthasdef, t.attnum
            FROM unnest({lookup_arr}::text[]) u(attname)
            JOIN target_cols t ON u.attname = t.attname
            UNION
            SELECT u.attname, t.atttypid, t.attnotnull, t.atthasdef, t.attnum
            FROM unnest({identity_arr}::text[]) u(attname)
            JOIN target_cols t ON u.attname = t.attname
            UNION
            SELECT pk.attname, t.atttypid, t.attnotnull, t.atthasdef, t.attnum
            FROM unnest({pk_arr}::text[]) pk(attname)
            JOIN target_cols t ON pk.attname = t.attname
        ),
        cols_for_insert AS (
            SELECT attname, atttypid, attnotnull, atthasdef, attnum FROM all_available_cols WHERE attname <> ALL({insert_def_arr}::text[])
            UNION
            SELECT attname, atttypid, attnotnull, atthasdef, attnum FROM all_available_cols WHERE attname = ANY({identity_arr}::text[]) OR attname = ANY({lookup_arr}::text[]) OR attname = ANY({pk_arr}::text[])
        ),
        cols_for_founding_insert AS (
            SELECT attname, atttypid, attnotnull, atthasdef, attnum
            FROM all_available_cols
            WHERE attname <> ALL({founding_def_arr}::text[])
        )
        SELECT
            (SELECT string_agg(
                format(
                    '%1$I = CASE WHEN p.data ? %2$L THEN %4$s ELSE t.%1$I END',
                    cdc.attname,
                    cdc.attname,
                    format_type(cdc.atttypid, -1),
                    CASE
                        WHEN cdc.attname = ANY({nn_def_arr}::text[])
                        THEN format('COALESCE((p.data->>%1$L)::%2$s, t.%3$I)', cdc.attname, format_type(cdc.atttypid, -1), cdc.attname)
                        ELSE format('(p.data->>%1$L)::%2$s', cdc.attname, format_type(cdc.atttypid, -1))
                    END
                ),
            ', ') FROM common_data_cols cdc),
            (SELECT string_agg(format('%I', cfi.attname), ', ' ORDER BY cfi.attnum) FROM cols_for_insert cfi WHERE {vf_is_distinct} AND {vu_is_distinct}),
            (SELECT string_agg(
                CASE
                    WHEN cfi.attnotnull AND cfi.atthasdef THEN
                        format('COALESCE(jpr_all.%1$I, %2$s)',
                            cfi.attname,
                            pg_get_expr(ad.adbin, ad.adrelid)
                        )
                    ELSE
                        format('jpr_all.%I', cfi.attname)
                END,
                ', ' ORDER BY cfi.attnum
             )
             FROM cols_for_insert cfi
             LEFT JOIN pg_attrdef ad ON ad.adrelid = {target_oid}::oid
                                    AND ad.adnum = cfi.attnum
             WHERE {vf_is_distinct} AND {vu_is_distinct}),
            (SELECT string_agg(
                CASE
                    WHEN cfi.attnotnull AND cfi.atthasdef THEN
                        format('COALESCE((s.full_data->>%1$L)::%2$s, %3$s)',
                            cfi.attname,
                            format_type(cfi.atttypid, -1),
                            pg_get_expr(ad.adbin, ad.adrelid)
                        )
                    ELSE
                        format('(s.full_data->>%1$L)::%2$s', cfi.attname, format_type(cfi.atttypid, -1))
                END,
                ', ' ORDER BY cfi.attnum
             )
             FROM cols_for_insert cfi
             LEFT JOIN pg_attrdef ad ON ad.adrelid = {target_oid}::oid
                                    AND ad.adnum = cfi.attnum
             WHERE {vf_is_distinct} AND {vu_is_distinct}),
            (SELECT string_agg(format('%I', cffi.attname), ', ' ORDER BY cffi.attnum) FROM cols_for_founding_insert cffi WHERE {cffi_vf_is_distinct} AND {cffi_vu_is_distinct}),
            (SELECT string_agg(
                CASE
                    WHEN cffi.attnotnull AND cffi.atthasdef THEN
                        format('COALESCE((s.full_data->>%1$L)::%2$s, %3$s)',
                            cffi.attname,
                            format_type(cffi.atttypid, -1),
                            pg_get_expr(ad.adbin, ad.adrelid)
                        )
                    ELSE
                        format('(s.full_data->>%1$L)::%2$s', cffi.attname, format_type(cffi.atttypid, -1))
                END,
                ', ' ORDER BY cffi.attnum
             )
             FROM cols_for_founding_insert cffi
             LEFT JOIN pg_attrdef ad ON ad.adrelid = {target_oid}::oid
                                    AND ad.adnum = cffi.attnum
             WHERE {cffi_vf_is_distinct} AND {cffi_vu_is_distinct})"#,
        target_oid = target_oid,
        range_filter = range_filter,
        vf_filter = vf_filter,
        vu_filter = vu_filter,
        vt_filter = vt_filter,
        identity_arr = identity_arr,
        lookup_arr = lookup_arr,
        pk_arr = pk_arr,
        insert_def_arr = insert_def_arr,
        founding_def_arr = founding_def_arr,
        nn_def_arr = nn_def_arr,
        vf_is_distinct = vf_is_distinct,
        vu_is_distinct = vu_is_distinct,
        cffi_vf_is_distinct = cffi_vf_is_distinct,
        cffi_vu_is_distinct = cffi_vu_is_distinct,
    )
}

/// Format a Rust string slice as a PostgreSQL text[] array literal.
/// E.g., ["a", "b"] → "ARRAY['a','b']"
fn pg_text_array_literal(values: &[String]) -> String {
    if values.is_empty() {
        return "'{}'".to_string();
    }
    let items: Vec<String> = values.iter()
        .map(|v| format!("'{}'", v.replace('\'', "''")))
        .collect();
    format!("ARRAY[{}]", items.join(","))
}
