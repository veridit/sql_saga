-- Native Rust implementation of the temporal_merge planner.
-- This file registers the Rust shared library function and provides a GUC toggle.

-- Register the native planner function.
-- The function is implemented in the sql_saga_native shared library (pgrx).
-- It takes the same parameters as sql_saga.temporal_merge_plan() but inserts
-- directly into pg_temp.temporal_merge_plan and returns a count.
CREATE OR REPLACE FUNCTION sql_saga.temporal_merge_plan_native(
    target_table oid,
    source_table oid,
    mode TEXT,
    era_name TEXT,
    identity_columns TEXT[] DEFAULT NULL,
    row_id_column TEXT DEFAULT 'row_id',
    founding_id_column TEXT DEFAULT NULL,
    delete_mode TEXT DEFAULT 'NONE',
    lookup_keys JSONB DEFAULT NULL,
    ephemeral_columns TEXT[] DEFAULT NULL,
    p_log_trace BOOLEAN DEFAULT false,
    p_log_sql BOOLEAN DEFAULT false
) RETURNS BIGINT
LANGUAGE c VOLATILE
AS 'sql_saga_native', 'temporal_merge_plan_native_wrapper';

COMMENT ON FUNCTION sql_saga.temporal_merge_plan_native IS
'Native Rust implementation of the temporal_merge planner (default). Drop-in replacement for
sql_saga.temporal_merge_plan(). 10-13x faster with O(1) scaling. Produces the same output
by inserting directly into pg_temp.temporal_merge_plan.
To fall back to PL/pgSQL: SET sql_saga.temporal_merge.use_plpgsql_planner = true;';

-- Composite type for executor introspection cache results.
-- Used as the return type of temporal_merge_executor_introspect().
DO $$ BEGIN
    CREATE TYPE sql_saga.temporal_merge_executor_cache AS (
        -- Era metadata (all text for Rust String compatibility)
        range_col text,
        range_constructor text,
        range_subtype text,
        valid_from_col text,
        valid_until_col text,
        valid_to_col text,
        valid_from_col_type text,
        valid_until_col_type text,
        -- Column info
        pk_cols text[],
        not_null_defaulted_cols text[],
        insert_defaulted_columns text[],
        founding_defaulted_columns text[],
        source_col_names text[],
        -- SQL fragments for DML
        update_set_clause text,
        all_cols_ident text,
        all_cols_select text,
        all_cols_from_jsonb text,
        founding_all_cols_ident text,
        founding_all_cols_from_jsonb text,
        entity_key_join_clause text,
        entity_key_select_list text
    );
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- Native executor introspection cache function.
-- Returns all metadata and SQL fragments needed by the executor, cached per-connection.
CREATE OR REPLACE FUNCTION sql_saga.temporal_merge_executor_introspect(
    target_table oid,
    source_table oid,
    identity_columns TEXT[] DEFAULT NULL,
    lookup_columns TEXT[] DEFAULT NULL,
    era_name TEXT DEFAULT 'valid',
    ephemeral_columns TEXT[] DEFAULT NULL,
    founding_id_column TEXT DEFAULT NULL,
    row_id_column TEXT DEFAULT 'row_id'
) RETURNS sql_saga.temporal_merge_executor_cache
LANGUAGE c VOLATILE
AS 'sql_saga_native', 'temporal_merge_executor_introspect_wrapper';

COMMENT ON FUNCTION sql_saga.temporal_merge_executor_introspect IS
'Native Rust executor introspection cache. Returns all metadata and SQL fragments
needed by temporal_merge_execute(), cached per-connection with thread_local storage.
On cache hit, returns immediately with zero SPI calls.';

-- Cache observability: returns per-connection stats for the native planner cache.
CREATE OR REPLACE FUNCTION sql_saga.temporal_merge_native_cache_stats(
    OUT stat_name TEXT,
    OUT stat_value BIGINT
) RETURNS SETOF RECORD
LANGUAGE c VOLATILE
AS 'sql_saga_native', 'temporal_merge_native_cache_stats_wrapper';

COMMENT ON FUNCTION sql_saga.temporal_merge_native_cache_stats IS
'Returns per-connection cache statistics for the native Rust temporal_merge planner and executor.
Stats: planner_cache_entries, target_read_stmts, source_read_stmts, cache_hits, cache_misses,
executor_cache_entries, executor_cache_hits, executor_cache_misses.';

-- Cache reset: clears all per-connection caches and counters.
CREATE OR REPLACE FUNCTION sql_saga.temporal_merge_native_cache_reset()
RETURNS VOID
LANGUAGE c VOLATILE
AS 'sql_saga_native', 'temporal_merge_native_cache_reset_wrapper';

COMMENT ON FUNCTION sql_saga.temporal_merge_native_cache_reset IS
'Clears all per-connection native planner and executor caches (planner cache, executor cache,
prepared statements) and resets hit/miss counters to zero.';
