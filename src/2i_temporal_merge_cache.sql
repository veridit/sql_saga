-- Helper functions for the temporal_merge two-level cache system.
-- L1 cache: pg_temp.temporal_merge_plan_cache (session-level, transaction-scoped)
-- L2 cache: sql_saga.temporal_merge_cache (persistent, cross-transaction)

-- Computes a hash of the relevant source table columns for cache compatibility validation.
-- Only includes columns that are actually used in the merge (lookup columns, temporal columns, data columns).
CREATE OR REPLACE FUNCTION sql_saga.temporal_merge_source_columns_hash(
    p_source_table regclass,
    p_lookup_columns text[],
    p_temporal_columns text[],
    p_row_id_column text,
    p_founding_id_column text DEFAULT NULL
) RETURNS TEXT
LANGUAGE sql STABLE AS $function$
    SELECT md5(string_agg(col_sig, ',' ORDER BY col_sig))
    FROM (
        SELECT a.attname || '::' || format_type(a.atttypid, a.atttypmod) AS col_sig
        FROM pg_attribute a
        WHERE a.attrelid = p_source_table
          AND a.attnum > 0
          AND NOT a.attisdropped
          -- Include all columns that affect the generated SQL
          AND (
              a.attname = ANY(p_lookup_columns)
              OR a.attname = ANY(p_temporal_columns)
              OR a.attname = p_row_id_column
              OR a.attname = p_founding_id_column
              -- Include all data columns (everything else)
              OR a.attname NOT IN (SELECT unnest(COALESCE(p_lookup_columns, '{}') || COALESCE(p_temporal_columns, '{}')))
          )
    ) cols;
$function$;

COMMENT ON FUNCTION sql_saga.temporal_merge_source_columns_hash IS
'Computes an MD5 hash of the source table column signatures (name::type) for cache validation.
Used to verify that a cached SQL plan is compatible with the current source table schema.';


-- Generates a schema-based cache key for temporal_merge (no OIDs).
-- This key is stable across sessions and can be used for the persistent L2 cache.
CREATE OR REPLACE FUNCTION sql_saga.temporal_merge_cache_key(
    p_target_table regclass,
    p_identity_columns text[],
    p_ephemeral_columns text[],
    p_mode sql_saga.temporal_merge_mode,
    p_era_name name,
    p_row_id_column text,
    p_founding_id_column text,
    p_range_constructor regtype,
    p_delete_mode sql_saga.temporal_merge_delete_mode,
    p_lookup_keys jsonb,
    p_log_trace boolean
) RETURNS TEXT
LANGUAGE sql STABLE AS $function$
    SELECT format('%s:%s:%s:%s:%s:%s:%s:%s:%s:%s:%s',
        p_target_table::regclass::text,  -- schema.table name, not OID
        COALESCE(array_to_string(p_identity_columns, ','), ''),
        COALESCE(array_to_string(p_ephemeral_columns, ','), ''),
        p_mode::text,
        p_era_name,
        p_row_id_column,
        COALESCE(p_founding_id_column, ''),
        p_range_constructor::text,
        p_delete_mode::text,
        COALESCE(p_lookup_keys::text, '[]'),
        p_log_trace::text
    );
$function$;

COMMENT ON FUNCTION sql_saga.temporal_merge_cache_key IS
'Generates a stable, OID-free cache key for temporal_merge plans.
Uses schema.table names instead of OIDs for cross-session stability.
Does not include source_table since that is parameterized in the cached SQL.';


-- Looks up a cached plan from the L2 persistent cache.
-- Returns the plan_sqls if found and the source columns hash matches, NULL otherwise.
CREATE OR REPLACE FUNCTION sql_saga.temporal_merge_cache_lookup(
    p_cache_key TEXT,
    p_source_columns_hash TEXT
) RETURNS JSONB
LANGUAGE plpgsql AS $function$
DECLARE
    v_plan_sqls JSONB;
    v_cached_hash TEXT;
BEGIN
    SELECT tmc.plan_sqls, tmc.source_columns_hash
    INTO v_plan_sqls, v_cached_hash
    FROM sql_saga.temporal_merge_cache tmc
    WHERE tmc.cache_key = p_cache_key;

    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    -- Validate source columns hash matches
    IF v_cached_hash <> p_source_columns_hash THEN
        -- Source schema changed, invalidate this cache entry
        DELETE FROM sql_saga.temporal_merge_cache WHERE cache_key = p_cache_key;
        RETURN NULL;
    END IF;

    -- Update usage statistics
    UPDATE sql_saga.temporal_merge_cache
    SET last_used_at = now(),
        use_count = use_count + 1
    WHERE cache_key = p_cache_key;

    RETURN v_plan_sqls;
END;
$function$;

COMMENT ON FUNCTION sql_saga.temporal_merge_cache_lookup IS
'Looks up a cached plan from the L2 persistent cache. Returns NULL if not found or if the
source columns hash does not match (indicating schema change). Updates usage statistics on hit.';


-- OPT-6: SECURITY DEFINER function for bounded cache growth with LRU eviction.
-- This function can delete any cache entry regardless of who created it,
-- ensuring the cache doesn't grow unboundedly even in multi-user environments.
CREATE OR REPLACE FUNCTION sql_saga.temporal_merge_cache_maybe_purge(
    p_max_entries INT DEFAULT 1000,
    p_purge_probability FLOAT DEFAULT 0.02,
    p_max_age_days INT DEFAULT 30
) RETURNS INT
LANGUAGE plpgsql SECURITY DEFINER AS $function$
DECLARE
    v_current_count INT;
    v_deleted_count INT := 0;
    v_excess INT;
BEGIN
    -- Amortized cost: only run purge logic probabilistically
    IF random() > p_purge_probability THEN
        RETURN 0;
    END IF;
    
    -- Get current cache size
    SELECT count(*) INTO v_current_count FROM sql_saga.temporal_merge_cache;
    
    -- Phase 1: Remove entries older than max_age_days (regardless of count)
    DELETE FROM sql_saga.temporal_merge_cache
    WHERE last_used_at < now() - (p_max_age_days || ' days')::interval;
    GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
    
    -- Phase 2: If still over limit, apply LRU eviction
    IF v_current_count - v_deleted_count > p_max_entries THEN
        v_excess := (v_current_count - v_deleted_count) - p_max_entries;
        -- Delete the least recently used entries to get back under the limit
        -- Add 10% buffer to avoid thrashing
        v_excess := v_excess + (p_max_entries / 10);
        
        DELETE FROM sql_saga.temporal_merge_cache
        WHERE cache_key IN (
            SELECT cache_key 
            FROM sql_saga.temporal_merge_cache
            ORDER BY last_used_at ASC, use_count ASC
            LIMIT v_excess
        );
        GET DIAGNOSTICS v_excess = ROW_COUNT;
        v_deleted_count := v_deleted_count + v_excess;
    END IF;
    
    IF v_deleted_count > 0 THEN
        RAISE DEBUG 'sql_saga: Purged % stale/excess entries from temporal_merge cache', v_deleted_count;
    END IF;
    
    RETURN v_deleted_count;
END;
$function$;

COMMENT ON FUNCTION sql_saga.temporal_merge_cache_maybe_purge IS
'Probabilistically purges old or excess entries from the L2 cache using SECURITY DEFINER
to bypass ownership restrictions. Called automatically during cache_store operations.

Parameters:
- p_max_entries: Maximum cache entries before LRU eviction kicks in (default: 1000)
- p_purge_probability: Chance of running purge on each call (default: 0.02 = 2%)
- p_max_age_days: Entries not used in this many days are always purged (default: 30)

The amortized cost ensures purge logic runs infrequently while guaranteeing cleanup.';


-- Stores a plan in the L2 persistent cache.
CREATE OR REPLACE PROCEDURE sql_saga.temporal_merge_cache_store(
    p_cache_key TEXT,
    p_source_columns_hash TEXT,
    p_plan_sqls JSONB
)
LANGUAGE plpgsql AS $procedure$
BEGIN
    INSERT INTO sql_saga.temporal_merge_cache (cache_key, source_columns_hash, plan_sqls)
    VALUES (p_cache_key, p_source_columns_hash, p_plan_sqls)
    ON CONFLICT (cache_key) DO UPDATE SET
        source_columns_hash = EXCLUDED.source_columns_hash,
        plan_sqls = EXCLUDED.plan_sqls,
        created_at = now(),
        last_used_at = now(),
        use_count = 1;
    
    -- OPT-6: Amortized cache cleanup - probabilistically purge old/excess entries
    PERFORM sql_saga.temporal_merge_cache_maybe_purge();
END;
$procedure$;

COMMENT ON PROCEDURE sql_saga.temporal_merge_cache_store IS
'Stores a plan in the L2 persistent cache. Uses upsert to handle concurrent access.
Automatically triggers amortized cache cleanup via temporal_merge_cache_maybe_purge().';


-- Invalidates cache entries for a specific target table.
CREATE OR REPLACE PROCEDURE sql_saga.temporal_merge_invalidate_cache(
    p_target_table regclass DEFAULT NULL
)
LANGUAGE plpgsql AS $procedure$
BEGIN
    IF p_target_table IS NULL THEN
        -- Clear entire cache
        DELETE FROM sql_saga.temporal_merge_cache;
        RAISE NOTICE 'sql_saga: Cleared entire temporal_merge cache';
    ELSE
        -- Clear entries for specific table
        DELETE FROM sql_saga.temporal_merge_cache
        WHERE cache_key LIKE p_target_table::regclass::text || ':%';
        RAISE NOTICE 'sql_saga: Invalidated temporal_merge cache for table %', p_target_table::regclass::text;
    END IF;
END;
$procedure$;

COMMENT ON PROCEDURE sql_saga.temporal_merge_invalidate_cache IS
'Invalidates cache entries for a specific target table, or clears the entire cache if no table specified.
Call this after schema changes to the target table (ALTER TABLE ADD/DROP/ALTER COLUMN).';


-- Returns cache statistics for monitoring.
CREATE OR REPLACE FUNCTION sql_saga.temporal_merge_cache_stats()
RETURNS TABLE (
    total_entries BIGINT,
    total_size_bytes BIGINT,
    oldest_entry TIMESTAMPTZ,
    newest_entry TIMESTAMPTZ,
    most_used_count INT,
    least_used_count INT
)
LANGUAGE sql STABLE AS $function$
    SELECT
        count(*),
        COALESCE(sum(pg_column_size(plan_sqls)), 0),
        min(created_at),
        max(created_at),
        max(use_count),
        min(use_count)
    FROM sql_saga.temporal_merge_cache;
$function$;

COMMENT ON FUNCTION sql_saga.temporal_merge_cache_stats IS
'Returns statistics about the temporal_merge L2 cache for monitoring and maintenance.';


-- Event trigger function for automatic cache invalidation on ALTER TABLE / DROP TABLE.
-- Only invalidates cache for tables that are registered with sql_saga eras.
CREATE OR REPLACE FUNCTION sql_saga.temporal_merge_cache_invalidation()
RETURNS event_trigger
LANGUAGE plpgsql AS $function$
DECLARE
    r RECORD;
    v_table_name TEXT;
    v_deleted_count INT;
BEGIN
    -- Get the affected objects from the DDL command
    FOR r IN SELECT * FROM pg_event_trigger_ddl_commands()
    LOOP
        -- Only process table alterations for tables that have eras
        IF r.object_type = 'table' THEN
            v_table_name := r.object_identity;
            
            -- Check if this table has an era (i.e., is managed by sql_saga)
            IF EXISTS (
                SELECT 1 FROM sql_saga.era e
                WHERE format('%I.%I', e.table_schema, e.table_name) = v_table_name
            ) THEN
                -- Delete cache entries for this table
                DELETE FROM sql_saga.temporal_merge_cache
                WHERE cache_key LIKE v_table_name || ':%';
                
                GET DIAGNOSTICS v_deleted_count = ROW_COUNT;
                
                IF v_deleted_count > 0 THEN
                    RAISE DEBUG 'sql_saga: Invalidated % temporal_merge cache entries for altered table %', 
                        v_deleted_count, v_table_name;
                END IF;
            END IF;
        END IF;
    END LOOP;
END;
$function$;

COMMENT ON FUNCTION sql_saga.temporal_merge_cache_invalidation IS
'Event trigger function that automatically invalidates temporal_merge L2 cache entries
when a table with an era is altered or dropped. This ensures cached plans stay in sync
with table schema changes.';
