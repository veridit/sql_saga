-- Scaling Analysis Benchmark
-- Tests O(n) linear scaling behavior of temporal_merge
--
-- This benchmark detects performance regressions that cause superlinear scaling.
-- With proper O(n) scaling, doubling entities should approximately double time.
-- 
-- Key metrics:
--   - efficiency_ratio: ms_per_entity change when scaling (target: ~1.0)
--   - time_ratio: total time change when doubling entities (target: ~2.0)
--   - STABLE means linear O(n) scaling
--   - DEGRADING indicates O(n^2) or worse behavior
--
-- Prerequisites:
--   - max_locks_per_transaction >= 256 (for 128K entities / 64 batches)
--   - Default PostgreSQL is 64, which limits to ~32K entities
\i sql/include/test_setup.sql
\i sql/include/benchmark_setup.sql

--------------------------------------------------------------------------------
-- SETUP: Create schema as superuser, then switch to unprivileged user
--------------------------------------------------------------------------------
DROP SCHEMA IF EXISTS scaling_bench CASCADE;
CREATE SCHEMA scaling_bench;
GRANT ALL ON SCHEMA scaling_bench TO sql_saga_unprivileged_user;

SET ROLE TO sql_saga_unprivileged_user;

-- Target table: temporal legal units
CREATE TABLE scaling_bench.legal_unit (
    id serial, 
    name text, 
    comment text, 
    valid_range daterange, 
    valid_from date, 
    valid_until date
);

SELECT sql_saga.add_era('scaling_bench.legal_unit', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(
    table_oid => 'scaling_bench.legal_unit'::regclass, 
    column_names => ARRAY['id'], 
    key_type => 'primary', 
    unique_key_name => 'scaling_bench_legal_unit_id_valid'
);
CREATE INDEX ON scaling_bench.legal_unit USING GIST (valid_range) WITH (fillfactor = 90);

-- Source staging table
-- NOTE: Includes generated valid_range column with GIST index, as callers should
-- prepare their staging tables for optimal temporal_merge performance.
CREATE TABLE scaling_bench.data_table (
    row_id serial primary key,
    batch int not null,
    identity_correlation int not null,
    legal_unit_id int,
    merge_statuses jsonb,
    merge_errors jsonb,
    comment text,
    lu_name text,
    valid_from date not null,
    valid_until date not null,
    valid_range daterange GENERATED ALWAYS AS (daterange(valid_from, valid_until)) STORED
);
CREATE INDEX ON scaling_bench.data_table (batch, identity_correlation);
CREATE INDEX ON scaling_bench.data_table (identity_correlation, batch, row_id);
CREATE INDEX ON scaling_bench.data_table USING GIST (valid_range);

-- Identity resolution tracking
CREATE TABLE scaling_bench.identity_resolution (
    identity_correlation int PRIMARY KEY,
    legal_unit_id int,
    resolved_batch int NOT NULL
);

--------------------------------------------------------------------------------
-- HELPER: Data generator with configurable batch size
--------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION scaling_bench.generate_test_data(
    p_total_entities int,
    p_entities_per_batch int
) RETURNS int LANGUAGE plpgsql AS $function$
DECLARE
    v_num_batches int;
BEGIN
    v_num_batches := CEIL(p_total_entities::numeric / p_entities_per_batch);
    
    TRUNCATE scaling_bench.data_table, scaling_bench.identity_resolution;
    
    INSERT INTO scaling_bench.data_table (
        row_id, batch, identity_correlation, comment, lu_name, valid_from, valid_until
    )
    SELECT 
        row_number() OVER () as row_id,
        CEIL(entity_num::numeric / p_entities_per_batch) as batch,
        entity_num as identity_correlation,
        format('Entity %s', entity_num) as comment,
        format('Company-%s', entity_num) as lu_name,
        ('2024-01-01'::date + ((CEIL(entity_num::numeric / p_entities_per_batch) - 1) * 30)::int) as valid_from,
        'infinity'::date as valid_until
    FROM generate_series(1, p_total_entities) as entity_num;
    
    ANALYZE scaling_bench.data_table;
    RETURN v_num_batches;
END;
$function$;

--------------------------------------------------------------------------------
-- HELPER: Process a single batch through temporal_merge
--------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE scaling_bench.process_batch(p_batch_id int)
LANGUAGE plpgsql AS $procedure$
BEGIN
    EXECUTE format($$
        CREATE OR REPLACE TEMP VIEW source_view_lu AS
        SELECT
            row_id,
            identity_correlation as founding_id,
            legal_unit_id AS id,
            lu_name AS name,
            comment,
            merge_statuses,
            merge_errors,
            valid_from,
            valid_until,
            valid_range
        FROM scaling_bench.data_table WHERE batch = %1$L AND lu_name IS NOT NULL;
    $$, p_batch_id);

    CALL sql_saga.temporal_merge(
        target_table => 'scaling_bench.legal_unit',
        source_table => 'source_view_lu',
        primary_identity_columns => ARRAY['id'],
        ephemeral_columns => ARRAY['comment'],
        mode => 'MERGE_ENTITY_PATCH',
        founding_id_column => 'founding_id',
        update_source_with_identity => true,
        update_source_with_feedback => false
    );

    -- Track identity resolution across batches
    INSERT INTO scaling_bench.identity_resolution (identity_correlation, legal_unit_id, resolved_batch)
    SELECT DISTINCT identity_correlation, legal_unit_id, p_batch_id
    FROM scaling_bench.data_table
    WHERE batch = p_batch_id AND legal_unit_id IS NOT NULL
    ON CONFLICT (identity_correlation) DO UPDATE SET
        legal_unit_id = EXCLUDED.legal_unit_id,
        resolved_batch = EXCLUDED.resolved_batch;

    -- Propagate resolved IDs to future batches
    UPDATE scaling_bench.data_table AS dt
    SET legal_unit_id = ir.legal_unit_id
    FROM scaling_bench.identity_resolution AS ir
    WHERE dt.identity_correlation = ir.identity_correlation
      AND dt.legal_unit_id IS NULL;
END;
$procedure$;

--------------------------------------------------------------------------------
\echo ''
\echo '================================================================================'
\echo 'SCALING BENCHMARK: O(n) Linear Scaling Analysis'
\echo '================================================================================'
\echo ''
\echo 'Testing temporal_merge with fixed batch size (2000 entities/batch).'
\echo 'Doubling total entities doubles batch count - time should scale linearly.'
\echo ''
--------------------------------------------------------------------------------

-- Results collection table
CREATE TEMP TABLE scaling_results (
    entities int,
    batch_size int,
    num_batches int,
    batch1_ms numeric,
    avg_batch_ms numeric,
    total_ms numeric,
    ms_per_entity numeric
);

-- Run scaling tests
-- Scale sizes chosen to double each step for clear ratio analysis
-- 64K requires max_locks_per_transaction >= 256
DO $$
DECLARE
    v_entities int;
    v_batch_size int := 2000;  -- Fixed batch size for consistent comparison
    v_num_batches int;
    v_batch_id int;
    v_start timestamptz;
    v_batch_start timestamptz;
    v_batch1_ms numeric;
    v_batch_ms numeric;
    v_total_batch_ms numeric := 0;
    v_total_ms numeric;
    v_scale_sizes int[] := ARRAY[2000, 4000, 8000, 16000, 32000, 64000, 128000];
    v_max_locks int;
BEGIN
    -- Check max_locks_per_transaction for guidance
    SELECT setting::int INTO v_max_locks 
    FROM pg_settings WHERE name = 'max_locks_per_transaction';
    
    IF v_max_locks < 256 THEN
        RAISE NOTICE 'NOTE: max_locks_per_transaction = %. For 128K entities, increase to 512.', v_max_locks;
    END IF;

    FOREACH v_entities IN ARRAY v_scale_sizes LOOP
        -- Reset tables
        TRUNCATE scaling_bench.data_table, scaling_bench.identity_resolution RESTART IDENTITY CASCADE;
        DELETE FROM scaling_bench.legal_unit;
        ALTER SEQUENCE scaling_bench.legal_unit_id_seq RESTART WITH 1;
        
        -- Generate data
        v_num_batches := scaling_bench.generate_test_data(v_entities, v_batch_size);
        
        ANALYZE scaling_bench.data_table;
        ANALYZE scaling_bench.legal_unit;
        
        v_start := clock_timestamp();
        v_total_batch_ms := 0;
        
        -- Process all batches (quiet mode - no per-batch output for deterministic test results)
        FOR v_batch_id IN 1..v_num_batches LOOP
            v_batch_start := clock_timestamp();
            CALL scaling_bench.process_batch(v_batch_id);
            v_batch_ms := EXTRACT(EPOCH FROM clock_timestamp() - v_batch_start) * 1000;
            v_total_batch_ms := v_total_batch_ms + v_batch_ms;
            
            IF v_batch_id = 1 THEN
                v_batch1_ms := v_batch_ms;
            END IF;
        END LOOP;
        
        v_total_ms := EXTRACT(EPOCH FROM clock_timestamp() - v_start) * 1000;
        
        INSERT INTO scaling_results VALUES (
            v_entities, 
            v_batch_size, 
            v_num_batches, 
            v_batch1_ms, 
            v_total_batch_ms / v_num_batches,
            v_total_ms,
            v_total_ms / v_entities
        );
    END LOOP;
    
    RAISE NOTICE 'Scaling benchmark completed. See results tables below.';
END;
$$;

--------------------------------------------------------------------------------
\echo ''
\echo '================================================================================'
\echo 'SCALING ANALYSIS: O(n) Detection (Deterministic Output)'
\echo '================================================================================'
\echo ''
\echo 'This test checks that temporal_merge maintains linear O(n) scaling.'
\echo 'STABLE = Good linear scaling (efficiency_ratio < 1.5)'
\echo 'DEGRADING = Superlinear scaling detected (regression!)'
\echo ''
--------------------------------------------------------------------------------

-- Output ONLY the deterministic scaling quality assessment
-- Actual timing numbers vary by machine/run and are logged separately
WITH prev AS (
    SELECT 
        entities,
        num_batches,
        total_ms,
        ms_per_entity,
        LAG(entities) OVER (ORDER BY entities) as prev_entities,
        LAG(num_batches) OVER (ORDER BY entities) as prev_batches,
        LAG(total_ms) OVER (ORDER BY entities) as prev_total_ms,
        LAG(ms_per_entity) OVER (ORDER BY entities) as prev_ms_per_entity
    FROM scaling_results
)
SELECT 
    prev_entities || ' -> ' || entities as scale,
    CASE 
        -- Only flag significant degradation (>1.5x efficiency loss)
        -- Minor fluctuations (1.0-1.5x) are normal due to timing noise
        WHEN ms_per_entity / prev_ms_per_entity > 1.5 THEN 'DEGRADING'
        ELSE 'STABLE'
    END as scaling_quality
FROM prev
WHERE prev_entities IS NOT NULL
ORDER BY entities;

\echo ''
\echo 'If any row shows DEGRADING, there is a performance regression!'
\echo 'Run with benchmark category to see full timing details.'
\echo ''

--------------------------------------------------------------------------------
-- CLEANUP
--------------------------------------------------------------------------------
RESET ROLE;
DROP SCHEMA scaling_bench CASCADE;

\i sql/include/benchmark_teardown.sql
\i sql/include/test_teardown.sql
