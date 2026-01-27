-- Large Scale Benchmark: 1.1M entities
-- Tests temporal_merge at production scale (1.1M legal units)
--
-- This benchmark runs a single massive load to verify O(n) scaling holds.
-- Expected time: ~6-8 minutes for 1.1M entities at ~3000 rows/sec
--
\i sql/include/test_setup.sql
\i sql/include/benchmark_setup.sql

--------------------------------------------------------------------------------
-- SETUP
--------------------------------------------------------------------------------
DROP SCHEMA IF EXISTS large_bench CASCADE;
CREATE SCHEMA large_bench;
GRANT ALL ON SCHEMA large_bench TO sql_saga_unprivileged_user;

SET ROLE TO sql_saga_unprivileged_user;

CREATE TABLE large_bench.legal_unit (
    id serial, 
    name text, 
    comment text, 
    valid_range daterange, 
    valid_from date, 
    valid_until date
);

SELECT sql_saga.add_era('large_bench.legal_unit', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(
    table_oid => 'large_bench.legal_unit'::regclass, 
    column_names => ARRAY['id'], 
    key_type => 'primary',
    unique_key_name => 'large_bench_legal_unit_pk');

CREATE TABLE large_bench.data_table (
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
CREATE INDEX ON large_bench.data_table (batch);
CREATE INDEX ON large_bench.data_table USING GIST (valid_range);

--------------------------------------------------------------------------------
\echo ''
\echo '================================================================================'
\echo 'LARGE SCALE BENCHMARK: 1.1M entities'
\echo '================================================================================'
\echo ''
--------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE large_bench.run_benchmark()
LANGUAGE plpgsql AS $proc$
DECLARE
    v_total_entities int := 1100000;
    v_batch_size int := 2000;
    v_num_batches int;
    v_batch int;
    v_start timestamptz;
    v_checkpoint timestamptz;
    v_total_ms numeric;
    v_last_report int := 0;
    v_checkpoint_entities int;
BEGIN
    v_num_batches := CEIL(v_total_entities::numeric / v_batch_size);
    
    RAISE NOTICE 'Generating % entities (% batches of %)...', 
        v_total_entities, v_num_batches, v_batch_size;
    
    INSERT INTO large_bench.data_table (row_id, batch, identity_correlation, comment, lu_name, valid_from, valid_until)
    SELECT row_number() OVER (), CEIL(n::numeric / v_batch_size), n, 
           format('E%s', n), format('C-%s', n),
           '2024-01-01'::date, 'infinity'
    FROM generate_series(1, v_total_entities) n;
    
    ANALYZE large_bench.data_table;
    ANALYZE large_bench.legal_unit;
    COMMIT;
    
    RAISE NOTICE 'Data generated. Starting temporal_merge processing...';
    RAISE NOTICE '';
    
    v_start := clock_timestamp();
    v_checkpoint := v_start;
    
    FOR v_batch IN 1..v_num_batches LOOP
        EXECUTE format($sql$
            CREATE OR REPLACE TEMP VIEW sv AS
            SELECT row_id, identity_correlation as founding_id, legal_unit_id AS id, 
                   lu_name AS name, comment, merge_statuses, merge_errors,
                   valid_from, valid_until, valid_range
            FROM large_bench.data_table WHERE batch = %L
        $sql$, v_batch);
        
        CALL sql_saga.temporal_merge(
            target_table => 'large_bench.legal_unit',
            source_table => 'sv',
            primary_identity_columns => ARRAY['id'],
            ephemeral_columns => ARRAY['comment'],
            mode => 'MERGE_ENTITY_PATCH',
            founding_id_column => 'founding_id',
            update_source_with_identity => true,
            update_source_with_feedback => false
        );
        
        COMMIT;
        
        -- Progress every 100 batches (200K entities)
        IF v_batch - v_last_report >= 100 THEN
            v_checkpoint_entities := v_batch * v_batch_size;
            RAISE NOTICE 'Progress: %/% batches (%K entities) - % rows/sec avg, % ms/batch',
                v_batch, v_num_batches, 
                v_checkpoint_entities / 1000,
                ROUND(v_checkpoint_entities / EXTRACT(EPOCH FROM clock_timestamp() - v_start)),
                ROUND(EXTRACT(EPOCH FROM clock_timestamp() - v_checkpoint) * 1000 / 100);
            v_checkpoint := clock_timestamp();
            v_last_report := v_batch;
        END IF;
    END LOOP;
    
    v_total_ms := EXTRACT(EPOCH FROM clock_timestamp() - v_start) * 1000;
    
    RAISE NOTICE '';
    RAISE NOTICE '================================================================================';
    RAISE NOTICE 'COMPLETE: % entities in % seconds', v_total_entities, ROUND(v_total_ms / 1000, 1);
    RAISE NOTICE 'Throughput: % rows/sec, % ms/batch avg', 
        ROUND(v_total_entities / (v_total_ms / 1000.0)),
        ROUND(v_total_ms / v_num_batches);
    RAISE NOTICE '================================================================================';
END;
$proc$;

CALL large_bench.run_benchmark();

SELECT COUNT(*) as final_count FROM large_bench.legal_unit;

--------------------------------------------------------------------------------
-- CLEANUP
--------------------------------------------------------------------------------
RESET ROLE;
DROP SCHEMA large_bench CASCADE;

\i sql/include/benchmark_teardown.sql
\i sql/include/test_teardown.sql
