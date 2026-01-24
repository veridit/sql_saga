\i sql/include/test_setup.sql
\i sql/include/benchmark_setup.sql
\i sql/include/benchmark_fixture_system_simple.sql
\i sql/include/benchmark_fixture_integration.sql

SET ROLE TO sql_saga_unprivileged_user;

--------------------------------------------------------------------------------
-- BENCHMARK: temporal_merge Performance with Fixture System
--
-- This benchmark demonstrates the new fixture system for temporal_merge performance testing.
-- It generates fixtures once and reuses them across multiple test runs for consistent results
-- and dramatically reduced test execution time.
--
-- Workflow:
-- 1. Ensure fixtures exist (auto-generate if missing)
-- 2. Load fixtures instantly from disk (2-3 seconds vs 25+ seconds generation)
-- 3. Run benchmarks on pre-loaded data
-- 4. Compare performance with different dataset sizes
--------------------------------------------------------------------------------

\echo '--- Fixture System Demonstration ---'

-- Show current fixture status
\echo 'Current fixtures:'
SELECT * FROM sql_saga_fixtures.list_fixtures();

-- Test fixture availability for different scales
\echo '--- Ensuring Fixtures for Multiple Scales ---'

-- Ensure fixtures exist for 1K entities (fast for demo)
SELECT benchmark_ensure_temporal_merge_fixture('1K', 'basic', true) as fixture_1k_ready;

-- Ensure fixtures exist for 10K entities (production scale)  
SELECT benchmark_ensure_temporal_merge_fixture('10K', 'basic', true) as fixture_10k_ready;

-- Show updated fixture list
\echo 'Fixtures after auto-generation:'
SELECT fixture_name, fixture_type, entities, total_rows, 
       ROUND(generation_time_sec, 2) as gen_time_sec
FROM sql_saga_fixtures.list_fixtures()
WHERE fixture_name LIKE 'temporal_merge_%'
ORDER BY entities;

--------------------------------------------------------------------------------
\echo '--- Performance Comparison: Generation vs Loading ---'
--------------------------------------------------------------------------------

-- Compare generation vs loading performance for 1K dataset
\echo 'Comparing generation vs loading performance (1K entities):'
SELECT * FROM benchmark_compare_generation_vs_loading('1K', 'temporal_merge');

--------------------------------------------------------------------------------
\echo '--- Benchmark Test with 1K Entities (using fixtures) ---'
--------------------------------------------------------------------------------

-- Create target tables for benchmark
CREATE TABLE legal_unit_tm (
  id INTEGER NOT NULL,
  valid_range daterange NOT NULL,
  name varchar NOT NULL
);

CREATE TABLE establishment_tm (
  id INTEGER NOT NULL,
  valid_range daterange NOT NULL,
  legal_unit_id INTEGER NOT NULL,
  postal_place TEXT NOT NULL
);

-- Enable sql_saga constraints (creates optimal GIST index for FK via create_index => true)
SELECT sql_saga.add_era('legal_unit_tm'::regclass, 'valid_range', 'valid');
SELECT sql_saga.add_era('establishment_tm'::regclass, 'valid_range', 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit_tm', column_names => ARRAY['id'], era_name => 'valid', unique_key_name => 'legal_unit_tm_id_valid');
SELECT sql_saga.add_unique_key(table_oid => 'establishment_tm', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'establishment_tm',
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'legal_unit_tm',
    pk_column_names => ARRAY['id'],
    fk_era_name => 'valid',
    create_index => true
);

-- Additional indices recommended by temporal_merge for source/target lookup
CREATE INDEX ON legal_unit_tm USING BTREE (id);
CREATE INDEX ON legal_unit_tm USING GIST (valid_range);
CREATE INDEX ON establishment_tm USING BTREE (id);
CREATE INDEX ON establishment_tm USING GIST (valid_range);

-- Show indices for documentation
\echo '--- Table Indices ---'
SELECT indexname, indexdef FROM pg_indexes WHERE tablename IN ('legal_unit_tm', 'establishment_tm') ORDER BY tablename, indexname;

-- Load 1K fixture data instantly
\echo '--- Loading 1K Fixture Data ---'
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Fixture Load 1K', 0, false);
CALL sql_saga.benchmark_reset();
SELECT benchmark_load_temporal_merge_fixture('1K', 'basic', 'public');
CALL sql_saga.benchmark_log_and_reset('Fixture Load 1K');
INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Fixture Load 1K end', 2000, true);

-- Verify loaded data
SELECT 'legal_unit_tm' AS table_name, COUNT(*) AS row_count FROM legal_unit_tm
UNION ALL
SELECT 'establishment_tm' AS table_name, COUNT(*) AS row_count FROM establishment_tm;

--------------------------------------------------------------------------------
\echo '--- Temporal Merge SEED Operations ---'
--------------------------------------------------------------------------------

-- Now run temporal_merge operations using the loaded data as targets
BEGIN;
  -- Create source data for 80% UPDATE + 20% INSERT pattern
  CREATE TEMPORARY TABLE legal_unit_source (row_id int, id int, valid_range daterange, name varchar);
  CREATE TEMPORARY TABLE establishment_source (row_id int, id int, valid_range daterange, legal_unit_id int, postal_place text);

  -- Source data for 80% PATCH (update existing 1-800)
  INSERT INTO legal_unit_source SELECT i, i, daterange('2015-01-01', 'infinity', '[)'), 'Updated Company ' || i FROM generate_series(1, 800) AS i;
  INSERT INTO establishment_source SELECT i, i, daterange('2015-01-01', 'infinity', '[)'), i, 'Updated Shop ' || i FROM generate_series(1, 800) AS i;

  -- Source data for 20% INSERT (new entities 801-1000)
  INSERT INTO legal_unit_source SELECT i, i, daterange('2015-01-01', 'infinity', '[)'), 'New Company ' || i FROM generate_series(801, 1000) AS i;
  INSERT INTO establishment_source SELECT i, i, daterange('2015-01-01', 'infinity', '[)'), i, 'New Shop ' || i FROM generate_series(801, 1000) AS i;

  CREATE INDEX ON legal_unit_source USING BTREE (id);
  CREATE INDEX ON legal_unit_source USING GIST (valid_range);
  CREATE INDEX ON establishment_source USING BTREE (id);
  CREATE INDEX ON establishment_source USING GIST (valid_range);

  ANALYZE legal_unit_source;
  ANALYZE establishment_source;

  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge 80% UPDATE 20% INSERT Parent', 0, false);
  CALL sql_saga.benchmark_reset();
  CALL sql_saga.temporal_merge(target_table => 'legal_unit_tm'::regclass, source_table => 'legal_unit_source'::regclass, primary_identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  CALL sql_saga.benchmark_log_and_reset('Temporal Merge 80% UPDATE 20% INSERT Parent');
  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge 80% UPDATE 20% INSERT Parent end', 1000, true);

  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge 80% UPDATE 20% INSERT Child', 0, false);
  CALL sql_saga.benchmark_reset();
  CALL sql_saga.temporal_merge(target_table => 'establishment_tm'::regclass, source_table => 'establishment_source'::regclass, primary_identity_columns => ARRAY['id'], ephemeral_columns => ARRAY[]::TEXT[]);
  CALL sql_saga.benchmark_log_and_reset('Temporal Merge 80% UPDATE 20% INSERT Child');
  INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Temporal Merge 80% UPDATE 20% INSERT Child end', 1000, true);
END;

-- Verify final counts
SELECT 'legal_unit_tm' AS table_name, COUNT(*) AS final_count FROM legal_unit_tm
UNION ALL
SELECT 'establishment_tm' AS table_name, COUNT(*) AS final_count FROM establishment_tm;

--------------------------------------------------------------------------------
\echo '--- Multi-Scale Performance Test ---'
--------------------------------------------------------------------------------

-- Demonstrate fixture system with 10K entities (if available)
\echo 'Testing 10K scale (if fixture exists):'

DO $$
BEGIN
    IF sql_saga_fixtures.fixture_exists('temporal_merge_10K_basic') THEN
        -- Clear existing tables
        TRUNCATE legal_unit_tm CASCADE;
        TRUNCATE establishment_tm CASCADE;
        
        -- Load 10K fixture
        INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Fixture Load 10K', 0, false);
        CALL sql_saga.benchmark_reset();
        PERFORM benchmark_load_temporal_merge_fixture('10K', 'basic', 'public');
        CALL sql_saga.benchmark_log_and_reset('Fixture Load 10K');
        INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('Fixture Load 10K end', 20000, true);
        
        -- Show counts
        RAISE NOTICE '10K fixture loaded: legal_unit=%, establishment=%', 
            (SELECT COUNT(*) FROM legal_unit_tm),
            (SELECT COUNT(*) FROM establishment_tm);
    ELSE
        RAISE NOTICE '10K fixture not available - would auto-generate in production';
    END IF;
END;
$$;

--------------------------------------------------------------------------------
\echo '--- Fixture System Health Check ---'
--------------------------------------------------------------------------------

-- Check fixture system health
\echo 'Fixture health status:'
SELECT * FROM benchmark_check_fixture_health();

-- Show fixture usage statistics
\echo 'Fixture usage statistics:'
SELECT 
    fixture_name,
    load_count,
    CASE 
        WHEN last_loaded_at IS NOT NULL THEN
            format_duration(clock_timestamp() - last_loaded_at) || ' ago'
        ELSE 'Never loaded'
    END as last_used,
    ROUND(generation_time_sec, 2) as gen_time_sec
FROM sql_saga_fixtures.list_fixtures()
WHERE fixture_name LIKE 'temporal_merge_%'
ORDER BY load_count DESC;

-- Cleanup target tables
SELECT sql_saga.drop_foreign_key('establishment_tm', ARRAY['legal_unit_id'], 'valid');
SELECT sql_saga.drop_unique_key('establishment_tm', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('establishment_tm', cleanup => true);
SELECT sql_saga.drop_unique_key('legal_unit_tm', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('legal_unit_tm', cleanup => true);

DROP TABLE establishment_tm;
DROP TABLE legal_unit_tm;

-- Performance log from pg_stat_monitor
\echo '-- Performance log from pg_stat_monitor --'
\set monitor_log_filename expected/performance/111_benchmark_temporal_merge_with_fixtures_monitor.csv
\i sql/include/benchmark_monitor_csv.sql

-- Verify the benchmark events and row counts
\echo 'Benchmark events summary:'
SELECT event, row_count, 
       CASE WHEN lag(timestamp) OVER (ORDER BY seq_id) IS NOT NULL 
            THEN format_duration(timestamp - lag(timestamp) OVER (ORDER BY seq_id))
            ELSE ''
       END as duration
FROM benchmark 
WHERE event NOT LIKE '%end' 
ORDER BY seq_id;

-- Capture performance metrics to a separate file for manual review
\set benchmark_log_filename expected/performance/111_benchmark_temporal_merge_with_fixtures_report.log
\i sql/include/benchmark_report_log.sql

\i sql/include/benchmark_teardown.sql
\i sql/include/test_teardown.sql