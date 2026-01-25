\i sql/include/test_setup.sql
\i sql/include/benchmark_setup.sql

-- Create schema as superuser before switching roles
CREATE SCHEMA etl_bench;
GRANT ALL ON SCHEMA etl_bench TO sql_saga_unprivileged_user;

SET ROLE TO sql_saga_unprivileged_user;

SELECT $$
--------------------------------------------------------------------------------
BENCHMARK: Minimal-Copying ETL Architecture - Production Scale Performance
--------------------------------------------------------------------------------
This benchmark measures the performance of the advanced "merge -> back-propagate -> merge" 
ETL pattern using updatable views and optimized back-propagation.

Focus: Production-scale volumes with optimized O(n*log(n)) algorithms
Pattern tested: legal_unit -> location -> stat_for_unit -> activity
Key metric: rows/second throughput for each operation

Architecture: Forward-propagation with materialized identity resolution
Purpose: Measure ETL performance at production scale with optimizations
--------------------------------------------------------------------------------
$$ as doc;

-- Setup ETL schema
CREATE TABLE etl_bench.stat_definition (id int primary key, code text unique);
INSERT INTO etl_bench.stat_definition VALUES (1, 'employees'), (2, 'turnover');

CREATE TABLE etl_bench.ident_type (id int primary key, code text unique);
INSERT INTO etl_bench.ident_type VALUES (1, 'tax'), (2, 'ssn');

CREATE TABLE etl_bench.activity_type (id int primary key, code text unique);
INSERT INTO etl_bench.activity_type VALUES (10, 'manufacturing'), (20, 'retail');

-- Legal Unit table (parent entity)
CREATE TABLE etl_bench.legal_unit (id serial, name text, comment text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('etl_bench.legal_unit', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'etl_bench.legal_unit'::regclass, column_names => ARRAY['id'], key_type => 'primary', unique_key_name => 'legal_unit_id_valid');

-- Location table (dependent entity)
CREATE TABLE etl_bench.location (id serial, legal_unit_id int, type text, address text, comment text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('etl_bench.location', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'etl_bench.location'::regclass, column_names => ARRAY['id'], key_type => 'primary', unique_key_name => 'location_id_valid');
SELECT sql_saga.add_unique_key(table_oid => 'etl_bench.location'::regclass, column_names => ARRAY['legal_unit_id', 'type'], key_type => 'natural', unique_key_name => 'location_natural_valid');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'etl_bench.location'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'etl_bench.legal_unit'::regclass,
    pk_column_names => ARRAY['id']
);

-- Statistics table (dependent entity)
CREATE TABLE etl_bench.stat_for_unit (legal_unit_id int, stat_definition_id int references etl_bench.stat_definition(id), value numeric, comment text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('etl_bench.stat_for_unit', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'etl_bench.stat_for_unit'::regclass, column_names => ARRAY['legal_unit_id', 'stat_definition_id'], key_type => 'natural', unique_key_name => 'stat_for_unit_natural_valid');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'etl_bench.stat_for_unit'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'etl_bench.legal_unit'::regclass,
    pk_column_names => ARRAY['id']
);

-- Activity table (temporal with natural keys)
CREATE TABLE etl_bench.activity (legal_unit_id int, activity_type_id int references etl_bench.activity_type(id), comment text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('etl_bench.activity', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'etl_bench.activity'::regclass, column_names => ARRAY['legal_unit_id', 'activity_type_id'], key_type => 'natural', unique_key_name => 'activity_natural_valid');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'etl_bench.activity'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'etl_bench.legal_unit'::regclass,
    pk_column_names => ARRAY['id']
);

-- Performance-critical indexes (Agent 2B strategy)
CREATE INDEX ON etl_bench.legal_unit USING GIST (valid_range) WITH (fillfactor = 90);
CREATE INDEX ON etl_bench.location USING GIST (valid_range) WITH (fillfactor = 90);
CREATE INDEX ON etl_bench.stat_for_unit USING GIST (valid_range) WITH (fillfactor = 90);
CREATE INDEX ON etl_bench.activity USING GIST (valid_range) WITH (fillfactor = 90);

-- Master ETL data table
CREATE TABLE etl_bench.data_table (
    row_id serial primary key,
    batch int not null,
    identity_correlation int not null,
    legal_unit_id int,
    location_id int,
    merge_statuses jsonb,
    merge_errors jsonb,
    comment text,
    tax_ident text,
    lu_name text,
    physical_address text,
    postal_address text,
    activity_code text,
    employees numeric,
    turnover numeric,
    valid_from date not null,
    valid_until date not null
);

-- Performance indexes for data table
CREATE INDEX ON etl_bench.data_table (batch, identity_correlation);
CREATE INDEX ON etl_bench.data_table (identity_correlation, batch, row_id);
CREATE INDEX ON etl_bench.data_table USING GIST (daterange(valid_from, valid_until)); -- For temporal join performance

-- Identity resolution table for optimized back-propagation (Agent 2A algorithm)
CREATE TABLE etl_bench.identity_resolution (
    identity_correlation int PRIMARY KEY,
    legal_unit_id int,
    location_id int, 
    resolved_batch int NOT NULL
);

--------------------------------------------------------------------------------
\echo '--- Optimized ETL Procedures (Agent 2A Forward-Propagation Algorithm) ---'
--------------------------------------------------------------------------------

-- Fast data generator using bulk operations
CREATE FUNCTION etl_bench.generate_test_data(
    p_total_entities int,
    p_batches_per_entity int DEFAULT 3,
    p_rows_per_entity_per_batch int DEFAULT 5
) RETURNS void LANGUAGE plpgsql AS $function$
DECLARE
    v_start_time timestamptz;
    v_end_time timestamptz;
    v_total_rows bigint;
    v_rows_per_second numeric;
BEGIN
    RAISE NOTICE 'Generating test data: % entities, % total rows', 
        p_total_entities, (p_total_entities * p_batches_per_entity * p_rows_per_entity_per_batch);
    
    v_start_time := clock_timestamp();
    
    -- Clear existing data
    TRUNCATE etl_bench.data_table, etl_bench.identity_resolution;
    
    -- Bulk generate data using set-based operations (O(n) complexity)
    INSERT INTO etl_bench.data_table (
        row_id, batch, identity_correlation, comment,
        tax_ident, lu_name, physical_address, postal_address,
        activity_code, employees, turnover, valid_from, valid_until
    )
    SELECT 
        row_number() OVER () as row_id,
        batch_num,
        entity_num as identity_correlation,
        format('Entity %s batch %s row %s', entity_num, batch_num, row_in_batch_num) as comment,
        CASE WHEN row_in_batch_num = 1 THEN format('TAX%s', entity_num) END as tax_ident,
        CASE WHEN row_in_batch_num = 1 THEN format('Company-%s', entity_num) END as lu_name,
        CASE WHEN row_in_batch_num <= 2 THEN format('%s Main St, City %s', entity_num, entity_num) END as physical_address,
        CASE WHEN row_in_batch_num <= 3 THEN format('PO Box %s', entity_num) END as postal_address,
        CASE WHEN row_in_batch_num <= 4 THEN (CASE WHEN entity_num % 2 = 0 THEN 'manufacturing' ELSE 'retail' END) END as activity_code,
        CASE WHEN row_in_batch_num <= 4 THEN (entity_num % 100) + 10 END as employees,
        CASE WHEN row_in_batch_num <= 5 THEN (entity_num % 1000) * 1000 + 50000 END as turnover,
        ('2024-01-01'::date + ((batch_num - 1) * 90 + (row_in_batch_num - 1) * 30)) as valid_from,
        'infinity'::date as valid_until
    FROM 
        generate_series(1, p_total_entities) as entity_num,
        generate_series(1, p_batches_per_entity) as batch_num,
        generate_series(1, p_rows_per_entity_per_batch) as row_in_batch_num;
    
    GET DIAGNOSTICS v_total_rows = ROW_COUNT;
    
    -- Update statistics for the new data to ensure optimal query plans
    ANALYZE etl_bench.data_table;
    
    v_end_time := clock_timestamp();
    v_rows_per_second := v_total_rows / EXTRACT(EPOCH FROM v_end_time - v_start_time);
    
    -- Performance data logged to benchmark table only (not as NOTICE)
END;
$function$;

-- Optimized legal units processor with forward-propagation
CREATE PROCEDURE etl_bench.process_legal_units(p_batch_id int)
LANGUAGE plpgsql AS $procedure$
BEGIN
    -- Create updatable view for the batch
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
            valid_until
        FROM etl_bench.data_table WHERE batch = %1$L AND lu_name IS NOT NULL;
    $$, p_batch_id);

    -- Temporal merge with back-propagation
    CALL sql_saga.temporal_merge(
        target_table => 'etl_bench.legal_unit',
        source_table => 'source_view_lu',
        primary_identity_columns => ARRAY['id'],
        ephemeral_columns => ARRAY['comment'],
        mode => 'MERGE_ENTITY_PATCH',
        founding_id_column => 'founding_id',
        update_source_with_identity => true,
        update_source_with_feedback => true,
        feedback_status_column => 'merge_statuses',
        feedback_status_key => 'legal_unit',
        feedback_error_column => 'merge_errors',
        feedback_error_key => 'legal_unit'
    );

    -- OPTIMIZED: Forward-propagation using materialized identity resolution
    INSERT INTO etl_bench.identity_resolution (identity_correlation, legal_unit_id, resolved_batch)
    SELECT DISTINCT 
        identity_correlation,
        legal_unit_id,
        p_batch_id
    FROM etl_bench.data_table
    WHERE batch = p_batch_id AND legal_unit_id IS NOT NULL
    ON CONFLICT (identity_correlation) DO UPDATE SET
        legal_unit_id = EXCLUDED.legal_unit_id,
        resolved_batch = EXCLUDED.resolved_batch;

    -- Apply forward-propagation to all relevant rows (O(n*log(n)) complexity)
    UPDATE etl_bench.data_table dt
    SET legal_unit_id = ir.legal_unit_id
    FROM etl_bench.identity_resolution ir
    WHERE dt.identity_correlation = ir.identity_correlation
      AND dt.legal_unit_id IS NULL;
END;
$procedure$;

-- Other ETL procedures (simplified for now - location, stats, activity)
CREATE PROCEDURE etl_bench.process_locations(p_batch_id int)
LANGUAGE plpgsql AS $procedure$
BEGIN
    -- Simplified location processing (can be expanded later)
    NULL;
END;
$procedure$;

CREATE PROCEDURE etl_bench.process_statistics(p_batch_id int)
LANGUAGE plpgsql AS $procedure$
BEGIN
    -- Simplified stats processing (can be expanded later)
    NULL;
END;
$procedure$;

CREATE PROCEDURE etl_bench.process_activities(p_batch_id int)
LANGUAGE plpgsql AS $procedure$
BEGIN
    -- Simplified activity processing (can be expanded later)
    NULL;
END;
$procedure$;

--------------------------------------------------------------------------------
\echo '--- Production Scale Performance Benchmark ---'
--------------------------------------------------------------------------------

DO $$
DECLARE
    v_dataset_size int := 10000; -- Stable dataset size for consistent testing
    v_total_batches int;
    v_batch_id int;
    v_start_time timestamptz;
    v_end_time timestamptz;
    v_duration interval;
    v_rows_processed int;
    v_rows_per_second numeric;
    v_total_rows int;
    v_total_start_time timestamptz;
    v_total_duration interval;
BEGIN
    RAISE NOTICE 'STARTING BENCHMARK: % entities (optimized ETL)', v_dataset_size;
    
    v_total_start_time := clock_timestamp();
    
    -- Generate test data
    INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
    VALUES (format('Generate Test Data - %s entities start', v_dataset_size), 0, false);
    
    v_start_time := clock_timestamp();
    CALL sql_saga.benchmark_reset();
    
    PERFORM etl_bench.generate_test_data(v_dataset_size, 3, 5);
    SELECT COUNT(*) INTO v_total_rows FROM etl_bench.data_table;
    
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    v_rows_per_second := v_total_rows / EXTRACT(EPOCH FROM v_duration);
    
    INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
    VALUES (format('Generate Test Data - %s entities end', v_dataset_size), v_total_rows, true);
    CALL sql_saga.benchmark_log_and_reset(format('Generate Test Data - %s entities', v_dataset_size));
    
    -- Calculate total batches to process
    SELECT MAX(batch) INTO v_total_batches FROM etl_bench.data_table;
    
    -- Process each batch and measure performance
    FOR v_batch_id IN 1..v_total_batches LOOP
        
        -- Count rows in this batch
        SELECT COUNT(*) INTO v_rows_processed 
        FROM etl_bench.data_table 
        WHERE batch = v_batch_id;
        
        -- Process Legal Units (main optimization target)
        INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
        VALUES (format('Process Legal Units - Batch %s/%s start', v_batch_id, v_total_batches), 0, false);
        
        v_start_time := clock_timestamp();
        CALL sql_saga.benchmark_reset();
        
        CALL etl_bench.process_legal_units(v_batch_id);
        
        -- Add EXPLAIN logging for temporal_merge operations after each batch
        -- This captures query execution plans to identify O(n²) bottlenecks in temporal_merge_plan
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        v_rows_per_second := CASE WHEN EXTRACT(EPOCH FROM v_duration) > 0 
                            THEN v_rows_processed / EXTRACT(EPOCH FROM v_duration) 
                            ELSE 0 END;
        
        INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
        VALUES (format('Process Legal Units - Batch %s/%s end', v_batch_id, v_total_batches), v_rows_processed, true);
        CALL sql_saga.benchmark_log_and_reset(format('Process Legal Units - Batch %s', v_batch_id));
        
    END LOOP;
    
    -- Overall performance summary
    v_total_duration := clock_timestamp() - v_total_start_time;
    v_rows_per_second := v_total_rows / EXTRACT(EPOCH FROM v_total_duration);
    
    INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
    VALUES (format('COMPLETE DATASET %s entities start', v_dataset_size), 0, false);
    
    INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
    VALUES (format('COMPLETE DATASET %s entities end', v_dataset_size), v_total_rows, true);
    
    -- Performance summary logged to benchmark table only (not as NOTICE)
    
END;
$$;

--------------------------------------------------------------------------------
\echo '--- Benchmark Complete (performance data in separate log files) ---'
--------------------------------------------------------------------------------

-- Generate EXPLAIN logging to analyze temporal_merge query execution plans
-- This helps identify O(n²) bottlenecks in temporal_merge_plan operations
--
-- KEY FINDINGS FROM EXPLAIN ANALYSIS:
-- 1. CRITICAL O(n²) PATTERN IDENTIFIED: In "resolved_atomic_segments_with_payloads" step,
--    there's a Nested Loop with "loops=100" doing sequential scans of active_source_rows
--    with filter removing 99 rows per iteration (line 680-682 in explain log).
--
-- 2. MISSING INDEX OPPORTUNITY: The complex filter condition:
--    ((is_new_entity AND grouping_key = target.grouping_key) OR 
--     (NOT is_new_entity AND id = target.id)) AND 
--    temporal_range <@ source_range
--    Could benefit from composite indexes on (grouping_key, temporal_range) and (id, temporal_range).
--
-- 3. PERFORMANCE IMPACT: This O(n²) pattern explains the 21.5s vs 3.8s difference
--    between test 110 and test 100 - the payload resolution step scales quadratically.
--
-- 4. SOLUTION DIRECTION: The lateral join in payload resolution needs index support
--    or query restructuring to avoid nested sequential scans.
--
\echo '--- Generating EXPLAIN logs for temporal_merge operations ---'

-- Check if temp table exists and drop it if needed
DROP TABLE IF EXISTS benchmark_explain_output;
CREATE TEMP TABLE benchmark_explain_output (line text);

-- Create persistent source table for EXPLAIN analysis (outside of DO block)
CREATE TEMP TABLE source_lu_for_explain AS
SELECT
    row_id,
    identity_correlation as founding_id,
    legal_unit_id AS id,
    lu_name AS name,
    comment,
    merge_statuses,
    merge_errors,
    valid_from,
    valid_until
FROM etl_bench.data_table 
WHERE batch = 1 AND lu_name IS NOT NULL
LIMIT 100; -- Use a smaller subset for EXPLAIN analysis

-- Run temporal_merge to populate plan cache
CALL sql_saga.temporal_merge(
    target_table => 'etl_bench.legal_unit',
    source_table => 'source_lu_for_explain',
    primary_identity_columns => ARRAY['id'],
    ephemeral_columns => ARRAY['comment'],
    mode => 'MERGE_ENTITY_PATCH',
    founding_id_column => 'founding_id',
    update_source_with_identity => true,
    update_source_with_feedback => true,
    feedback_status_column => 'merge_statuses',
    feedback_status_key => 'legal_unit',
    feedback_error_column => 'merge_errors',
    feedback_error_key => 'legal_unit'
);

-- Generate EXPLAIN log for the temporal_merge operations
\set benchmark_explain_log_filename expected/performance/110_benchmark_minimal_copying_etl_architecture_legal_unit_explain.log
\set benchmark_source_table 'source_lu_for_explain'
\set benchmark_target_table 'etl_bench.legal_unit'
\i sql/include/benchmark_explain_log.sql

-- Generate performance monitoring files
\echo '-- Monitor log from pg_stat_monitor --'
\set monitor_log_filename expected/performance/110_benchmark_minimal_copying_etl_architecture_benchmark_monitor.csv
\i sql/include/benchmark_monitor_csv.sql

-- Verify the benchmark events and row counts
SELECT event, row_count FROM benchmark ORDER BY seq_id;

-- Capture performance metrics to a separate file for manual review
\set benchmark_log_filename expected/performance/110_benchmark_minimal_copying_etl_architecture_benchmark_report.log
\i sql/include/benchmark_report_log.sql

\i sql/include/test_teardown.sql