\i sql/include/test_setup.sql
\i sql/include/benchmark_setup.sql
\i sql/include/benchmark_fixture_system_simple.sql
\i sql/include/benchmark_fixture_integration.sql

-- Create schema as superuser before switching roles
CREATE SCHEMA etl_bench;
GRANT ALL ON SCHEMA etl_bench TO sql_saga_unprivileged_user;

SET ROLE TO sql_saga_unprivileged_user;

SELECT $$
--------------------------------------------------------------------------------
BENCHMARK: Minimal-Copying ETL Architecture - Production Scale Performance
--------------------------------------------------------------------------------
This benchmark measures the performance of the advanced "merge -> back-propagate -> merge" 
ETL pattern using updatable views and ID back-propagation from test 071.

Focus: Production-scale volumes (1M+ parent records) with full
complexity dependency chains to detect O(n^2) vs O(n*log(n)) scaling behavior.

Pattern tested: legal_unit -> location -> stat_for_unit -> activity (full 071)
Key metric: rows/second throughput for each operation

Architecture: Updatable TEMP VIEWs with back-filling (minimal data copying)
Purpose: Reveal scaling bottlenecks in real ETL workloads

UPDATED: Now uses fixture system for fast data loading (2-3 seconds vs hours)
Workflow: Ensure fixtures exist -> Load fixtures -> Run ETL benchmarks
--------------------------------------------------------------------------------
$$ as doc;

-- Setup schema matching test 071 structure (schema already created above)

-- Reference tables (not temporal)
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

-- External Identifiers table (not temporal)
CREATE TABLE etl_bench.ident (
    id serial primary key,
    legal_unit_id int not null,
    ident_type_id int not null references etl_bench.ident_type(id),
    value text not null,
    unique (legal_unit_id, ident_type_id)
);

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

-- Critical indexes for performance
CREATE INDEX ON etl_bench.legal_unit USING GIST (valid_range);
CREATE INDEX ON etl_bench.location USING GIST (valid_range);
CREATE INDEX ON etl_bench.stat_for_unit USING GIST (valid_range);
CREATE INDEX ON etl_bench.activity USING GIST (valid_range);

-- Master ETL data table (like test 071)
CREATE TABLE etl_bench.data_table (
    row_id serial primary key,
    batch int not null,
    identity_correlation int not null, -- Groups rows belonging to same conceptual entity
    
    -- Writeback columns for generated IDs
    legal_unit_id int,
    location_id int,
    
    -- Feedback columns
    merge_statuses jsonb,
    merge_errors jsonb,
    
    -- Business data columns
    comment text,
    tax_ident text,
    lu_name text,
    physical_address text,
    postal_address text,
    activity_code text,
    employees numeric,
    turnover numeric,
    
    -- Temporal columns
    valid_from date not null,
    valid_until date not null
);

CREATE INDEX ON etl_bench.data_table (batch, identity_correlation);
CREATE INDEX ON etl_bench.data_table (identity_correlation, batch, row_id);

--------------------------------------------------------------------------------
\echo '--- ETL Driver Procedures (Production-Scale Optimized) ---'
--------------------------------------------------------------------------------

-- Legal Units processor
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

    -- Intra-batch back-propagation
    UPDATE etl_bench.data_table dt
    SET legal_unit_id = sub.legal_unit_id
    FROM (
        SELECT DISTINCT ON (identity_correlation)
            identity_correlation,
            legal_unit_id
        FROM etl_bench.data_table
        WHERE batch = p_batch_id AND legal_unit_id IS NOT NULL
        ORDER BY identity_correlation, row_id DESC
    ) AS sub
    WHERE dt.batch = p_batch_id
      AND dt.identity_correlation = sub.identity_correlation;

    -- Inter-batch propagation
    UPDATE etl_bench.data_table dt_future
    SET legal_unit_id = dt_latest.legal_unit_id
    FROM (
        SELECT DISTINCT ON (identity_correlation)
            identity_correlation,
            legal_unit_id
        FROM etl_bench.data_table
        WHERE batch <= p_batch_id AND legal_unit_id IS NOT NULL
        ORDER BY identity_correlation, batch DESC, row_id DESC
    ) AS dt_latest
    WHERE dt_future.batch > p_batch_id
      AND dt_future.identity_correlation = dt_latest.identity_correlation;
END;
$procedure$;

-- Location processor  
CREATE PROCEDURE etl_bench.process_locations(p_batch_id int)
LANGUAGE plpgsql AS $procedure$
BEGIN
    -- Physical locations
    EXECUTE format($$
        CREATE OR REPLACE TEMP VIEW source_view_loc_physical AS
        SELECT
            row_id,
            legal_unit_id,
            'physical' as type,
            physical_address as address,
            comment,
            merge_statuses,
            merge_errors,
            valid_from,
            valid_until
        FROM etl_bench.data_table
        WHERE batch = %1$L AND physical_address IS NOT NULL;
    $$, p_batch_id);

    CALL sql_saga.temporal_merge(
        target_table => 'etl_bench.location',
        source_table => 'source_view_loc_physical',
        natural_identity_columns => ARRAY['legal_unit_id', 'type'],
        ephemeral_columns => ARRAY['comment'],
        mode => 'MERGE_ENTITY_PATCH',
        update_source_with_feedback => true,
        feedback_status_column => 'merge_statuses',
        feedback_status_key => 'location_physical',
        feedback_error_column => 'merge_errors',
        feedback_error_key => 'location_physical'
    );

    -- Postal locations
    EXECUTE format($$
        CREATE OR REPLACE TEMP VIEW source_view_loc_postal AS
        SELECT
            row_id,
            legal_unit_id,
            'postal' as type,
            postal_address as address,
            comment,
            merge_statuses,
            merge_errors,
            valid_from,
            valid_until
        FROM etl_bench.data_table
        WHERE batch = %1$L AND postal_address IS NOT NULL;
    $$, p_batch_id);

    CALL sql_saga.temporal_merge(
        target_table => 'etl_bench.location',
        source_table => 'source_view_loc_postal',
        natural_identity_columns => ARRAY['legal_unit_id', 'type'],
        ephemeral_columns => ARRAY['comment'],
        mode => 'MERGE_ENTITY_PATCH',
        update_source_with_feedback => true,
        feedback_status_column => 'merge_statuses',
        feedback_status_key => 'location_postal',
        feedback_error_column => 'merge_errors',
        feedback_error_key => 'location_postal'
    );
END;
$procedure$;

-- Statistics processor
CREATE PROCEDURE etl_bench.process_statistics(p_batch_id int)
LANGUAGE plpgsql AS $procedure$
BEGIN
    -- Employee statistics
    EXECUTE format($$
        CREATE OR REPLACE TEMP VIEW source_view_stat_employees AS
        SELECT
            row_id,
            legal_unit_id,
            1 as stat_definition_id,
            employees as value,
            comment,
            merge_statuses,
            merge_errors,
            valid_from,
            valid_until
        FROM etl_bench.data_table
        WHERE batch = %1$L AND employees IS NOT NULL;
    $$, p_batch_id);

    CALL sql_saga.temporal_merge(
        target_table => 'etl_bench.stat_for_unit',
        source_table => 'source_view_stat_employees',
        natural_identity_columns => ARRAY['legal_unit_id', 'stat_definition_id'],
        ephemeral_columns => ARRAY['comment'],
        mode => 'MERGE_ENTITY_PATCH',
        update_source_with_feedback => true,
        feedback_status_column => 'merge_statuses',
        feedback_status_key => 'stat_employees',
        feedback_error_column => 'merge_errors',
        feedback_error_key => 'stat_employees'
    );

    -- Turnover statistics
    EXECUTE format($$
        CREATE OR REPLACE TEMP VIEW source_view_stat_turnover AS
        SELECT
            row_id,
            legal_unit_id,
            2 as stat_definition_id,
            turnover as value,
            comment,
            merge_statuses,
            merge_errors,
            valid_from,
            valid_until
        FROM etl_bench.data_table
        WHERE batch = %1$L AND turnover IS NOT NULL;
    $$, p_batch_id);

    CALL sql_saga.temporal_merge(
        target_table => 'etl_bench.stat_for_unit',
        source_table => 'source_view_stat_turnover',
        natural_identity_columns => ARRAY['legal_unit_id', 'stat_definition_id'],
        ephemeral_columns => ARRAY['comment'],
        mode => 'MERGE_ENTITY_PATCH',
        update_source_with_feedback => true,
        feedback_status_column => 'merge_statuses',
        feedback_status_key => 'stat_turnover',
        feedback_error_column => 'merge_errors',
        feedback_error_key => 'stat_turnover'
    );
END;
$procedure$;

-- Activity processor
CREATE PROCEDURE etl_bench.process_activities(p_batch_id int)
LANGUAGE plpgsql AS $procedure$
BEGIN
    EXECUTE format($$
        CREATE OR REPLACE TEMP VIEW source_view_activity AS
        SELECT
            row_id,
            legal_unit_id,
            CASE activity_code 
                WHEN 'manufacturing' THEN 10
                WHEN 'retail' THEN 20
            END as activity_type_id,
            comment,
            merge_statuses,
            merge_errors,
            valid_from,
            valid_until
        FROM etl_bench.data_table
        WHERE batch = %1$L AND activity_code IS NOT NULL;
    $$, p_batch_id);

    CALL sql_saga.temporal_merge(
        target_table => 'etl_bench.activity',
        source_table => 'source_view_activity',
        natural_identity_columns => ARRAY['legal_unit_id', 'activity_type_id'],
        ephemeral_columns => ARRAY['comment'],
        mode => 'MERGE_ENTITY_PATCH',
        update_source_with_feedback => true,
        feedback_status_column => 'merge_statuses',
        feedback_status_key => 'activity',
        feedback_error_column => 'merge_errors',
        feedback_error_key => 'activity'
    );
END;
$procedure$;

-- Complete batch processor
CREATE PROCEDURE etl_bench.process_batch(p_batch_id int)
LANGUAGE plpgsql AS $procedure$
BEGIN
    CALL etl_bench.process_legal_units(p_batch_id);
    CALL etl_bench.process_locations(p_batch_id);
    CALL etl_bench.process_statistics(p_batch_id);
    CALL etl_bench.process_activities(p_batch_id);
END;
$procedure$;

--------------------------------------------------------------------------------
\echo '--- Fixture Management for ETL Benchmark ---'
--------------------------------------------------------------------------------

-- Fixture management functions for ETL benchmark schema
CREATE FUNCTION etl_bench.ensure_benchmark_fixture(
    p_scale text, -- '1K', '10K', '100K', '1M'
    p_auto_generate boolean DEFAULT true,
    p_force_regenerate boolean DEFAULT false
) RETURNS boolean LANGUAGE plpgsql AS $function$
DECLARE
    v_fixture_name text;
    v_entities bigint;
    v_fixture_ready boolean;
BEGIN
    -- Parse scale to entity count  
    v_entities := CASE p_scale
        WHEN '1K' THEN 1000
        WHEN '10K' THEN 10000
        WHEN '100K' THEN 100000
        WHEN '1M' THEN 1000000
        ELSE CAST(p_scale AS bigint)
    END;
    
    v_fixture_name := format('etl_bench_%s_standard', p_scale);
    
    -- Force regeneration if requested
    IF p_force_regenerate AND sql_saga_fixtures.fixture_exists(v_fixture_name) THEN
        RAISE NOTICE 'sql_saga: Force regenerating fixture %', v_fixture_name;
        PERFORM sql_saga_fixtures.delete_fixture(v_fixture_name);
    END IF;
    
    -- Ensure fixture exists (auto-generate if missing and enabled)
    v_fixture_ready := sql_saga_fixtures.ensure_etl_fixture(
        p_scale, 'standard', p_auto_generate
    );
    
    IF NOT v_fixture_ready THEN
        RAISE NOTICE 'sql_saga: Fixture % not available and auto-generation disabled', v_fixture_name;
    ELSE
        RAISE NOTICE 'sql_saga: Fixture % ready (% entities)', v_fixture_name, v_entities;
    END IF;
    
    RETURN v_fixture_ready;
END;
$function$;

-- Load fixture data into the ETL benchmark schema
CREATE FUNCTION etl_bench.load_benchmark_fixture(
    p_scale text
) RETURNS void LANGUAGE plpgsql AS $function$
DECLARE
    v_start_time timestamptz;
    v_end_time timestamptz;
    v_duration interval;
    v_total_rows bigint;
    v_rows_per_second numeric;
BEGIN
    v_start_time := clock_timestamp();
    
    RAISE NOTICE 'sql_saga: Loading ETL benchmark fixture for % entities', p_scale;
    
    -- Clear existing data
    TRUNCATE etl_bench.data_table;
    
    -- Temporarily make table unlogged for faster loading
    ALTER TABLE etl_bench.data_table SET UNLOGGED;
    
    -- Load fixture data using the fixture system
    PERFORM benchmark_load_etl_fixture(p_scale, 'etl_bench.data_table'::regclass, 'standard');
    
    -- Restore table to logged state
    ALTER TABLE etl_bench.data_table SET LOGGED;
    
    -- Get row count and calculate performance
    SELECT COUNT(*) INTO v_total_rows FROM etl_bench.data_table;
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    v_rows_per_second := v_total_rows / GREATEST(EXTRACT(EPOCH FROM v_duration), 0.001);
    
    RAISE NOTICE 'sql_saga: Loaded % rows in % (% rows/sec)', 
        v_total_rows, v_duration, ROUND(v_rows_per_second);
END;
$function$;

-- Show fixture status for the benchmark
CREATE FUNCTION etl_bench.show_fixture_status()
RETURNS TABLE(
    fixture_name text,
    entities bigint,
    total_rows bigint,
    age_hours numeric,
    load_count bigint,
    status text
) LANGUAGE plpgsql AS $function$
BEGIN
    RETURN QUERY
    SELECT 
        fr.fixture_name,
        fr.total_entities as entities,
        fr.total_rows,
        ROUND(EXTRACT(EPOCH FROM clock_timestamp() - fr.created_at) / 3600, 1) as age_hours,
        fr.load_count,
        CASE 
            WHEN fr.last_loaded_at IS NULL THEN 'NEVER_LOADED'
            WHEN fr.created_at > clock_timestamp() - interval '1 day' THEN 'FRESH'
            WHEN fr.created_at > clock_timestamp() - interval '7 days' THEN 'RECENT'
            ELSE 'OLD'
        END as status
    FROM sql_saga_fixtures.fixture_registry fr
    WHERE fr.fixture_name LIKE 'etl_bench_%'
    ORDER BY fr.total_entities;
END;
$function$;

--------------------------------------------------------------------------------
\echo '--- Fixture Management and Status ---'
--------------------------------------------------------------------------------

\echo 'Available ETL benchmark fixtures:'
SELECT * FROM etl_bench.show_fixture_status();

\echo 'All available fixtures in system:'
SELECT fixture_name, fixture_type, entities, total_rows, 
       ROUND(generation_time_sec, 2) as gen_time_sec,
       CASE 
           WHEN last_loaded_at IS NOT NULL THEN load_count || ' loads'
           ELSE 'Never loaded'
       END as usage
FROM sql_saga_fixtures.list_fixtures()
WHERE fixture_name LIKE 'etl_bench_%'
ORDER BY entities;

\echo 'Fixture system health check:'
SELECT * FROM benchmark_check_fixture_health()
WHERE fixture_name LIKE 'etl_bench_%';

--------------------------------------------------------------------------------
\echo '--- Production Scale Performance Benchmarks ---'
--------------------------------------------------------------------------------

-- User can uncomment the following lines to force fixture regeneration for testing:
-- SELECT etl_bench.ensure_benchmark_fixture('1M', true, true) as force_regenerate_1m;
-- SELECT etl_bench.ensure_benchmark_fixture('100K', true, true) as force_regenerate_100k;

DO $$
DECLARE
    -- BENCHMARK CONFIGURATION:
    -- Change this variable to test different scales:
    -- '1K'   = 1,000 entities   (15K rows)   - Fast demo/development testing
    -- '10K'  = 10,000 entities  (150K rows)  - Medium scale testing  
    -- '100K' = 100,000 entities (1.5M rows)  - Large scale testing
    -- '1M'   = 1,000,000 entities (15M rows) - Production scale testing
    v_dataset_scale text := '1M'; -- Uses fixture system for fast loading (2-3 seconds vs hours)
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
    v_fixture_ready boolean;
BEGIN
    RAISE NOTICE 'STARTING BENCHMARK: % scale production ETL performance test', v_dataset_scale;
    
    v_total_start_time := clock_timestamp();
    
    -- Display current fixture status
    RAISE NOTICE 'Current fixture status:';
    INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
    VALUES (format('Fixture Status Check - %s scale', v_dataset_scale), 0, false);
    
    -- Ensure fixture exists (auto-generate if missing, with user confirmation)
    v_fixture_ready := etl_bench.ensure_benchmark_fixture(v_dataset_scale, true, false);
    
    IF NOT v_fixture_ready THEN
        RAISE EXCEPTION 'sql_saga: Cannot proceed - fixture for scale % is not available', v_dataset_scale;
    END IF;
    
    -- Load fixture data (replaces old generation step)
    INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
    VALUES (format('Load Fixture Data - %s scale', v_dataset_scale), 0, true);
    
    v_start_time := clock_timestamp();
    CALL sql_saga.benchmark_reset();
    
    PERFORM etl_bench.load_benchmark_fixture(v_dataset_scale);
    SELECT COUNT(*) INTO v_total_rows FROM etl_bench.data_table;
    
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    v_rows_per_second := v_total_rows / GREATEST(EXTRACT(EPOCH FROM v_duration), 0.001);
    
    INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
    VALUES (format('Fixture Load Complete - %s rows/sec', ROUND(v_rows_per_second)), v_total_rows, true);
    CALL sql_saga.benchmark_log_and_reset(format('Load Fixture Data - %s scale', v_dataset_scale));
        
        -- Calculate total batches to process
        SELECT MAX(batch) INTO v_total_batches FROM etl_bench.data_table;
        
        -- Process each batch and measure performance
        FOR v_batch_id IN 1..v_total_batches LOOP
            
            -- Count rows in this batch for accurate rate calculation
            SELECT COUNT(*) INTO v_rows_processed 
            FROM etl_bench.data_table 
            WHERE batch = v_batch_id;
            
            -- Process Legal Units
            INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
            VALUES (format('Process Legal Units - Batch %s/%s', v_batch_id, v_total_batches), v_rows_processed, true);
            
            v_start_time := clock_timestamp();
            CALL sql_saga.benchmark_reset();
            
            CALL etl_bench.process_legal_units(v_batch_id);
            
            v_end_time := clock_timestamp();
            v_duration := v_end_time - v_start_time;
            v_rows_per_second := CASE WHEN EXTRACT(EPOCH FROM v_duration) > 0 
                                THEN v_rows_processed / EXTRACT(EPOCH FROM v_duration) 
                                ELSE 0 END;
            
            INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
            VALUES (format('Legal Units Complete - %s rows/sec', ROUND(v_rows_per_second)), v_rows_processed, true);
            CALL sql_saga.benchmark_log_and_reset(format('Process Legal Units - Batch %s', v_batch_id));
            
            -- Process Locations
            INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
            VALUES (format('Process Locations - Batch %s/%s', v_batch_id, v_total_batches), v_rows_processed, true);
            
            v_start_time := clock_timestamp();
            CALL sql_saga.benchmark_reset();
            
            CALL etl_bench.process_locations(v_batch_id);
            
            v_end_time := clock_timestamp();
            v_duration := v_end_time - v_start_time;
            v_rows_per_second := CASE WHEN EXTRACT(EPOCH FROM v_duration) > 0 
                                THEN v_rows_processed / EXTRACT(EPOCH FROM v_duration) 
                                ELSE 0 END;
            
            INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
            VALUES (format('Locations Complete - %s rows/sec', ROUND(v_rows_per_second)), v_rows_processed, true);
            CALL sql_saga.benchmark_log_and_reset(format('Process Locations - Batch %s', v_batch_id));
            
            -- Process Statistics
            INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
            VALUES (format('Process Statistics - Batch %s/%s', v_batch_id, v_total_batches), v_rows_processed, true);
            
            v_start_time := clock_timestamp();
            CALL sql_saga.benchmark_reset();
            
            CALL etl_bench.process_statistics(v_batch_id);
            
            v_end_time := clock_timestamp();
            v_duration := v_end_time - v_start_time;
            v_rows_per_second := CASE WHEN EXTRACT(EPOCH FROM v_duration) > 0 
                                THEN v_rows_processed / EXTRACT(EPOCH FROM v_duration) 
                                ELSE 0 END;
            
            INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
            VALUES (format('Statistics Complete - %s rows/sec', ROUND(v_rows_per_second)), v_rows_processed, true);
            CALL sql_saga.benchmark_log_and_reset(format('Process Statistics - Batch %s', v_batch_id));
            
            -- Process Activities
            INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
            VALUES (format('Process Activities - Batch %s/%s', v_batch_id, v_total_batches), v_rows_processed, true);
            
            v_start_time := clock_timestamp();
            CALL sql_saga.benchmark_reset();
            
            CALL etl_bench.process_activities(v_batch_id);
            
            v_end_time := clock_timestamp();
            v_duration := v_end_time - v_start_time;
            v_rows_per_second := CASE WHEN EXTRACT(EPOCH FROM v_duration) > 0 
                                THEN v_rows_processed / EXTRACT(EPOCH FROM v_duration) 
                                ELSE 0 END;
            
            INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
            VALUES (format('Activities Complete - %s rows/sec', ROUND(v_rows_per_second)), v_rows_processed, true);
            CALL sql_saga.benchmark_log_and_reset(format('Process Activities - Batch %s', v_batch_id));
            
            -- Progress reporting for large datasets  
            IF v_batch_id % 1000 = 0 OR v_batch_id = v_total_batches THEN
                RAISE NOTICE 'Processed batch %s/%s for % scale dataset', v_batch_id, v_total_batches, v_dataset_scale;
            END IF;
        END LOOP;
        
        -- Overall performance summary
        v_total_duration := clock_timestamp() - v_total_start_time;
        v_rows_per_second := v_total_rows / EXTRACT(EPOCH FROM v_total_duration);
        
        INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
        VALUES (format('COMPLETE %s SCALE DATASET - %s rows/sec overall', v_dataset_scale, ROUND(v_rows_per_second)), v_total_rows, true);
        
        RAISE NOTICE 'COMPLETED: %s scale dataset in % (% rows/sec overall)', v_dataset_scale, v_total_duration, ROUND(v_rows_per_second);
        
    -- Clear benchmark tables for next test (keep fixture data intact)
    TRUNCATE etl_bench.legal_unit, etl_bench.location, etl_bench.stat_for_unit, etl_bench.activity, etl_bench.ident, etl_bench.data_table;
END;
$$;

--------------------------------------------------------------------------------
\echo '--- Performance Summary Report ---'
--------------------------------------------------------------------------------

\echo '--- Benchmark Results Summary ---'
SELECT 
    event,
    row_count,
    format_duration(timestamp - LAG(timestamp) OVER (ORDER BY seq_id)) as duration,
    CASE 
        WHEN event ~ 'rows/sec' THEN 'THROUGHPUT'
        WHEN event ~ 'Complete' THEN 'RESULT'  
        WHEN event ~ 'Process' THEN 'OPERATION'
        ELSE 'SETUP'
    END as event_type
FROM benchmark 
WHERE is_performance_benchmark = true
ORDER BY seq_id;

\echo '--- Production Scale Performance Summary ---'
SELECT 
    REGEXP_REPLACE(event, '.* (\d+) entities.*', '\1')::int as dataset_size,
    format_duration(timestamp - LAG(timestamp) OVER (ORDER BY seq_id)) as total_duration,
    ROUND(row_count / NULLIF(EXTRACT(EPOCH FROM timestamp - LAG(timestamp) OVER (ORDER BY seq_id)), 0)) as overall_rows_per_sec,
    row_count as total_rows
FROM benchmark 
WHERE is_performance_benchmark = true
  AND event ~ 'COMPLETE DATASET.*entities';

-- Performance monitor data (if available)
\echo '--- Performance Monitor Data (Top Queries by Execution Time) ---'
SELECT 
    event,
    label,
    calls,
    ROUND(total_exec_time::numeric, 2) as exec_time_ms,
    rows,
    CASE WHEN rows > 0 THEN ROUND((rows::numeric / total_exec_time::numeric * 1000), 1) END as rows_per_sec,
    LEFT(query, 100) || '...' as query_preview
FROM benchmark_monitor_log_filtered
WHERE event ~ 'Process|Generate'
ORDER BY total_exec_time DESC
LIMIT 20;

\echo '--- Fixture System Performance Summary ---'
SELECT 
    'Data Loading Method' as metric,
    'Fixture System' as method,
    'Fast CSV-based loading' as description
UNION ALL
SELECT 
    'Generation vs Loading',
    CASE 
        WHEN EXISTS (SELECT 1 FROM sql_saga_fixtures.fixture_registry WHERE fixture_name LIKE 'etl_bench_%') 
        THEN 'Fixtures Available'
        ELSE 'No Fixtures - Would Generate'
    END,
    CASE 
        WHEN EXISTS (SELECT 1 FROM sql_saga_fixtures.fixture_registry WHERE fixture_name LIKE 'etl_bench_%')
        THEN 'Loading: 2-3 seconds vs Generation: 30-60+ minutes'  
        ELSE 'First run generates fixtures, subsequent runs load instantly'
    END;

\echo '--- Updated Fixture Usage Statistics ---'
SELECT 
    fixture_name,
    entities,
    total_rows,
    load_count,
    CASE 
        WHEN last_loaded_at IS NOT NULL 
        THEN EXTRACT(EPOCH FROM clock_timestamp() - last_loaded_at) / 60 
        ELSE NULL 
    END as minutes_since_last_load
FROM sql_saga_fixtures.list_fixtures()
WHERE fixture_name LIKE 'etl_bench_%'
ORDER BY entities;

-- Cleanup options (commented out - user can uncomment if needed)
-- \echo 'To cleanup test fixtures (uncomment if needed):'
-- SELECT sql_saga_fixtures.delete_fixture(fixture_name) FROM sql_saga_fixtures.fixture_registry WHERE fixture_name LIKE 'etl_bench_%test%';

\i sql/include/test_teardown.sql