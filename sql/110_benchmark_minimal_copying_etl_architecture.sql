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
ETL pattern using updatable views and ID back-propagation from test 071.

Focus: Production-scale volumes (100K, 500K, 1M+ parent records) with full
complexity dependency chains to detect O(n^2) vs O(n*log(n)) scaling behavior.

Pattern tested: legal_unit -> location -> stat_for_unit -> activity (full 071)
Key metric: rows/second throughput for each operation

Architecture: Updatable TEMP VIEWs with back-filling (minimal data copying)
Purpose: Reveal scaling bottlenecks in real ETL workloads
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
\echo '--- Performance Test Data Generation ---'
--------------------------------------------------------------------------------

-- Data generation function for production-scale volumes
CREATE FUNCTION etl_bench.generate_test_data(
    p_total_entities int,
    p_batches_per_entity int DEFAULT 3,
    p_rows_per_entity_per_batch int DEFAULT 5
) RETURNS void LANGUAGE plpgsql AS $function$
DECLARE
    v_entity int;
    v_batch int;
    v_row_num int;
    v_row_id int := 0;
    v_base_date date := '2024-01-01'::date;
    v_activity_codes text[] := ARRAY['manufacturing', 'retail'];
    v_activity_code text;
BEGIN
    -- Clear existing data
    TRUNCATE etl_bench.data_table;
    
    -- Generate test data
    FOR v_entity IN 1..p_total_entities LOOP
        FOR v_batch IN 1..p_batches_per_entity LOOP
            FOR v_row_num IN 1..p_rows_per_entity_per_batch LOOP
                v_row_id := v_row_id + 1;
                v_activity_code := v_activity_codes[(v_entity % 2) + 1];
                
                INSERT INTO etl_bench.data_table (
                    row_id, batch, identity_correlation, comment,
                    tax_ident, lu_name, physical_address, postal_address,
                    activity_code, employees, turnover,
                    valid_from, valid_until
                ) VALUES (
                    v_row_id,
                    v_batch,
                    v_entity,
                    format('Entity %s batch %s row %s', v_entity, v_batch, v_row_num),
                    format('TAX%s', v_entity),
                    format('Company-%s', v_entity),
                    CASE WHEN v_row_num <= 2 THEN format('%s Main St, City %s', v_entity, v_entity) END,
                    CASE WHEN v_row_num <= 3 THEN format('PO Box %s', v_entity) END,
                    CASE WHEN v_row_num <= 4 THEN v_activity_code END,
                    CASE WHEN v_row_num <= 4 THEN (v_entity % 100) + 10 END,
                    CASE WHEN v_row_num <= 5 THEN (v_entity % 1000) * 1000 + 50000 END,
                    v_base_date + ((v_batch - 1) * 90 + (v_row_num - 1) * 30),
                    'infinity'
                );
            END LOOP;
        END LOOP;
        
        -- Progress reporting for large datasets
        IF v_entity % 100000 = 0 THEN
            RAISE NOTICE 'Generated data for % entities (% rows)', v_entity, v_row_id;
        END IF;
    END LOOP;
    
    RAISE NOTICE 'Generated % total rows for % entities in % batches', v_row_id, p_total_entities, p_batches_per_entity;
END;
$function$;

--------------------------------------------------------------------------------
\echo '--- Production Scale Performance Benchmarks ---'
--------------------------------------------------------------------------------

DO $$
DECLARE
    v_dataset_size int := 1000000; -- Production size: 1.1M parent + 0.8M child from client
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
    RAISE NOTICE 'STARTING BENCHMARK: % entities (production-scale)', v_dataset_size;
        
        v_total_start_time := clock_timestamp();
        
        -- Generate test data
        INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
        VALUES (format('Generate Test Data - %s entities', v_dataset_size), v_dataset_size, true);
        
        v_start_time := clock_timestamp();
        CALL sql_saga.benchmark_reset();
        
        PERFORM etl_bench.generate_test_data(v_dataset_size, 3, 5);
        SELECT COUNT(*) INTO v_total_rows FROM etl_bench.data_table;
        
        v_end_time := clock_timestamp();
        v_duration := v_end_time - v_start_time;
        v_rows_per_second := v_total_rows / EXTRACT(EPOCH FROM v_duration);
        
        INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
        VALUES (format('Data Generation Complete - %s rows/sec', ROUND(v_rows_per_second)), v_total_rows, true);
        CALL sql_saga.benchmark_log_and_reset(format('Generate Test Data - %s entities', v_dataset_size));
        
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
                RAISE NOTICE 'Processed batch %s/%s for % entities', v_batch_id, v_total_batches, v_dataset_size;
            END IF;
        END LOOP;
        
        -- Overall performance summary
        v_total_duration := clock_timestamp() - v_total_start_time;
        v_rows_per_second := v_total_rows / EXTRACT(EPOCH FROM v_total_duration);
        
        INSERT INTO benchmark (event, row_count, is_performance_benchmark) 
        VALUES (format('COMPLETE DATASET %s entities - %s rows/sec overall', v_dataset_size, ROUND(v_rows_per_second)), v_total_rows, true);
        
        RAISE NOTICE 'COMPLETED: % entities in % (% rows/sec overall)', v_dataset_size, v_total_duration, ROUND(v_rows_per_second);
        
    -- Clear data for next test
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

\i sql/include/test_teardown.sql