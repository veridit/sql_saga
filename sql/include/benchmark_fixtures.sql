--
-- benchmark_fixtures.sql
--
-- Efficient fixture generation and loading system for benchmark tests.
-- Uses unlogged tables and bulk operations for maximum performance.
--

\set ECHO none

-- Create fixtures directory if it doesn't exist
\! mkdir -p fixtures/

-- Helper to check if fixture files exist
CREATE OR REPLACE FUNCTION check_fixture_exists(p_fixture_name text) 
RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN
    -- Check if fixture directory and files exist
    PERFORM 1 FROM pg_stat_file('fixtures/' || p_fixture_name || '_data_table.dump') s;
    RETURN true;
EXCEPTION WHEN OTHERS THEN
    RETURN false;
END;
$$;

-- Fast bulk data generator using COPY
CREATE OR REPLACE FUNCTION generate_benchmark_fixtures(
    p_fixture_name text,
    p_total_entities int,
    p_batches_per_entity int DEFAULT 3,
    p_rows_per_entity_per_batch int DEFAULT 5,
    p_force_regenerate boolean DEFAULT false
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_fixture_path text;
    v_start_time timestamptz;
    v_end_time timestamptz;
    v_total_rows bigint;
    v_rows_per_second numeric;
BEGIN
    v_fixture_path := 'fixtures/' || p_fixture_name;
    
    -- Check if fixtures already exist (unless forcing regeneration)
    IF NOT p_force_regenerate AND check_fixture_exists(p_fixture_name) THEN
        RAISE NOTICE 'Fixtures already exist for %. Use p_force_regenerate=true to recreate.', p_fixture_name;
        RETURN;
    END IF;
    
    RAISE NOTICE 'Generating benchmark fixtures: % (% entities, % total rows)', 
        p_fixture_name, p_total_entities, (p_total_entities * p_batches_per_entity * p_rows_per_entity_per_batch);
    
    v_start_time := clock_timestamp();
    
    -- Create temporary unlogged table for fast generation
    DROP TABLE IF EXISTS temp_fixture_data CASCADE;
    CREATE UNLOGGED TABLE temp_fixture_data (
        row_id bigint,
        batch int,
        identity_correlation int,
        comment text,
        tax_ident text,
        lu_name text,
        physical_address text,
        postal_address text,
        activity_code text,
        employees numeric,
        turnover numeric,
        valid_from date,
        valid_until date
    );
    
    -- Generate data using efficient Python script via COPY
    RAISE NOTICE 'Starting bulk data generation...';
    
    -- Use COPY FROM PROGRAM for maximum efficiency
    EXECUTE format($$
        COPY temp_fixture_data FROM PROGRAM '
        python3 -c "
import sys
entities = %s
batches_per_entity = %s
rows_per_entity_per_batch = %s
base_date = '2024-01-01'
activity_codes = ['manufacturing', 'retail']

row_id = 0
for entity in range(1, entities + 1):
    for batch in range(1, batches_per_entity + 1):
        for row_num in range(1, rows_per_entity_per_batch + 1):
            row_id += 1
            activity_code = activity_codes[entity %% 2]
            
            # Generate row data
            comment = f'Entity {entity} batch {batch} row {row_num}'
            tax_ident = f'TAX{entity}' if row_num == 1 else ''
            lu_name = f'Company-{entity}' if row_num == 1 else ''
            physical_addr = f'{entity} Main St, City {entity}' if row_num <= 2 else ''
            postal_addr = f'PO Box {entity}' if row_num <= 3 else ''
            activity = activity_code if row_num <= 4 else ''
            employees = (entity %% 100) + 10 if row_num <= 4 else ''
            turnover = (entity %% 1000) * 1000 + 50000 if row_num <= 5 else ''
            valid_from_offset = (batch - 1) * 90 + (row_num - 1) * 30
            
            # Output tab-separated values
            print(f'{row_id}\t{batch}\t{entity}\t{comment}\t{tax_ident}\t{lu_name}\t{physical_addr}\t{postal_addr}\t{activity}\t{employees}\t{turnover}\t2024-01-01\tinfinity')
            
            # Progress reporting
            if row_id %% 1000000 == 0:
                print(f'Generated {row_id} rows...', file=sys.stderr)
        "' WITH (FORMAT csv, DELIMITER E'\t')
    $$, p_total_entities, p_batches_per_entity, p_rows_per_entity_per_batch);
    
    -- Create indexes for performance
    CREATE INDEX ON temp_fixture_data (batch, identity_correlation);
    CREATE INDEX ON temp_fixture_data (identity_correlation, batch, row_id);
    
    -- Get final counts
    SELECT COUNT(*) INTO v_total_rows FROM temp_fixture_data;
    
    v_end_time := clock_timestamp();
    v_rows_per_second := v_total_rows / EXTRACT(EPOCH FROM v_end_time - v_start_time);
    
    RAISE NOTICE 'Generated % rows in % (% rows/sec)', 
        v_total_rows, (v_end_time - v_start_time), ROUND(v_rows_per_second);
    
    -- Export to fixture files using pg_dump for fast loading
    RAISE NOTICE 'Saving fixtures to disk...';
    
    -- Export data table
    EXECUTE format('COPY temp_fixture_data TO ''%s_data_table.csv'' WITH (FORMAT csv, HEADER)', v_fixture_path);
    
    -- Create schema dump (structure only)
    PERFORM system(format('pg_dump -d %s --schema-only --table=temp_fixture_data > %s_schema.sql', 
                         current_database(), v_fixture_path));
                         
    RAISE NOTICE 'Fixtures saved: %_data_table.csv, %_schema.sql', v_fixture_path, v_fixture_path;
    
    -- Cleanup
    DROP TABLE temp_fixture_data;
    
    RAISE NOTICE 'Fixture generation complete for %', p_fixture_name;
END;
$$;

-- Fast fixture loader
CREATE OR REPLACE FUNCTION load_benchmark_fixtures(
    p_fixture_name text,
    p_target_table regclass
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_fixture_path text;
    v_start_time timestamptz;
    v_end_time timestamptz;
    v_total_rows bigint;
    v_rows_per_second numeric;
BEGIN
    v_fixture_path := 'fixtures/' || p_fixture_name;
    
    -- Check if fixtures exist
    IF NOT check_fixture_exists(p_fixture_name) THEN
        RAISE EXCEPTION 'Fixtures not found for %. Run generate_benchmark_fixtures() first.', p_fixture_name;
    END IF;
    
    RAISE NOTICE 'Loading benchmark fixtures: % into %', p_fixture_name, p_target_table;
    v_start_time := clock_timestamp();
    
    -- Clear target table
    EXECUTE format('TRUNCATE %s', p_target_table);
    
    -- Load data using COPY for maximum speed
    EXECUTE format('COPY %s FROM ''%s_data_table.csv'' WITH (FORMAT csv, HEADER)', 
                  p_target_table, v_fixture_path);
    
    -- Get final counts
    EXECUTE format('SELECT COUNT(*) FROM %s', p_target_table) INTO v_total_rows;
    
    v_end_time := clock_timestamp();
    v_rows_per_second := v_total_rows / EXTRACT(EPOCH FROM v_end_time - v_start_time);
    
    RAISE NOTICE 'Loaded % rows in % (% rows/sec)', 
        v_total_rows, (v_end_time - v_start_time), ROUND(v_rows_per_second);
END;
$$;

-- Fixture management helpers
CREATE OR REPLACE FUNCTION list_benchmark_fixtures() 
RETURNS TABLE(fixture_name text, file_size_mb numeric, modified timestamp) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT 
        regexp_replace(filename, '_data_table\.csv$', '') as fixture_name,
        ROUND((size_bytes / 1024.0 / 1024.0)::numeric, 2) as file_size_mb,
        modification as modified
    FROM pg_stat_file_listing('fixtures') 
    WHERE filename LIKE '%_data_table.csv'
    ORDER BY modification DESC;
END;
$$;

CREATE OR REPLACE FUNCTION delete_benchmark_fixtures(p_fixture_name text) 
RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_fixture_path text;
BEGIN
    v_fixture_path := 'fixtures/' || p_fixture_name;
    
    -- Remove fixture files
    PERFORM system(format('rm -f %s_data_table.csv %s_schema.sql', v_fixture_path, v_fixture_path));
    
    RAISE NOTICE 'Deleted fixtures for %', p_fixture_name;
END;
$$;

GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO sql_saga_unprivileged_user;

\set ECHO all