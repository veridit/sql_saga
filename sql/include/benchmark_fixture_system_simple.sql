--
-- benchmark_fixture_system_simple.sql
--
-- Simplified fixture system for benchmark tests that focuses on data generation and loading
-- without complex file operations. This version uses CSV export/import for portability.
--

\set ECHO none

-- Core fixture metadata tracking
CREATE SCHEMA IF NOT EXISTS sql_saga_fixtures;

-- Table to track fixture metadata and versions
CREATE TABLE IF NOT EXISTS sql_saga_fixtures.fixture_registry (
    fixture_name text PRIMARY KEY,
    fixture_type text NOT NULL, -- 'temporal_merge', 'etl_benchmark', etc.
    schema_version text NOT NULL DEFAULT '1.0',
    total_entities bigint NOT NULL,
    total_rows bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_loaded_at timestamptz,
    load_count bigint NOT NULL DEFAULT 0,
    metadata jsonb
);

--------------------------------------------------------------------------------
-- Fast Fixture Generation Functions
--------------------------------------------------------------------------------

-- Generate fixtures for temporal_merge benchmarks (in-memory only version)
CREATE OR REPLACE FUNCTION sql_saga_fixtures.generate_temporal_merge_fixture(
    p_fixture_name text,
    p_total_entities bigint,
    p_force_regenerate boolean DEFAULT false,
    p_include_establishments boolean DEFAULT true
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $function$
DECLARE
    v_start_time timestamptz;
    v_end_time timestamptz;
    v_duration interval;
    v_total_rows bigint := 0;
    v_lu_rows bigint;
    v_est_rows bigint;
    v_rows_per_second numeric;
BEGIN
    -- Check if fixtures already exist (unless forcing regeneration)
    IF NOT p_force_regenerate AND EXISTS (
        SELECT 1 FROM sql_saga_fixtures.fixture_registry 
        WHERE fixture_name = p_fixture_name
    ) THEN
        RAISE NOTICE 'sql_saga: Fixture % already exists. Use p_force_regenerate=true to recreate.', p_fixture_name;
        RETURN;
    END IF;
    
    v_start_time := clock_timestamp();
    
    RAISE NOTICE 'sql_saga: Generating temporal_merge fixture % (% entities)', p_fixture_name, p_total_entities;
    
    -- Create temporary schema for fixture generation
    DROP SCHEMA IF EXISTS temp_fixture_schema CASCADE;
    CREATE SCHEMA temp_fixture_schema;
    
    -- Create legal_unit table with optimized structure
    CREATE UNLOGGED TABLE temp_fixture_schema.legal_unit_tm (
        id INTEGER NOT NULL,
        valid_range daterange NOT NULL,
        name varchar NOT NULL
    );
    
    -- Create establishment table if requested
    IF p_include_establishments THEN
        CREATE UNLOGGED TABLE temp_fixture_schema.establishment_tm (
            id INTEGER NOT NULL,
            valid_range daterange NOT NULL,
            legal_unit_id INTEGER NOT NULL,
            postal_place TEXT NOT NULL
        );
    END IF;
    
    -- Generate legal_unit data using efficient set-based operations
    INSERT INTO temp_fixture_schema.legal_unit_tm (id, valid_range, name)
    SELECT 
        i,
        daterange('2015-01-01', 'infinity', '[)'),
        'Company ' || i
    FROM generate_series(1, p_total_entities) AS i;
    
    GET DIAGNOSTICS v_lu_rows = ROW_COUNT;
    v_total_rows := v_lu_rows;
    
    -- Generate establishment data if requested
    IF p_include_establishments THEN
        INSERT INTO temp_fixture_schema.establishment_tm (id, valid_range, legal_unit_id, postal_place)
        SELECT 
            i,
            daterange('2015-01-01', 'infinity', '[)'),
            i,
            'Shop ' || i
        FROM generate_series(1, p_total_entities) AS i;
        
        GET DIAGNOSTICS v_est_rows = ROW_COUNT;
        v_total_rows := v_total_rows + v_est_rows;
    END IF;
    
    -- Create indexes for performance (these will be included in the dump)
    CREATE INDEX ON temp_fixture_schema.legal_unit_tm USING BTREE (id);
    CREATE INDEX ON temp_fixture_schema.legal_unit_tm USING GIST (valid_range);
    
    IF p_include_establishments THEN
        CREATE INDEX ON temp_fixture_schema.establishment_tm USING BTREE (id);
        CREATE INDEX ON temp_fixture_schema.establishment_tm USING GIST (valid_range);
        CREATE INDEX ON temp_fixture_schema.establishment_tm USING BTREE (legal_unit_id);
    END IF;
    
    -- Analyze tables for accurate statistics
    ANALYZE temp_fixture_schema.legal_unit_tm;
    IF p_include_establishments THEN
        ANALYZE temp_fixture_schema.establishment_tm;
    END IF;
    
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    v_rows_per_second := v_total_rows / GREATEST(EXTRACT(EPOCH FROM v_duration), 0.001);
    
    RAISE NOTICE 'sql_saga: Generated % rows in % (% rows/sec)', v_total_rows, v_duration, ROUND(v_rows_per_second);
    
    -- Export to CSV files if fixtures directory exists
    BEGIN
        EXECUTE format('COPY temp_fixture_schema.legal_unit_tm TO ''fixtures/%s_legal_unit_tm.csv'' WITH (FORMAT csv, HEADER)', p_fixture_name);
        
        IF p_include_establishments THEN
            EXECUTE format('COPY temp_fixture_schema.establishment_tm TO ''fixtures/%s_establishment_tm.csv'' WITH (FORMAT csv, HEADER)', p_fixture_name);
        END IF;
        
        RAISE NOTICE 'sql_saga: Fixture % saved to CSV files', p_fixture_name;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'sql_saga: Could not save to CSV files (fixtures directory may not exist), keeping in-memory only';
    END;
    
    -- Register fixture in metadata registry
    INSERT INTO sql_saga_fixtures.fixture_registry (
        fixture_name, fixture_type, total_entities, total_rows, 
        metadata
    ) VALUES (
        p_fixture_name, 'temporal_merge', p_total_entities, v_total_rows,
        jsonb_build_object(
            'include_establishments', p_include_establishments,
            'generation_duration_seconds', EXTRACT(EPOCH FROM v_duration),
            'rows_per_second', ROUND(v_rows_per_second)
        )
    ) ON CONFLICT (fixture_name) DO UPDATE SET
        total_entities = EXCLUDED.total_entities,
        total_rows = EXCLUDED.total_rows,
        created_at = EXCLUDED.created_at,
        metadata = EXCLUDED.metadata;
    
    -- Keep temporary schema for immediate use (don't drop it yet)
    RAISE NOTICE 'sql_saga: Fixture % ready (data available in temp_fixture_schema)', p_fixture_name;
END;
$function$;

-- Generate fixtures for ETL benchmarks  
CREATE OR REPLACE FUNCTION sql_saga_fixtures.generate_etl_fixture(
    p_fixture_name text,
    p_total_entities bigint,
    p_batches_per_entity int DEFAULT 3,
    p_rows_per_entity_per_batch int DEFAULT 5,
    p_force_regenerate boolean DEFAULT false
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $function$
DECLARE
    v_start_time timestamptz;
    v_end_time timestamptz;
    v_duration interval;
    v_total_rows bigint;
    v_rows_per_second numeric;
BEGIN
    v_total_rows := p_total_entities * p_batches_per_entity * p_rows_per_entity_per_batch;
    
    -- Check if fixtures already exist (unless forcing regeneration)
    IF NOT p_force_regenerate AND EXISTS (
        SELECT 1 FROM sql_saga_fixtures.fixture_registry 
        WHERE fixture_name = p_fixture_name
    ) THEN
        RAISE NOTICE 'sql_saga: Fixture % already exists. Use p_force_regenerate=true to recreate.', p_fixture_name;
        RETURN;
    END IF;
    
    v_start_time := clock_timestamp();
    
    RAISE NOTICE 'sql_saga: Generating ETL fixture % (% entities, % total rows)', 
        p_fixture_name, p_total_entities, v_total_rows;
    
    -- Create temporary table for fixture generation
    DROP TABLE IF EXISTS temp_etl_fixture_data CASCADE;
    CREATE UNLOGGED TABLE temp_etl_fixture_data (
        row_id bigint NOT NULL,
        batch int NOT NULL,
        identity_correlation int NOT NULL,
        legal_unit_id INTEGER,
        location_id INTEGER,
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
        valid_from date NOT NULL,
        valid_until date NOT NULL
    );
    
    -- Generate all data in one efficient set-based INSERT
    INSERT INTO temp_etl_fixture_data (
        row_id, batch, identity_correlation, legal_unit_id, location_id,
        merge_statuses, merge_errors, comment, tax_ident, lu_name,
        physical_address, postal_address, activity_code, 
        employees, turnover, valid_from, valid_until
    )
    SELECT
        ROW_NUMBER() OVER (ORDER BY entity_num, batch_num, row_in_batch_num) as row_id,
        batch_num as batch,
        entity_num as identity_correlation,
        NULL::int as legal_unit_id,
        NULL::int as location_id,
        NULL::jsonb as merge_statuses,
        NULL::jsonb as merge_errors,
        format('Entity %s batch %s row %s', entity_num, batch_num, row_in_batch_num) as comment,
        CASE WHEN row_in_batch_num = 1 THEN format('TAX%s', entity_num) END as tax_ident,
        CASE WHEN row_in_batch_num = 1 THEN format('Company-%s', entity_num) END as lu_name,
        CASE WHEN row_in_batch_num <= 2 THEN format('%s Main St, City %s', entity_num, entity_num) END as physical_address,
        CASE WHEN row_in_batch_num <= 3 THEN format('PO Box %s', entity_num) END as postal_address,
        CASE WHEN row_in_batch_num <= 4 THEN 
            CASE WHEN entity_num % 2 = 1 THEN 'manufacturing' ELSE 'retail' END 
        END as activity_code,
        CASE WHEN row_in_batch_num <= 4 THEN (entity_num % 100) + 10 END as employees,
        CASE WHEN row_in_batch_num <= 5 THEN (entity_num % 1000) * 1000 + 50000 END as turnover,
        DATE '2024-01-01' + ((batch_num - 1) * 90 + (row_in_batch_num - 1) * 30) as valid_from,
        'infinity'::date as valid_until
    FROM 
        generate_series(1, p_total_entities) as entity_num,
        generate_series(1, p_batches_per_entity) as batch_num,
        generate_series(1, p_rows_per_entity_per_batch) as row_in_batch_num;
    
    -- Create indexes for performance
    CREATE INDEX ON temp_etl_fixture_data (batch, identity_correlation);
    CREATE INDEX ON temp_etl_fixture_data (identity_correlation, batch, row_id);
    
    -- Analyze for accurate statistics
    ANALYZE temp_etl_fixture_data;
    
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    v_rows_per_second := v_total_rows / GREATEST(EXTRACT(EPOCH FROM v_duration), 0.001);
    
    RAISE NOTICE 'sql_saga: Generated % rows in % (% rows/sec)', v_total_rows, v_duration, ROUND(v_rows_per_second);
    
    -- Export to CSV file if fixtures directory exists
    BEGIN
        EXECUTE format('COPY temp_etl_fixture_data TO ''fixtures/%s_data_table.csv'' WITH (FORMAT csv, HEADER)', p_fixture_name);
        RAISE NOTICE 'sql_saga: ETL fixture % saved to CSV file', p_fixture_name;
    EXCEPTION WHEN OTHERS THEN
        RAISE NOTICE 'sql_saga: Could not save to CSV file (fixtures directory may not exist), keeping in-memory only';
    END;
    
    -- Register fixture
    INSERT INTO sql_saga_fixtures.fixture_registry (
        fixture_name, fixture_type, total_entities, total_rows,
        metadata
    ) VALUES (
        p_fixture_name, 'etl_benchmark', p_total_entities, v_total_rows,
        jsonb_build_object(
            'batches_per_entity', p_batches_per_entity,
            'rows_per_entity_per_batch', p_rows_per_entity_per_batch,
            'generation_duration_seconds', EXTRACT(EPOCH FROM v_duration),
            'rows_per_second', ROUND(v_rows_per_second)
        )
    ) ON CONFLICT (fixture_name) DO UPDATE SET
        total_entities = EXCLUDED.total_entities,
        total_rows = EXCLUDED.total_rows,
        created_at = EXCLUDED.created_at,
        metadata = EXCLUDED.metadata;
    
    RAISE NOTICE 'sql_saga: ETL fixture % ready (data available in temp_etl_fixture_data)', p_fixture_name;
END;
$function$;

--------------------------------------------------------------------------------
-- Fast Fixture Loading Functions  
--------------------------------------------------------------------------------

-- Load temporal_merge fixtures from CSV or generate in-memory
CREATE OR REPLACE FUNCTION sql_saga_fixtures.load_temporal_merge_fixture(
    p_fixture_name text,
    p_target_schema text DEFAULT 'public'
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $function$
DECLARE
    v_start_time timestamptz;
    v_end_time timestamptz;
    v_duration interval;
    v_total_rows bigint := 0;
    v_lu_rows bigint;
    v_est_rows bigint;
    v_rows_per_second numeric;
    v_registry_row sql_saga_fixtures.fixture_registry%ROWTYPE;
    v_loaded_from_csv boolean := false;
BEGIN
    -- Check if fixture exists in registry
    SELECT * INTO v_registry_row 
    FROM sql_saga_fixtures.fixture_registry 
    WHERE fixture_name = p_fixture_name AND fixture_type = 'temporal_merge';
    
    IF NOT FOUND THEN
        RAISE EXCEPTION 'sql_saga: Temporal merge fixture % not found. Run generate_temporal_merge_fixture() first.', p_fixture_name;
    END IF;
    
    v_start_time := clock_timestamp();
    RAISE NOTICE 'sql_saga: Loading temporal_merge fixture % into schema %', p_fixture_name, p_target_schema;
    
    -- Drop existing tables if they exist
    EXECUTE format('DROP TABLE IF EXISTS %I.legal_unit_tm CASCADE', p_target_schema);
    IF (v_registry_row.metadata->>'include_establishments')::boolean THEN
        EXECUTE format('DROP TABLE IF EXISTS %I.establishment_tm CASCADE', p_target_schema);
    END IF;
    
    -- Create tables with proper schema
    EXECUTE format($sql$
        CREATE TABLE %I.legal_unit_tm (
            id INTEGER NOT NULL,
            valid_range daterange NOT NULL,
            name varchar NOT NULL
        )
    $sql$, p_target_schema);
    
    IF (v_registry_row.metadata->>'include_establishments')::boolean THEN
        EXECUTE format($sql$
            CREATE TABLE %I.establishment_tm (
                id INTEGER NOT NULL,
                valid_range daterange NOT NULL,
                legal_unit_id INTEGER NOT NULL,
                postal_place TEXT NOT NULL
            )
        $sql$, p_target_schema);
    END IF;
    
    -- Try to load from CSV files first, then fallback to temp schema or regeneration
    BEGIN
        EXECUTE format('COPY %I.legal_unit_tm FROM ''fixtures/%s_legal_unit_tm.csv'' WITH (FORMAT csv, HEADER)', 
                       p_target_schema, p_fixture_name);
        GET DIAGNOSTICS v_lu_rows = ROW_COUNT;
        v_total_rows := v_lu_rows;
        v_loaded_from_csv := true;
        
        IF (v_registry_row.metadata->>'include_establishments')::boolean THEN
            EXECUTE format('COPY %I.establishment_tm FROM ''fixtures/%s_establishment_tm.csv'' WITH (FORMAT csv, HEADER)', 
                           p_target_schema, p_fixture_name);
            GET DIAGNOSTICS v_est_rows = ROW_COUNT;
            v_total_rows := v_total_rows + v_est_rows;
        END IF;
        
        RAISE NOTICE 'sql_saga: Loaded fixture from CSV files';
        
    EXCEPTION WHEN OTHERS THEN
        -- CSV loading failed, try to load from temp schema or regenerate
        IF to_regclass('temp_fixture_schema.legal_unit_tm') IS NOT NULL THEN
            RAISE NOTICE 'sql_saga: CSV loading failed, using temp schema data';
            EXECUTE format('INSERT INTO %I.legal_unit_tm SELECT * FROM temp_fixture_schema.legal_unit_tm', p_target_schema);
            GET DIAGNOSTICS v_lu_rows = ROW_COUNT;
            v_total_rows := v_lu_rows;
            
            IF (v_registry_row.metadata->>'include_establishments')::boolean AND to_regclass('temp_fixture_schema.establishment_tm') IS NOT NULL THEN
                EXECUTE format('INSERT INTO %I.establishment_tm SELECT * FROM temp_fixture_schema.establishment_tm', p_target_schema);
                GET DIAGNOSTICS v_est_rows = ROW_COUNT;
                v_total_rows := v_total_rows + v_est_rows;
            END IF;
        ELSE
            -- Regenerate fixture data in-memory
            RAISE NOTICE 'sql_saga: No existing data found, regenerating fixture';
            PERFORM sql_saga_fixtures.generate_temporal_merge_fixture(
                p_fixture_name, 
                v_registry_row.total_entities,
                true, -- force regenerate 
                (v_registry_row.metadata->>'include_establishments')::boolean
            );
            -- Recursively call this function to load the newly generated data
            PERFORM sql_saga_fixtures.load_temporal_merge_fixture(p_fixture_name, p_target_schema);
            RETURN;
        END IF;
    END;
    
    -- Create indexes for performance
    EXECUTE format('CREATE INDEX ON %I.legal_unit_tm USING BTREE (id)', p_target_schema);
    EXECUTE format('CREATE INDEX ON %I.legal_unit_tm USING GIST (valid_range)', p_target_schema);
    
    IF (v_registry_row.metadata->>'include_establishments')::boolean THEN
        EXECUTE format('CREATE INDEX ON %I.establishment_tm USING BTREE (id)', p_target_schema);
        EXECUTE format('CREATE INDEX ON %I.establishment_tm USING GIST (valid_range)', p_target_schema);
        EXECUTE format('CREATE INDEX ON %I.establishment_tm USING BTREE (legal_unit_id)', p_target_schema);
    END IF;
    
    -- Analyze tables for optimal query planning
    EXECUTE format('ANALYZE %I.legal_unit_tm', p_target_schema);
    IF (v_registry_row.metadata->>'include_establishments')::boolean THEN
        EXECUTE format('ANALYZE %I.establishment_tm', p_target_schema);
    END IF;
    
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    v_rows_per_second := v_total_rows / GREATEST(EXTRACT(EPOCH FROM v_duration), 0.001);
    
    -- Update registry stats
    UPDATE sql_saga_fixtures.fixture_registry 
    SET last_loaded_at = v_end_time, load_count = load_count + 1
    WHERE fixture_name = p_fixture_name;
    
    RAISE NOTICE 'sql_saga: Loaded % rows in % (% rows/sec) from %', 
        v_total_rows, v_duration, ROUND(v_rows_per_second),
        CASE WHEN v_loaded_from_csv THEN 'CSV' ELSE 'memory' END;
END;
$function$;

-- Load ETL fixtures from CSV or temp table
CREATE OR REPLACE FUNCTION sql_saga_fixtures.load_etl_fixture(
    p_fixture_name text,
    p_target_table regclass
) RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $function$
DECLARE
    v_start_time timestamptz;
    v_end_time timestamptz;
    v_duration interval;
    v_total_rows bigint;
    v_rows_per_second numeric;
    v_loaded_from_csv boolean := false;
BEGIN
    -- Check if fixture exists
    IF NOT EXISTS (
        SELECT 1 FROM sql_saga_fixtures.fixture_registry 
        WHERE fixture_name = p_fixture_name AND fixture_type = 'etl_benchmark'
    ) THEN
        RAISE EXCEPTION 'sql_saga: ETL fixture % not found. Run generate_etl_fixture() first.', p_fixture_name;
    END IF;
    
    v_start_time := clock_timestamp();
    RAISE NOTICE 'sql_saga: Loading ETL fixture % into %', p_fixture_name, p_target_table;
    
    -- Clear target table
    EXECUTE format('TRUNCATE %s', p_target_table);
    
    -- Try to load from CSV first, then fallback to temp table
    BEGIN
        EXECUTE format('COPY %s FROM ''fixtures/%s_data_table.csv'' WITH (FORMAT csv, HEADER)', 
                       p_target_table, p_fixture_name);
        GET DIAGNOSTICS v_total_rows = ROW_COUNT;
        v_loaded_from_csv := true;
        RAISE NOTICE 'sql_saga: Loaded fixture from CSV file';
        
    EXCEPTION WHEN OTHERS THEN
        -- CSV loading failed, try temp table
        IF to_regclass('temp_etl_fixture_data') IS NOT NULL THEN
            RAISE NOTICE 'sql_saga: CSV loading failed, using temp table data';
            EXECUTE format('INSERT INTO %s SELECT * FROM temp_etl_fixture_data', p_target_table);
            GET DIAGNOSTICS v_total_rows = ROW_COUNT;
        ELSE
            RAISE EXCEPTION 'sql_saga: No fixture data found for % (CSV and temp table both unavailable)', p_fixture_name;
        END IF;
    END;
    
    v_end_time := clock_timestamp();
    v_duration := v_end_time - v_start_time;
    v_rows_per_second := v_total_rows / GREATEST(EXTRACT(EPOCH FROM v_duration), 0.001);
    
    -- Update registry stats
    UPDATE sql_saga_fixtures.fixture_registry 
    SET last_loaded_at = v_end_time, load_count = load_count + 1
    WHERE fixture_name = p_fixture_name;
    
    RAISE NOTICE 'sql_saga: Loaded % rows in % (% rows/sec) from %', 
        v_total_rows, v_duration, ROUND(v_rows_per_second),
        CASE WHEN v_loaded_from_csv THEN 'CSV' ELSE 'memory' END;
END;
$function$;

--------------------------------------------------------------------------------
-- Fixture Management Functions
--------------------------------------------------------------------------------

-- List all available fixtures with metadata
CREATE OR REPLACE FUNCTION sql_saga_fixtures.list_fixtures()
RETURNS TABLE(
    fixture_name text,
    fixture_type text,
    entities bigint,
    total_rows bigint,
    created_at timestamptz,
    last_loaded_at timestamptz,
    load_count bigint,
    generation_time_sec numeric
) LANGUAGE plpgsql SECURITY DEFINER AS $function$
BEGIN
    RETURN QUERY
    SELECT 
        fr.fixture_name,
        fr.fixture_type,
        fr.total_entities as entities,
        fr.total_rows,
        fr.created_at,
        fr.last_loaded_at,
        fr.load_count,
        ROUND((fr.metadata->>'generation_duration_seconds')::numeric, 2) as generation_time_sec
    FROM sql_saga_fixtures.fixture_registry fr
    ORDER BY fr.created_at DESC;
END;
$function$;

-- Check if a specific fixture exists
CREATE OR REPLACE FUNCTION sql_saga_fixtures.fixture_exists(p_fixture_name text)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER AS $function$
BEGIN
    RETURN EXISTS (
        SELECT 1 FROM sql_saga_fixtures.fixture_registry 
        WHERE fixture_name = p_fixture_name
    );
END;
$function$;

-- Delete a fixture (removes from registry only, CSV files remain)
CREATE OR REPLACE FUNCTION sql_saga_fixtures.delete_fixture(p_fixture_name text)
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $function$
BEGIN
    DELETE FROM sql_saga_fixtures.fixture_registry WHERE fixture_name = p_fixture_name;
    
    IF FOUND THEN
        RAISE NOTICE 'sql_saga: Deleted fixture % from registry', p_fixture_name;
    ELSE
        RAISE NOTICE 'sql_saga: Fixture % not found in registry', p_fixture_name;
    END IF;
END;
$function$;

-- Auto-generate fixture if it doesn't exist
CREATE OR REPLACE FUNCTION sql_saga_fixtures.ensure_temporal_merge_fixture(
    p_fixture_name text,
    p_total_entities bigint,
    p_auto_generate boolean DEFAULT true,
    p_include_establishments boolean DEFAULT true
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER AS $function$
BEGIN
    -- Check if fixture exists
    IF sql_saga_fixtures.fixture_exists(p_fixture_name) THEN
        RETURN true;
    END IF;
    
    -- Auto-generate if enabled
    IF p_auto_generate THEN
        RAISE NOTICE 'sql_saga: Auto-generating missing fixture %', p_fixture_name;
        PERFORM sql_saga_fixtures.generate_temporal_merge_fixture(
            p_fixture_name, p_total_entities, false, p_include_establishments
        );
        RETURN true;
    ELSE
        RAISE NOTICE 'sql_saga: Fixture % not found and auto-generation disabled', p_fixture_name;
        RETURN false;
    END IF;
END;
$function$;

-- Auto-generate ETL fixture if it doesn't exist (by scale)
CREATE OR REPLACE FUNCTION sql_saga_fixtures.ensure_etl_fixture(
    p_scale text, -- '1K', '10K', '100K', '1M' or numeric string
    p_variant text DEFAULT 'standard',
    p_auto_generate boolean DEFAULT true
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER AS $function$
DECLARE
    v_fixture_name text;
    v_entities bigint;
    v_batches_per_entity int;
    v_rows_per_batch int;
BEGIN
    -- Parse scale to entity count
    v_entities := CASE p_scale
        WHEN '1K' THEN 1000
        WHEN '10K' THEN 10000
        WHEN '100K' THEN 100000
        WHEN '1M' THEN 1000000
        ELSE CAST(p_scale AS bigint)
    END;
    
    -- Determine variant configuration
    CASE p_variant
        WHEN 'standard' THEN
            v_batches_per_entity := 3;
            v_rows_per_batch := 5;
        WHEN 'high_batch' THEN
            v_batches_per_entity := 10;
            v_rows_per_batch := 5;
        WHEN 'low_batch' THEN
            v_batches_per_entity := 1;
            v_rows_per_batch := 15;
        ELSE
            v_batches_per_entity := 3;
            v_rows_per_batch := 5;
    END CASE;
    
    v_fixture_name := format('etl_bench_%s_%s', p_scale, p_variant);
    
    -- Check if fixture exists
    IF sql_saga_fixtures.fixture_exists(v_fixture_name) THEN
        RETURN true;
    END IF;
    
    -- Auto-generate if enabled
    IF p_auto_generate THEN
        RAISE NOTICE 'sql_saga: Auto-generating missing ETL fixture %', v_fixture_name;
        PERFORM sql_saga_fixtures.generate_etl_fixture(
            v_fixture_name, v_entities, v_batches_per_entity, v_rows_per_batch, false
        );
        RETURN true;
    ELSE
        RAISE NOTICE 'sql_saga: ETL fixture % not found and auto-generation disabled', v_fixture_name;
        RETURN false;
    END IF;
END;
$function$;

-- Auto-generate ETL fixture if it doesn't exist (by exact parameters)
CREATE OR REPLACE FUNCTION sql_saga_fixtures.ensure_etl_fixture_exact(
    p_fixture_name text,
    p_total_entities bigint,
    p_batches_per_entity int DEFAULT 3,
    p_rows_per_entity_per_batch int DEFAULT 5,
    p_auto_generate boolean DEFAULT true
) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER AS $function$
BEGIN
    -- Check if fixture exists
    IF sql_saga_fixtures.fixture_exists(p_fixture_name) THEN
        RETURN true;
    END IF;
    
    -- Auto-generate if enabled
    IF p_auto_generate THEN
        RAISE NOTICE 'sql_saga: Auto-generating missing ETL fixture %', p_fixture_name;
        PERFORM sql_saga_fixtures.generate_etl_fixture(
            p_fixture_name, p_total_entities, p_batches_per_entity, 
            p_rows_per_entity_per_batch, false
        );
        RETURN true;
    ELSE
        RAISE NOTICE 'sql_saga: ETL fixture % not found and auto-generation disabled', p_fixture_name;
        RETURN false;
    END IF;
END;
$function$;

-- Convenience function to get fixture info
CREATE OR REPLACE FUNCTION sql_saga_fixtures.get_fixture_info(p_fixture_name text)
RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER AS $function$
DECLARE
    v_result jsonb;
BEGIN
    SELECT jsonb_build_object(
        'fixture_name', fixture_name,
        'fixture_type', fixture_type,
        'total_entities', total_entities,
        'total_rows', total_rows,
        'created_at', created_at,
        'last_loaded_at', last_loaded_at,
        'load_count', load_count,
        'metadata', metadata
    ) INTO v_result
    FROM sql_saga_fixtures.fixture_registry
    WHERE fixture_name = p_fixture_name;
    
    RETURN COALESCE(v_result, '{"error": "Fixture not found"}'::jsonb);
END;
$function$;

-- Grant permissions to unprivileged user for benchmark tests
GRANT USAGE ON SCHEMA sql_saga_fixtures TO sql_saga_unprivileged_user;
GRANT ALL ON ALL TABLES IN SCHEMA sql_saga_fixtures TO sql_saga_unprivileged_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sql_saga_fixtures TO sql_saga_unprivileged_user;

-- Also grant to public for broader access (adjust as needed for security)
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sql_saga_fixtures TO public;

\set ECHO all