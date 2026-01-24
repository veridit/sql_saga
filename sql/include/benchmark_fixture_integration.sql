--
-- benchmark_fixture_integration.sql
--
-- Integration helpers for using the fixture system in benchmark tests.
-- Provides standardized fixture names and convenient wrapper functions.
--

\set ECHO none

-- Standard fixture naming conventions
-- Format: {test_type}_{scale}_{variant}
-- Examples:
--   temporal_merge_1K_basic     - Basic temporal merge with 1K entities
--   temporal_merge_10K_with_fk  - Temporal merge 10K entities with foreign keys
--   etl_bench_100K_standard     - ETL benchmark 100K entities, standard config
--   etl_bench_1M_high_batch     - ETL benchmark 1M entities, high batch size

-- Wrapper functions for common benchmark patterns
CREATE OR REPLACE FUNCTION benchmark_ensure_temporal_merge_fixture(
    p_scale text, -- '1K', '10K', '100K', '1M'
    p_variant text DEFAULT 'basic', -- 'basic', 'with_fk', 'large_batch'
    p_auto_generate boolean DEFAULT true
) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    v_fixture_name text;
    v_entities bigint;
    v_include_establishments boolean := true;
BEGIN
    -- Parse scale to entity count
    v_entities := CASE p_scale
        WHEN '1K' THEN 1000
        WHEN '10K' THEN 10000  
        WHEN '100K' THEN 100000
        WHEN '1M' THEN 1000000
        ELSE CAST(p_scale AS bigint) -- Allow direct numeric input
    END;
    
    -- Determine variant configuration
    v_include_establishments := CASE p_variant
        WHEN 'basic' THEN true
        WHEN 'with_fk' THEN true
        WHEN 'parent_only' THEN false
        ELSE true
    END;
    
    v_fixture_name := format('temporal_merge_%s_%s', p_scale, p_variant);
    
    RETURN sql_saga_fixtures.ensure_temporal_merge_fixture(
        v_fixture_name, v_entities, p_auto_generate, v_include_establishments
    );
END;
$$;

CREATE OR REPLACE FUNCTION benchmark_load_temporal_merge_fixture(
    p_scale text, -- '1K', '10K', '100K', '1M' 
    p_variant text DEFAULT 'basic',
    p_target_schema text DEFAULT 'public'
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_fixture_name text;
BEGIN
    v_fixture_name := format('temporal_merge_%s_%s', p_scale, p_variant);
    
    PERFORM sql_saga_fixtures.load_temporal_merge_fixture(v_fixture_name, p_target_schema);
END;
$$;

CREATE OR REPLACE FUNCTION benchmark_ensure_etl_fixture(
    p_scale text, -- '1K', '10K', '100K', '1M'
    p_variant text DEFAULT 'standard', -- 'standard', 'high_batch', 'low_batch'
    p_auto_generate boolean DEFAULT true
) RETURNS boolean LANGUAGE plpgsql AS $$
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
            v_rows_per_batch := 15; -- Same total rows per entity
        ELSE
            v_batches_per_entity := 3;
            v_rows_per_batch := 5;
    END CASE;
    
    v_fixture_name := format('etl_bench_%s_%s', p_scale, p_variant);
    
    RETURN sql_saga_fixtures.ensure_etl_fixture(
        v_fixture_name, v_entities, v_batches_per_entity, v_rows_per_batch, p_auto_generate
    );
END;
$$;

CREATE OR REPLACE FUNCTION benchmark_load_etl_fixture(
    p_scale text,
    p_target_table regclass,
    p_variant text DEFAULT 'standard'
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_fixture_name text;
BEGIN
    v_fixture_name := format('etl_bench_%s_%s', p_scale, p_variant);
    
    PERFORM sql_saga_fixtures.load_etl_fixture(v_fixture_name, p_target_table);
END;
$$;

-- Performance comparison helper
CREATE OR REPLACE FUNCTION benchmark_compare_generation_vs_loading(
    p_scale text DEFAULT '10K',
    p_fixture_type text DEFAULT 'temporal_merge'
) RETURNS TABLE(
    operation text,
    duration_seconds numeric,
    rows_per_second numeric,
    speedup_factor numeric
) LANGUAGE plpgsql AS $$
DECLARE
    v_fixture_name text;
    v_entities bigint;
    v_gen_start timestamptz;
    v_gen_end timestamptz;
    v_gen_duration numeric;
    v_load_start timestamptz;
    v_load_end timestamptz;
    v_load_duration numeric;
    v_total_rows bigint;
    v_gen_rps numeric;
    v_load_rps numeric;
BEGIN
    v_fixture_name := format('%s_%s_perf_test', p_fixture_type, p_scale);
    v_entities := CASE p_scale
        WHEN '1K' THEN 1000
        WHEN '10K' THEN 10000
        WHEN '100K' THEN 100000  
        WHEN '1M' THEN 1000000
        ELSE CAST(p_scale AS bigint)
    END;
    
    -- Clean up any existing fixture
    PERFORM sql_saga_fixtures.delete_fixture(v_fixture_name);
    
    -- Time generation
    RAISE NOTICE 'Testing fixture generation for % %...', p_scale, p_fixture_type;
    v_gen_start := clock_timestamp();
    
    IF p_fixture_type = 'temporal_merge' THEN
        PERFORM sql_saga_fixtures.generate_temporal_merge_fixture(v_fixture_name, v_entities, true, true);
        v_total_rows := v_entities * 2; -- legal_unit + establishment
    ELSE
        PERFORM sql_saga_fixtures.generate_etl_fixture(v_fixture_name, v_entities, 3, 5, true);
        v_total_rows := v_entities * 3 * 5; -- entities * batches * rows_per_batch
    END IF;
    
    v_gen_end := clock_timestamp();
    v_gen_duration := EXTRACT(EPOCH FROM v_gen_end - v_gen_start);
    v_gen_rps := v_total_rows / GREATEST(v_gen_duration, 0.001);
    
    -- Time loading (drop/recreate tables first)
    RAISE NOTICE 'Testing fixture loading for % %...', p_scale, p_fixture_type;
    
    IF p_fixture_type = 'temporal_merge' THEN
        DROP TABLE IF EXISTS public.legal_unit_tm CASCADE;
        DROP TABLE IF EXISTS public.establishment_tm CASCADE;
    ELSE
        DROP TABLE IF EXISTS temp_etl_load_test CASCADE;
        CREATE TABLE temp_etl_load_test (
            row_id bigint, batch int, identity_correlation int,
            legal_unit_id INTEGER, location_id INTEGER,
            merge_statuses jsonb, merge_errors jsonb,
            comment text, tax_ident text, lu_name text,
            physical_address text, postal_address text, activity_code text,
            employees numeric, turnover numeric,
            valid_from date, valid_until date
        );
    END IF;
    
    v_load_start := clock_timestamp();
    
    IF p_fixture_type = 'temporal_merge' THEN
        PERFORM sql_saga_fixtures.load_temporal_merge_fixture(v_fixture_name, 'public');
    ELSE
        PERFORM sql_saga_fixtures.load_etl_fixture(v_fixture_name, 'temp_etl_load_test'::regclass);
    END IF;
    
    v_load_end := clock_timestamp();
    v_load_duration := EXTRACT(EPOCH FROM v_load_end - v_load_start);
    v_load_rps := v_total_rows / GREATEST(v_load_duration, 0.001);
    
    -- Clean up test fixture
    PERFORM sql_saga_fixtures.delete_fixture(v_fixture_name);
    DROP TABLE IF EXISTS temp_etl_load_test;
    
    -- Return results
    RETURN QUERY VALUES 
        ('Generation'::text, v_gen_duration, v_gen_rps, 1.0::numeric),
        ('Loading'::text, v_load_duration, v_load_rps, (v_gen_duration / GREATEST(v_load_duration, 0.001))::numeric);
END;
$$;

-- Batch fixture management for multiple scales
CREATE OR REPLACE FUNCTION benchmark_prepare_all_fixtures(
    p_fixture_type text DEFAULT 'temporal_merge',
    p_scales text[] DEFAULT ARRAY['1K', '10K', '100K'],
    p_force_regenerate boolean DEFAULT false
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_scale text;
    v_start_time timestamptz;
    v_end_time timestamptz;
    v_fixture_count int := 0;
BEGIN
    v_start_time := clock_timestamp();
    
    RAISE NOTICE 'Preparing % fixtures for scales: %', p_fixture_type, p_scales;
    
    FOREACH v_scale IN ARRAY p_scales LOOP
        RAISE NOTICE 'Processing scale %...', v_scale;
        
        IF p_fixture_type = 'temporal_merge' THEN
            IF p_force_regenerate THEN
                PERFORM sql_saga_fixtures.generate_temporal_merge_fixture(
                    format('temporal_merge_%s_basic', v_scale),
                    CASE v_scale
                        WHEN '1K' THEN 1000
                        WHEN '10K' THEN 10000
                        WHEN '100K' THEN 100000
                        WHEN '1M' THEN 1000000
                        ELSE CAST(v_scale AS bigint)
                    END,
                    true, true
                );
            ELSE
                PERFORM benchmark_ensure_temporal_merge_fixture(v_scale, 'basic', true);
            END IF;
        ELSE
            IF p_force_regenerate THEN
                PERFORM sql_saga_fixtures.generate_etl_fixture(
                    format('etl_bench_%s_standard', v_scale),
                    CASE v_scale
                        WHEN '1K' THEN 1000
                        WHEN '10K' THEN 10000
                        WHEN '100K' THEN 100000
                        WHEN '1M' THEN 1000000
                        ELSE CAST(v_scale AS bigint)
                    END,
                    3, 5, true
                );
            ELSE
                PERFORM benchmark_ensure_etl_fixture(v_scale, 'standard', true);
            END IF;
        END IF;
        
        v_fixture_count := v_fixture_count + 1;
    END LOOP;
    
    v_end_time := clock_timestamp();
    
    RAISE NOTICE 'Prepared % % fixtures in %', 
        v_fixture_count, p_fixture_type, (v_end_time - v_start_time);
END;
$$;

-- Fixture health check
CREATE OR REPLACE FUNCTION benchmark_check_fixture_health()
RETURNS TABLE(
    fixture_name text,
    status text,
    issue text
) LANGUAGE plpgsql AS $$
DECLARE
    v_fixture RECORD;
    v_file_exists boolean;
    v_file_path text;
BEGIN
    -- Check all fixtures in registry
    FOR v_fixture IN SELECT * FROM sql_saga_fixtures.fixture_registry LOOP
        v_file_path := 'fixtures/' || v_fixture.fixture_name;
        v_file_exists := true;
        
        -- Check primary files exist
        BEGIN
            IF v_fixture.fixture_type = 'temporal_merge' THEN
                PERFORM pg_stat_file(v_file_path || '_legal_unit_tm.csv');
                -- establishments file is optional based on metadata
            ELSIF v_fixture.fixture_type = 'etl_benchmark' THEN
                PERFORM pg_stat_file(v_file_path || '_data_table.csv');
            END IF;
            PERFORM pg_stat_file(v_file_path || '_schema.sql');
        EXCEPTION WHEN OTHERS THEN
            v_file_exists := false;
        END;
        
        -- Return status
        IF v_file_exists THEN
            RETURN QUERY VALUES (v_fixture.fixture_name, 'OK', NULL::text);
        ELSE
            RETURN QUERY VALUES (v_fixture.fixture_name, 'MISSING_FILES', 'Required fixture files not found on disk');
        END IF;
    END LOOP;
END;
$$;

-- Grant permissions
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO sql_saga_unprivileged_user;

\set ECHO all