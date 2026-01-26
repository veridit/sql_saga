\i sql/include/test_setup.sql
\i sql/include/benchmark_setup.sql

--------------------------------------------------------------------------------
-- Test 112: Realistic SEED + UPDATE Benchmark
--------------------------------------------------------------------------------
-- Models real-world StatBus-like ETL workloads:
-- - SEED phase: Bulk initial load of LU, ES, external_ident, stat_for_unit
-- - UPDATE phase: Incremental updates (base data + stat variables)
--
-- Based on Norway production data: 1.1M LU, 0.8M ES (ratio ~0.73:1)
-- Development scale: 10K LU, ~10K ES
--------------------------------------------------------------------------------

CREATE SCHEMA seed_bench;
GRANT ALL ON SCHEMA seed_bench TO sql_saga_unprivileged_user;

SET ROLE TO sql_saga_unprivileged_user;

SELECT $$
================================================================================
BENCHMARK: Realistic SEED + UPDATE Pattern
================================================================================
This benchmark measures performance for production-like ETL workloads:

SEED Phase:
  - legal_unit: 10K entities (1 row each)
  - establishment: ~10K entities (FK to LU, distribution-based)
  - external_ident: ~20K rows (1 per LU + 1 per ES, NON-temporal)
  - stat_for_unit: ~40K rows (2 stats per LU + 2 stats per ES)

UPDATE Phase:
  - 5 batches of 1280 entities each
  - 5% base data changes, 75% stat variable changes

Target: Measure rows/sec for each operation type
================================================================================
$$ as doc;

--------------------------------------------------------------------------------
\echo ''
\echo '--- Reference Tables (non-temporal) ---'
--------------------------------------------------------------------------------

CREATE TABLE seed_bench.ident_type (
    id int PRIMARY KEY,
    code text UNIQUE NOT NULL
);
INSERT INTO seed_bench.ident_type VALUES (1, 'tax_ident');

CREATE TABLE seed_bench.stat_definition (
    id int PRIMARY KEY,
    code text UNIQUE NOT NULL
);
INSERT INTO seed_bench.stat_definition VALUES (1, 'employees'), (2, 'turnover');

--------------------------------------------------------------------------------
\echo '--- Core Temporal Tables ---'
--------------------------------------------------------------------------------

-- Legal Unit (parent entity)
CREATE TABLE seed_bench.legal_unit (
    id serial,
    name text NOT NULL,
    physical_address text,
    valid_range daterange NOT NULL,
    valid_from date GENERATED ALWAYS AS (lower(valid_range)) STORED,
    valid_until date GENERATED ALWAYS AS (upper(valid_range)) STORED
);

SELECT sql_saga.add_era('seed_bench.legal_unit', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(
    table_oid => 'seed_bench.legal_unit'::regclass,
    column_names => ARRAY['id'],
    key_type => 'primary',
    unique_key_name => 'legal_unit_pk');

-- Establishment (child entity with temporal FK to LU)
CREATE TABLE seed_bench.establishment (
    id serial,
    legal_unit_id int NOT NULL,
    address text,
    valid_range daterange NOT NULL,
    valid_from date GENERATED ALWAYS AS (lower(valid_range)) STORED,
    valid_until date GENERATED ALWAYS AS (upper(valid_range)) STORED
);

SELECT sql_saga.add_era('seed_bench.establishment', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(
    table_oid => 'seed_bench.establishment'::regclass,
    column_names => ARRAY['id'],
    key_type => 'primary',
    unique_key_name => 'establishment_pk');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'seed_bench.establishment'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'seed_bench.legal_unit'::regclass,
    pk_column_names => ARRAY['id']);

--------------------------------------------------------------------------------
\echo '--- External Identifiers (NON-temporal - IDs persist over time) ---'
--------------------------------------------------------------------------------

CREATE TABLE seed_bench.external_ident (
    id serial PRIMARY KEY,
    legal_unit_id int,
    establishment_id int,
    ident_type_id int NOT NULL REFERENCES seed_bench.ident_type(id),
    ident_value text NOT NULL UNIQUE,
    CONSTRAINT exactly_one_entity CHECK (
        (legal_unit_id IS NOT NULL AND establishment_id IS NULL) OR
        (legal_unit_id IS NULL AND establishment_id IS NOT NULL)
    )
);

-- Regular FK constraints (not temporal - ident persists even if entity timeline changes)
-- Note: These reference the entity tables but don't use PERIOD
CREATE INDEX ON seed_bench.external_ident (legal_unit_id) WHERE legal_unit_id IS NOT NULL;
CREATE INDEX ON seed_bench.external_ident (establishment_id) WHERE establishment_id IS NOT NULL;

--------------------------------------------------------------------------------
\echo '--- Stat Variables (temporal, on both LU and ES) ---'
--------------------------------------------------------------------------------

CREATE TABLE seed_bench.stat_for_unit (
    legal_unit_id int,
    establishment_id int,
    stat_definition_id int NOT NULL REFERENCES seed_bench.stat_definition(id),
    value numeric NOT NULL,
    valid_range daterange NOT NULL,
    valid_from date GENERATED ALWAYS AS (lower(valid_range)) STORED,
    valid_until date GENERATED ALWAYS AS (upper(valid_range)) STORED,
    CONSTRAINT exactly_one_entity CHECK (
        (legal_unit_id IS NOT NULL AND establishment_id IS NULL) OR
        (legal_unit_id IS NULL AND establishment_id IS NOT NULL)
    )
);

SELECT sql_saga.add_era('seed_bench.stat_for_unit', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');

-- Natural key: entity + stat_definition (no surrogate id)
SELECT sql_saga.add_unique_key(
    table_oid => 'seed_bench.stat_for_unit'::regclass,
    column_names => ARRAY['legal_unit_id', 'establishment_id', 'stat_definition_id'],
    key_type => 'natural',
    unique_key_name => 'stat_for_unit_nk');

-- Temporal FK to legal_unit (for LU stats)
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'seed_bench.stat_for_unit'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'seed_bench.legal_unit'::regclass,
    pk_column_names => ARRAY['id']);

-- Temporal FK to establishment (for ES stats)
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'seed_bench.stat_for_unit'::regclass,
    fk_column_names => ARRAY['establishment_id'],
    pk_table_oid => 'seed_bench.establishment'::regclass,
    pk_column_names => ARRAY['id']);

--------------------------------------------------------------------------------
\echo '--- Performance Indexes ---'
--------------------------------------------------------------------------------

CREATE INDEX ON seed_bench.legal_unit USING GIST (valid_range);
CREATE INDEX ON seed_bench.establishment USING GIST (valid_range);
CREATE INDEX ON seed_bench.establishment (legal_unit_id);
CREATE INDEX ON seed_bench.stat_for_unit USING GIST (valid_range);
CREATE INDEX ON seed_bench.stat_for_unit (legal_unit_id) WHERE legal_unit_id IS NOT NULL;
CREATE INDEX ON seed_bench.stat_for_unit (establishment_id) WHERE establishment_id IS NOT NULL;

--------------------------------------------------------------------------------
\echo '--- Staging Table ---'
--------------------------------------------------------------------------------

-- Central staging table for all incoming data
CREATE TABLE seed_bench.staging (
    row_id serial PRIMARY KEY,
    batch int NOT NULL DEFAULT 1,
    entity_type text NOT NULL,  -- 'lu', 'es', 'stat_lu', 'stat_es'
    -- Identity correlation
    identity_correlation int NOT NULL,
    founding_id int,  -- For new entity creation
    -- Back-propagated IDs
    legal_unit_id int,
    establishment_id int,
    -- LU data
    lu_name text,
    lu_address text,
    lu_tax_ident text,
    -- ES data
    es_address text,
    es_tax_ident text,
    es_lu_correlation int,  -- Links ES to its parent LU
    -- Stat data
    stat_code text,
    stat_value numeric,
    -- Temporal
    valid_from date DEFAULT '2024-01-01',
    valid_until date DEFAULT 'infinity',
    -- Feedback
    merge_status text,
    merge_error text
);

CREATE INDEX ON seed_bench.staging (batch, entity_type);
CREATE INDEX ON seed_bench.staging (identity_correlation);
CREATE INDEX ON seed_bench.staging (es_lu_correlation) WHERE es_lu_correlation IS NOT NULL;

--------------------------------------------------------------------------------
\echo '--- Benchmark Log Table ---'
--------------------------------------------------------------------------------

CREATE TABLE seed_bench.benchmark_log (
    step text NOT NULL,
    row_count int,
    duration interval NOT NULL
);

--------------------------------------------------------------------------------
\echo '--- Data Generators ---'
--------------------------------------------------------------------------------

-- Generate LU seed data (1 row per entity)
CREATE FUNCTION seed_bench.generate_lu_seed_data(p_count int) 
RETURNS TABLE(lu_count int, es_distribution jsonb) 
LANGUAGE plpgsql AS $function$
DECLARE
    v_r float;
    v_es_count int;
    v_lu_id int;
BEGIN
    -- Set seed for reproducible distribution
    PERFORM setseed(0.42);
    
    -- Clear staging
    DELETE FROM seed_bench.staging WHERE entity_type IN ('lu', 'es');
    
    -- Generate LU rows with ES count assignment
    FOR v_lu_id IN 1..p_count LOOP
        v_r := random();
        
        -- Assign ES count based on Norway-realistic distribution
        v_es_count := CASE 
            WHEN v_r < 0.55 THEN 0                              -- 55%: no ES
            WHEN v_r < 0.90 THEN 1                              -- 35%: single ES
            WHEN v_r < 0.98 THEN 2 + floor(random() * 4)::int   -- 8%: 2-5 ES
            WHEN v_r < 0.995 THEN 6 + floor(random() * 15)::int -- 1.5%: 6-20 ES
            ELSE 21 + floor(random() * 180)::int                -- 0.5%: 21-200 ES (big chains)
        END;
        
        -- Insert LU staging row
        INSERT INTO seed_bench.staging (
            entity_type, identity_correlation, founding_id,
            lu_name, lu_address, lu_tax_ident,
            valid_from, valid_until
        ) VALUES (
            'lu', v_lu_id, v_lu_id,
            format('Company %s', v_lu_id),
            format('%s Main Street, City %s', v_lu_id, (v_lu_id % 100) + 1),
            format('TAX-LU-%s', v_lu_id),
            '2024-01-01', 'infinity'
        );
        
        -- Generate ES staging rows for this LU
        FOR v_es_idx IN 1..v_es_count LOOP
            INSERT INTO seed_bench.staging (
                entity_type, identity_correlation, founding_id,
                es_address, es_tax_ident, es_lu_correlation,
                valid_from, valid_until
            ) VALUES (
                'es', 
                p_count + (v_lu_id - 1) * 200 + v_es_idx,  -- Unique ES correlation
                p_count + (v_lu_id - 1) * 200 + v_es_idx,
                format('Branch %s, Location %s', v_es_idx, v_lu_id),
                format('TAX-ES-%s-%s', v_lu_id, v_es_idx),
                v_lu_id,  -- Links to parent LU
                '2024-01-01', 'infinity'
            );
        END LOOP;
    END LOOP;
    
    -- Return summary
    RETURN QUERY
    SELECT 
        (SELECT count(*)::int FROM seed_bench.staging WHERE entity_type = 'lu') as lu_count,
        jsonb_build_object(
            'total_es', (SELECT count(*) FROM seed_bench.staging WHERE entity_type = 'es'),
            'lu_with_0_es', (SELECT count(*) FROM seed_bench.staging s1 
                WHERE s1.entity_type = 'lu' 
                AND NOT EXISTS (SELECT 1 FROM seed_bench.staging s2 
                    WHERE s2.entity_type = 'es' AND s2.es_lu_correlation = s1.identity_correlation)),
            'lu_with_1_es', (SELECT count(*) FROM (
                SELECT es_lu_correlation FROM seed_bench.staging WHERE entity_type = 'es'
                GROUP BY es_lu_correlation HAVING count(*) = 1) x),
            'lu_with_multi_es', (SELECT count(*) FROM (
                SELECT es_lu_correlation FROM seed_bench.staging WHERE entity_type = 'es'
                GROUP BY es_lu_correlation HAVING count(*) > 1) x)
        ) as es_distribution;
END;
$function$;

-- Generate stat staging data for entities
CREATE FUNCTION seed_bench.generate_stat_seed_data(p_entity_type text)
RETURNS int
LANGUAGE plpgsql AS $function$
DECLARE
    v_count int := 0;
BEGIN
    IF p_entity_type = 'lu' THEN
        -- Generate 2 stat rows per LU (employees + turnover)
        INSERT INTO seed_bench.staging (
            entity_type, identity_correlation,
            legal_unit_id, stat_code, stat_value,
            valid_from, valid_until
        )
        SELECT 
            'stat_lu',
            s.identity_correlation * 10 + stat.ord,  -- Unique correlation
            s.legal_unit_id,
            stat.code,
            CASE stat.code 
                WHEN 'employees' THEN (s.identity_correlation % 100) + 5
                WHEN 'turnover' THEN (s.identity_correlation % 1000) * 1000 + 100000
            END,
            s.valid_from, s.valid_until
        FROM seed_bench.staging s
        CROSS JOIN (VALUES (1, 'employees'), (2, 'turnover')) AS stat(ord, code)
        WHERE s.entity_type = 'lu' AND s.legal_unit_id IS NOT NULL;
        
        GET DIAGNOSTICS v_count = ROW_COUNT;
        
    ELSIF p_entity_type = 'es' THEN
        -- Generate 2 stat rows per ES (employees + turnover)
        INSERT INTO seed_bench.staging (
            entity_type, identity_correlation,
            establishment_id, stat_code, stat_value,
            valid_from, valid_until
        )
        SELECT 
            'stat_es',
            s.identity_correlation * 10 + stat.ord,
            s.establishment_id,
            stat.code,
            CASE stat.code 
                WHEN 'employees' THEN (s.identity_correlation % 50) + 2
                WHEN 'turnover' THEN (s.identity_correlation % 500) * 1000 + 50000
            END,
            s.valid_from, s.valid_until
        FROM seed_bench.staging s
        CROSS JOIN (VALUES (1, 'employees'), (2, 'turnover')) AS stat(ord, code)
        WHERE s.entity_type = 'es' AND s.establishment_id IS NOT NULL;
        
        GET DIAGNOSTICS v_count = ROW_COUNT;
    END IF;
    
    RETURN v_count;
END;
$function$;

--------------------------------------------------------------------------------
\echo '--- SEED Procedures ---'
--------------------------------------------------------------------------------

-- SEED legal_unit
CREATE PROCEDURE seed_bench.seed_legal_units()
LANGUAGE plpgsql AS $procedure$
BEGIN
    -- Create view for temporal_merge
    CREATE OR REPLACE TEMP VIEW source_lu AS
    SELECT
        row_id,
        founding_id,
        legal_unit_id AS id,
        lu_name AS name,
        lu_address AS physical_address,
        daterange(valid_from, valid_until) AS valid_range,
        valid_from,
        valid_until
    FROM seed_bench.staging
    WHERE entity_type = 'lu';
    
    -- Merge into legal_unit
    CALL sql_saga.temporal_merge(
        target_table => 'seed_bench.legal_unit',
        source_table => 'source_lu',
        primary_identity_columns => ARRAY['id'],
        founding_id_column => 'founding_id',
        update_source_with_identity => true
    );
    
    -- Back-propagate generated IDs to staging
    UPDATE seed_bench.staging dst
    SET legal_unit_id = src.id
    FROM (
        SELECT row_id, id 
        FROM source_lu 
        WHERE id IS NOT NULL
    ) src
    WHERE dst.row_id = src.row_id;
    
    -- Propagate LU IDs to ES staging rows
    UPDATE seed_bench.staging es
    SET legal_unit_id = lu.legal_unit_id
    FROM seed_bench.staging lu
    WHERE es.entity_type = 'es'
      AND lu.entity_type = 'lu'
      AND es.es_lu_correlation = lu.identity_correlation;
END;
$procedure$;

-- SEED external_ident for LU
CREATE PROCEDURE seed_bench.seed_external_ident_lu()
LANGUAGE plpgsql AS $procedure$
BEGIN
    INSERT INTO seed_bench.external_ident (legal_unit_id, ident_type_id, ident_value)
    SELECT 
        legal_unit_id,
        1,  -- tax_ident
        lu_tax_ident
    FROM seed_bench.staging
    WHERE entity_type = 'lu' AND legal_unit_id IS NOT NULL;
END;
$procedure$;

-- SEED stat_for_unit for LU
CREATE PROCEDURE seed_bench.seed_stats_lu()
LANGUAGE plpgsql AS $procedure$
BEGIN
    CREATE OR REPLACE TEMP VIEW source_stat_lu AS
    SELECT
        row_id,
        legal_unit_id,
        NULL::int AS establishment_id,
        sd.id AS stat_definition_id,
        stat_value AS value,
        daterange(s.valid_from, s.valid_until) AS valid_range,
        s.valid_from,
        s.valid_until
    FROM seed_bench.staging s
    JOIN seed_bench.stat_definition sd ON sd.code = s.stat_code
    WHERE s.entity_type = 'stat_lu';
    
    CALL sql_saga.temporal_merge(
        target_table => 'seed_bench.stat_for_unit',
        source_table => 'source_stat_lu',
        natural_identity_columns => ARRAY['legal_unit_id', 'establishment_id', 'stat_definition_id']
    );
END;
$procedure$;

-- SEED establishment
CREATE PROCEDURE seed_bench.seed_establishments()
LANGUAGE plpgsql AS $procedure$
BEGIN
    CREATE OR REPLACE TEMP VIEW source_es AS
    SELECT
        row_id,
        founding_id,
        establishment_id AS id,
        legal_unit_id,
        es_address AS address,
        daterange(valid_from, valid_until) AS valid_range,
        valid_from,
        valid_until
    FROM seed_bench.staging
    WHERE entity_type = 'es' AND legal_unit_id IS NOT NULL;
    
    CALL sql_saga.temporal_merge(
        target_table => 'seed_bench.establishment',
        source_table => 'source_es',
        primary_identity_columns => ARRAY['id'],
        founding_id_column => 'founding_id',
        update_source_with_identity => true
    );
    
    -- Back-propagate generated ES IDs to staging
    UPDATE seed_bench.staging dst
    SET establishment_id = src.id
    FROM (
        SELECT row_id, id 
        FROM source_es 
        WHERE id IS NOT NULL
    ) src
    WHERE dst.row_id = src.row_id;
END;
$procedure$;

-- SEED external_ident for ES
CREATE PROCEDURE seed_bench.seed_external_ident_es()
LANGUAGE plpgsql AS $procedure$
BEGIN
    INSERT INTO seed_bench.external_ident (establishment_id, ident_type_id, ident_value)
    SELECT 
        establishment_id,
        1,  -- tax_ident
        es_tax_ident
    FROM seed_bench.staging
    WHERE entity_type = 'es' AND establishment_id IS NOT NULL;
END;
$procedure$;

-- SEED stat_for_unit for ES
CREATE PROCEDURE seed_bench.seed_stats_es()
LANGUAGE plpgsql AS $procedure$
BEGIN
    CREATE OR REPLACE TEMP VIEW source_stat_es AS
    SELECT
        row_id,
        NULL::int AS legal_unit_id,
        establishment_id,
        sd.id AS stat_definition_id,
        stat_value AS value,
        daterange(s.valid_from, s.valid_until) AS valid_range,
        s.valid_from,
        s.valid_until
    FROM seed_bench.staging s
    JOIN seed_bench.stat_definition sd ON sd.code = s.stat_code
    WHERE s.entity_type = 'stat_es';
    
    CALL sql_saga.temporal_merge(
        target_table => 'seed_bench.stat_for_unit',
        source_table => 'source_stat_es',
        natural_identity_columns => ARRAY['legal_unit_id', 'establishment_id', 'stat_definition_id']
    );
END;
$procedure$;

--------------------------------------------------------------------------------
\echo '--- UPDATE Phase Generators ---'
--------------------------------------------------------------------------------

-- Generate UPDATE batch data
-- Simulates incremental updates: some base data changes, mostly stat changes
CREATE FUNCTION seed_bench.generate_update_batch(
    p_batch_num int,
    p_batch_size int DEFAULT 1280,
    p_base_change_pct int DEFAULT 5,
    p_stat_change_pct int DEFAULT 75
) RETURNS TABLE(
    lu_base_changes int,
    lu_stat_changes int,
    es_stat_changes int
) LANGUAGE plpgsql AS $function$
DECLARE
    v_valid_from date;
    v_lu_base_count int;
    v_lu_stat_count int;
    v_es_stat_count int;
BEGIN
    -- Each batch represents a new time period (quarterly updates)
    v_valid_from := ('2024-01-01'::date + (p_batch_num * interval '3 months'))::date;
    
    -- Use deterministic seed based on batch number
    PERFORM setseed(0.42 + p_batch_num * 0.01);
    
    -- Clear previous batch data from staging
    DELETE FROM seed_bench.staging WHERE batch = p_batch_num;
    
    -- Calculate counts
    v_lu_base_count := (p_batch_size * p_base_change_pct / 100);
    v_lu_stat_count := (p_batch_size * p_stat_change_pct / 100);
    -- ES stat changes: proportional to ES/LU ratio (~1.4)
    v_es_stat_count := (v_lu_stat_count * 1.4)::int;
    
    -- Generate LU base data changes (name/address updates)
    INSERT INTO seed_bench.staging (
        batch, entity_type, identity_correlation,
        legal_unit_id, lu_name, lu_address,
        valid_from, valid_until
    )
    SELECT 
        p_batch_num,
        'lu_update',
        lu.id,
        lu.id,
        format('Company %s (Updated Q%s)', lu.id, p_batch_num),
        format('%s Updated Street, City %s', lu.id, (lu.id % 100) + 1),
        v_valid_from,
        'infinity'::date
    FROM seed_bench.legal_unit lu
    WHERE lu.id IN (
        SELECT id FROM seed_bench.legal_unit 
        ORDER BY random() 
        LIMIT v_lu_base_count
    );
    
    -- Generate LU stat changes (employees/turnover updates)
    INSERT INTO seed_bench.staging (
        batch, entity_type, identity_correlation,
        legal_unit_id, stat_code, stat_value,
        valid_from, valid_until
    )
    SELECT 
        p_batch_num,
        'stat_lu_update',
        lu.id * 10 + stat.ord,
        lu.id,
        stat.code,
        CASE stat.code 
            WHEN 'employees' THEN (lu.id % 100) + 5 + p_batch_num * 2  -- Slight growth
            WHEN 'turnover' THEN (lu.id % 1000) * 1000 + 100000 + p_batch_num * 10000
        END,
        v_valid_from,
        'infinity'::date
    FROM (
        SELECT id FROM seed_bench.legal_unit 
        ORDER BY random() 
        LIMIT v_lu_stat_count
    ) lu
    CROSS JOIN (VALUES (1, 'employees'), (2, 'turnover')) AS stat(ord, code);
    
    -- Generate ES stat changes
    INSERT INTO seed_bench.staging (
        batch, entity_type, identity_correlation,
        establishment_id, stat_code, stat_value,
        valid_from, valid_until
    )
    SELECT 
        p_batch_num,
        'stat_es_update',
        es.id * 10 + stat.ord,
        es.id,
        stat.code,
        CASE stat.code 
            WHEN 'employees' THEN (es.id % 50) + 2 + p_batch_num
            WHEN 'turnover' THEN (es.id % 500) * 1000 + 50000 + p_batch_num * 5000
        END,
        v_valid_from,
        'infinity'::date
    FROM (
        SELECT id FROM seed_bench.establishment 
        ORDER BY random() 
        LIMIT v_es_stat_count
    ) es
    CROSS JOIN (VALUES (1, 'employees'), (2, 'turnover')) AS stat(ord, code);
    
    RETURN QUERY SELECT 
        v_lu_base_count,
        v_lu_stat_count * 2,  -- 2 stats per LU
        v_es_stat_count * 2;  -- 2 stats per ES
END;
$function$;

-- UPDATE legal_unit base data
CREATE PROCEDURE seed_bench.update_legal_units(p_batch int)
LANGUAGE plpgsql AS $procedure$
BEGIN
    -- Create temp table instead of view to capture p_batch value
    DROP TABLE IF EXISTS pg_temp.source_lu_update;
    CREATE TEMP TABLE source_lu_update AS
    SELECT
        row_id,
        legal_unit_id AS id,
        lu_name AS name,
        lu_address AS physical_address,
        daterange(valid_from, valid_until) AS valid_range,
        valid_from,
        valid_until
    FROM seed_bench.staging
    WHERE entity_type = 'lu_update' AND batch = p_batch;
    
    CALL sql_saga.temporal_merge(
        target_table => 'seed_bench.legal_unit',
        source_table => 'source_lu_update',
        primary_identity_columns => ARRAY['id']
    );
END;
$procedure$;

-- UPDATE stat_for_unit for LU
CREATE PROCEDURE seed_bench.update_stats_lu(p_batch int)
LANGUAGE plpgsql AS $procedure$
BEGIN
    DROP TABLE IF EXISTS pg_temp.source_stat_lu_update;
    CREATE TEMP TABLE source_stat_lu_update AS
    SELECT
        row_id,
        legal_unit_id,
        NULL::int AS establishment_id,
        sd.id AS stat_definition_id,
        stat_value AS value,
        daterange(s.valid_from, s.valid_until) AS valid_range,
        s.valid_from,
        s.valid_until
    FROM seed_bench.staging s
    JOIN seed_bench.stat_definition sd ON sd.code = s.stat_code
    WHERE s.entity_type = 'stat_lu_update' AND s.batch = p_batch;
    
    CALL sql_saga.temporal_merge(
        target_table => 'seed_bench.stat_for_unit',
        source_table => 'source_stat_lu_update',
        natural_identity_columns => ARRAY['legal_unit_id', 'establishment_id', 'stat_definition_id']
    );
END;
$procedure$;

-- UPDATE stat_for_unit for ES
CREATE PROCEDURE seed_bench.update_stats_es(p_batch int)
LANGUAGE plpgsql AS $procedure$
BEGIN
    DROP TABLE IF EXISTS pg_temp.source_stat_es_update;
    CREATE TEMP TABLE source_stat_es_update AS
    SELECT
        row_id,
        NULL::int AS legal_unit_id,
        establishment_id,
        sd.id AS stat_definition_id,
        stat_value AS value,
        daterange(s.valid_from, s.valid_until) AS valid_range,
        s.valid_from,
        s.valid_until
    FROM seed_bench.staging s
    JOIN seed_bench.stat_definition sd ON sd.code = s.stat_code
    WHERE s.entity_type = 'stat_es_update' AND s.batch = p_batch;
    
    CALL sql_saga.temporal_merge(
        target_table => 'seed_bench.stat_for_unit',
        source_table => 'source_stat_es_update',
        natural_identity_columns => ARRAY['legal_unit_id', 'establishment_id', 'stat_definition_id']
    );
END;
$procedure$;

--------------------------------------------------------------------------------
\echo ''
\echo '================================================================================'
\echo 'SEED PHASE'
\echo '================================================================================'
--------------------------------------------------------------------------------

DO $$
DECLARE
    v_lu_count int := 10000;  -- Development scale (change to 50000/100000 for production)
    v_start timestamptz;
    v_duration interval;
    v_rows int;
    v_es_count int;
    v_lu_result record;
    v_total_start timestamptz;
BEGIN
    v_total_start := clock_timestamp();
    RAISE NOTICE 'Starting SEED phase with % legal units', v_lu_count;
    
    ------------------------------------------------------------------------
    -- Step 1: Generate LU and ES staging data
    ------------------------------------------------------------------------
    v_start := clock_timestamp();
    
    SELECT * INTO v_lu_result FROM seed_bench.generate_lu_seed_data(v_lu_count);
    SELECT count(*) INTO v_es_count FROM seed_bench.staging WHERE entity_type = 'es';
    
    v_duration := clock_timestamp() - v_start;
    
    INSERT INTO seed_bench.benchmark_log VALUES 
        ('Generate staging', v_lu_result.lu_count + v_es_count, v_duration);
    
    ANALYZE seed_bench.staging;
    
    ------------------------------------------------------------------------
    -- Step 2: SEED legal_unit
    ------------------------------------------------------------------------
    v_start := clock_timestamp();
    CALL seed_bench.seed_legal_units();
    v_duration := clock_timestamp() - v_start;
    SELECT count(*) INTO v_rows FROM seed_bench.legal_unit;
    INSERT INTO seed_bench.benchmark_log VALUES ('SEED legal_unit', v_rows, v_duration);
    
    ------------------------------------------------------------------------
    -- Step 3: SEED external_ident for LU
    ------------------------------------------------------------------------
    v_start := clock_timestamp();
    CALL seed_bench.seed_external_ident_lu();
    v_duration := clock_timestamp() - v_start;
    SELECT count(*) INTO v_rows FROM seed_bench.external_ident WHERE legal_unit_id IS NOT NULL;
    INSERT INTO seed_bench.benchmark_log VALUES ('INSERT external_ident (LU)', v_rows, v_duration);
    
    ------------------------------------------------------------------------
    -- Step 4: Generate and SEED stat_for_unit for LU
    ------------------------------------------------------------------------
    v_start := clock_timestamp();
    PERFORM seed_bench.generate_stat_seed_data('lu');
    CALL seed_bench.seed_stats_lu();
    v_duration := clock_timestamp() - v_start;
    SELECT count(*) INTO v_rows FROM seed_bench.stat_for_unit WHERE legal_unit_id IS NOT NULL;
    INSERT INTO seed_bench.benchmark_log VALUES ('SEED stat_for_unit (LU)', v_rows, v_duration);
    
    ------------------------------------------------------------------------
    -- Step 5: SEED establishment
    ------------------------------------------------------------------------
    v_start := clock_timestamp();
    CALL seed_bench.seed_establishments();
    v_duration := clock_timestamp() - v_start;
    SELECT count(*) INTO v_rows FROM seed_bench.establishment;
    INSERT INTO seed_bench.benchmark_log VALUES ('SEED establishment', v_rows, v_duration);
    
    ------------------------------------------------------------------------
    -- Step 6: SEED external_ident for ES
    ------------------------------------------------------------------------
    v_start := clock_timestamp();
    CALL seed_bench.seed_external_ident_es();
    v_duration := clock_timestamp() - v_start;
    SELECT count(*) INTO v_rows FROM seed_bench.external_ident WHERE establishment_id IS NOT NULL;
    INSERT INTO seed_bench.benchmark_log VALUES ('INSERT external_ident (ES)', v_rows, v_duration);
    
    ------------------------------------------------------------------------
    -- Step 7: Generate and SEED stat_for_unit for ES
    ------------------------------------------------------------------------
    v_start := clock_timestamp();
    PERFORM seed_bench.generate_stat_seed_data('es');
    CALL seed_bench.seed_stats_es();
    v_duration := clock_timestamp() - v_start;
    SELECT count(*) INTO v_rows FROM seed_bench.stat_for_unit WHERE establishment_id IS NOT NULL;
    INSERT INTO seed_bench.benchmark_log VALUES ('SEED stat_for_unit (ES)', v_rows, v_duration);
    
    -- Total time
    INSERT INTO seed_bench.benchmark_log VALUES 
        ('TOTAL SEED', NULL, clock_timestamp() - v_total_start);
END;
$$;

--------------------------------------------------------------------------------
\echo ''
\echo '================================================================================'
\echo 'UPDATE PHASE'
\echo '================================================================================'
--------------------------------------------------------------------------------

DO $$
DECLARE
    v_batch_count int := 5;
    v_batch_size int := 1280;
    v_batch int;
    v_start timestamptz;
    v_duration interval;
    v_rows int;
    v_batch_result record;
    v_total_start timestamptz;
    v_total_lu_base int := 0;
    v_total_lu_stat int := 0;
    v_total_es_stat int := 0;
BEGIN
    v_total_start := clock_timestamp();
    RAISE NOTICE 'Starting UPDATE phase: % batches of ~% entities each', v_batch_count, v_batch_size;
    
    FOR v_batch IN 1..v_batch_count LOOP
        RAISE NOTICE 'Processing batch %...', v_batch;
        
        ------------------------------------------------------------------------
        -- Generate batch data
        ------------------------------------------------------------------------
        v_start := clock_timestamp();
        SELECT * INTO v_batch_result 
        FROM seed_bench.generate_update_batch(v_batch, v_batch_size);
        v_duration := clock_timestamp() - v_start;
        
        v_total_lu_base := v_total_lu_base + v_batch_result.lu_base_changes;
        v_total_lu_stat := v_total_lu_stat + v_batch_result.lu_stat_changes;
        v_total_es_stat := v_total_es_stat + v_batch_result.es_stat_changes;
        
        ANALYZE seed_bench.staging;
        
        ------------------------------------------------------------------------
        -- UPDATE legal_unit base data
        ------------------------------------------------------------------------
        v_start := clock_timestamp();
        CALL seed_bench.update_legal_units(v_batch);
        v_duration := clock_timestamp() - v_start;
        SELECT count(*) INTO v_rows FROM seed_bench.staging 
        WHERE entity_type = 'lu_update' AND batch = v_batch;
        INSERT INTO seed_bench.benchmark_log VALUES 
            (format('UPDATE[%s] legal_unit', v_batch), v_rows, v_duration);
        
        ------------------------------------------------------------------------
        -- UPDATE stat_for_unit for LU
        ------------------------------------------------------------------------
        v_start := clock_timestamp();
        CALL seed_bench.update_stats_lu(v_batch);
        v_duration := clock_timestamp() - v_start;
        SELECT count(*) INTO v_rows FROM seed_bench.staging 
        WHERE entity_type = 'stat_lu_update' AND batch = v_batch;
        INSERT INTO seed_bench.benchmark_log VALUES 
            (format('UPDATE[%s] stat_for_unit (LU)', v_batch), v_rows, v_duration);
        
        ------------------------------------------------------------------------
        -- UPDATE stat_for_unit for ES
        ------------------------------------------------------------------------
        v_start := clock_timestamp();
        CALL seed_bench.update_stats_es(v_batch);
        v_duration := clock_timestamp() - v_start;
        SELECT count(*) INTO v_rows FROM seed_bench.staging 
        WHERE entity_type = 'stat_es_update' AND batch = v_batch;
        INSERT INTO seed_bench.benchmark_log VALUES 
            (format('UPDATE[%s] stat_for_unit (ES)', v_batch), v_rows, v_duration);
    END LOOP;
    
    -- Total UPDATE time
    INSERT INTO seed_bench.benchmark_log VALUES 
        ('TOTAL UPDATE', v_total_lu_base + v_total_lu_stat + v_total_es_stat, 
         clock_timestamp() - v_total_start);
    
    RAISE NOTICE 'UPDATE phase complete. Total changes: % LU base, % LU stats, % ES stats',
        v_total_lu_base, v_total_lu_stat, v_total_es_stat;
END;
$$;

--------------------------------------------------------------------------------
\echo ''
\echo '--- SEED Performance Results (see expected/performance/ for timing details) ---'
--------------------------------------------------------------------------------

-- Output timing to performance file (variable between runs)
\set perf_file expected/performance/112_benchmark_seed_and_update.log
\pset format unaligned
\pset fieldsep ','
\o :perf_file

SELECT 
    step,
    row_count,
    round(EXTRACT(EPOCH FROM duration)::numeric, 2) as seconds,
    CASE WHEN row_count > 0 
        THEN round(row_count / EXTRACT(EPOCH FROM duration))
        ELSE NULL
    END as rows_per_sec
FROM seed_bench.benchmark_log
ORDER BY 
    CASE 
        -- SEED phase
        WHEN step = 'Generate staging' THEN 1
        WHEN step = 'SEED legal_unit' THEN 2
        WHEN step = 'INSERT external_ident (LU)' THEN 3
        WHEN step = 'SEED stat_for_unit (LU)' THEN 4
        WHEN step = 'SEED establishment' THEN 5
        WHEN step = 'INSERT external_ident (ES)' THEN 6
        WHEN step = 'SEED stat_for_unit (ES)' THEN 7
        WHEN step = 'TOTAL SEED' THEN 8
        -- UPDATE phase (batches 1-5)
        WHEN step LIKE 'UPDATE[1]%' THEN 10 + position('legal_unit' in step) + position('stat_for_unit (LU)' in step) * 2 + position('stat_for_unit (ES)' in step) * 3
        WHEN step LIKE 'UPDATE[2]%' THEN 20 + position('legal_unit' in step) + position('stat_for_unit (LU)' in step) * 2 + position('stat_for_unit (ES)' in step) * 3
        WHEN step LIKE 'UPDATE[3]%' THEN 30 + position('legal_unit' in step) + position('stat_for_unit (LU)' in step) * 2 + position('stat_for_unit (ES)' in step) * 3
        WHEN step LIKE 'UPDATE[4]%' THEN 40 + position('legal_unit' in step) + position('stat_for_unit (LU)' in step) * 2 + position('stat_for_unit (ES)' in step) * 3
        WHEN step LIKE 'UPDATE[5]%' THEN 50 + position('legal_unit' in step) + position('stat_for_unit (LU)' in step) * 2 + position('stat_for_unit (ES)' in step) * 3
        WHEN step = 'TOTAL UPDATE' THEN 99
        ELSE 100
    END;

\o
\pset format aligned
\pset fieldsep ''

\echo 'Performance data written to expected/performance/112_benchmark_seed_and_update.log'

--------------------------------------------------------------------------------
\echo ''
\echo '--- Entity Counts ---'
--------------------------------------------------------------------------------

SELECT 'legal_unit' as entity, count(*) as count FROM seed_bench.legal_unit
UNION ALL
SELECT 'establishment', count(*) FROM seed_bench.establishment
UNION ALL
SELECT 'external_ident', count(*) FROM seed_bench.external_ident
UNION ALL
SELECT 'stat_for_unit', count(*) FROM seed_bench.stat_for_unit
ORDER BY entity;

--------------------------------------------------------------------------------
\echo ''
\echo '--- ES Distribution Verification ---'
--------------------------------------------------------------------------------

SELECT 
    CASE 
        WHEN es_count = 0 THEN '0 ES (LU alone)'
        WHEN es_count = 1 THEN '1 ES'
        WHEN es_count BETWEEN 2 AND 5 THEN '2-5 ES'
        WHEN es_count BETWEEN 6 AND 20 THEN '6-20 ES'
        ELSE '21+ ES (big chains)'
    END as category,
    count(*) as lu_count,
    round(100.0 * count(*) / sum(count(*)) OVER (), 1) as percentage
FROM (
    SELECT 
        lu.id,
        count(es.id) as es_count
    FROM seed_bench.legal_unit lu
    LEFT JOIN seed_bench.establishment es ON es.legal_unit_id = lu.id
    GROUP BY lu.id
) sub
GROUP BY 1
ORDER BY min(es_count);

--------------------------------------------------------------------------------
-- CLEANUP
--------------------------------------------------------------------------------
RESET ROLE;
DROP SCHEMA seed_bench CASCADE;

\i sql/include/benchmark_teardown.sql
\i sql/include/test_teardown.sql
