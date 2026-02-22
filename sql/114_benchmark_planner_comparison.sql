\i sql/include/test_setup.sql
\i sql/include/benchmark_setup.sql

--------------------------------------------------------------------------------
-- Test 114: PL/pgSQL vs Native Planner Comparison Benchmark
--------------------------------------------------------------------------------
-- Side-by-side comparison of PL/pgSQL and Native (Rust) planners.
-- Tests both NOT NULL key tables (legal_unit, establishment) and
-- XOR nullable key tables (stat_for_unit).
--
-- This directly measures the impact of the partition-by-NULL optimization
-- in the native planner for nullable identity columns.
--------------------------------------------------------------------------------

CREATE SCHEMA planner_bench;
GRANT ALL ON SCHEMA planner_bench TO sql_saga_unprivileged_user;
SET ROLE TO sql_saga_unprivileged_user;

SELECT $$
================================================================================
BENCHMARK: PL/pgSQL vs Native Planner Comparison
================================================================================
Measures planner performance for:
  - NOT NULL key tables (legal_unit, establishment)
  - XOR nullable key tables (stat_for_unit)

Scale: 5K LU, ~5K ES, ~20K stat_for_unit rows
================================================================================
$$ as doc;

--------------------------------------------------------------------------------
\echo '--- Reference Tables ---'
--------------------------------------------------------------------------------
CREATE TABLE planner_bench.stat_definition (
    id int PRIMARY KEY,
    code text UNIQUE NOT NULL
);
INSERT INTO planner_bench.stat_definition VALUES (1, 'employees'), (2, 'turnover');

--------------------------------------------------------------------------------
\echo '--- Temporal Tables ---'
--------------------------------------------------------------------------------
CREATE TABLE planner_bench.legal_unit (
    id serial,
    name text NOT NULL,
    physical_address text,
    valid_range daterange NOT NULL,
    valid_from date GENERATED ALWAYS AS (lower(valid_range)) STORED,
    valid_until date GENERATED ALWAYS AS (upper(valid_range)) STORED
);
SELECT sql_saga.add_era('planner_bench.legal_unit', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(
    table_oid => 'planner_bench.legal_unit'::regclass,
    column_names => ARRAY['id'],
    key_type => 'primary',
    unique_key_name => 'planner_lu_pk');

CREATE TABLE planner_bench.establishment (
    id serial,
    legal_unit_id int NOT NULL,
    address text,
    valid_range daterange NOT NULL,
    valid_from date GENERATED ALWAYS AS (lower(valid_range)) STORED,
    valid_until date GENERATED ALWAYS AS (upper(valid_range)) STORED
);
SELECT sql_saga.add_era('planner_bench.establishment', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(
    table_oid => 'planner_bench.establishment'::regclass,
    column_names => ARRAY['id'],
    key_type => 'primary',
    unique_key_name => 'planner_es_pk');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'planner_bench.establishment'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'planner_bench.legal_unit'::regclass,
    pk_column_names => ARRAY['id']);

CREATE TABLE planner_bench.stat_for_unit (
    legal_unit_id int,
    establishment_id int,
    stat_definition_id int NOT NULL REFERENCES planner_bench.stat_definition(id),
    value numeric NOT NULL,
    valid_range daterange NOT NULL,
    valid_from date GENERATED ALWAYS AS (lower(valid_range)) STORED,
    valid_until date GENERATED ALWAYS AS (upper(valid_range)) STORED,
    CONSTRAINT exactly_one_entity CHECK (
        (legal_unit_id IS NOT NULL AND establishment_id IS NULL) OR
        (legal_unit_id IS NULL AND establishment_id IS NOT NULL)
    )
);
SELECT sql_saga.add_era('planner_bench.stat_for_unit', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(
    table_oid => 'planner_bench.stat_for_unit'::regclass,
    column_names => ARRAY['legal_unit_id', 'establishment_id', 'stat_definition_id'],
    key_type => 'natural',
    unique_key_name => 'planner_stat_nk');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'planner_bench.stat_for_unit'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'planner_bench.legal_unit'::regclass,
    pk_column_names => ARRAY['id']);
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'planner_bench.stat_for_unit'::regclass,
    fk_column_names => ARRAY['establishment_id'],
    pk_table_oid => 'planner_bench.establishment'::regclass,
    pk_column_names => ARRAY['id']);

CREATE INDEX ON planner_bench.stat_for_unit (legal_unit_id) WHERE legal_unit_id IS NOT NULL;
CREATE INDEX ON planner_bench.stat_for_unit (establishment_id) WHERE establishment_id IS NOT NULL;

--------------------------------------------------------------------------------
\echo '--- Benchmark Results Table ---'
--------------------------------------------------------------------------------
CREATE TABLE planner_bench.results (
    planner text NOT NULL,
    operation text NOT NULL,
    row_count int,
    duration_sec numeric(8,2) NOT NULL
);

--------------------------------------------------------------------------------
\echo '--- Data Generation ---'
--------------------------------------------------------------------------------
DO $$
DECLARE
    v_lu_count int := 5000;
    v_es_count int;
BEGIN
    PERFORM setseed(0.42);

    -- Generate LU data directly
    INSERT INTO planner_bench.legal_unit (name, physical_address, valid_range)
    SELECT
        format('Company %s', i),
        format('%s Main St, City %s', i, (i % 100) + 1),
        daterange('2024-01-01', 'infinity')
    FROM generate_series(1, v_lu_count) i;

    -- Generate ES data (roughly 1:1 ratio)
    INSERT INTO planner_bench.establishment (legal_unit_id, address, valid_range)
    SELECT
        lu.id,
        format('Branch %s, Location %s', row_number() OVER (PARTITION BY lu.id), lu.id),
        daterange('2024-01-01', 'infinity')
    FROM planner_bench.legal_unit lu
    CROSS JOIN generate_series(1, CASE WHEN random() < 0.55 THEN 0 WHEN random() < 0.9 THEN 1 ELSE 2 END) s
    WHERE s > 0 OR random() < 0.45;

    SELECT count(*) INTO v_es_count FROM planner_bench.establishment;
    RAISE NOTICE 'Generated % LU, % ES', v_lu_count, v_es_count;

    -- Generate stat_for_unit for LU (2 stats each)
    INSERT INTO planner_bench.stat_for_unit (legal_unit_id, stat_definition_id, value, valid_range)
    SELECT lu.id, sd.id,
        CASE sd.id WHEN 1 THEN (lu.id % 100) + 5 ELSE (lu.id % 1000) * 1000 + 100000 END,
        daterange('2024-01-01', 'infinity')
    FROM planner_bench.legal_unit lu
    CROSS JOIN planner_bench.stat_definition sd;

    -- Generate stat_for_unit for ES (2 stats each)
    INSERT INTO planner_bench.stat_for_unit (establishment_id, stat_definition_id, value, valid_range)
    SELECT es.id, sd.id,
        CASE sd.id WHEN 1 THEN (es.id % 50) + 2 ELSE (es.id % 500) * 1000 + 50000 END,
        daterange('2024-01-01', 'infinity')
    FROM planner_bench.establishment es
    CROSS JOIN planner_bench.stat_definition sd;
END;
$$;

ANALYZE planner_bench.legal_unit;
ANALYZE planner_bench.establishment;
ANALYZE planner_bench.stat_for_unit;

-- Save original data for reset between phases
CREATE TABLE planner_bench.legal_unit_orig AS TABLE planner_bench.legal_unit;
CREATE TABLE planner_bench.establishment_orig AS TABLE planner_bench.establishment;
CREATE TABLE planner_bench.stat_for_unit_orig AS TABLE planner_bench.stat_for_unit;

\echo ''
\echo '--- Entity Counts ---'
SELECT 'legal_unit' as entity, count(*) FROM planner_bench.legal_unit
UNION ALL SELECT 'establishment', count(*) FROM planner_bench.establishment
UNION ALL SELECT 'stat_for_unit', count(*) FROM planner_bench.stat_for_unit
ORDER BY entity;

--------------------------------------------------------------------------------
\echo ''
\echo '================================================================================'
\echo 'PHASE 1: Native Planner (default)'
\echo '================================================================================'
--------------------------------------------------------------------------------
DO $$
DECLARE
    v_start timestamptz;
    v_dur numeric;
    v_rows int;
BEGIN
    RAISE NOTICE 'Running with NATIVE planner...';

    -- Reset native planner cache for clean measurement
    PERFORM sql_saga.temporal_merge_native_cache_reset();

    -- SEED legal_unit
    v_start := clock_timestamp();
    CREATE OR REPLACE TEMP VIEW source_lu_native AS
    SELECT row_number() OVER () AS row_id, lu.id, lu.name, lu.physical_address,
        daterange('2024-07-01', 'infinity') AS valid_range, '2024-07-01'::date AS valid_from, 'infinity'::date AS valid_until
    FROM planner_bench.legal_unit lu;
    CALL sql_saga.temporal_merge(
        target_table => 'planner_bench.legal_unit',
        source_table => 'source_lu_native',
        primary_identity_columns => ARRAY['id']
    );
    v_dur := round(EXTRACT(EPOCH FROM clock_timestamp() - v_start)::numeric, 2);
    SELECT count(*) INTO v_rows FROM planner_bench.legal_unit;
    INSERT INTO planner_bench.results VALUES ('native', 'UPDATE legal_unit', v_rows, v_dur);

    -- SEED stat_for_unit (LU)
    v_start := clock_timestamp();
    CREATE OR REPLACE TEMP VIEW source_stat_lu_native AS
    SELECT row_number() OVER () AS row_id,
        sfu.legal_unit_id, NULL::int AS establishment_id, sfu.stat_definition_id,
        sfu.value + 10 AS value,
        daterange('2024-07-01', 'infinity') AS valid_range, '2024-07-01'::date AS valid_from, 'infinity'::date AS valid_until
    FROM planner_bench.stat_for_unit sfu
    WHERE sfu.legal_unit_id IS NOT NULL;
    CALL sql_saga.temporal_merge(
        target_table => 'planner_bench.stat_for_unit',
        source_table => 'source_stat_lu_native',
        natural_identity_columns => ARRAY['legal_unit_id', 'establishment_id', 'stat_definition_id']
    );
    v_dur := round(EXTRACT(EPOCH FROM clock_timestamp() - v_start)::numeric, 2);
    SELECT count(*) INTO v_rows FROM planner_bench.stat_for_unit WHERE legal_unit_id IS NOT NULL;
    INSERT INTO planner_bench.results VALUES ('native', 'UPDATE stat_for_unit (LU)', v_rows, v_dur);

    -- SEED stat_for_unit (ES)
    v_start := clock_timestamp();
    CREATE OR REPLACE TEMP VIEW source_stat_es_native AS
    SELECT row_number() OVER () AS row_id,
        NULL::int AS legal_unit_id, sfu.establishment_id, sfu.stat_definition_id,
        sfu.value + 10 AS value,
        daterange('2024-07-01', 'infinity') AS valid_range, '2024-07-01'::date AS valid_from, 'infinity'::date AS valid_until
    FROM planner_bench.stat_for_unit sfu
    WHERE sfu.establishment_id IS NOT NULL;
    CALL sql_saga.temporal_merge(
        target_table => 'planner_bench.stat_for_unit',
        source_table => 'source_stat_es_native',
        natural_identity_columns => ARRAY['legal_unit_id', 'establishment_id', 'stat_definition_id']
    );
    v_dur := round(EXTRACT(EPOCH FROM clock_timestamp() - v_start)::numeric, 2);
    SELECT count(*) INTO v_rows FROM planner_bench.stat_for_unit WHERE establishment_id IS NOT NULL;
    INSERT INTO planner_bench.results VALUES ('native', 'UPDATE stat_for_unit (ES)', v_rows, v_dur);
END;
$$;

--------------------------------------------------------------------------------
\echo ''
\echo '--- Resetting data for PL/pgSQL run ---'
--------------------------------------------------------------------------------
-- TRUNCATE all three together to satisfy FK constraints between them
TRUNCATE planner_bench.stat_for_unit, planner_bench.establishment, planner_bench.legal_unit;

-- Re-insert in FK order, listing non-generated columns explicitly
INSERT INTO planner_bench.legal_unit (id, name, physical_address, valid_range)
SELECT id, name, physical_address, valid_range FROM planner_bench.legal_unit_orig;

INSERT INTO planner_bench.establishment (id, legal_unit_id, address, valid_range)
SELECT id, legal_unit_id, address, valid_range FROM planner_bench.establishment_orig;

INSERT INTO planner_bench.stat_for_unit (legal_unit_id, establishment_id, stat_definition_id, value, valid_range)
SELECT legal_unit_id, establishment_id, stat_definition_id, value, valid_range
FROM planner_bench.stat_for_unit_orig;

ANALYZE planner_bench.legal_unit;
ANALYZE planner_bench.establishment;
ANALYZE planner_bench.stat_for_unit;

--------------------------------------------------------------------------------
\echo ''
\echo '================================================================================'
\echo 'PHASE 2: PL/pgSQL Planner'
\echo '================================================================================'
--------------------------------------------------------------------------------
SET sql_saga.temporal_merge.use_plpgsql_planner = true;

DO $$
DECLARE
    v_start timestamptz;
    v_dur numeric;
    v_rows int;
BEGIN
    RAISE NOTICE 'Running with PL/pgSQL planner...';

    -- UPDATE legal_unit
    v_start := clock_timestamp();
    CREATE OR REPLACE TEMP VIEW source_lu_plpgsql AS
    SELECT row_number() OVER () AS row_id, lu.id, lu.name, lu.physical_address,
        daterange('2024-07-01', 'infinity') AS valid_range, '2024-07-01'::date AS valid_from, 'infinity'::date AS valid_until
    FROM planner_bench.legal_unit lu;
    CALL sql_saga.temporal_merge(
        target_table => 'planner_bench.legal_unit',
        source_table => 'source_lu_plpgsql',
        primary_identity_columns => ARRAY['id']
    );
    v_dur := round(EXTRACT(EPOCH FROM clock_timestamp() - v_start)::numeric, 2);
    SELECT count(*) INTO v_rows FROM planner_bench.legal_unit;
    INSERT INTO planner_bench.results VALUES ('plpgsql', 'UPDATE legal_unit', v_rows, v_dur);

    -- UPDATE stat_for_unit (LU)
    v_start := clock_timestamp();
    CREATE OR REPLACE TEMP VIEW source_stat_lu_plpgsql AS
    SELECT row_number() OVER () AS row_id,
        sfu.legal_unit_id, NULL::int AS establishment_id, sfu.stat_definition_id,
        sfu.value + 10 AS value,
        daterange('2024-07-01', 'infinity') AS valid_range, '2024-07-01'::date AS valid_from, 'infinity'::date AS valid_until
    FROM planner_bench.stat_for_unit sfu
    WHERE sfu.legal_unit_id IS NOT NULL;
    CALL sql_saga.temporal_merge(
        target_table => 'planner_bench.stat_for_unit',
        source_table => 'source_stat_lu_plpgsql',
        natural_identity_columns => ARRAY['legal_unit_id', 'establishment_id', 'stat_definition_id']
    );
    v_dur := round(EXTRACT(EPOCH FROM clock_timestamp() - v_start)::numeric, 2);
    SELECT count(*) INTO v_rows FROM planner_bench.stat_for_unit WHERE legal_unit_id IS NOT NULL;
    INSERT INTO planner_bench.results VALUES ('plpgsql', 'UPDATE stat_for_unit (LU)', v_rows, v_dur);

    -- UPDATE stat_for_unit (ES)
    v_start := clock_timestamp();
    CREATE OR REPLACE TEMP VIEW source_stat_es_plpgsql AS
    SELECT row_number() OVER () AS row_id,
        NULL::int AS legal_unit_id, sfu.establishment_id, sfu.stat_definition_id,
        sfu.value + 10 AS value,
        daterange('2024-07-01', 'infinity') AS valid_range, '2024-07-01'::date AS valid_from, 'infinity'::date AS valid_until
    FROM planner_bench.stat_for_unit sfu
    WHERE sfu.establishment_id IS NOT NULL;
    CALL sql_saga.temporal_merge(
        target_table => 'planner_bench.stat_for_unit',
        source_table => 'source_stat_es_plpgsql',
        natural_identity_columns => ARRAY['legal_unit_id', 'establishment_id', 'stat_definition_id']
    );
    v_dur := round(EXTRACT(EPOCH FROM clock_timestamp() - v_start)::numeric, 2);
    SELECT count(*) INTO v_rows FROM planner_bench.stat_for_unit WHERE establishment_id IS NOT NULL;
    INSERT INTO planner_bench.results VALUES ('plpgsql', 'UPDATE stat_for_unit (ES)', v_rows, v_dur);
END;
$$;

RESET sql_saga.temporal_merge.use_plpgsql_planner;

--------------------------------------------------------------------------------
\echo ''
\echo '================================================================================'
\echo 'COMPARISON RESULTS'
\echo '================================================================================'
--------------------------------------------------------------------------------
-- Output to performance file (timings are variable)
\set perf_file expected/performance/114_benchmark_planner_comparison_report.log
\pset format unaligned
\pset fieldsep ','
\o :perf_file
SELECT
    n.operation,
    n.row_count,
    n.duration_sec AS native_sec,
    p.duration_sec AS plpgsql_sec,
    CASE WHEN n.duration_sec > 0
        THEN round(p.duration_sec / n.duration_sec, 2)
        ELSE NULL
    END AS native_speedup
FROM planner_bench.results n
JOIN planner_bench.results p
    ON n.operation = p.operation
    AND n.planner = 'native'
    AND p.planner = 'plpgsql'
ORDER BY n.operation;
\o
\pset format aligned
\pset fieldsep ''
\echo 'Performance data written to expected/performance/114_benchmark_planner_comparison_report.log'

-- Display summary for test output (stable columns only)
\echo ''
\echo '--- Comparison Summary ---'
SELECT
    n.operation,
    n.row_count,
    CASE
        WHEN n.duration_sec <= p.duration_sec THEN 'NATIVE faster'
        ELSE 'PLPGSQL faster'
    END AS winner
FROM planner_bench.results n
JOIN planner_bench.results p
    ON n.operation = p.operation
    AND n.planner = 'native'
    AND p.planner = 'plpgsql'
ORDER BY n.operation;

--------------------------------------------------------------------------------
-- CLEANUP
--------------------------------------------------------------------------------
RESET ROLE;
DROP SCHEMA planner_bench CASCADE;

\i sql/include/benchmark_teardown.sql
\i sql/include/test_teardown.sql
