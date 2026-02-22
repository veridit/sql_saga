\i sql/include/test_setup.sql

-- Regression test: executor NULL join clause fix for XOR nullable identity columns.
-- Tables like stat_for_unit have mutually exclusive FKs (legal_unit_id XOR establishment_id).
-- The executor's entity_key_join_clause must use COALESCE for nullable columns,
-- because NULL = NULL evaluates to UNKNOWN, causing DELETE/UPDATE to silently fail
-- and duplicates to accumulate.

\set ECHO all

BEGIN;

CREATE SCHEMA xnk;

-- Parent tables with temporal primary keys
CREATE TABLE xnk.legal_unit (
    id INT NOT NULL,
    valid_range daterange NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    name TEXT,
    PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)
);
SELECT sql_saga.add_era('xnk.legal_unit', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('xnk.legal_unit', ARRAY['id'], 'valid',
    unique_key_name => 'xnk_lu_uk');

CREATE TABLE xnk.establishment (
    id INT NOT NULL,
    valid_range daterange NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    name TEXT,
    PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)
);
SELECT sql_saga.add_era('xnk.establishment', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('xnk.establishment', ARRAY['id'], 'valid',
    unique_key_name => 'xnk_est_uk');

-- Seed parent data
INSERT INTO xnk.legal_unit VALUES (1, '[2024-01-01,infinity)', '2024-01-01', 'infinity', 'LU One');
INSERT INTO xnk.establishment VALUES (1, '[2024-01-01,infinity)', '2024-01-01', 'infinity', 'Est One');

-- stat_for_unit: XOR nullable pattern (legal_unit_id XOR establishment_id)
CREATE TABLE xnk.stat_for_unit (
    stat_definition_id INT NOT NULL,
    legal_unit_id INT,
    establishment_id INT,
    valid_range daterange NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    value BIGINT,
    CONSTRAINT sfu_xor_check CHECK (legal_unit_id IS NOT NULL OR establishment_id IS NOT NULL)
);
SELECT sql_saga.add_era('xnk.stat_for_unit', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('xnk.stat_for_unit',
    ARRAY['stat_definition_id', 'legal_unit_id', 'establishment_id'], 'valid',
    unique_key_name => 'xnk_sfu_uk');

\echo '--- Step 1: SEED legal unit stats (establishment_id = NULL) ---'
CREATE TEMP TABLE source_lu (
    row_id INT, stat_definition_id INT, legal_unit_id INT, establishment_id INT,
    value BIGINT, valid_from DATE, valid_until DATE
) ON COMMIT DROP;
INSERT INTO source_lu VALUES
    (1, 1, 1, NULL, 100, '2024-01-01', 'infinity'),
    (2, 2, 1, NULL, 200, '2024-01-01', 'infinity');

CALL sql_saga.temporal_merge(
    target_table => 'xnk.stat_for_unit',
    source_table => 'source_lu',
    natural_identity_columns => '{stat_definition_id, legal_unit_id, establishment_id}',
    mode => 'MERGE_ENTITY_REPLACE'
);

\echo '--- After SEED LU stats ---'
SELECT stat_definition_id, legal_unit_id, establishment_id, value, valid_from, valid_until
FROM xnk.stat_for_unit ORDER BY stat_definition_id, legal_unit_id, establishment_id;

\echo '--- Step 2: SEED establishment stats (legal_unit_id = NULL) ---'
CREATE TEMP TABLE source_es (
    row_id INT, stat_definition_id INT, legal_unit_id INT, establishment_id INT,
    value BIGINT, valid_from DATE, valid_until DATE
) ON COMMIT DROP;
INSERT INTO source_es VALUES
    (1, 1, NULL, 1, 300, '2024-01-01', 'infinity'),
    (2, 2, NULL, 1, 400, '2024-01-01', 'infinity');

CALL sql_saga.temporal_merge(
    target_table => 'xnk.stat_for_unit',
    source_table => 'source_es',
    natural_identity_columns => '{stat_definition_id, legal_unit_id, establishment_id}',
    mode => 'MERGE_ENTITY_REPLACE'
);

\echo '--- After SEED ES stats (should be 4 rows total) ---'
SELECT stat_definition_id, legal_unit_id, establishment_id, value, valid_from, valid_until
FROM xnk.stat_for_unit ORDER BY stat_definition_id, legal_unit_id, establishment_id;
SELECT count(*) AS row_count FROM xnk.stat_for_unit;

\echo '--- Step 3: UPDATE legal unit stats (tests DELETE/UPDATE with NULL establishment_id) ---'
TRUNCATE source_lu;
INSERT INTO source_lu VALUES
    (1, 1, 1, NULL, 150, '2024-01-01', 'infinity'),
    (2, 2, 1, NULL, 250, '2024-01-01', 'infinity');

CALL sql_saga.temporal_merge(
    target_table => 'xnk.stat_for_unit',
    source_table => 'source_lu',
    natural_identity_columns => '{stat_definition_id, legal_unit_id, establishment_id}',
    mode => 'MERGE_ENTITY_REPLACE'
);

\echo '--- After UPDATE LU stats (should still be 4 rows, values changed to 150/250) ---'
SELECT stat_definition_id, legal_unit_id, establishment_id, value, valid_from, valid_until
FROM xnk.stat_for_unit ORDER BY stat_definition_id, legal_unit_id, establishment_id;
SELECT count(*) AS row_count FROM xnk.stat_for_unit;

\echo '--- Step 4: UPDATE establishment stats (tests DELETE/UPDATE with NULL legal_unit_id) ---'
TRUNCATE source_es;
INSERT INTO source_es VALUES
    (1, 1, NULL, 1, 350, '2024-01-01', 'infinity'),
    (2, 2, NULL, 1, 450, '2024-01-01', 'infinity');

CALL sql_saga.temporal_merge(
    target_table => 'xnk.stat_for_unit',
    source_table => 'source_es',
    natural_identity_columns => '{stat_definition_id, legal_unit_id, establishment_id}',
    mode => 'MERGE_ENTITY_REPLACE'
);

\echo '--- After UPDATE ES stats (should still be 4 rows, values changed to 350/450) ---'
SELECT stat_definition_id, legal_unit_id, establishment_id, value, valid_from, valid_until
FROM xnk.stat_for_unit ORDER BY stat_definition_id, legal_unit_id, establishment_id;
SELECT count(*) AS row_count FROM xnk.stat_for_unit;

\echo '--- Verify: no duplicates accumulated ---'
SELECT stat_definition_id, legal_unit_id, establishment_id, count(*) AS cnt
FROM xnk.stat_for_unit
GROUP BY stat_definition_id, legal_unit_id, establishment_id
HAVING count(*) > 1;

ROLLBACK;

\i sql/include/test_teardown.sql
