\i sql/include/test_setup.sql

BEGIN;
\echo '----------------------------------------------------------------------------'
\echo 'Test Suite: `sql_saga.temporal_merge` Data Type Support'
\echo 'Description:'
\echo '  This test suite verifies that `temporal_merge` works correctly with all'
\echo '  supported range types for temporal eras.'
\echo '----------------------------------------------------------------------------'

SET client_min_messages TO WARNING;
CREATE SCHEMA tmt; -- Temporal Merge Types

--------------------------------------------------------------------------------
\echo 'Test Type: INTEGER'
--------------------------------------------------------------------------------
CREATE TABLE tmt.target_int (id int, valid_from int, valid_until int, value text);
SELECT sql_saga.add_era('tmt.target_int', 'valid_from', 'valid_until');
INSERT INTO tmt.target_int VALUES (1, 10, 20, 'Original');

CREATE TEMP TABLE source_int (row_id int, id int, valid_from int, valid_until int, value text);
INSERT INTO source_int VALUES
    (101, 1, 15, 18, 'Patched'), -- Update
    (102, 2, 10, 20, 'New');     -- Insert

CALL sql_saga.temporal_merge(
    p_target_table => 'tmt.target_int'::regclass,
    p_source_table => 'source_int'::regclass,
    p_identity_columns => '{id}'::text[]
);

\echo '--- Expected Final State ---'
SELECT * FROM (VALUES
    (1, 10, 15, 'Original'),
    (1, 15, 18, 'Patched'),
    (1, 18, 20, 'Original'),
    (2, 10, 20, 'New')
) t(id, valid_from, valid_until, value) ORDER BY id, valid_from;
\echo '--- Actual Final State ---'
SELECT * FROM tmt.target_int ORDER BY id, valid_from;

--------------------------------------------------------------------------------
\echo 'Test Type: BIGINT'
--------------------------------------------------------------------------------
CREATE TABLE tmt.target_bigint (id int, valid_from bigint, valid_until bigint, value text);
SELECT sql_saga.add_era('tmt.target_bigint', 'valid_from', 'valid_until');
INSERT INTO tmt.target_bigint VALUES (1, 10, 20, 'Original');

CREATE TEMP TABLE source_bigint (row_id int, id int, valid_from bigint, valid_until bigint, value text);
INSERT INTO source_bigint VALUES
    (101, 1, 15, 18, 'Patched'), -- Update
    (102, 2, 10, 20, 'New');     -- Insert

CALL sql_saga.temporal_merge(
    p_target_table => 'tmt.target_bigint'::regclass,
    p_source_table => 'source_bigint'::regclass,
    p_identity_columns => '{id}'::text[]
);

\echo '--- Expected Final State ---'
SELECT * FROM (VALUES
    (1, 10::bigint, 15::bigint, 'Original'),
    (1, 15::bigint, 18::bigint, 'Patched'),
    (1, 18::bigint, 20::bigint, 'Original'),
    (2, 10::bigint, 20::bigint, 'New')
) t(id, valid_from, valid_until, value) ORDER BY id, valid_from;
\echo '--- Actual Final State ---'
SELECT * FROM tmt.target_bigint ORDER BY id, valid_from;

--------------------------------------------------------------------------------
\echo 'Test Type: NUMERIC'
--------------------------------------------------------------------------------
CREATE TABLE tmt.target_numeric (id int, valid_from numeric, valid_until numeric, value text);
SELECT sql_saga.add_era('tmt.target_numeric', 'valid_from', 'valid_until');
INSERT INTO tmt.target_numeric VALUES (1, 10.0, 20.0, 'Original');

CREATE TEMP TABLE source_numeric (row_id int, id int, valid_from numeric, valid_until numeric, value text);
INSERT INTO source_numeric VALUES
    (101, 1, 15.5, 18.5, 'Patched'), -- Update
    (102, 2, 10.0, 20.0, 'New');     -- Insert

CALL sql_saga.temporal_merge(
    p_target_table => 'tmt.target_numeric'::regclass,
    p_source_table => 'source_numeric'::regclass,
    p_identity_columns => '{id}'::text[]
);

\echo '--- Expected Final State ---'
SELECT * FROM (VALUES
    (1, 10.0::numeric, 15.5::numeric, 'Original'),
    (1, 15.5::numeric, 18.5::numeric, 'Patched'),
    (1, 18.5::numeric, 20.0::numeric, 'Original'),
    (2, 10.0::numeric, 20.0::numeric, 'New')
) t(id, valid_from, valid_until, value) ORDER BY id, valid_from;
\echo '--- Actual Final State ---'
SELECT * FROM tmt.target_numeric ORDER BY id, valid_from;

--------------------------------------------------------------------------------
\echo 'Test Type: TIMESTAMP'
--------------------------------------------------------------------------------
CREATE TABLE tmt.target_timestamp (id int, valid_from timestamp, valid_until timestamp, value text);
SELECT sql_saga.add_era('tmt.target_timestamp', 'valid_from', 'valid_until');
INSERT INTO tmt.target_timestamp VALUES (1, '2024-01-01 10:00', '2024-01-01 20:00', 'Original');

CREATE TEMP TABLE source_timestamp (row_id int, id int, valid_from timestamp, valid_until timestamp, value text);
INSERT INTO source_timestamp VALUES
    (101, 1, '2024-01-01 15:00', '2024-01-01 18:00', 'Patched'), -- Update
    (102, 2, '2024-01-01 10:00', '2024-01-01 20:00', 'New');     -- Insert

CALL sql_saga.temporal_merge(
    p_target_table => 'tmt.target_timestamp'::regclass,
    p_source_table => 'source_timestamp'::regclass,
    p_identity_columns => '{id}'::text[]
);

\echo '--- Expected Final State ---'
SELECT * FROM (VALUES
    (1, '2024-01-01 10:00'::timestamp, '2024-01-01 15:00'::timestamp, 'Original'),
    (1, '2024-01-01 15:00'::timestamp, '2024-01-01 18:00'::timestamp, 'Patched'),
    (1, '2024-01-01 18:00'::timestamp, '2024-01-01 20:00'::timestamp, 'Original'),
    (2, '2024-01-01 10:00'::timestamp, '2024-01-01 20:00'::timestamp, 'New')
) t(id, valid_from, valid_until, value) ORDER BY id, valid_from;
\echo '--- Actual Final State ---'
SELECT * FROM tmt.target_timestamp ORDER BY id, valid_from;

--------------------------------------------------------------------------------
\echo 'Test Type: TIMESTAMPTZ'
--------------------------------------------------------------------------------
CREATE TABLE tmt.target_timestamptz (id int, valid_from timestamptz, valid_until timestamptz, value text);
SELECT sql_saga.add_era('tmt.target_timestamptz', 'valid_from', 'valid_until');
INSERT INTO tmt.target_timestamptz VALUES (1, '2024-01-01 10:00Z', '2024-01-01 20:00Z', 'Original');

CREATE TEMP TABLE source_timestamptz (row_id int, id int, valid_from timestamptz, valid_until timestamptz, value text);
INSERT INTO source_timestamptz VALUES
    (101, 1, '2024-01-01 15:00Z', '2024-01-01 18:00Z', 'Patched'), -- Update
    (102, 2, '2024-01-01 10:00Z', '2024-01-01 20:00Z', 'New');     -- Insert

CALL sql_saga.temporal_merge(
    p_target_table => 'tmt.target_timestamptz'::regclass,
    p_source_table => 'source_timestamptz'::regclass,
    p_identity_columns => '{id}'::text[]
);

\echo '--- Expected Final State ---'
SELECT * FROM (VALUES
    (1, '2024-01-01 10:00Z'::timestamptz, '2024-01-01 15:00Z'::timestamptz, 'Original'),
    (1, '2024-01-01 15:00Z'::timestamptz, '2024-01-01 18:00Z'::timestamptz, 'Patched'),
    (1, '2024-01-01 18:00Z'::timestamptz, '2024-01-01 20:00Z'::timestamptz, 'Original'),
    (2, '2024-01-01 10:00Z'::timestamptz, '2024-01-01 20:00Z'::timestamptz, 'New')
) t(id, valid_from, valid_until, value) ORDER BY id, valid_from;
\echo '--- Actual Final State ---'
SELECT * FROM tmt.target_timestamptz ORDER BY id, valid_from;


ROLLBACK;
\i sql/include/test_teardown.sql
