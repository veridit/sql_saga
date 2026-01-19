\i sql/include/test_setup.sql

BEGIN;
\echo '----------------------------------------------------------------------------'
\echo 'Test Suite: `sql_saga.temporal_merge` Range-Only Tables'
\echo 'Description:'
\echo '  This test suite verifies that `temporal_merge` works correctly with tables'
\echo '  that have ONLY a range column (no valid_from/valid_until/valid_to columns).'
\echo '  This validates the new valid_range-first API where synchronized columns'
\echo '  are optional.'
\echo '----------------------------------------------------------------------------'

SET client_min_messages TO NOTICE;
CREATE SCHEMA tmro; -- Temporal Merge Range Only

--------------------------------------------------------------------------------
\echo 'Scenario 1: Basic PATCH with daterange (range-only target)'
--------------------------------------------------------------------------------
CREATE TABLE tmro.target_date (
    id int,
    valid_range daterange NOT NULL,
    value text,
    PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)
);
SELECT sql_saga.add_era('tmro.target_date', 'valid_range');
SELECT sql_saga.add_unique_key('tmro.target_date', ARRAY['id']);

INSERT INTO tmro.target_date (id, valid_range, value) VALUES
    (1, '[2024-01-01, 2024-12-31)', 'Original');

\echo '--- Target: Initial State ---'
TABLE tmro.target_date ORDER BY id, valid_range;

-- Source also uses range-only
CREATE TEMP TABLE source_date_1 (row_id int, id int, valid_range daterange, value text);
INSERT INTO source_date_1 VALUES
    (101, 1, '[2024-03-01, 2024-06-01)', 'Patched'),  -- Update middle portion
    (102, 2, '[2024-01-01, 2024-12-31)', 'New');      -- Insert new entity

\echo '--- Source: Data to merge ---'
TABLE source_date_1 ORDER BY row_id;

CALL sql_saga.temporal_merge(
    target_table => 'tmro.target_date'::regclass,
    source_table => 'source_date_1'::regclass,
    primary_identity_columns => '{id}'::text[]
);

\echo '--- Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Expected Final State ---'
SELECT * FROM (VALUES
    (1, '[2024-01-01, 2024-03-01)'::daterange, 'Original'),
    (1, '[2024-03-01, 2024-06-01)'::daterange, 'Patched'),
    (1, '[2024-06-01, 2024-12-31)'::daterange, 'Original'),
    (2, '[2024-01-01, 2024-12-31)'::daterange, 'New')
) t(id, valid_range, value) ORDER BY id, valid_range;

\echo '--- Actual Final State ---'
TABLE tmro.target_date ORDER BY id, valid_range;

--------------------------------------------------------------------------------
\echo 'Scenario 2: Basic PATCH with tstzrange (range-only target)'
--------------------------------------------------------------------------------
CREATE TABLE tmro.target_tstz (
    id int,
    valid_range tstzrange NOT NULL,
    value text,
    PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)
);
SELECT sql_saga.add_era('tmro.target_tstz', 'valid_range');
SELECT sql_saga.add_unique_key('tmro.target_tstz', ARRAY['id']);

INSERT INTO tmro.target_tstz (id, valid_range, value) VALUES
    (1, '[2024-01-01 00:00:00+00, 2024-12-31 00:00:00+00)', 'Original');

\echo '--- Target: Initial State ---'
TABLE tmro.target_tstz ORDER BY id, valid_range;

CREATE TEMP TABLE source_tstz (row_id int, id int, valid_range tstzrange, value text);
INSERT INTO source_tstz VALUES
    (101, 1, '[2024-03-01 00:00:00+00, 2024-06-01 00:00:00+00)', 'Patched');

\echo '--- Source: Data to merge ---'
TABLE source_tstz ORDER BY row_id;

CALL sql_saga.temporal_merge(
    target_table => 'tmro.target_tstz'::regclass,
    source_table => 'source_tstz'::regclass,
    primary_identity_columns => '{id}'::text[]
);

\echo '--- Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Actual Final State ---'
TABLE tmro.target_tstz ORDER BY id, valid_range;

--------------------------------------------------------------------------------
\echo 'Scenario 3: REPLACE mode with range-only table'
--------------------------------------------------------------------------------
SAVEPOINT scenario_3;

TRUNCATE tmro.target_date;
INSERT INTO tmro.target_date (id, valid_range, value) VALUES
    (1, '[2024-01-01, 2024-12-31)', 'Original');

CREATE TEMP TABLE source_replace (row_id int, id int, valid_range daterange, value text) ON COMMIT DROP;
INSERT INTO source_replace VALUES
    (101, 1, '[2024-03-01, 2024-06-01)', 'Replaced');

CALL sql_saga.temporal_merge(
    target_table => 'tmro.target_date'::regclass,
    source_table => 'source_replace'::regclass,
    primary_identity_columns => '{id}'::text[],
    mode => 'MERGE_ENTITY_REPLACE'
);

\echo '--- Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Expected: Original segments kept, middle replaced ---'
SELECT * FROM (VALUES
    (1, '[2024-01-01, 2024-03-01)'::daterange, 'Original'),
    (1, '[2024-03-01, 2024-06-01)'::daterange, 'Replaced'),
    (1, '[2024-06-01, 2024-12-31)'::daterange, 'Original')
) t(id, valid_range, value) ORDER BY id, valid_range;

\echo '--- Actual Final State ---'
TABLE tmro.target_date ORDER BY id, valid_range;

ROLLBACK TO SAVEPOINT scenario_3;

--------------------------------------------------------------------------------
\echo 'Scenario 4: Source with valid_from/valid_until into range-only target'
\echo '  When target has only a range column (no synchronized boundary columns),'
\echo '  the source MUST provide data using the same range column.'
\echo '  This test verifies that an appropriate error is raised.'
--------------------------------------------------------------------------------
SAVEPOINT scenario_4;

TRUNCATE tmro.target_date;
INSERT INTO tmro.target_date (id, valid_range, value) VALUES
    (1, '[2024-01-01, 2024-12-31)', 'Original');

-- Source uses boundary columns instead of range - should fail
CREATE TEMP TABLE source_boundary (row_id int, id int, valid_from date, valid_until date, value text) ON COMMIT DROP;
INSERT INTO source_boundary VALUES
    (101, 1, '2024-03-01', '2024-06-01', 'From Boundary Source');

-- This should fail because target has no synchronized columns to fall back to
SAVEPOINT expect_error;
CALL sql_saga.temporal_merge(
    target_table => 'tmro.target_date'::regclass,
    source_table => 'source_boundary'::regclass,
    primary_identity_columns => '{id}'::text[]
);
ROLLBACK TO SAVEPOINT expect_error;

\echo '--- Workaround: Use a view to compute the range from boundary columns ---'
CREATE TEMP VIEW source_boundary_with_range AS
SELECT row_id, id, daterange(valid_from, valid_until) AS valid_range, value
FROM source_boundary;

CALL sql_saga.temporal_merge(
    target_table => 'tmro.target_date'::regclass,
    source_table => 'source_boundary_with_range'::regclass,
    primary_identity_columns => '{id}'::text[]
);

\echo '--- Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Actual Final State ---'
TABLE tmro.target_date ORDER BY id, valid_range;

ROLLBACK TO SAVEPOINT scenario_4;

--------------------------------------------------------------------------------
\echo 'Scenario 5: Range-only with int4range'
--------------------------------------------------------------------------------
CREATE TABLE tmro.target_int (
    id int,
    valid_range int4range NOT NULL,
    value text,
    PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)
);
SELECT sql_saga.add_era('tmro.target_int', 'valid_range');
SELECT sql_saga.add_unique_key('tmro.target_int', ARRAY['id']);

INSERT INTO tmro.target_int (id, valid_range, value) VALUES
    (1, '[10, 100)', 'Original');

CREATE TEMP TABLE source_int (row_id int, id int, valid_range int4range, value text);
INSERT INTO source_int VALUES
    (101, 1, '[30, 60)', 'Patched');

CALL sql_saga.temporal_merge(
    target_table => 'tmro.target_int'::regclass,
    source_table => 'source_int'::regclass,
    primary_identity_columns => '{id}'::text[]
);

\echo '--- Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Expected Final State ---'
SELECT * FROM (VALUES
    (1, '[10, 30)'::int4range, 'Original'),
    (1, '[30, 60)'::int4range, 'Patched'),
    (1, '[60, 100)'::int4range, 'Original')
) t(id, valid_range, value) ORDER BY id, valid_range;

\echo '--- Actual Final State ---'
TABLE tmro.target_int ORDER BY id, valid_range;

--------------------------------------------------------------------------------
\echo 'Scenario 6: Coalescing adjacent identical segments (range-only)'
\echo '  Verifies that temporal_merge correctly coalesces when the patch'
\echo '  results in identical adjacent segments.'
--------------------------------------------------------------------------------
SAVEPOINT scenario_6;

TRUNCATE tmro.target_date;
INSERT INTO tmro.target_date (id, valid_range, value) VALUES
    (1, '[2024-01-01, 2024-06-01)', 'A'),
    (1, '[2024-06-01, 2024-12-31)', 'B');

\echo '--- Target: Initial State (two segments) ---'
TABLE tmro.target_date ORDER BY id, valid_range;

-- Patch the second segment to match the first - should coalesce
CREATE TEMP TABLE source_coalesce (row_id int, id int, valid_range daterange, value text) ON COMMIT DROP;
INSERT INTO source_coalesce VALUES
    (101, 1, '[2024-06-01, 2024-12-31)', 'A');

CALL sql_saga.temporal_merge(
    target_table => 'tmro.target_date'::regclass,
    source_table => 'source_coalesce'::regclass,
    primary_identity_columns => '{id}'::text[]
);

\echo '--- Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Expected: Single coalesced segment ---'
SELECT * FROM (VALUES
    (1, '[2024-01-01, 2024-12-31)'::daterange, 'A')
) t(id, valid_range, value) ORDER BY id, valid_range;

\echo '--- Actual Final State ---'
TABLE tmro.target_date ORDER BY id, valid_range;

ROLLBACK TO SAVEPOINT scenario_6;

--------------------------------------------------------------------------------
\echo 'Scenario 7: DELETE_FOR_PORTION_OF with range-only table'
--------------------------------------------------------------------------------
SAVEPOINT scenario_7;

TRUNCATE tmro.target_date;
INSERT INTO tmro.target_date (id, valid_range, value) VALUES
    (1, '[2024-01-01, 2024-12-31)', 'Original');

CREATE TEMP TABLE source_delete (row_id int, id int, valid_range daterange) ON COMMIT DROP;
INSERT INTO source_delete VALUES
    (101, 1, '[2024-03-01, 2024-06-01)');

CALL sql_saga.temporal_merge(
    target_table => 'tmro.target_date'::regclass,
    source_table => 'source_delete'::regclass,
    primary_identity_columns => '{id}'::text[],
    mode => 'DELETE_FOR_PORTION_OF'
);

\echo '--- Plan ---'
TABLE pg_temp.temporal_merge_plan ORDER BY plan_op_seq;

\echo '--- Expected: Gap in middle ---'
SELECT * FROM (VALUES
    (1, '[2024-01-01, 2024-03-01)'::daterange, 'Original'),
    (1, '[2024-06-01, 2024-12-31)'::daterange, 'Original')
) t(id, valid_range, value) ORDER BY id, valid_range;

\echo '--- Actual Final State ---'
TABLE tmro.target_date ORDER BY id, valid_range;

ROLLBACK TO SAVEPOINT scenario_7;

ROLLBACK;

\i sql/include/test_teardown.sql
