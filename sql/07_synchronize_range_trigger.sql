\i sql/include/test_setup.sql

BEGIN;

SET ROLE TO sql_saga_unprivileged_user;
SET client_min_messages TO NOTICE;

\echo '----------------------------------------------------------------------------'
\echo 'Test: Automatic range column synchronization via add_era'
\echo '----------------------------------------------------------------------------'

\echo '--- 1. Test default behavior: DATE range ---'
SAVEPOINT test_1;
CREATE TABLE sync_range_default (id int, valid_from date, valid_until date, validity daterange);
SELECT sql_saga.add_era('sync_range_default', synchronize_range_column := 'validity');
\d sync_range_default
-- 1a. INSERT with bounds, range is auto-populated
INSERT INTO sync_range_default (id, valid_from, valid_until) VALUES (1, '2024-01-01', '2025-01-01');
SELECT id, valid_from, valid_until, validity FROM sync_range_default;
-- 1b. INSERT with range, bounds are auto-populated
INSERT INTO sync_range_default (id, validity) VALUES (2, '[2024-02-01, 2025-02-01)');
SELECT id, valid_from, valid_until, validity FROM sync_range_default WHERE id = 2;
-- 1c. UPDATE bounds, range is auto-updated
UPDATE sync_range_default SET valid_from = '2026-01-01', valid_until = '2027-01-01' WHERE id = 1;
SELECT id, valid_from, valid_until, validity FROM sync_range_default WHERE id = 1;
-- 1d. UPDATE range, bounds are auto-updated
UPDATE sync_range_default SET validity = '[2027-01-01, 2028-01-01)' WHERE id = 1;
SELECT id, valid_from, valid_until, validity FROM sync_range_default WHERE id = 1;
ROLLBACK TO SAVEPOINT test_1;

\echo '--- 2. Test disabling the feature with NULL ---'
SAVEPOINT test_2;
CREATE TABLE sync_range_disabled (id int, valid_from date, valid_until date, validity daterange);
SELECT sql_saga.add_era('sync_range_disabled');
\d sync_range_disabled
-- This will succeed with inconsistent data, proving the trigger was not created.
INSERT INTO sync_range_disabled (id, valid_from, valid_until, validity) VALUES (1, '2024-01-01', '2025-01-01', '[2099-01-01, 2099-12-31)');
SELECT id, valid_from, valid_until, validity FROM sync_range_disabled;
ROLLBACK TO SAVEPOINT test_2;

\echo '--- 3. Test with a custom column name and INT4RANGE (a discrete type) ---'
SAVEPOINT test_3;
CREATE TABLE sync_range_custom (id int, start_num int, until_num int, num_range int4range, to_num int);
SELECT sql_saga.add_era('sync_range_custom', 'start_num', 'until_num', synchronize_valid_to_column := 'to_num', synchronize_range_column := 'num_range');
\d sync_range_custom
INSERT INTO sync_range_custom (id, num_range) VALUES (1, '[100, 200)');
SELECT id, start_num, until_num, num_range, to_num FROM sync_range_custom;
ROLLBACK TO SAVEPOINT test_3;

\echo '--- 4. Test error case: inconsistent data on INSERT ---'
SAVEPOINT test_4;
CREATE TABLE sync_range_error (id int, valid_from date, valid_until date, validity daterange);
SELECT sql_saga.add_era('sync_range_error', synchronize_range_column := 'validity');
-- This should fail due to inconsistency between the bounds and the range.
DO $$
BEGIN
    INSERT INTO sync_range_error (id, valid_from, valid_until, validity) VALUES (1, '2024-01-01', '2025-01-01', '[2099-01-01, 2099-12-31)');
EXCEPTION WHEN others THEN
    RAISE NOTICE 'Caught expected error: %', SQLERRM;
END;
$$;
-- This should succeed because the bounds and range are consistent.
INSERT INTO sync_range_error (id, valid_from, valid_until, validity) VALUES (2, '2023-01-01', '2024-01-01', '[2023-01-01, 2024-01-01)');
SELECT * FROM sync_range_error;
ROLLBACK TO SAVEPOINT test_4;

\echo '--- 5. Test error case: inclusive-upper bound range is rejected ---'
SAVEPOINT test_5;
CREATE TABLE sync_range_bounds (id int, valid_from date, valid_until date, validity daterange);
SELECT sql_saga.add_era('sync_range_bounds', synchronize_range_column := 'validity');
-- This should fail because the range is '[ , ]' not '[ , )'
DO $$
BEGIN
    INSERT INTO sync_range_bounds (id, validity) VALUES (1, daterange('2024-01-01', '2024-12-31', '[]'));
EXCEPTION WHEN others THEN
    RAISE NOTICE 'Caught expected error: %', SQLERRM;
END;
$$;
ROLLBACK TO SAVEPOINT test_5;

\echo '--- 6. Test edge case: generated column is skipped ---'
SAVEPOINT test_6;
CREATE TABLE sync_range_generated (id int, valid_from date, valid_until date, validity daterange GENERATED ALWAYS AS (daterange(valid_from, valid_until, '[)')) STORED);
SELECT sql_saga.add_era('sync_range_generated', synchronize_range_column := 'validity');
\d sync_range_generated
-- This will succeed, proving no trigger was created.
INSERT INTO sync_range_generated (id, valid_from, valid_until) VALUES (1, '2024-01-01', '2025-01-01');
SELECT id, valid_from, valid_until, validity FROM sync_range_generated;
ROLLBACK TO SAVEPOINT test_6;

\echo '--- 7. Test interaction between both synchronization triggers ---'
SAVEPOINT test_7;
CREATE TABLE sync_both (id int, valid_from date, valid_to date, valid_until date, validity daterange);
-- Enable both triggers
SELECT sql_saga.add_era('sync_both', synchronize_valid_to_column := 'valid_to', synchronize_range_column := 'validity', add_defaults := false);
\d sync_both
-- 7a. INSERT with range, other columns are auto-populated
INSERT INTO sync_both (id, validity) VALUES (1, '[2024-01-01, 2025-01-01)');
SELECT id, valid_from, valid_until, valid_to, validity FROM sync_both WHERE id=1;
-- 7b. INSERT with valid_to, other columns are auto-populated
INSERT INTO sync_both (id, valid_from, valid_to) VALUES (2, '2026-01-01', '2026-12-31');
SELECT id, valid_from, valid_until, valid_to, validity FROM sync_both WHERE id=2;
-- 7c. INSERT with valid_until, other columns are auto-populated
INSERT INTO sync_both (id, valid_from, valid_until) VALUES (3, '2028-01-01', '2029-01-01');
SELECT id, valid_from, valid_until, valid_to, validity FROM sync_both WHERE id=3;

-- 7c2. INSERT with NULL valid_until, should get default from trigger
SAVEPOINT with_defaults_test;
-- To test this, we must re-add the era with defaults enabled.
SELECT sql_saga.drop_era('sync_both');
SELECT sql_saga.add_era('sync_both', synchronize_valid_to_column := 'valid_to', synchronize_range_column := 'validity', add_defaults := true);
\d sync_both
INSERT INTO sync_both (id, valid_from, valid_until) VALUES (4, '2028-01-01', NULL);
SELECT * FROM sync_both WHERE id = 4;
ROLLBACK TO with_defaults_test;
-- The era should be back to its previous state (no defaults).
\d sync_both

-- 7d. INSERT with CONSISTENT multiple representations (valid_to and range)
\echo '--- 7d. Test consistent multiple inputs ---'
INSERT INTO sync_both (id, valid_from, valid_to, validity) VALUES (5, '2030-01-01', '2030-12-31', '[2030-01-01, 2031-01-01)');
SELECT id, valid_from, valid_until, valid_to, validity FROM sync_both WHERE id=5;

-- 7e. INSERT with INCONSISTENT multiple representations (valid_to and range)
\echo '--- 7e. Test inconsistent multiple inputs ---'
DO $$
BEGIN
    INSERT INTO sync_both (id, valid_from, valid_to, validity) VALUES (6, '2030-01-01', '2030-12-31', '[2099-01-01, 2100-01-01)');
EXCEPTION WHEN others THEN
    RAISE NOTICE 'Caught expected error: %', SQLERRM;
END;
$$;

-- 7f. UPDATE with INCONSISTENT multiple representations
\echo '--- 7f. Test inconsistent update ---'
DO $$
BEGIN
    UPDATE sync_both SET valid_to = '2025-12-31', validity = '[2099-01-01, 2100-01-01)' WHERE id = 1;
EXCEPTION WHEN others THEN
    RAISE NOTICE 'Caught expected error: %', SQLERRM;
END;
$$;

ROLLBACK TO SAVEPOINT test_7;

\echo '--- 8. Test continuous types disable valid_to sync ---'
SAVEPOINT test_8a;
\echo '--- 8a. timestamptz ---'
CREATE TABLE sync_continuous_tstz (id int, valid_from timestamptz, valid_to timestamptz, valid_until timestamptz);
SELECT sql_saga.add_era('sync_continuous_tstz', synchronize_valid_to_column := 'valid_to');
\d sync_continuous_tstz
INSERT INTO sync_continuous_tstz (id, valid_from, valid_until, valid_to) VALUES (1, '2024-01-01', '2025-01-01', '2099-12-31');
SELECT * FROM sync_continuous_tstz;
ROLLBACK TO SAVEPOINT test_8a;

SAVEPOINT test_8b;
\echo '--- 8b. timestamp ---'
CREATE TABLE sync_continuous_ts (id int, valid_from timestamp, valid_to timestamp, valid_until timestamp);
SELECT sql_saga.add_era('sync_continuous_ts', synchronize_valid_to_column := 'valid_to');
\d sync_continuous_ts
INSERT INTO sync_continuous_ts (id, valid_from, valid_until, valid_to) VALUES (1, '2024-01-01', '2025-01-01', '2099-12-31');
SELECT * FROM sync_continuous_ts;
ROLLBACK TO SAVEPOINT test_8b;

SAVEPOINT test_8c;
\echo '--- 8c. numeric ---'
CREATE TABLE sync_continuous_numeric (id int, valid_from numeric, valid_to numeric, valid_until numeric);
SELECT sql_saga.add_era('sync_continuous_numeric', synchronize_valid_to_column := 'valid_to');
\d sync_continuous_numeric
INSERT INTO sync_continuous_numeric (id, valid_from, valid_until, valid_to) VALUES (1, 100.5, 200.5, 999.9);
SELECT * FROM sync_continuous_numeric;
ROLLBACK TO SAVEPOINT test_8c;

ROLLBACK;

\i sql/include/test_teardown.sql
