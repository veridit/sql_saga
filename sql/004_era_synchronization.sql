\i sql/include/test_setup.sql

BEGIN;

SET ROLE TO sql_saga_unprivileged_user;
SET client_min_messages TO NOTICE;

\echo '----------------------------------------------------------------------------'
\echo 'Test: Automatic `valid_to` synchronization via add_era'
\echo '----------------------------------------------------------------------------'

\echo '--- 1. Test default behavior: trigger is auto-created for `valid_to` ---'
SAVEPOINT test_1;
CREATE TABLE sync_test_default (id int, valid_range daterange, valid_from date, valid_to date, valid_until date);
-- This will auto-detect all conventional columns: valid_from, valid_to, valid_until
SELECT sql_saga.add_era('sync_test_default', 'valid_range');
\d sync_test_default
INSERT INTO sync_test_default (id, valid_from, valid_to) VALUES (1, '2024-01-01', '2024-12-31');
SELECT id, valid_range, valid_from, valid_to, valid_until FROM sync_test_default;
UPDATE sync_test_default SET valid_until = '2026-01-01' WHERE id = 1;
SELECT id, valid_range, valid_from, valid_to, valid_until FROM sync_test_default;
ROLLBACK TO SAVEPOINT test_1;

\echo '--- 2. Test disabling the feature with synchronize_columns => false ---'
SAVEPOINT test_2;
CREATE TABLE sync_test_disabled (id int, valid_range daterange, valid_from date, valid_to date, valid_until date);
-- Note: synchronize_columns is set to false to disable all synchronization.
SELECT sql_saga.add_era('sync_test_disabled', 'valid_range', synchronize_columns => false);

\d sync_test_disabled
INSERT INTO sync_test_disabled (id, valid_range              , valid_from  , valid_until , valid_to    )
                        VALUES (1, '[2023-01-01, 2023-12-31)'::DATERANGE, '2024-01-01', '2025-01-01', '2099-12-31');
SELECT id, valid_range, valid_from, valid_to, valid_until FROM sync_test_disabled;
ROLLBACK TO SAVEPOINT test_2;

\echo '--- 3. Test with a custom column name ---'
SAVEPOINT test_3;
CREATE TABLE sync_test_custom_name (id int, date_range daterange, start_date date, until_date date, to_date date);
SELECT sql_saga.add_era('sync_test_custom_name', 'date_range', valid_from_column_name => 'start_date', valid_until_column_name => 'until_date', valid_to_column_name => 'to_date');
\d sync_test_custom_name
INSERT INTO sync_test_custom_name (id, start_date, to_date) VALUES (1, '2024-01-01', '2024-12-31');
SELECT id, date_range, start_date, until_date, to_date FROM sync_test_custom_name;
ROLLBACK TO SAVEPOINT test_3;

\echo '--- 4. Test edge case: generated column is skipped by auto-detection ---'
SAVEPOINT test_4;
CREATE TABLE sync_test_generated (id int, valid_range daterange, valid_from date, valid_until date, valid_to date GENERATED ALWAYS AS (valid_until - INTERVAL '1 day') STORED);
-- Auto-detection should find from/until but skip the generated `valid_to`
SELECT sql_saga.add_era('sync_test_generated', 'valid_range');
\d sync_test_generated
INSERT INTO sync_test_generated (id, valid_range) VALUES (1, '[2024-01-01,2025-01-01)');
SELECT id, valid_from, valid_to, valid_until FROM sync_test_generated;
ROLLBACK TO SAVEPOINT test_4;

\echo '--- 5. Test edge case: wrong data type is skipped by auto-detection ---'
SAVEPOINT test_5;
CREATE TABLE sync_test_wrong_type (id int, valid_range daterange, valid_from date, valid_to text, valid_until date);
-- Auto-detection should find from/until but skip the text `valid_to`
SELECT sql_saga.add_era('sync_test_wrong_type', 'valid_range');
\d sync_test_wrong_type
INSERT INTO sync_test_wrong_type (id, valid_from, valid_to, valid_until) VALUES (1, '2024-01-01', 'some text', '2025-01-01');
SELECT id, valid_range, valid_from, valid_to, valid_until FROM sync_test_wrong_type;
ROLLBACK TO SAVEPOINT test_5;

\echo '--- 6. Test NOT NULL on synchronized columns ---'
SAVEPOINT test_6_top;

SAVEPOINT test_6a;
\echo '--- 6a. Trigger populates NOT NULL column when other bound is provided ---'
-- valid_to is NOT NULL, but the trigger should populate it before the constraint is checked.
CREATE TABLE sync_test_not_null (id int, valid_range daterange, valid_from date, valid_to date NOT NULL, valid_until date);
SELECT sql_saga.add_era('sync_test_not_null', 'valid_range');

-- Verify schema. Note that valid_from and valid_until are NOT NULL because
-- add_era adds the constraint, and valid_to is NOT NULL because we declared it.
\d sync_test_not_null

-- This insert omits valid_to, but provides valid_until. The trigger will derive
-- valid_to, and the INSERT should succeed.
INSERT INTO sync_test_not_null (id, valid_from, valid_until) VALUES (1, '2024-01-01', '2025-01-01');
SELECT id, valid_range, valid_from, valid_to, valid_until FROM sync_test_not_null;
RELEASE SAVEPOINT test_6a;


SAVEPOINT test_6b;
\echo '--- 6b. INSERT fails when no temporal bounds are provided and defaults are off ---'
CREATE TABLE sync_test_no_defaults (id int, valid_range daterange, valid_from date, valid_to date NOT NULL, valid_until date);
-- Note: add_defaults => false
-- This prevents the trigger from defaulting valid_until to 'infinity'.
SELECT sql_saga.add_era('sync_test_no_defaults', 'valid_range', add_defaults => false);

-- This should fail with an exception from the trigger because no bounds can be determined.
INSERT INTO sync_test_no_defaults (id, valid_from) VALUES (1, '2024-01-01');
ROLLBACK TO SAVEPOINT test_6b;


SAVEPOINT test_6c;
\echo '--- 6c. INSERT succeeds with defaults enabled when only lower bound is provided ---'
CREATE TABLE sync_test_nullable_with_defaults (id int, valid_range daterange, valid_from date NOT NULL, valid_to date, valid_until date);
-- Note: add_defaults => true (default)
SELECT sql_saga.add_era('sync_test_nullable_with_defaults', 'valid_range');

\d sync_test_nullable_with_defaults

-- This should succeed. The trigger will see that valid_until is NULL, apply the
-- 'infinity' default, and then derive all other representations from that.
INSERT INTO sync_test_nullable_with_defaults (id, valid_from) VALUES (1, '2024-01-01');
SELECT id, valid_range, valid_from, valid_to, valid_until FROM sync_test_nullable_with_defaults;
RELEASE SAVEPOINT test_6c;

ROLLBACK TO SAVEPOINT test_6_top;

\echo '--- 7. Test UPDATEs setting synchronized columns to NULL ---'
SAVEPOINT test_7_top;
CREATE TABLE sync_test_null_update (id int, valid_range daterange, valid_from date, valid_to date, valid_until date);
SELECT sql_saga.add_era('sync_test_null_update', 'valid_range', add_defaults => false);
INSERT INTO sync_test_null_update (id, valid_from, valid_to) VALUES (1, '2024-01-01', '2024-12-31');
SELECT * FROM sync_test_null_update;

--- 7a. UPDATE setting valid_to to NULL should fail ---
SAVEPOINT test_7a;
-- This should fail because it creates an inconsistent state. The trigger raises an exception.
UPDATE sync_test_null_update SET valid_to = NULL WHERE id = 1;
ROLLBACK TO SAVEPOINT test_7a;

--- 7b. UPDATE setting valid_until to NULL should fail ---
SAVEPOINT test_7b;
-- This should also fail for the same reason.
UPDATE sync_test_null_update SET valid_until = NULL WHERE id = 1;
ROLLBACK TO SAVEPOINT test_7b;

--- 7c. UPDATE setting valid_from to NULL should fail ---
SAVEPOINT test_7c;
-- This should also fail for the same reason.
UPDATE sync_test_null_update SET valid_from = NULL WHERE id = 1;
ROLLBACK TO SAVEPOINT test_7c;

-- Verify the original row is unchanged after the failed update
SELECT * FROM sync_test_null_update;

ROLLBACK TO SAVEPOINT test_7_top;


ROLLBACK;

\i sql/include/test_teardown.sql
