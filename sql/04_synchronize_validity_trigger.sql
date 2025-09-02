\i sql/include/test_setup.sql

BEGIN;

SET ROLE TO sql_saga_unprivileged_user;
SET client_min_messages TO NOTICE;

\echo '----------------------------------------------------------------------------'
\echo 'Test: Automatic `valid_to` synchronization via add_era'
\echo '----------------------------------------------------------------------------'

\echo '--- 1. Test default behavior: trigger is created for `valid_to` ---'
SAVEPOINT test_1;
CREATE TABLE sync_test_default (id int, valid_from date, valid_to date, valid_until date);
SELECT sql_saga.add_era('sync_test_default', p_synchronize_valid_to_column := 'valid_to');
\d sync_test_default
INSERT INTO sync_test_default (id, valid_from, valid_to) VALUES (1, '2024-01-01', '2024-12-31');
SELECT id, valid_from, valid_until, valid_to FROM sync_test_default;
UPDATE sync_test_default SET valid_until = '2026-01-01' WHERE id = 1;
SELECT id, valid_from, valid_until, valid_to FROM sync_test_default;
ROLLBACK TO SAVEPOINT test_1;

\echo '--- 2. Test disabling the feature with NULL ---'
SAVEPOINT test_2;
CREATE TABLE sync_test_disabled (id int, valid_from date, valid_to date, valid_until date);
SELECT sql_saga.add_era('sync_test_disabled');
\d sync_test_disabled
INSERT INTO sync_test_disabled (id, valid_from, valid_until, valid_to) VALUES (1, '2024-01-01', '2025-01-01', '2099-12-31');
SELECT id, valid_from, valid_until, valid_to FROM sync_test_disabled;
ROLLBACK TO SAVEPOINT test_2;

\echo '--- 3. Test with a custom column name ---'
SAVEPOINT test_3;
CREATE TABLE sync_test_custom_name (id int, start_date date, until_date date, to_date date);
SELECT sql_saga.add_era('sync_test_custom_name', 'start_date', 'until_date', p_synchronize_valid_to_column := 'to_date');
\d sync_test_custom_name
INSERT INTO sync_test_custom_name (id, start_date, to_date) VALUES (1, '2024-01-01', '2024-12-31');
SELECT id, start_date, until_date, to_date FROM sync_test_custom_name;
ROLLBACK TO SAVEPOINT test_3;

\echo '--- 4. Test edge case: generated column is skipped ---'
SAVEPOINT test_4;
CREATE TABLE sync_test_generated (id int, valid_from date, valid_until date, valid_to date GENERATED ALWAYS AS (valid_until - INTERVAL '1 day') STORED);
SELECT sql_saga.add_era('sync_test_generated', p_synchronize_valid_to_column := 'valid_to');
\d sync_test_generated
INSERT INTO sync_test_generated (id, valid_from, valid_until) VALUES (1, '2024-01-01', '2025-01-01');
SELECT id, valid_from, valid_until, valid_to FROM sync_test_generated;
ROLLBACK TO SAVEPOINT test_4;

\echo '--- 5. Test edge case: wrong data type is skipped ---'
SAVEPOINT test_5;
CREATE TABLE sync_test_wrong_type (id int, valid_from date, valid_to text, valid_until date);
SELECT sql_saga.add_era('sync_test_wrong_type', p_synchronize_valid_to_column := 'valid_to');
\d sync_test_wrong_type
INSERT INTO sync_test_wrong_type (id, valid_from, valid_until, valid_to) VALUES (1, '2024-01-01', '2025-01-01', 'some text');
SELECT id, valid_from, valid_until, valid_to FROM sync_test_wrong_type;
ROLLBACK TO SAVEPOINT test_5;

ROLLBACK;

\i sql/include/test_teardown.sql
