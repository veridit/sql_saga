\i sql/include/test_setup.sql

BEGIN;

SET ROLE TO sql_saga_unprivileged_user;

\i sql/support/shifts_houses_rooms_tables.sql

-- Before using sql_saga
\d rooms
\d houses
\d shifts

-- Verify that enable and disable each work correctly.
SELECT sql_saga.add_era(table_oid => 'shifts', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_era(table_oid => 'houses', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until', era_name => 'valid');
SELECT sql_saga.add_era(table_oid => 'rooms', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
TABLE sql_saga.era;

SELECT sql_saga.add_unique_key(table_oid => 'shifts', column_names => ARRAY['job_id','worker_id'], era_name => 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'houses', column_names => ARRAY['id'], era_name => 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'rooms', column_names => ARRAY['id'], era_name => 'valid');
SELECT * FROM sql_saga.unique_keys ORDER BY unique_key_name;

SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'rooms',
    fk_column_names => ARRAY['house_id'],
    fk_era_name => 'valid',
    unique_key_name => 'houses_id_valid'
);
TABLE sql_saga.foreign_keys;

-- While sql_saga is active
\d rooms
\d houses
\d shifts

-- Remove sql_saga
SELECT sql_saga.drop_foreign_key(table_oid => 'rooms', key_name => 'rooms_house_id_valid');
TABLE sql_saga.foreign_keys;

SELECT sql_saga.drop_unique_key(table_oid => 'rooms', key_name => 'rooms_id_valid');
SELECT sql_saga.drop_unique_key(table_oid => 'houses', key_name => 'houses_id_valid');
SELECT sql_saga.drop_unique_key(table_oid => 'shifts', key_name => 'shifts_job_id_worker_id_valid');
SELECT * FROM sql_saga.unique_keys ORDER BY unique_key_name;

SELECT sql_saga.drop_era('rooms');
SELECT sql_saga.drop_era('houses');
SELECT sql_saga.drop_era('shifts');
TABLE sql_saga.era;

-- After removing sql_saga, it should be as before.
\d rooms
\d houses
\d shifts

ROLLBACK;

\i sql/include/test_teardown.sql
