--
-- Activates sql_saga for the shifts, houses, and rooms tables.
-- This was formerly the enable_sql_saga_for_shifts_houses_and_rooms() function.
--
SELECT sql_saga.add_era(table_oid => 'shifts', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'shifts', column_names => ARRAY['job_id','worker_id'], era_name => 'valid', key_type => 'natural');

SELECT sql_saga.add_era(table_oid => 'houses', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until', era_name => 'valid', add_bounds_check := false);
SELECT sql_saga.add_unique_key(table_oid => 'houses', column_names => ARRAY['id'], era_name => 'valid', key_type => 'natural');

SELECT sql_saga.add_era(table_oid => 'rooms', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until', add_bounds_check := false);
SELECT sql_saga.add_unique_key(table_oid => 'rooms', column_names => ARRAY['id'], era_name => 'valid', key_type => 'natural');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'rooms',
    fk_column_names => ARRAY['house_id'],
    fk_era_name => 'valid',
    unique_key_name => 'houses_id_valid'
);
