--
-- Activates sql_saga for the shifts, houses, and rooms tables.
-- This was formerly the enable_sql_saga_for_shifts_houses_and_rooms() function.
--
SELECT sql_saga.add_era(table_oid => 'shifts', range_column_name => 'valid_range');
SELECT sql_saga.add_unique_key(table_oid => 'shifts', column_names => ARRAY['job_id','worker_id'], era_name => 'valid', key_type => 'natural');

SELECT sql_saga.add_era(table_oid => 'houses', range_column_name => 'valid_range', era_name => 'valid', add_bounds_check := false);
SELECT sql_saga.add_unique_key(table_oid => 'houses', column_names => ARRAY['id'], era_name => 'valid', key_type => 'natural');

SELECT sql_saga.add_era(table_oid => 'rooms', range_column_name => 'valid_range', add_bounds_check := false);
SELECT sql_saga.add_unique_key(table_oid => 'rooms', column_names => ARRAY['id'], era_name => 'valid', key_type => 'natural');
SELECT sql_saga.add_temporal_foreign_key(
    fk_table_oid => 'rooms',
    fk_column_names => ARRAY['house_id'],
    fk_era_name => 'valid',
    unique_key_name => 'houses_id_valid'
);
