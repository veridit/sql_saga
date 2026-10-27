\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/* Era names are user data. A regular era literally named system_time must keep the normal drop path. */
CREATE SCHEMA stn;
CREATE TABLE stn.t (id int, valid_from date, valid_until date, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('stn.t', 'valid_range', 'system_time', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.drop_era('stn.t', 'system_time', cleanup => false);
\o
SELECT 'regular era named system_time, cleanup false | ' || (
    NOT EXISTS (SELECT FROM sql_saga.era WHERE table_schema = 'stn' AND table_name = 't' AND era_name = 'system_time')
    AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger WHERE tgrelid = 'stn.t'::regclass AND tgname = 't_system_time_sync_temporal_trg')
    AND EXISTS (SELECT FROM pg_catalog.pg_attribute WHERE attrelid = 'stn.t'::regclass AND attname = 'valid_range' AND NOT attisdropped))::text;

\o /dev/null
SELECT sql_saga.add_era('stn.t', 'valid_range', 'system_time', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.drop_era('stn.t', 'system_time', cleanup => true);
\o
SELECT 'regular era named system_time, cleanup true | ' || (
    NOT EXISTS (SELECT FROM sql_saga.era WHERE table_schema = 'stn' AND table_name = 't' AND era_name = 'system_time')
    AND NOT EXISTS (SELECT FROM pg_catalog.pg_trigger WHERE tgrelid = 'stn.t'::regclass AND tgname = 't_system_time_sync_temporal_trg')
    AND NOT EXISTS (SELECT FROM pg_catalog.pg_attribute WHERE attrelid = 'stn.t'::regclass AND attname = 'valid_range' AND NOT attisdropped))::text;
SELECT 'integrity after regular era name | ok' FROM (SELECT sql_saga.check_integrity() AS ignored) AS x;

\o /dev/null
DROP SCHEMA stn CASCADE;
\i sql/include/test_teardown.sql
