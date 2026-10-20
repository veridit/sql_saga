\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
BEGIN;
CREATE SCHEMA ddr;
CREATE FUNCTION ddr.run(statement text) RETURNS text LANGUAGE plpgsql AS $run$
BEGIN
    EXECUTE statement;
    RETURN 'accepted';
EXCEPTION WHEN OTHERS THEN
    RETURN 'refused ' || SQLSTATE;
END;
$run$;
CREATE FUNCTION ddr.view_diagnostic(statement text, view_name text, drop_api text) RETURNS boolean LANGUAGE plpgsql AS $view_diagnostic$
DECLARE
    message text;
    hint text;
BEGIN
    EXECUTE statement;
    RETURN false;
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS message = MESSAGE_TEXT, hint = PG_EXCEPTION_HINT;
    RETURN position(view_name IN message) > 0
       AND position(drop_api IN message || coalesce(hint, '')) > 0;
END;
$view_diagnostic$;
CREATE FUNCTION ddr.rename_diagnostic(statement text) RETURNS boolean LANGUAGE plpgsql AS $rename_diagnostic$
DECLARE
    message text;
    hint text;
    diagnostic text;
    view_position integer;
    foreign_key_position integer;
    unique_key_position integer;
    era_position integer;
BEGIN
    EXECUTE statement;
    RETURN false;
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS message = MESSAGE_TEXT, hint = PG_EXCEPTION_HINT;
    diagnostic := message || coalesce(hint, '');
    view_position := coalesce(
        nullif(position('sql_saga.drop_current_view' IN diagnostic), 0),
        nullif(position('sql_saga.drop_for_portion_of_view' IN diagnostic), 0), 0);
    foreign_key_position := position('sql_saga.drop_foreign_key_by_name' IN diagnostic);
    unique_key_position := position('sql_saga.drop_unique_key_by_name' IN diagnostic);
    era_position := position('sql_saga.drop_era' IN diagnostic);
    RETURN 0 < view_position AND view_position < foreign_key_position
       AND foreign_key_position < unique_key_position AND unique_key_position < era_position;
END;
$rename_diagnostic$;

CREATE TABLE ddr.t (id integer NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era(table_oid => 'ddr.t', range_column_name => 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until') \g /dev/null
SELECT sql_saga.add_unique_key(table_oid => 'ddr.t', column_names => ARRAY['id']::name[], key_type => 'primary') \g /dev/null
SELECT sql_saga.add_current_view('ddr.t') \g /dev/null
SELECT sql_saga.add_for_portion_of_view('ddr.t') \g /dev/null

\echo '--- cascading era removal removes both managed view types'
SAVEPOINT cascade_views;
SELECT 'drop era CASCADE | ' || ddr.run($$SELECT sql_saga.drop_era('ddr.t', drop_behavior => 'CASCADE')$$);
SELECT 'managed views remaining | ' || count(*) FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace WHERE n.nspname = 'ddr' AND c.relname IN ('t__current_valid', 't__for_portion_of_valid');
SELECT 'catalogued views remaining | ' || count(*) FROM sql_saga.updatable_view AS uv WHERE uv.table_schema = 'ddr' AND uv.table_name = 't';
SELECT 'recreate era, key and both views | ' || ddr.run($$SELECT sql_saga.add_era(table_oid => 'ddr.t', range_column_name => 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until'); SELECT sql_saga.add_unique_key(table_oid => 'ddr.t', column_names => ARRAY['id']::name[], key_type => 'primary'); SELECT sql_saga.add_current_view('ddr.t'); SELECT sql_saga.add_for_portion_of_view('ddr.t')$$);
ROLLBACK TO SAVEPOINT cascade_views;

\echo '--- removal never silently cascades through a reused synchronization function'
SAVEPOINT reused_function;
SELECT sql_saga.drop_current_view('ddr.t') \g /dev/null
SELECT sql_saga.drop_for_portion_of_view('ddr.t') \g /dev/null
SELECT sql_saga.drop_unique_key('ddr.t', ARRAY['id']::name[]) \g /dev/null
CREATE TABLE ddr.other (id integer, valid_range daterange, valid_from date, valid_until date);
SELECT format('CREATE TRIGGER user_sync BEFORE INSERT ON ddr.other FOR EACH ROW EXECUTE FUNCTION sql_saga.%I()', e.sync_temporal_trg_function_name) FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name, e.era_name) = ('ddr', 't', 'valid') \gexec
SAVEPOINT drop_era_restrict;
SELECT 'drop era RESTRICT | ' || ddr.run($$SELECT sql_saga.drop_era('ddr.t')$$);
SELECT 'user trigger kept after drop era | ' || EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = 'ddr.other'::regclass AND t.tgname = 'user_sync');
SELECT 'era kept after refusal | ' || EXISTS (SELECT FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name, e.era_name) = ('ddr', 't', 'valid'));
ROLLBACK TO SAVEPOINT drop_era_restrict;
SAVEPOINT drop_sync_restrict;
SELECT 'drop synchronization trigger | ' || ddr.run($$CALL sql_saga.drop_synchronize_temporal_columns_trigger('ddr.t', 'valid')$$);
SELECT 'user trigger kept after sync cleanup | ' || EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = 'ddr.other'::regclass AND t.tgname = 'user_sync');
ROLLBACK TO SAVEPOINT drop_sync_restrict;
SAVEPOINT drop_table_restrict;
SELECT 'DROP TABLE without CASCADE | ' || ddr.run('DROP TABLE ddr.t');
SELECT 'base table kept after refusal | ' || (to_regclass('ddr.t') IS NOT NULL);
SELECT 'user trigger kept after DROP TABLE | ' || EXISTS (SELECT FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = 'ddr.other'::regclass AND t.tgname = 'user_sync');
ROLLBACK TO SAVEPOINT drop_table_restrict;
DROP TRIGGER user_sync ON ddr.other;
SELECT 'era drops after user removes the dependency | ' || ddr.run($$SELECT sql_saga.drop_era('ddr.t')$$);
ROLLBACK TO SAVEPOINT reused_function;

\echo '--- dependency errors name the managed view and give the correct removal order'
SAVEPOINT key_diagnostic;
SELECT sql_saga.drop_for_portion_of_view('ddr.t') \g /dev/null
SELECT 'key removal names current view and its drop API | ' || ddr.view_diagnostic($$SELECT sql_saga.drop_unique_key('ddr.t', ARRAY['id']::name[])$$, 'ddr.t__current_valid', 'sql_saga.drop_current_view');
ROLLBACK TO SAVEPOINT key_diagnostic;
SAVEPOINT portion_diagnostic;
SELECT sql_saga.drop_current_view('ddr.t') \g /dev/null
SELECT 'key removal names portion view and its drop API | ' || ddr.view_diagnostic($$SELECT sql_saga.drop_unique_key('ddr.t', ARRAY['id']::name[])$$, 'ddr.t__for_portion_of_valid', 'sql_saga.drop_for_portion_of_view');
ROLLBACK TO SAVEPOINT portion_diagnostic;
SELECT 'table rename guidance drops views before keys | ' || ddr.rename_diagnostic('ALTER TABLE ddr.t RENAME TO renamed');
SELECT 'schema rename guidance drops views before keys | ' || ddr.rename_diagnostic('ALTER SCHEMA ddr RENAME TO renamed');
SELECT 'integrity after refused operations | ' || ddr.run('SELECT sql_saga.check_integrity()');
ROLLBACK;
RESET client_min_messages;
\i sql/include/test_teardown.sql
