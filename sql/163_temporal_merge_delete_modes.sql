\set ECHO none
\set QUIET 1
SET client_min_messages = warning;
\i sql/include/test_setup.sql
-- temporal_merge delete modes, the same under both planners (hug rtwrp6, 2hxkwx).
-- DELETE_MISSING_TIMELINE deletes all time of a source entity the source does not cover; DELETE_MISSING_ENTITIES
-- deletes the entities the source does not name; both apply to the MERGE_ENTITY_* modes, and the other modes refuse
-- them.
CREATE SCHEMA dm_163;
SET search_path TO dm_163, public;
SET datestyle TO ISO, YMD;
CREATE TABLE g (id int, v text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('g', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until') AS ignored \gset
CREATE TABLE src (row_id int, id int, v text, valid_from date, valid_until date);
\set ECHO all
\set QUIET 0
\set VERBOSITY terse

BEGIN;
\echo '==== native planner (default)'
\i sql/include/163_delete_modes_scenario.sql
\echo '==== PL/pgSQL planner (sql_saga.temporal_merge.use_plpgsql_planner)'
SET LOCAL sql_saga.temporal_merge.use_plpgsql_planner = true;
\i sql/include/163_delete_modes_scenario.sql
ROLLBACK;

\set ECHO none
\set QUIET 1
\set VERBOSITY default
DROP SCHEMA dm_163 CASCADE;
RESET search_path;
RESET datestyle;
\i sql/include/test_teardown.sql
