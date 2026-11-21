\set ECHO none
\set QUIET 1
SET client_min_messages = warning;
\i sql/include/test_setup.sql
-- Provenance columns (doc/design/provenance.md, hug f86twg). Written before the feature: the expected output is
-- the designed behaviour; on 1.8 it fails where that document says today's behaviour is wrong.
-- edit_by and edit_at are provenance columns (ephemeral, filled from their DEFAULT); edit_comment stays a plain
-- ephemeral column. Each scenario runs in a savepoint; everything is rolled back.
\echo '==== native planner (default)'
\i sql/include/161_provenance_scenarios.sql
\echo '==== PL/pgSQL planner (sql_saga.temporal_merge.use_plpgsql_planner)'
SET sql_saga.temporal_merge.use_plpgsql_planner = true;
\i sql/include/161_provenance_scenarios.sql
RESET sql_saga.temporal_merge.use_plpgsql_planner;
\set ON_ERROR_STOP on
\i sql/include/test_teardown.sql
