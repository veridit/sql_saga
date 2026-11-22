\set ECHO none
\set QUIET 1
SET client_min_messages = warning;
\i sql/include/test_setup.sql
-- temporal_merge writes array, jsonb, composite and numeric data columns identically under both planners (hug 7t5hd7).
-- The native planner carries each value as its text form; the PL/pgSQL planner as native JSON, which the executor
-- must read with the column's type rather than through ->> and a text cast.
CREATE SCHEMA tv_162;
SET search_path TO tv_162, public;
CREATE TYPE pair AS (a int, b text);
CREATE TABLE w (id int NOT NULL, t text[], j jsonb, c pair, n numeric, i int[],
    valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('w', 'valid_range') AS ignored \gset
SELECT sql_saga.add_unique_key('w', ARRAY['id']) AS ignored \gset
CREATE TABLE src (row_id int, id int, t text[], j jsonb, c pair, n numeric, i int[], valid_from date, valid_until date);
\set ECHO all
\set QUIET 0

\echo '==== native planner (default)'
\i sql/include/162_typed_payload_scenario.sql
\echo '==== PL/pgSQL planner (sql_saga.temporal_merge.use_plpgsql_planner)'
SET sql_saga.temporal_merge.use_plpgsql_planner = true;
\i sql/include/162_typed_payload_scenario.sql
RESET sql_saga.temporal_merge.use_plpgsql_planner;

\set ECHO none
\set QUIET 1
DROP SCHEMA tv_162 CASCADE;
RESET search_path;
\i sql/include/test_teardown.sql
