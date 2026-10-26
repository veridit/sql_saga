\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * health_checks is an event-triggered SECURITY INVOKER function. Its ownership repair is limited to the
 * families in the current DDL event; a caller does not need USAGE on an unrelated family's schema and cannot
 * cause that family's history to be re-owned. The second family's owner is deliberately different.
 */
CREATE ROLE he_a;
CREATE ROLE he_b;
CREATE ROLE he_drift;
CREATE SCHEMA he_a_schema AUTHORIZATION he_a;
CREATE SCHEMA he_b_schema AUTHORIZATION he_b;
GRANT USAGE ON SCHEMA sql_saga TO he_a, he_b;

SET ROLE he_a;
CREATE TABLE he_a_schema.a (id int);
SELECT sql_saga.add_system_versioning('he_a_schema.a');
RESET ROLE;

SET ROLE he_b;
CREATE TABLE he_b_schema.b (id int);
SELECT sql_saga.add_system_versioning('he_b_schema.b');
RESET ROLE;

-- Drift B while the health trigger is disabled. A later DDL by A must not repair B or need its schema USAGE.
ALTER EVENT TRIGGER sql_saga_health_checks DISABLE;
ALTER TABLE he_b_schema.b_history OWNER TO he_drift;
ALTER EVENT TRIGGER sql_saga_health_checks ENABLE;

SET ROLE he_a;
ALTER TABLE he_a_schema.a ADD COLUMN note text;
RESET ROLE;

\o
SELECT 'caller column added | ' || EXISTS (
    SELECT 1 FROM pg_catalog.pg_attribute AS a
    WHERE a.attrelid = 'he_a_schema.a'::regclass AND a.attname = 'note' AND NOT a.attisdropped)::text;
SELECT 'unrelated history owner unchanged | ' || (c.relowner = 'he_drift'::regrole)::text
FROM pg_catalog.pg_class AS c WHERE c.oid = 'he_b_schema.b_history'::regclass;
SELECT 'unrelated family remains registered | ' || EXISTS (
    SELECT 1 FROM sql_saga.system_versioning AS sv
    WHERE (sv.table_schema, sv.table_name) = ('he_b_schema', 'b'))::text;

\o /dev/null
DROP SCHEMA he_a_schema CASCADE;
DROP SCHEMA he_b_schema CASCADE;
DROP OWNED BY he_a, he_b, he_drift;
DROP ROLE he_a, he_b, he_drift;
\i sql/include/test_teardown.sql
