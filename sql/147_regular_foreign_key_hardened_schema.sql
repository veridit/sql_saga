\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * Regular foreign keys work for writers without any privilege on schema sql_saga (1.4, review round 2, R5).
 *
 * A native foreign key needs no privilege of the writer beyond the table it writes. A hardened installation may
 * revoke USAGE on schema sql_saga from PUBLIC. The triggers' verification query (F1) names sql_saga's catalog
 * tables; it was planned as the first writer of the backend, which then needed USAGE on the schema. It is planned
 * and run as the function owner now.
 *
 * The plan is kept per backend, so each check starts in a fresh session (\c) whose first firing is the writer's.
 */

SELECT current_user AS test_superuser \gset
CREATE ROLE hs_owner;
CREATE ROLE hs_writer LOGIN;
GRANT USAGE ON SCHEMA sql_saga TO hs_owner;
CREATE SCHEMA hs AUTHORIZATION hs_owner;
GRANT USAGE ON SCHEMA hs TO hs_writer;
SET ROLE hs_owner;
CREATE TABLE hs.unit (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('hs.unit', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('hs.unit', ARRAY['id'], key_type => 'primary');
CREATE TABLE hs.note (id int, unit_id int);
SELECT sql_saga.add_foreign_key('hs.note', ARRAY['unit_id'], 'hs.unit', ARRAY['id'], delete_action => 'CASCADE');
INSERT INTO hs.unit (id, valid_from, valid_until) VALUES (1, '2020-01-01', 'infinity'), (2, '2020-01-01', 'infinity');
GRANT SELECT, INSERT, DELETE ON hs.unit, hs.note TO hs_writer;
RESET ROLE;
REVOKE USAGE ON SCHEMA sql_saga FROM PUBLIC;
REVOKE ALL ON SCHEMA sql_saga FROM sql_saga_unprivileged_user;
SELECT 'writer has USAGE on sql_saga | ' || has_schema_privilege('hs_writer', 'sql_saga', 'USAGE');
\o
SELECT 'writer has USAGE on sql_saga | ' || has_schema_privilege('hs_writer', 'sql_saga', 'USAGE');

\echo '--- child insert, fresh session'
\c - :test_superuser
SET ROLE hs_writer;
\set VERBOSITY terse
INSERT INTO hs.note VALUES (1, 1);
SELECT 'child insert with a present key | ok';
\c - :test_superuser
SET ROLE hs_writer;
\set VERBOSITY terse
INSERT INTO hs.note VALUES (2, 99);

\echo '--- parent delete with cascade, fresh session'
\c - :test_superuser
SET ROLE hs_writer;
\set VERBOSITY terse
DELETE FROM hs.unit WHERE id = 1;
SELECT 'notes left after deleting unit 1 | ' || count(*) FROM hs.note;

\c - :test_superuser
\o /dev/null
GRANT USAGE ON SCHEMA sql_saga TO PUBLIC;
SET client_min_messages = warning;
DROP SCHEMA hs CASCADE;
DROP ROLE hs_writer;
REVOKE USAGE ON SCHEMA sql_saga FROM hs_owner;
DROP ROLE hs_owner;
\i sql/include/test_teardown.sql
