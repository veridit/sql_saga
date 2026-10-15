\i sql/include/test_setup.sql

-- Error messages name relations as PostgreSQL's own messages do: the plain name in double quotes,
-- schema-qualified when the relation is not on the search_path, never with identifier quoting inside.
SET client_min_messages = warning;
\set SHOW_CONTEXT never

BEGIN;
CREATE SCHEMA "my schema";
CREATE TABLE "my schema"."My Table" (id int, valid_range daterange NOT NULL, x text);
SELECT sql_saga.add_era('"my schema"."My Table"', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('"my schema"."My Table"', ARRAY['id'], key_type => 'primary', unique_key_name => 'my_table_id_valid');
SELECT sql_saga.add_for_portion_of_view('"my schema"."My Table"', 'valid');
CREATE TABLE "my schema"."Ref Table" (id int PRIMARY KEY, p_id int, note text);
SELECT sql_saga.add_regular_foreign_key('"my schema"."Ref Table"', ARRAY['p_id'], 'my_table_id_valid', foreign_key_name => 'ref_table_p_id_fkey');
SELECT sql_saga.add_system_versioning('"my schema"."Ref Table"');

-- drop_protection
SAVEPOINT s;
DROP TRIGGER "Ref Table_truncate" ON "my schema"."Ref Table";
ROLLBACK TO SAVEPOINT s;
SAVEPOINT s;
DROP TRIGGER for_portion_of_valid ON "my schema"."My Table__for_portion_of_valid";
ROLLBACK TO SAVEPOINT s;
SAVEPOINT s;
DROP TRIGGER ref_table_p_id_fkey_uk_update ON "my schema"."My Table";
ROLLBACK TO SAVEPOINT s;
SAVEPOINT s;
DROP VIEW "my schema"."Ref Table_with_history" CASCADE;
ROLLBACK TO SAVEPOINT s;

-- health_checks
SAVEPOINT s;
ALTER TABLE "my schema"."Ref Table_history" DISABLE ROW LEVEL SECURITY;
ROLLBACK TO SAVEPOINT s;
SAVEPOINT s;
CREATE POLICY mine ON "my schema"."Ref Table_history" FOR SELECT USING (true);
ROLLBACK TO SAVEPOINT s;
SAVEPOINT s;
ALTER VIEW "my schema"."Ref Table_with_history" SET (security_invoker = off);
ROLLBACK TO SAVEPOINT s;

-- rename_following
SAVEPOINT s;
ALTER TRIGGER ref_table_p_id_fkey_uk_update ON "my schema"."My Table" RENAME TO renamed;
ROLLBACK TO SAVEPOINT s;
ROLLBACK;

RESET client_min_messages;
\i sql/include/test_teardown.sql
