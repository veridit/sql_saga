-- The scenarios of test 161, run once per planner (the native planner, the default, and the PL/pgSQL planner).
\set ON_ERROR_STOP off
\set SHOW_CONTEXT never
BEGIN;
CREATE SCHEMA pv_161;
SET LOCAL search_path = pv_161, public;
DO $$BEGIN CREATE ROLE pv_161_admin; EXCEPTION WHEN duplicate_object THEN NULL; END$$;
DO $$BEGIN CREATE ROLE pv_161_user; EXCEPTION WHEN duplicate_object THEN NULL; END$$;
GRANT USAGE ON SCHEMA pv_161 TO pv_161_admin, pv_161_user;
-- edit_at counts from a sequence, never reused, so the history view can show whether a row kept its start value.
-- Wrapped in a function: sql_saga treats a bare nextval() DEFAULT as a generated key and never copies it; a real
-- edit_at is a timestamp DEFAULT such as statement_timestamp().
CREATE SEQUENCE edit_seq;
GRANT USAGE ON SEQUENCE edit_seq TO pv_161_admin, pv_161_user;
CREATE FUNCTION next_edit() RETURNS bigint LANGUAGE sql AS $$ SELECT nextval('pv_161.edit_seq') $$;
GRANT EXECUTE ON FUNCTION next_edit() TO pv_161_admin, pv_161_user;
CREATE TABLE lu (id int NOT NULL, name text, valid_range daterange NOT NULL, valid_from date, valid_until date,
                 edit_by name NOT NULL DEFAULT current_user,
                 edit_at bigint NOT NULL DEFAULT pv_161.next_edit(),
                 edit_comment text);
SELECT sql_saga.add_era('lu', 'valid_range', 'valid', valid_from_column_name => 'valid_from',
                        valid_until_column_name => 'valid_until',
                        ephemeral_columns => ARRAY['edit_by', 'edit_at', 'edit_comment']) AS ignored \gset
SELECT sql_saga.add_unique_key('lu', ARRAY['id'], key_type => 'primary') AS ignored \gset
SELECT sql_saga.add_for_portion_of_view('lu') AS ignored \gset
GRANT SELECT, INSERT, UPDATE, DELETE ON lu, lu__for_portion_of_valid TO pv_161_admin, pv_161_user;
CREATE TABLE started (name text, edit_at bigint);
CREATE VIEW history AS
SELECT id, name, valid_range, edit_by,
       coalesce((SELECT 'kept from ' || s.name FROM started AS s WHERE s.edit_at = lu.edit_at), 'new') AS edit_at,
       edit_comment
FROM lu ORDER BY id, valid_from;

\echo '-- registration'
SAVEPOINT reg;
SELECT sql_saga.set_era_provenance_columns('lu', 'valid', ARRAY['edit_by', 'edit_at']);
SELECT era_name, ephemeral_columns, provenance_columns FROM sql_saga.era WHERE table_name = 'lu';
\if :ERROR
ROLLBACK TO SAVEPOINT reg;
\else
RELEASE SAVEPOINT reg;
\endif

-- The admin writes the starting history: two pieces.
SET ROLE pv_161_admin;
INSERT INTO lu (id, name, valid_from, valid_until, edit_comment) VALUES (1, 'A', '2024-01-01', '2025-01-01', 'admin set A'),
                                                                        (1, 'B', '2025-01-01', 'infinity', 'admin set B');
RESET ROLE;
INSERT INTO started SELECT name, edit_at FROM lu;
\echo '-- start: both pieces by the admin'
TABLE history;

\echo '-- 1+6: the user splits 2024 through the view, comment untouched: untouched pieces keep writer, time and comment; the changed piece is the user''s and keeps the comment the user left'
SAVEPOINT s;
SET ROLE pv_161_user;
UPDATE lu__for_portion_of_valid SET name = 'mid', valid_from = '2024-04-01', valid_until = '2024-07-01' WHERE id = 1;
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 1b: the same split with the comment modified'
SAVEPOINT s;
SET ROLE pv_161_user;
UPDATE lu__for_portion_of_valid SET name = 'mid', edit_comment = 'user fixed mid-2024',
       valid_from = '2024-04-01', valid_until = '2024-07-01' WHERE id = 1;
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 1c: the same split with the comment wiped'
SAVEPOINT s;
SET ROLE pv_161_user;
UPDATE lu__for_portion_of_valid SET name = 'mid', edit_comment = NULL,
       valid_from = '2024-04-01', valid_until = '2024-07-01' WHERE id = 1;
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 2: a re-save of unchanged data writes nothing'
SAVEPOINT s;
SET ROLE pv_161_user;
UPDATE lu__for_portion_of_valid SET name = 'A', valid_from = '2024-01-01', valid_until = '2025-01-01' WHERE id = 1;
UPDATE lu SET name = name WHERE valid_from = '2025-01-01';
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 3: the user sets 2025+ to the admin''s 2024 value: two rows, the new one credited to the user'
SAVEPOINT s;
SET ROLE pv_161_user;
UPDATE lu__for_portion_of_valid SET name = 'A', valid_from = '2025-01-01', valid_until = 'infinity' WHERE id = 1;
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 4: a direct UPDATE that sets the writer gets the real writer'
SAVEPOINT s;
SET ROLE pv_161_user;
UPDATE lu SET name = 'B2', edit_by = 'pv_161_admin', edit_at = 0 WHERE valid_from = '2025-01-01';
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 5: a direct UPDATE that does not touch the writer still records the user (the comment is the user''s to keep)'
SAVEPOINT s;
SET ROLE pv_161_user;
UPDATE lu SET name = 'B3' WHERE valid_from = '2025-01-01';
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 5b: a direct UPDATE that only moves the period keeps writer and time, whatever it sets'
SAVEPOINT s;
SET ROLE pv_161_user;
UPDATE lu SET valid_until = '2030-01-01', edit_by = 'pv_161_user' WHERE valid_from = '2025-01-01';
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 7: temporal_merge PATCH by the user: the new piece is the user''s, the shrunk untouched piece stays the admin''s'
SAVEPOINT s;
SET ROLE pv_161_user;
CREATE TEMP TABLE src (row_id int, id int, name text, valid_from date, valid_until date);
INSERT INTO src VALUES (1, 1, 'late', '2024-10-01', '2025-01-01');
CALL sql_saga.temporal_merge(target_table => 'pv_161.lu', source_table => 'src', primary_identity_columns => ARRAY['id'],
                             mode => 'MERGE_ENTITY_PATCH', era_name => 'valid');
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 8: a source that supplies the writer is overruled by the DEFAULT'
SAVEPOINT s;
SET ROLE pv_161_user;
CREATE TEMP TABLE src2 (row_id int, id int, name text, valid_from date, valid_until date, edit_by name, edit_at bigint);
INSERT INTO src2 VALUES (1, 2, 'new', '2024-01-01', 'infinity', 'pv_161_admin', 0);
CALL sql_saga.temporal_merge(target_table => 'pv_161.lu', source_table => 'src2', primary_identity_columns => ARRAY['id'],
                             mode => 'MERGE_ENTITY_PATCH', era_name => 'valid');
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 9: a provenance DEFAULT that gives NULL for the writing role is refused with the column and role named'
SAVEPOINT s;
ALTER TABLE lu ALTER COLUMN edit_by SET DEFAULT NULLIF(current_user, 'pv_161_user'::name);
SET ROLE pv_161_user;
UPDATE lu SET name = 'B5' WHERE valid_from = '2025-01-01';
RESET ROLE;
ROLLBACK TO SAVEPOINT s;

\echo '-- 10: the owner backfills inside one transaction with the managed trigger disabled'
SAVEPOINT s;
ALTER TABLE lu DISABLE TRIGGER valid_provenance;
UPDATE lu SET edit_by = 'pv_161_user' WHERE valid_from = '2025-01-01';
ALTER TABLE lu ENABLE TRIGGER valid_provenance;
TABLE history;
\echo '-- 10b: a provenance trigger left disabled is reported by check_integrity'
ALTER TABLE lu DISABLE TRIGGER valid_provenance;
SELECT sql_saga.check_integrity();
ROLLBACK TO SAVEPOINT s;

\echo '-- 11: registration refusals: no DEFAULT; not an ephemeral column; removing a provenance column from ephemeral'
SAVEPOINT s;
ALTER TABLE lu ADD COLUMN note text;
SELECT sql_saga.set_era_ephemeral_columns('lu', 'valid', ARRAY['edit_by', 'edit_at', 'edit_comment', 'note']);
SELECT sql_saga.set_era_provenance_columns('lu', 'valid', ARRAY['edit_by', 'edit_at', 'note']);
ROLLBACK TO SAVEPOINT s;
SAVEPOINT s;
SELECT sql_saga.set_era_provenance_columns('lu', 'valid', ARRAY['edit_by', 'name']);
ROLLBACK TO SAVEPOINT s;
SAVEPOINT s;
SELECT sql_saga.set_era_ephemeral_columns('lu', 'valid', ARRAY['edit_comment']);
ROLLBACK TO SAVEPOINT s;

\echo '-- 12: removing provenance after the fact drops the managed trigger; a direct UPDATE then keeps a forged writer again'
SAVEPOINT s;
SELECT sql_saga.set_era_provenance_columns('lu', 'valid', NULL);
SELECT era_name, provenance_columns, (SELECT count(*) FROM pg_trigger WHERE tgrelid = 'lu'::regclass AND tgname = 'valid_provenance') AS triggers
FROM sql_saga.era WHERE table_name = 'lu';
UPDATE lu SET name = 'B6', edit_by = 'someone' WHERE valid_from = '2025-01-01';
SELECT name, edit_by FROM lu WHERE valid_from = '2025-01-01';
ROLLBACK TO SAVEPOINT s;

\echo '-- 13: provenance given when the era is created, through add_era'
SAVEPOINT s;
CREATE TABLE lu2 (id int NOT NULL, name text, valid_range daterange NOT NULL,
                  edit_by name NOT NULL DEFAULT current_user, edit_comment text);
SELECT sql_saga.add_era('lu2', 'valid_range', 'valid', ephemeral_columns => ARRAY['edit_by', 'edit_comment'],
                        provenance_columns => ARRAY['edit_by']);
SELECT era_name, provenance_columns, (SELECT count(*) FROM pg_trigger WHERE tgrelid = 'lu2'::regclass AND tgname = 'valid_provenance') AS triggers
FROM sql_saga.era WHERE table_name = 'lu2';
GRANT INSERT, SELECT ON lu2 TO pv_161_user;
SET ROLE pv_161_user;
INSERT INTO lu2 (id, name, valid_range, edit_by) VALUES (1, 'x', '[2024-01-01,infinity)', 'pv_161_admin');
RESET ROLE;
SELECT name, edit_by FROM lu2;
\echo '-- 13b: add_era refuses a provenance column that is not ephemeral'
SELECT sql_saga.drop_era('lu2');
SELECT sql_saga.add_era('lu2', 'valid_range', 'valid', provenance_columns => ARRAY['edit_by']);
ROLLBACK TO SAVEPOINT s;

\echo '-- 14: a provenance column cannot be dropped, nor the managed trigger; a rename is followed'
SAVEPOINT s;
-- lu's views depend on every column, so the column guard is shown on a table without views.
CREATE TABLE lu3 (id int NOT NULL, valid_range daterange NOT NULL, edit_by name NOT NULL DEFAULT current_user);
SELECT sql_saga.add_era('lu3', 'valid_range', ephemeral_columns => ARRAY['edit_by'], provenance_columns => ARRAY['edit_by']) AS ignored \gset
ALTER TABLE lu3 DROP COLUMN edit_by;
ROLLBACK TO SAVEPOINT s;
SAVEPOINT s;
DROP TRIGGER valid_provenance ON lu;
ROLLBACK TO SAVEPOINT s;
SAVEPOINT s;
ALTER TABLE lu RENAME COLUMN edit_by TO written_by;
SELECT ephemeral_columns, provenance_columns FROM sql_saga.era WHERE table_name = 'lu';
SET ROLE pv_161_user;
UPDATE lu SET name = 'B7' WHERE valid_from = '2025-01-01';
RESET ROLE;
SELECT name, written_by FROM lu WHERE valid_from = '2025-01-01';
ROLLBACK TO SAVEPOINT s;

ROLLBACK;
