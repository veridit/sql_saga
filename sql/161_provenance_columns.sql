\set ECHO none
\set QUIET 1
SET client_min_messages = warning;
\i sql/include/test_setup.sql
-- Provenance columns (doc/design/provenance.md, hug f86twg). Written before the feature: on 1.8 the
-- registration fails and every scenario shows today's wrong writer. The expected output states the
-- proposal's answers to the owner's questions 2-4 (source values overwritten; no coalescing across writers;
-- carried pieces keep writer and time). Each scenario runs in its own savepoint; all is rolled back.
\set ON_ERROR_STOP off
BEGIN;
CREATE SCHEMA pv_161;
SET LOCAL search_path = pv_161, public;
DO $$BEGIN CREATE ROLE pv_161_admin; EXCEPTION WHEN duplicate_object THEN NULL; END$$;
DO $$BEGIN CREATE ROLE pv_161_user; EXCEPTION WHEN duplicate_object THEN NULL; END$$;
GRANT USAGE ON SCHEMA pv_161 TO pv_161_admin, pv_161_user;
-- edit_at counts from a sequence, so it is never reused; history shows whether a row kept its start value.
CREATE SEQUENCE edit_seq;
GRANT USAGE ON SEQUENCE edit_seq TO pv_161_admin, pv_161_user;
CREATE TABLE lu (id int NOT NULL, name text, valid_range daterange NOT NULL, valid_from date, valid_until date,
                 edit_by name NOT NULL DEFAULT current_user,
                 edit_at bigint NOT NULL DEFAULT nextval('pv_161.edit_seq'),
                 edit_comment text);
SELECT sql_saga.add_era('lu', 'valid_range', 'valid', valid_from_column_name => 'valid_from',
                        valid_until_column_name => 'valid_until') AS ignored \gset
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
SELECT sql_saga.set_era_annotation_columns('lu', 'valid', ARRAY['edit_comment']);
SELECT era_name, provenance_columns, annotation_columns FROM sql_saga.era WHERE table_name = 'lu';
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

\echo '-- 1+6: the user splits 2024 through the view: carried pieces keep admin/1, the changed piece is the user''s'
SAVEPOINT s;
SET ROLE pv_161_user;
UPDATE lu__for_portion_of_valid SET name = 'mid', valid_from = '2024-04-01', valid_until = '2024-07-01' WHERE id = 1;
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 2: a re-save of unchanged data writes nothing (edit_by and edit_at unchanged)'
SAVEPOINT s;
SET ROLE pv_161_user;
UPDATE lu__for_portion_of_valid SET name = 'A', valid_from = '2024-01-01', valid_until = '2025-01-01' WHERE id = 1;
UPDATE lu SET name = name WHERE valid_from = '2025-01-01';
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 3: the user sets 2025+ to the admin''s 2024 value: two rows, not one credited to either'
SAVEPOINT s;
SET ROLE pv_161_user;
UPDATE lu__for_portion_of_valid SET name = 'A', valid_from = '2025-01-01', valid_until = 'infinity' WHERE id = 1;
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 4: a direct UPDATE that tries to set the writer gets the real writer'
SAVEPOINT s;
SET ROLE pv_161_user;
UPDATE lu SET name = 'B2', edit_by = 'pv_161_admin', edit_at = 0 WHERE valid_from = '2025-01-01';
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 5: a direct UPDATE that does not touch the writer still records the user'
SAVEPOINT s;
SET ROLE pv_161_user;
UPDATE lu SET name = 'B3' WHERE valid_from = '2025-01-01';
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 7: temporal_merge PATCH by the user: the new piece is the user''s, the shrunk carried piece stays admin/1'
SAVEPOINT s;
SET ROLE pv_161_user;
CREATE TEMP TABLE src (row_id int, id int, name text, valid_from date, valid_until date);
INSERT INTO src VALUES (1, 1, 'late', '2024-10-01', '2025-01-01');
CALL sql_saga.temporal_merge(target_table => 'pv_161.lu', source_table => 'src', primary_identity_columns => ARRAY['id'],
                             mode => 'MERGE_ENTITY_PATCH', era_name => 'valid');
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 8: a source that supplies the writer (import on behalf of someone) is overwritten too'
SAVEPOINT s;
SET ROLE pv_161_user;
CREATE TEMP TABLE src2 (row_id int, id int, name text, valid_from date, valid_until date, edit_by name, edit_at bigint);
INSERT INTO src2 VALUES (1, 2, 'new', '2024-01-01', 'infinity', 'pv_161_admin', 0);
CALL sql_saga.temporal_merge(target_table => 'pv_161.lu', source_table => 'src2', primary_identity_columns => ARRAY['id'],
                             mode => 'MERGE_ENTITY_PATCH', era_name => 'valid');
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 10: a comment written with a change lands on the changed piece only; carried pieces keep theirs'
SAVEPOINT s;
SET ROLE pv_161_user;
UPDATE lu__for_portion_of_valid SET name = 'mid', edit_comment = 'user fixed mid-2024',
       valid_from = '2024-04-01', valid_until = '2024-07-01' WHERE id = 1;
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 11: a direct UPDATE of data without a new comment does not keep the old comment'
SAVEPOINT s;
SET ROLE pv_161_user;
UPDATE lu SET name = 'B4' WHERE valid_from = '2025-01-01';
RESET ROLE;
TABLE history;
ROLLBACK TO SAVEPOINT s;

\echo '-- 12: a provenance DEFAULT that gives NULL for the writing role is refused with the column and role named'
SAVEPOINT s;
ALTER TABLE lu ALTER COLUMN edit_by SET DEFAULT NULLIF(current_user, 'pv_161_user'::name);
SET ROLE pv_161_user;
UPDATE lu SET name = 'B5' WHERE valid_from = '2025-01-01';
RESET ROLE;
ROLLBACK TO SAVEPOINT s;

\echo '-- 13: the owner backfills inside one transaction with the managed trigger disabled'
SAVEPOINT s;
ALTER TABLE lu DISABLE TRIGGER valid_provenance;
UPDATE lu SET edit_by = 'pv_161_user' WHERE valid_from = '2025-01-01';
ALTER TABLE lu ENABLE TRIGGER valid_provenance;
TABLE history;
\echo '-- 13b: a provenance trigger left disabled is reported by check_integrity'
ALTER TABLE lu DISABLE TRIGGER valid_provenance;
SELECT sql_saga.check_integrity();
ROLLBACK TO SAVEPOINT s;

\echo '-- 9: registration refusals'
SAVEPOINT s;
ALTER TABLE lu ADD COLUMN note text;
SELECT sql_saga.set_era_provenance_columns('lu', 'valid', ARRAY['note']);
ROLLBACK TO SAVEPOINT s;
SAVEPOINT s;
SELECT sql_saga.add_era('lu', 'valid_range', 'valid2', ephemeral_columns => ARRAY['edit_by'],
                        provenance_columns => ARRAY['edit_by']);
ROLLBACK TO SAVEPOINT s;

ROLLBACK;
\set ON_ERROR_STOP on
\i sql/include/test_teardown.sql
