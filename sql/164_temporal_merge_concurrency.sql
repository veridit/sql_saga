\set ECHO none
\set QUIET 1
SET client_min_messages = warning;
\i sql/include/test_setup.sql
-- temporal_merge under concurrent writers, the single-session parts (hug 7rzgv5, doc/design/temporal-merge-concurrency.md).
-- The concurrent scenarios are isolation tests (make test-isolation); this test holds what one session can show:
-- the executor refuses a write that changed other rows than planned, the entity locks follow the target's types and
-- every key a source row names, and the for_portion_of view merges only the columns a statement changed.
CREATE SCHEMA cc_164;
SET search_path TO cc_164, public;
SET datestyle TO ISO, YMD;
CREATE TABLE t (id int NOT NULL, code text, v text, note text, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('t', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until') AS ignored \gset
SELECT sql_saga.add_unique_key('t', ARRAY['id'], key_type => 'primary') AS ignored \gset
SELECT sql_saga.add_unique_key('t', ARRAY['code'], key_type => 'natural') AS ignored \gset
SELECT sql_saga.add_for_portion_of_view('t') AS ignored \gset
-- The advisory locks this session holds on a table's entities: classid is the table oid, objid the stripe. Advisory
-- transaction locks outlive a rollback to a savepoint, so each case below uses its own table.
CREATE FUNCTION entity_locks(table_oid regclass) RETURNS SETOF oid LANGUAGE sql AS $$
    SELECT l.objid FROM pg_locks AS l
    WHERE l.locktype = 'advisory' AND l.pid = pg_backend_pid() AND l.objsubid = 2
      AND l.classid = (table_oid::oid::bigint & 4294967295)::oid
    ORDER BY l.objid
$$;
\set ECHO all
\set QUIET 0
\set VERBOSITY terse

BEGIN;
INSERT INTO t (id, code, v, note, valid_from, valid_until) VALUES (1, 'A', 'a', 'orig', '2020-01-01', '2030-01-01'),
                                                             (2, 'B', 'b', 'orig', '2020-01-01', '2030-01-01');

\echo '-- A merge takes one advisory lock per key a source row names: the identity and the natural key.'
SAVEPOINT locks;
CREATE TEMP TABLE src (row_id int, id int, code text, v text, valid_from date, valid_until date) ON COMMIT DROP;
INSERT INTO src VALUES (1, 1, 'A', 'x', '2025-01-01', '2026-01-01'), (2, 2, 'B', 'y', '2025-01-01', '2026-01-01');
CALL sql_saga.temporal_merge(target_table => 't', source_table => 'src', primary_identity_columns => ARRAY['id']);
SELECT count(*) AS locks_on_t FROM entity_locks('t');
ROLLBACK TO SAVEPOINT locks;

\echo '-- The lock follows the target type: for a numeric key, source text ''1'' and ''1.0'' name one entity and take one lock.'
CREATE TABLE tn (n numeric NOT NULL, v text, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('tn', 'valid_range') AS ignored \gset
SELECT sql_saga.add_unique_key('tn', ARRAY['n'], key_type => 'primary') AS ignored \gset
CREATE TEMP TABLE src_one (row_id int, n text, v text, valid_range daterange) ON COMMIT DROP;
INSERT INTO src_one VALUES (1, '1', 'x', '[2025-01-01,2026-01-01)');
CREATE TEMP TABLE src_one_zero (row_id int, n text, v text, valid_range daterange) ON COMMIT DROP;
INSERT INTO src_one_zero VALUES (1, '1.0', 'x', '[2025-01-01,2026-01-01)');
SAVEPOINT typed;
CALL sql_saga.temporal_merge(target_table => 'tn', source_table => 'src_one', primary_identity_columns => ARRAY['n']);
SELECT entity_locks('tn') AS one_key \gset
ROLLBACK TO SAVEPOINT typed;
CALL sql_saga.temporal_merge(target_table => 'tn', source_table => 'src_one_zero', primary_identity_columns => ARRAY['n']);
SELECT count(*) AS locks_on_tn, bool_and(k = :'one_key') AS same_lock FROM entity_locks('tn') AS k;
ROLLBACK TO SAVEPOINT typed;

\echo '-- A source row naming an entity only by its natural key locks that key and the identity of the entity it finds: two locks.'
CREATE TABLE tk (id int NOT NULL, code text, v text, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('tk', 'valid_range') AS ignored \gset
SELECT sql_saga.add_unique_key('tk', ARRAY['id'], key_type => 'primary') AS ignored \gset
SELECT sql_saga.add_unique_key('tk', ARRAY['code'], key_type => 'natural') AS ignored \gset
INSERT INTO tk VALUES (2, 'B', 'b', '[2020-01-01,2030-01-01)');
SAVEPOINT nk;
CREATE TEMP TABLE src_nk (row_id int, code text, v text, valid_range daterange) ON COMMIT DROP;
INSERT INTO src_nk VALUES (1, 'B', 'nk', '[2025-01-01,2026-01-01)');
CALL sql_saga.temporal_merge(target_table => 'tk', source_table => 'src_nk', mode => 'PATCH_FOR_PORTION_OF', natural_identity_columns => ARRAY['code']);
SELECT count(*) AS locks_on_tk FROM entity_locks('tk');
SELECT id, code, v, valid_range FROM tk ORDER BY valid_range;
ROLLBACK TO SAVEPOINT nk;

\echo '-- The executor refuses an UPDATE that changed fewer rows than planned (a BEFORE trigger that skips it), instead of'
\echo '-- inserting the pieces around a row it did not shrink.'
SAVEPOINT skip;
CREATE FUNCTION skip_update() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$;
CREATE TRIGGER skip_update BEFORE UPDATE ON t FOR EACH ROW EXECUTE FUNCTION skip_update();
CREATE TEMP TABLE src_skip (row_id int, id int, v text, valid_from date, valid_until date) ON COMMIT DROP;
INSERT INTO src_skip VALUES (1, 1, 'x', '2025-01-01', '2026-01-01');
CALL sql_saga.temporal_merge(target_table => 't', source_table => 'src_skip', primary_identity_columns => ARRAY['id']);
ROLLBACK TO SAVEPOINT skip;

\echo '-- The for_portion_of view merges the columns a statement changed; the others keep the target value.'
SAVEPOINT view_columns;
UPDATE t SET note = 'committed meanwhile' WHERE id = 1 AND valid_from = '2020-01-01';
UPDATE t__for_portion_of_valid SET v = 'w', valid_from = '2025-01-01', valid_until = '2026-01-01' WHERE id = 1;
SELECT id, v, note, valid_from, valid_until FROM t WHERE id = 1 ORDER BY valid_from;
\echo '-- Rows of one statement that changed different columns are merged per set of columns.'
UPDATE t__for_portion_of_valid SET v = CASE WHEN id = 1 THEN v ELSE 'z' END, note = CASE WHEN id = 1 THEN 'n1' ELSE note END,
                                   valid_from = '2027-01-01', valid_until = '2028-01-01';
SELECT id, v, note, valid_from, valid_until FROM t ORDER BY id, valid_from;
ROLLBACK TO SAVEPOINT view_columns;
ROLLBACK;

\set ECHO none
\set QUIET 1
\set VERBOSITY default
DROP SCHEMA cc_164 CASCADE;
RESET search_path;
RESET datestyle;
\i sql/include/test_teardown.sql
