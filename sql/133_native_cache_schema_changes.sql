\i sql/include/test_setup.sql

/*
 * temporal_merge's per-backend native caches follow schema changes (1.3).
 *
 * temporal_merge_executor_introspect (native/src/executor_cache.rs) caches, per backend, the SQL fragments it
 * generates for a target table, including the casts of every target column ((s.full_data->>'payload')::integer).
 * The cache key is the target OID, the source column NAMES and the call's arguments. The column TYPES of the target
 * are not part of it, so after ALTER TABLE ... ALTER COLUMN ... TYPE in the same session (or a ROLLBACK of one), a
 * warm cache keeps casting to the old type.
 *
 * The rule: a warm cache must behave exactly like a cold one, i.e. like a fresh session. Every result below is what a
 * fresh session (or temporal_merge_native_cache_reset() before each merge) gives. No view is involved, and the test
 * never calls the reset. Each section uses its own target table, so it warms only its own cache entries.
 *
 * 1. Target integer -> bigint after warming, source already bigint: a value only bigint holds is merged.
 * 2. The identity column integer -> bigint after warming: a new entity with a bigint id is merged.
 * 3. A type change (integer -> numeric) whose first merge warms the cache, then ROLLBACK: the merge casts to the
 *    restored integer again (2.5 refused, as in a fresh session).
 * 4. Guards: a source-only type change (same column names, part of the cache key already), and a DROP COLUMN whose
 *    merge warms the cache, then ROLLBACK: the restored column is written and kept again.
 */
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;

CREATE SCHEMA nc;
SET search_path = nc, public;

-- Each section merges into its own fresh target (the cache is keyed by target OID, so a section never sees the
-- cache another section warmed), created as a copy of this shape.
CREATE FUNCTION nc.new_target(name text) RETURNS void LANGUAGE plpgsql AS $new_target$
BEGIN
    EXECUTE format($$CREATE TABLE nc.%I (
        id integer NOT NULL,
        payload integer,
        note text,
        valid_range daterange NOT NULL,
        valid_from date,
        valid_until date)$$, name);
    PERFORM sql_saga.add_era(format('nc.%I', name)::regclass, 'valid_range',
                             valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
    PERFORM sql_saga.add_unique_key(format('nc.%I', name)::regclass, ARRAY['id'], key_type => 'primary');
    EXECUTE format($$INSERT INTO nc.%I (id, payload, note, valid_from, valid_until) VALUES (1, 1, 'kept', '2024-01-01', 'infinity')$$, name);
END;
$new_target$;

-- One temporal_merge of a source into a target, in a subtransaction: 'ok' (the change is kept) or the error.
CREATE FUNCTION nc.merge(target regclass, source regclass) RETURNS text LANGUAGE plpgsql AS $merge$
BEGIN
    CALL sql_saga.temporal_merge(target_table => target, source_table => source,
                                 primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_PATCH');
    RETURN 'ok';
EXCEPTION WHEN OTHERS THEN
    RETURN format('%s: %s', SQLSTATE, SQLERRM);
END;
$merge$;

-- A target's rows with the types they are stored as. A function, not a view: a view on the target would itself
-- block ALTER COLUMN TYPE and DROP COLUMN. The query is planned at each call, so it sees the current columns.
CREATE FUNCTION nc.target_rows(target regclass)
RETURNS TABLE (id text, payload text, payload_type regtype, id_type regtype, note text, valid_from date, valid_until date)
LANGUAGE plpgsql AS $target_rows$
BEGIN
    RETURN QUERY EXECUTE format($$
        SELECT t.id::text, t.payload::text, pg_typeof(t.payload)::regtype, pg_typeof(t.id)::regtype,
               (to_jsonb(t) ->> 'note'), t.valid_from, t.valid_until
        FROM %s AS t ORDER BY t.id, t.valid_from$$, target);
END;
$target_rows$;

SELECT nc.new_target('t1'), nc.new_target('t2'), nc.new_target('t3'), nc.new_target('t4a'), nc.new_target('t4b');

CREATE TABLE nc.src_big (row_id integer, id bigint, payload bigint, valid_from date, valid_until date);
CREATE TABLE nc.src_num (row_id integer, id integer, payload numeric, valid_from date, valid_until date);
CREATE TABLE nc.src_text (row_id integer, id integer, payload text, valid_from date, valid_until date);
CREATE TABLE nc.src_int (row_id integer, id integer, payload integer, valid_from date, valid_until date);

\echo '--- 1. Target column integer -> bigint after warming the cache; the source is already bigint'
BEGIN;
INSERT INTO nc.src_big VALUES (1, 1, 2, '2024-02-01', '2024-03-01');
SELECT nc.merge('nc.t1', 'nc.src_big') AS warm_merge_into_integer;
ALTER TABLE nc.t1 ALTER COLUMN payload TYPE bigint;
TRUNCATE nc.src_big;
INSERT INTO nc.src_big VALUES (1, 1, 5000000000, '2024-04-01', '2024-05-01');
SELECT nc.merge('nc.t1', 'nc.src_big') AS merge_5e9_into_bigint;
SELECT * FROM nc.target_rows('nc.t1');
ROLLBACK;

\echo '--- 2. Identity column integer -> bigint after warming the cache'
BEGIN;
INSERT INTO nc.src_big VALUES (1, 1, 2, '2024-02-01', '2024-03-01');
SELECT nc.merge('nc.t2', 'nc.src_big') AS warm_merge_with_integer_id;
ALTER TABLE nc.t2 ALTER COLUMN id TYPE bigint;
TRUNCATE nc.src_big;
INSERT INTO nc.src_big VALUES (1, 3000000000, 9, '2024-02-01', '2024-03-01');
SELECT nc.merge('nc.t2', 'nc.src_big') AS merge_new_entity_with_bigint_id;
SELECT * FROM nc.target_rows('nc.t2');
ROLLBACK;

\echo '--- 3. A type change rolled back after warming the cache under it'
-- Warm the cache while payload is numeric (2.5 is stored as 2.5), then roll the type change back.
BEGIN;
ALTER TABLE nc.t3 ALTER COLUMN payload TYPE numeric;
INSERT INTO nc.src_num VALUES (1, 1, 2.5, '2024-02-01', '2024-03-01');
SELECT nc.merge('nc.t3', 'nc.src_num') AS warm_merge_into_numeric;
SELECT * FROM nc.target_rows('nc.t3');
ROLLBACK;
SELECT format_type(a.atttypid, a.atttypmod) AS restored_payload_type
FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = 'nc.t3'::regclass AND a.attname = 'payload';
-- payload is integer again: the same merge is refused, as in a fresh session.
BEGIN;
INSERT INTO nc.src_num VALUES (1, 1, 2.5, '2024-02-01', '2024-03-01');
SELECT nc.merge('nc.t3', 'nc.src_num') AS merge_2_5_into_restored_integer;
SELECT * FROM nc.target_rows('nc.t3');
-- A value integer holds is merged as an integer.
TRUNCATE nc.src_num;
INSERT INTO nc.src_num VALUES (1, 1, 3, '2024-02-01', '2024-03-01');
SELECT nc.merge('nc.t3', 'nc.src_num') AS merge_3_into_restored_integer;
SELECT * FROM nc.target_rows('nc.t3');
ROLLBACK;

\echo '--- 4a. Source-only type change (same column names): already followed'
BEGIN;
INSERT INTO nc.src_text VALUES (1, 1, '4', '2024-02-01', '2024-03-01');
SELECT nc.merge('nc.t4a', 'nc.src_text') AS warm_merge_from_text;
ALTER TABLE nc.src_text ALTER COLUMN payload TYPE integer USING payload::integer;
UPDATE nc.src_text SET payload = 5, valid_from = '2024-03-01', valid_until = '2024-04-01';
SELECT nc.merge('nc.t4a', 'nc.src_text') AS merge_from_integer;
SELECT * FROM nc.target_rows('nc.t4a');
ROLLBACK;

\echo '--- 4b. DROP COLUMN rolled back after warming the cache: the restored column is kept'
BEGIN;
ALTER TABLE nc.t4b DROP COLUMN note;
INSERT INTO nc.src_int VALUES (1, 1, 6, '2024-02-01', '2024-03-01');
SELECT nc.merge('nc.t4b', 'nc.src_int') AS warm_merge_without_note;
ROLLBACK;
BEGIN;
INSERT INTO nc.src_int VALUES (1, 1, 7, '2024-02-01', '2024-03-01');
SELECT nc.merge('nc.t4b', 'nc.src_int') AS merge_with_note_restored;
SELECT * FROM nc.target_rows('nc.t4b');
ROLLBACK;

SELECT sql_saga.check_integrity();

SET client_min_messages = warning;
RESET search_path;
DROP SCHEMA nc CASCADE;
\i sql/include/test_teardown.sql
RESET client_min_messages;
