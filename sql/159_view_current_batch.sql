\set ECHO none
\set QUIET 1
\set ON_ERROR_STOP on
SET client_min_messages = warning;
SET datestyle = 'ISO, YMD';
\i sql/include/test_setup.sql
-- 1.7: an INSERT through a current view is applied with one temporal_merge (MERGE_ENTITY_UPSERT, each row
-- its own founding id) by the view's AFTER INSERT FOR EACH STATEMENT apply trigger. Results must equal the
-- former per-row application; everything here is rolled back at the end.
BEGIN;
CREATE SCHEMA view_batch_159;
SET LOCAL search_path = view_batch_159, public;
CREATE ROLE view_batch_159_user;
GRANT USAGE ON SCHEMA view_batch_159, sql_saga TO view_batch_159_user;
GRANT SELECT ON ALL TABLES IN SCHEMA sql_saga TO view_batch_159_user;
CREATE FUNCTION test_now() RETURNS date LANGUAGE sql AS $$ SELECT '2026-03-01'::date $$;
GRANT EXECUTE ON FUNCTION test_now() TO view_batch_159_user;
CREATE TABLE item (id int, payload text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('item', 'valid_range') AS ignored \gset
SELECT sql_saga.add_unique_key('item', ARRAY['id']) AS ignored \gset
SELECT sql_saga.add_current_view('item'::regclass, current_func_name => 'view_batch_159.test_now()') AS ignored \gset
-- Range-only era with the identifier from a temporal primary key.
CREATE TABLE ranged (id int, valid_range daterange NOT NULL, payload text, PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));
SELECT sql_saga.add_era('ranged', 'valid_range') AS ignored \gset
SELECT sql_saga.add_current_view('ranged'::regclass, current_func_name => 'view_batch_159.test_now()') AS ignored \gset
CREATE FUNCTION rows_of(t regclass) RETURNS text[] LANGUAGE plpgsql AS $rows_of$
DECLARE result text[];
BEGIN
    EXECUTE format('SELECT array_agg(format(''%%s:%%s:%%s'', id, coalesce(payload, ''<NULL>''), valid_range) ORDER BY id, valid_range) FROM %s', t)
    INTO result;
    RETURN result;
END;
$rows_of$;
CREATE FUNCTION expect(t regclass, expected text[]) RETURNS void LANGUAGE plpgsql AS $expect$
BEGIN
    IF rows_of(t) IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'Unexpected rows in %: %', t, rows_of(t);
    END IF;
END;
$expect$;
INSERT INTO item (id, payload, valid_from, valid_until)
SELECT g, 'p' || g, '2024-01-01', 'infinity' FROM generate_series(1, 2) AS g;
INSERT INTO ranged VALUES (1, '[2024-01-01,infinity)', 'r1');

SELECT t.tgname, t.tgtype, t.tgfoid::regprocedure AS function
FROM pg_trigger AS t WHERE t.tgrelid = 'item__current_valid'::regclass ORDER BY t.tgname;

SAVEPOINT many;
DO $assert$
DECLARE affected bigint;
BEGIN
    INSERT INTO item__current_valid (id, payload) SELECT g, 'n' || g FROM generate_series(10, 12) AS g;
    GET DIAGNOSTICS affected = ROW_COUNT;
    IF affected <> 3 THEN RAISE EXCEPTION 'Expected INSERT 3, got %', affected; END IF;
    PERFORM expect('item', ARRAY[
        '1:p1:[2024-01-01,infinity)','2:p2:[2024-01-01,infinity)',
        '10:n10:[2026-03-01,infinity)','11:n11:[2026-03-01,infinity)','12:n12:[2026-03-01,infinity)']);
END $assert$;
\echo PASS one statement inserts several new entities from now
ROLLBACK TO many;

SAVEPOINT existing;
INSERT INTO item__current_valid (id, payload) VALUES (1, 'changed'), (20, 'fresh');
DO $assert$ BEGIN PERFORM expect('item', ARRAY[
    '1:p1:[2024-01-01,2026-03-01)','1:changed:[2026-03-01,infinity)',
    '2:p2:[2024-01-01,infinity)','20:fresh:[2026-03-01,infinity)']); END $assert$;
\echo PASS an existing entity gets a new version from now, a new one starts now
ROLLBACK TO existing;

SAVEPOINT duplicates;
INSERT INTO item__current_valid (id, payload) VALUES (5, 'first'), (5, 'second'), (2, 'a'), (2, 'b');
DO $assert$ BEGIN PERFORM expect('item', ARRAY[
    '1:p1:[2024-01-01,infinity)',
    '2:p2:[2024-01-01,2026-03-01)','2:b:[2026-03-01,infinity)',
    '5:second:[2026-03-01,infinity)']); END $assert$;
\echo PASS the same entity twice in one statement ends with the later row
ROLLBACK TO duplicates;

SAVEPOINT ranged_rows;
INSERT INTO ranged__current_valid (id, payload) VALUES (1, 'r1b'), (2, 'r2');
DO $assert$ BEGIN PERFORM expect('ranged', ARRAY[
    '1:r1:[2024-01-01,2026-03-01)','1:r1b:[2026-03-01,infinity)','2:r2:[2026-03-01,infinity)']); END $assert$;
\echo PASS a range-only table with a temporal primary key
ROLLBACK TO ranged_rows;

SAVEPOINT returned_rows;
CREATE TEMP TABLE returned AS SELECT 0 AS id, ''::text AS payload WITH NO DATA;
WITH inserted AS (
    INSERT INTO item__current_valid (id, payload) VALUES (30, 'r30'), (31, 'r31') RETURNING id, payload)
INSERT INTO returned SELECT * FROM inserted;
DO $assert$ BEGIN
    IF (SELECT array_agg(format('%s:%s', id, payload) ORDER BY id) FROM returned) IS DISTINCT FROM ARRAY['30:r30','31:r31'] THEN
        RAISE EXCEPTION 'Unexpected RETURNING rows';
    END IF;
    PERFORM expect('item', ARRAY['1:p1:[2024-01-01,infinity)','2:p2:[2024-01-01,infinity)',
        '30:r30:[2026-03-01,infinity)','31:r31:[2026-03-01,infinity)']);
END $assert$;
\echo PASS RETURNING reports the inserted rows
ROLLBACK TO returned_rows;

SAVEPOINT failing;
ALTER TABLE item ADD CONSTRAINT payload_not_bad CHECK (payload <> 'bad');
DO $assert$
BEGIN
    BEGIN
        INSERT INTO item__current_valid (id, payload) VALUES (40, 'good'), (41, 'bad');
        RAISE EXCEPTION 'Expected CHECK violation' USING ERRCODE = 'P0002';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
    PERFORM expect('item', ARRAY['1:p1:[2024-01-01,infinity)','2:p2:[2024-01-01,infinity)']);
    -- Nothing of the failed statement remains queued: the next statement applies only its own rows.
    INSERT INTO item__current_valid (id, payload) VALUES (42, 'after');
    PERFORM expect('item', ARRAY['1:p1:[2024-01-01,infinity)','2:p2:[2024-01-01,infinity)','42:after:[2026-03-01,infinity)']);
END $assert$;
\echo PASS a failing statement inserts nothing and leaves nothing queued
ROLLBACK TO failing;

SAVEPOINT cte;
WITH a AS (INSERT INTO item__current_valid (id, payload) VALUES (60, 'a') RETURNING id)
INSERT INTO ranged__current_valid (id, payload) VALUES (60, 'b');
WITH a AS (INSERT INTO item__current_valid (id, payload) VALUES (61, 'c') RETURNING id)
INSERT INTO item__current_valid (id, payload) VALUES (62, 'd');
DO $assert$ BEGIN
    PERFORM expect('item', ARRAY['1:p1:[2024-01-01,infinity)','2:p2:[2024-01-01,infinity)',
        '60:a:[2026-03-01,infinity)','61:c:[2026-03-01,infinity)','62:d:[2026-03-01,infinity)']);
    PERFORM expect('ranged', ARRAY['1:r1:[2024-01-01,infinity)','60:b:[2026-03-01,infinity)']);
END $assert$;
\echo PASS writable CTEs over the same and different views
ROLLBACK TO cte;

SAVEPOINT updates_unchanged;
-- UPDATE and DELETE through the view are not batched and behave as before.
UPDATE item__current_valid SET payload = 'upd' WHERE id = 1;
DELETE FROM item__current_valid WHERE id = 2;
DO $assert$ BEGIN PERFORM expect('item', ARRAY[
    '1:p1:[2024-01-01,2026-03-01)','1:upd:[2026-03-01,infinity)','2:p2:[2024-01-01,2026-03-01)']); END $assert$;
\echo PASS UPDATE and DELETE through the view are unchanged
ROLLBACK TO updates_unchanged;

SAVEPOINT nested;
CREATE FUNCTION write_through_view() RETURNS trigger LANGUAGE plpgsql AS $write_through_view$
BEGIN
    INSERT INTO item__current_valid (id, payload) VALUES (71, 'inner');
    RETURN NULL;
END;
$write_through_view$;
CREATE TRIGGER write_through_view AFTER INSERT ON item FOR EACH STATEMENT EXECUTE FUNCTION write_through_view();
DO $assert$
BEGIN
    BEGIN
        INSERT INTO item__current_valid (id, payload) VALUES (70, 'outer');
        RAISE EXCEPTION 'Expected nested refusal' USING ERRCODE = 'P0002';
    EXCEPTION WHEN object_in_use THEN NULL;
    END;
    PERFORM expect('item', ARRAY['1:p1:[2024-01-01,infinity)','2:p2:[2024-01-01,infinity)']);
END $assert$;
DROP TRIGGER write_through_view ON item;
\echo PASS a base-table trigger cannot insert through the view being applied
ROLLBACK TO nested;

SAVEPOINT protected;
DO $assert$
BEGIN
    BEGIN
        DROP TRIGGER current_valid_apply ON item__current_valid;
        RAISE EXCEPTION 'Expected drop protection' USING ERRCODE = 'P0002';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE 'cannot drop trigger "current_valid_apply"%' THEN RAISE; END IF;
    END;
END $assert$;
\echo PASS the apply trigger is protected like the row trigger
ROLLBACK TO protected;

SAVEPOINT missing_apply;
-- Dropped behind drop protection's back: the row trigger refuses instead of reporting rows never stored.
ALTER EVENT TRIGGER sql_saga_drop_protection DISABLE;
DROP TRIGGER current_valid_apply ON item__current_valid;
ALTER EVENT TRIGGER sql_saga_drop_protection ENABLE;
DO $assert$
BEGIN
    BEGIN
        INSERT INTO item__current_valid (id, payload) VALUES (80, 'lost');
        RAISE EXCEPTION 'Expected refusal without apply trigger' USING ERRCODE = 'P0002';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN NULL;
    END;
    PERFORM expect('item', ARRAY['1:p1:[2024-01-01,infinity)','2:p2:[2024-01-01,infinity)']);
END $assert$;
\echo PASS an insert is refused when the view has no apply trigger
ROLLBACK TO missing_apply;

SAVEPOINT integrity;
SELECT sql_saga.check_integrity() AS ignored \gset
ALTER EXTENSION sql_saga DROP FUNCTION sql_saga.current_view_apply_trigger();
CREATE TRIGGER extra_apply AFTER INSERT ON item__current_valid FOR EACH STATEMENT EXECUTE FUNCTION sql_saga.current_view_apply_trigger();
ALTER EXTENSION sql_saga ADD FUNCTION sql_saga.current_view_apply_trigger();
DO $assert$
BEGIN
    BEGIN
        PERFORM sql_saga.check_integrity();
        RAISE EXCEPTION 'Expected integrity violation' USING ERRCODE = 'P0002';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE '%integrity violation for updatable view%' THEN RAISE; END IF;
    END;
END $assert$;
\echo PASS check_integrity requires exactly one apply trigger
ROLLBACK TO integrity;

SAVEPOINT renamed;
ALTER TRIGGER current_valid ON item__current_valid RENAME TO renamed_current;
INSERT INTO item__current_valid (id, payload) VALUES (90, 'renamed');
DO $assert$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_trigger WHERE tgrelid = 'item__current_valid'::regclass AND tgname = 'renamed_current_apply') THEN
        RAISE EXCEPTION 'The apply trigger did not follow the rename of the row trigger';
    END IF;
    PERFORM expect('item', ARRAY['1:p1:[2024-01-01,infinity)','2:p2:[2024-01-01,infinity)','90:renamed:[2026-03-01,infinity)']);
END $assert$;
SELECT sql_saga.check_integrity() AS ignored \gset
\echo PASS the apply trigger follows a rename of the row trigger
ROLLBACK TO renamed;

-- Row-level security and privileges apply to the batched insert, for the invoker, as before.
GRANT SELECT, INSERT ON item__current_valid TO view_batch_159_user;
GRANT SELECT, UPDATE, INSERT ON item TO view_batch_159_user;
ALTER TABLE item ENABLE ROW LEVEL SECURITY;
CREATE POLICY visible ON item FOR SELECT USING (true);
CREATE POLICY changeable ON item FOR UPDATE USING (true) WITH CHECK (true);
CREATE POLICY insertable ON item FOR INSERT WITH CHECK (id < 100);
SAVEPOINT rls;
SET LOCAL ROLE view_batch_159_user;
INSERT INTO item__current_valid (id, payload) VALUES (1, 'rls'), (95, 'rls');
RESET ROLE;
DO $assert$ BEGIN PERFORM expect('item', ARRAY[
    '1:p1:[2024-01-01,2026-03-01)','1:rls:[2026-03-01,infinity)',
    '2:p2:[2024-01-01,infinity)','95:rls:[2026-03-01,infinity)']); END $assert$;
\echo PASS the invoker may insert through the view under row-level security
ROLLBACK TO rls;
SAVEPOINT rls_denied;
SET LOCAL ROLE view_batch_159_user;
DO $assert$
BEGIN
    BEGIN
        INSERT INTO item__current_valid (id, payload) VALUES (96, 'ok'), (150, 'denied');
        RAISE EXCEPTION 'Expected INSERT policy refusal' USING ERRCODE = 'P0002';
    EXCEPTION WHEN insufficient_privilege THEN NULL;
    END;
END $assert$;
RESET ROLE;
DO $assert$ BEGIN PERFORM expect('item', ARRAY['1:p1:[2024-01-01,infinity)','2:p2:[2024-01-01,infinity)']); END $assert$;
\echo PASS a policy refusal rolls back the whole batched insert
ROLLBACK TO rls_denied;
ROLLBACK;
\i sql/include/test_teardown.sql
\echo PASS 1.7 batched current view inserts
