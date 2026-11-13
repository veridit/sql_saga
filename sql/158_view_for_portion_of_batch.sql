\set ECHO none
\set QUIET 1
\set ON_ERROR_STOP on
SET client_min_messages = warning;
SET datestyle = 'ISO, YMD';
\i sql/include/test_setup.sql
-- 1.6: a statement through a for_portion_of view is applied with one temporal_merge per mode (removals,
-- then changes), by the view's AFTER UPDATE FOR EACH STATEMENT apply trigger. Results must equal the
-- former per-row application; everything here is rolled back at the end.
BEGIN;
CREATE SCHEMA view_batch_158;
SET LOCAL search_path = view_batch_158, public;
CREATE ROLE view_batch_158_user;
GRANT USAGE ON SCHEMA view_batch_158, sql_saga TO view_batch_158_user;
GRANT SELECT ON ALL TABLES IN SCHEMA sql_saga TO view_batch_158_user;
CREATE TABLE item (id int, payload text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('item', 'valid_range') AS ignored \gset
SELECT sql_saga.add_unique_key('item', ARRAY['id']) AS ignored \gset
SELECT sql_saga.add_for_portion_of_view('item'::regclass) AS ignored \gset
CREATE TABLE other (id int, payload text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('other', 'valid_range') AS ignored \gset
SELECT sql_saga.add_unique_key('other', ARRAY['id']) AS ignored \gset
SELECT sql_saga.add_for_portion_of_view('other'::regclass) AS ignored \gset
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
SELECT g, 'p' || g, '2024-01-01', 'infinity' FROM generate_series(1, 4) AS g;
INSERT INTO other (id, payload, valid_from, valid_until) VALUES (1, 'o1', '2024-01-01', 'infinity');

SELECT t.tgname, t.tgtype, t.tgfoid::regprocedure AS function
FROM pg_trigger AS t WHERE t.tgrelid = 'item__for_portion_of_valid'::regclass ORDER BY t.tgname;

SAVEPOINT many;
DO $assert$
DECLARE affected bigint;
BEGIN
    UPDATE item__for_portion_of_valid SET payload = 'new', valid_from = '2026-03-01', valid_until = '2026-06-01' WHERE id <= 3;
    GET DIAGNOSTICS affected = ROW_COUNT;
    IF affected <> 3 THEN RAISE EXCEPTION 'Expected UPDATE 3, got %', affected; END IF;
    PERFORM expect('item', ARRAY[
        '1:p1:[2024-01-01,2026-03-01)','1:new:[2026-03-01,2026-06-01)','1:p1:[2026-06-01,infinity)',
        '2:p2:[2024-01-01,2026-03-01)','2:new:[2026-03-01,2026-06-01)','2:p2:[2026-06-01,infinity)',
        '3:p3:[2024-01-01,2026-03-01)','3:new:[2026-03-01,2026-06-01)','3:p3:[2026-06-01,infinity)',
        '4:p4:[2024-01-01,infinity)']);
END $assert$;
\echo PASS one statement splits several entities and reports each matched row
-- Several stored rows of one entity in one statement: each row's own intersection.
UPDATE item__for_portion_of_valid SET payload = 'cross', valid_from = '2026-01-01', valid_until = '2026-04-01' WHERE id = 1;
DO $assert$ BEGIN PERFORM expect('item', ARRAY[
    '1:p1:[2024-01-01,2026-01-01)','1:cross:[2026-01-01,2026-04-01)','1:new:[2026-04-01,2026-06-01)','1:p1:[2026-06-01,infinity)',
    '2:p2:[2024-01-01,2026-03-01)','2:new:[2026-03-01,2026-06-01)','2:p2:[2026-06-01,infinity)',
    '3:p3:[2024-01-01,2026-03-01)','3:new:[2026-03-01,2026-06-01)','3:p3:[2026-06-01,infinity)',
    '4:p4:[2024-01-01,infinity)']); END $assert$;
\echo PASS one entity spanning several stored rows
ROLLBACK TO many;

SAVEPOINT mixed;
UPDATE item__for_portion_of_valid
SET id = CASE WHEN id = 1 THEN NULL ELSE id END,
    payload = CASE WHEN id = 1 THEN payload ELSE 'mixed' END,
    valid_from = '2026-03-01', valid_until = '2026-06-01'
WHERE id IN (1, 2);
DO $assert$ BEGIN PERFORM expect('item', ARRAY[
    '1:p1:[2024-01-01,2026-03-01)','1:p1:[2026-06-01,infinity)',
    '2:p2:[2024-01-01,2026-03-01)','2:mixed:[2026-03-01,2026-06-01)','2:p2:[2026-06-01,infinity)',
    '3:p3:[2024-01-01,infinity)','4:p4:[2024-01-01,infinity)']); END $assert$;
\echo PASS removal and change in one statement
ROLLBACK TO mixed;

SAVEPOINT returned_rows;
CREATE TEMP TABLE returned AS SELECT 0 AS id, ''::text AS payload WITH NO DATA;
WITH changed AS (
    UPDATE item__for_portion_of_valid SET payload = 'ret', valid_from = '2026-03-01', valid_until = '2026-06-01'
    WHERE id IN (3, 4) RETURNING id, payload)
INSERT INTO returned SELECT * FROM changed;
DO $assert$ BEGIN
    IF (SELECT array_agg(format('%s:%s', id, payload) ORDER BY id) FROM returned) IS DISTINCT FROM ARRAY['3:ret','4:ret'] THEN
        RAISE EXCEPTION 'Unexpected RETURNING rows';
    END IF;
END $assert$;
\echo PASS RETURNING reports the requested rows
ROLLBACK TO returned_rows;

SAVEPOINT failing;
ALTER TABLE item ADD CONSTRAINT payload_not_bad CHECK (payload <> 'bad');
DO $assert$
BEGIN
    BEGIN
        UPDATE item__for_portion_of_valid SET payload = 'bad', valid_from = '2026-03-01', valid_until = '2026-06-01' WHERE id IN (1, 2);
        RAISE EXCEPTION 'Expected CHECK violation' USING ERRCODE = 'P0002';
    EXCEPTION WHEN check_violation THEN NULL;
    END;
    PERFORM expect('item', ARRAY['1:p1:[2024-01-01,infinity)','2:p2:[2024-01-01,infinity)','3:p3:[2024-01-01,infinity)','4:p4:[2024-01-01,infinity)']);
    -- Nothing of the failed statement remains queued: the next statement applies only its own rows.
    UPDATE item__for_portion_of_valid SET payload = 'after', valid_from = '2026-03-01', valid_until = '2026-06-01' WHERE id = 3;
    PERFORM expect('item', ARRAY['1:p1:[2024-01-01,infinity)','2:p2:[2024-01-01,infinity)',
        '3:p3:[2024-01-01,2026-03-01)','3:after:[2026-03-01,2026-06-01)','3:p3:[2026-06-01,infinity)','4:p4:[2024-01-01,infinity)']);
END $assert$;
\echo PASS a failing statement changes nothing and leaves nothing queued
ROLLBACK TO failing;

SAVEPOINT cte;
WITH a AS (UPDATE item__for_portion_of_valid SET payload = 'a' WHERE id = 1 RETURNING id)
UPDATE other__for_portion_of_valid SET payload = 'b' WHERE id = 1;
WITH a AS (UPDATE item__for_portion_of_valid SET payload = 'c' WHERE id = 2 RETURNING id)
UPDATE item__for_portion_of_valid SET payload = 'd' WHERE id = 3;
DO $assert$ BEGIN
    PERFORM expect('item', ARRAY['1:a:[2024-01-01,infinity)','2:c:[2024-01-01,infinity)','3:d:[2024-01-01,infinity)','4:p4:[2024-01-01,infinity)']);
    PERFORM expect('other', ARRAY['1:b:[2024-01-01,infinity)']);
END $assert$;
\echo PASS writable CTEs over the same and different views
ROLLBACK TO cte;

SAVEPOINT nested;
CREATE FUNCTION write_through_view() RETURNS trigger LANGUAGE plpgsql AS $write_through_view$
BEGIN
    UPDATE item__for_portion_of_valid SET payload = 'inner' WHERE id = 4;
    RETURN NULL;
END;
$write_through_view$;
CREATE TRIGGER write_through_view AFTER UPDATE ON item FOR EACH STATEMENT EXECUTE FUNCTION write_through_view();
DO $assert$
BEGIN
    BEGIN
        UPDATE item__for_portion_of_valid SET payload = 'outer' WHERE id = 1;
        RAISE EXCEPTION 'Expected nested refusal' USING ERRCODE = 'P0002';
    EXCEPTION WHEN object_in_use THEN NULL;
    END;
    PERFORM expect('item', ARRAY['1:p1:[2024-01-01,infinity)','2:p2:[2024-01-01,infinity)','3:p3:[2024-01-01,infinity)','4:p4:[2024-01-01,infinity)']);
END $assert$;
DROP TRIGGER write_through_view ON item;
\echo PASS a base-table trigger cannot write through the view being applied
ROLLBACK TO nested;

SAVEPOINT disabled;
-- A view's triggers cannot be disabled, and in session_replication_role = replica neither of a view's
-- triggers fires (as for every ordinary trigger): the row trigger does not run, and nothing is queued.
SET LOCAL session_replication_role = replica;
UPDATE item__for_portion_of_valid SET payload = 'not_applied' WHERE id = 1;
RESET session_replication_role;
DO $assert$ BEGIN PERFORM expect('item', ARRAY['1:p1:[2024-01-01,infinity)','2:p2:[2024-01-01,infinity)','3:p3:[2024-01-01,infinity)','4:p4:[2024-01-01,infinity)']); END $assert$;
\echo PASS replica role fires neither view trigger and queues nothing
ROLLBACK TO disabled;

SAVEPOINT protected;
DO $assert$
BEGIN
    BEGIN
        DROP TRIGGER for_portion_of_valid_apply ON item__for_portion_of_valid;
        RAISE EXCEPTION 'Expected drop protection' USING ERRCODE = 'P0002';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM NOT LIKE 'cannot drop trigger "for_portion_of_valid_apply"%' THEN RAISE; END IF;
    END;
END $assert$;
\echo PASS the apply trigger is protected like the row trigger
ROLLBACK TO protected;

SAVEPOINT missing_apply;
-- Dropped behind drop protection's back: the row trigger refuses instead of reporting changes never made.
ALTER EVENT TRIGGER sql_saga_drop_protection DISABLE;
DROP TRIGGER for_portion_of_valid_apply ON item__for_portion_of_valid;
ALTER EVENT TRIGGER sql_saga_drop_protection ENABLE;
DO $assert$
BEGIN
    BEGIN
        UPDATE item__for_portion_of_valid SET payload = 'lost' WHERE id = 1;
        RAISE EXCEPTION 'Expected refusal without apply trigger' USING ERRCODE = 'P0002';
    EXCEPTION WHEN object_not_in_prerequisite_state THEN NULL;
    END;
    PERFORM expect('item', ARRAY['1:p1:[2024-01-01,infinity)','2:p2:[2024-01-01,infinity)','3:p3:[2024-01-01,infinity)','4:p4:[2024-01-01,infinity)']);
END $assert$;
\echo PASS a change is refused when the view has no apply trigger
ROLLBACK TO missing_apply;

SAVEPOINT integrity;
SELECT sql_saga.check_integrity() AS ignored \gset
ALTER EXTENSION sql_saga DROP FUNCTION sql_saga.for_portion_of_apply_trigger();
CREATE TRIGGER extra_apply AFTER UPDATE ON item__for_portion_of_valid FOR EACH STATEMENT EXECUTE FUNCTION sql_saga.for_portion_of_apply_trigger();
ALTER EXTENSION sql_saga ADD FUNCTION sql_saga.for_portion_of_apply_trigger();
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

-- Row-level security and privileges apply to the batched change, for the invoker, as before.
GRANT SELECT, UPDATE ON item__for_portion_of_valid TO view_batch_158_user;
GRANT SELECT, UPDATE, INSERT ON item TO view_batch_158_user;
ALTER TABLE item ENABLE ROW LEVEL SECURITY;
CREATE POLICY visible ON item FOR SELECT USING (id <> 2);
CREATE POLICY changeable ON item FOR UPDATE USING (id <> 2) WITH CHECK (id <> 2);
CREATE POLICY insertable ON item FOR INSERT WITH CHECK (id <> 2);
SAVEPOINT rls;
SET LOCAL ROLE view_batch_158_user;
DO $assert$
DECLARE affected bigint;
BEGIN
    UPDATE item__for_portion_of_valid SET payload = 'rls', valid_from = '2026-03-01', valid_until = '2026-06-01' WHERE id IN (1, 2, 3);
    GET DIAGNOSTICS affected = ROW_COUNT;
    IF affected <> 2 THEN RAISE EXCEPTION 'Expected only the 2 visible rows, got %', affected; END IF;
END $assert$;
RESET ROLE;
DO $assert$ BEGIN PERFORM expect('item', ARRAY[
    '1:p1:[2024-01-01,2026-03-01)','1:rls:[2026-03-01,2026-06-01)','1:p1:[2026-06-01,infinity)',
    '2:p2:[2024-01-01,infinity)',
    '3:p3:[2024-01-01,2026-03-01)','3:rls:[2026-03-01,2026-06-01)','3:p3:[2026-06-01,infinity)',
    '4:p4:[2024-01-01,infinity)']); END $assert$;
\echo PASS invoker row-level security limits a batched change to visible rows
ROLLBACK TO rls;
ALTER POLICY insertable ON item WITH CHECK (false);
SAVEPOINT rls_denied;
SET LOCAL ROLE view_batch_158_user;
DO $assert$
BEGIN
    BEGIN
        UPDATE item__for_portion_of_valid SET payload = 'denied', valid_from = '2026-03-01', valid_until = '2026-06-01' WHERE id IN (1, 3);
        RAISE EXCEPTION 'Expected INSERT policy refusal' USING ERRCODE = 'P0002';
    EXCEPTION WHEN insufficient_privilege THEN NULL;
    END;
END $assert$;
RESET ROLE;
DO $assert$ BEGIN PERFORM expect('item', ARRAY['1:p1:[2024-01-01,infinity)','2:p2:[2024-01-01,infinity)','3:p3:[2024-01-01,infinity)','4:p4:[2024-01-01,infinity)']); END $assert$;
\echo PASS a physical policy refusal rolls back the whole batched statement
ROLLBACK TO rls_denied;
ROLLBACK;
\i sql/include/test_teardown.sql
\echo PASS 1.6 batched for_portion_of view writes
