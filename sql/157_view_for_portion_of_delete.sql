\set ECHO none
\set QUIET 1
\set ON_ERROR_STOP on
SET client_min_messages = warning;
SET datestyle = 'ISO, YMD';
\i sql/include/test_setup.sql
BEGIN;
CREATE SCHEMA portion_delete_157;
SET LOCAL search_path = portion_delete_157, public;
-- Everything, including the role and its grants, is rolled back at the end.
CREATE ROLE portion_delete_157_user;
GRANT USAGE ON SCHEMA portion_delete_157, sql_saga TO portion_delete_157_user;
GRANT SELECT ON ALL TABLES IN SCHEMA sql_saga TO portion_delete_157_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sql_saga TO portion_delete_157_user;
CREATE TABLE item (
    id int, part int, payload text,
    derived text GENERATED ALWAYS AS (upper(payload)) STORED,
    valid_range daterange, valid_from date, valid_until date, valid_to date,
    PRIMARY KEY (id, part, valid_range WITHOUT OVERLAPS)
);
SELECT sql_saga.add_era('item', 'valid_range', valid_to_column_name => 'valid_to') AS ignored \gset
SELECT sql_saga.add_unique_key('item', ARRAY['id','part']) AS ignored \gset
SELECT sql_saga.add_for_portion_of_view('item'::regclass) AS ignored \gset
CREATE FUNCTION check_item(expected text[]) RETURNS void LANGUAGE plpgsql AS $check_item$
BEGIN
    IF (SELECT array_agg(format('%s/%s:%s:%s:%s', id, part, coalesce(payload,'<NULL>'), coalesce(derived,'<NULL>'), valid_range) ORDER BY id, part, valid_range) FROM item) IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'Unexpected item rows: %', (SELECT jsonb_agg(to_jsonb(i) ORDER BY id,part,valid_range) FROM item AS i);
    END IF;
END;
$check_item$;
INSERT INTO item(id,part,payload,valid_from,valid_until) VALUES (1,7,'keep','2024-01-01','infinity');
SAVEPOINT middle;
UPDATE item__for_portion_of_valid SET id = NULL, valid_from = '2026-03-01', valid_until = '2026-06-01' WHERE id = 1;
DO $assert$ BEGIN PERFORM check_item(ARRAY['1/7:keep:KEEP:[2024-01-01,2026-03-01)','1/7:keep:KEEP:[2026-06-01,infinity)']); END $assert$;
\echo PASS middle split preserves OLD composite identity, payload and generated payload
ROLLBACK TO middle;
SAVEPOINT component;
UPDATE item__for_portion_of_valid SET part = NULL, valid_range = '[2026-03-01,2026-06-01)' WHERE id = 1;
DO $assert$ BEGIN PERFORM check_item(ARRAY['1/7:keep:KEEP:[2024-01-01,2026-03-01)','1/7:keep:KEEP:[2026-06-01,infinity)']); END $assert$;
\echo PASS any composite component and direct range
ROLLBACK TO component;
SAVEPOINT inclusive_end;
UPDATE item__for_portion_of_valid SET id = NULL, valid_from = '2026-03-01', valid_to = '2026-05-31' WHERE id = 1;
DO $assert$ BEGIN PERFORM check_item(ARRAY['1/7:keep:KEEP:[2024-01-01,2026-03-01)','1/7:keep:KEEP:[2026-06-01,infinity)']); END $assert$;
\echo PASS inclusive valid_to
ROLLBACK TO inclusive_end;
DO $assert$
DECLARE affected bigint;
BEGIN
    UPDATE item__for_portion_of_valid SET id = NULL, valid_range = '[2020-01-01,2021-01-01)' WHERE id = 1;
    GET DIAGNOSTICS affected = ROW_COUNT;
    IF affected <> 0 THEN RAISE EXCEPTION 'Empty intersection must report UPDATE 0, got %', affected; END IF;
    PERFORM check_item(ARRAY['1/7:keep:KEEP:[2024-01-01,infinity)']);
END $assert$;
\echo PASS empty intersection reports UPDATE 0 and leaves data unchanged
SAVEPOINT ambiguous;
DO $assert$
DECLARE before_rows jsonb;
BEGIN
    SELECT jsonb_agg(to_jsonb(i)) INTO before_rows FROM item AS i;
    BEGIN
        UPDATE item__for_portion_of_valid SET id = NULL, payload = 'changed' WHERE id = 1;
        RAISE EXCEPTION 'Expected ambiguous payload rejection' USING ERRCODE = 'P0002';
    EXCEPTION WHEN invalid_parameter_value THEN NULL;
    END;
    IF (SELECT jsonb_agg(to_jsonb(i)) FROM item AS i) IS DISTINCT FROM before_rows THEN RAISE EXCEPTION 'Ambiguous payload was not atomic'; END IF;
    BEGIN
        UPDATE item__for_portion_of_valid SET id = NULL, part = 8 WHERE id = 1;
        RAISE EXCEPTION 'Expected changed other identity rejection' USING ERRCODE = 'P0002';
    EXCEPTION WHEN invalid_parameter_value THEN NULL;
    END;
    IF (SELECT jsonb_agg(to_jsonb(i)) FROM item AS i) IS DISTINCT FROM before_rows THEN RAISE EXCEPTION 'Ambiguous identity was not atomic'; END IF;
    BEGIN
        UPDATE item__for_portion_of_valid SET id = NULL, derived = 'FORGED' WHERE id = 1;
        RAISE EXCEPTION 'Expected changed generated view payload rejection' USING ERRCODE = 'P0002';
    EXCEPTION WHEN invalid_parameter_value THEN NULL;
    END;
    IF (SELECT jsonb_agg(to_jsonb(i)) FROM item AS i) IS DISTINCT FROM before_rows THEN RAISE EXCEPTION 'Ambiguous generated payload was not atomic'; END IF;
END $assert$;
\echo PASS ambiguous payload, generated payload and changed nonnull identity reject atomically
ROLLBACK TO ambiguous;
SAVEPOINT null_payload;
UPDATE item__for_portion_of_valid SET payload = NULL WHERE id = 1;
DO $assert$ BEGIN PERFORM check_item(ARRAY['1/7:<NULL>:<NULL>:[2024-01-01,infinity)']); END $assert$;
\echo PASS ordinary NULL payload is an update, not a delete
ROLLBACK TO null_payload;
TRUNCATE item;
INSERT INTO item(id,part,payload,valid_from,valid_until) VALUES
(1,7,'a','2024-01-01','2025-01-01'),(1,7,'b','2025-01-01','2026-01-01'),(1,7,'c','2026-01-01','infinity'),(2,9,'other','2024-01-01','infinity');
SAVEPOINT spanning;
UPDATE item__for_portion_of_valid SET id = NULL, valid_from = '2024-07-01', valid_until = '2026-01-01' WHERE id = 1;
DO $assert$ BEGIN PERFORM check_item(ARRAY['1/7:a:A:[2024-01-01,2024-07-01)','1/7:c:C:[2026-01-01,infinity)','2/9:other:OTHER:[2024-01-01,infinity)']); END $assert$;
\echo PASS spanning multiple historical rows
ROLLBACK TO spanning;
SAVEPOINT aligned;
UPDATE item__for_portion_of_valid SET id = NULL, valid_from = '2025-01-01', valid_until = '2026-01-01' WHERE id = 1;
DO $assert$ BEGIN PERFORM check_item(ARRAY['1/7:a:A:[2024-01-01,2025-01-01)','1/7:c:C:[2026-01-01,infinity)','2/9:other:OTHER:[2024-01-01,infinity)']); END $assert$;
\echo PASS exactly aligned deletion leaves neighbours unchanged
ROLLBACK TO aligned;
SAVEPOINT complete;
UPDATE item__for_portion_of_valid SET id = NULL WHERE id = 1;
DO $assert$ BEGIN PERFORM check_item(ARRAY['2/9:other:OTHER:[2024-01-01,infinity)']); END $assert$;
\echo PASS no dates removes whole identity but not other entities
ROLLBACK TO complete;
-- Invoker security: view visibility is not sufficient authority to mutate hidden base rows.
ALTER TABLE item ENABLE ROW LEVEL SECURITY;
CREATE POLICY readable ON item FOR SELECT TO portion_delete_157_user USING (id = 1);
CREATE POLICY writable ON item FOR UPDATE TO portion_delete_157_user USING (id = 1) WITH CHECK (id = 1);
CREATE POLICY insertable ON item FOR INSERT TO portion_delete_157_user WITH CHECK (id = 1);
GRANT SELECT, UPDATE, INSERT ON item TO portion_delete_157_user;
GRANT SELECT, UPDATE ON item__for_portion_of_valid TO portion_delete_157_user;
SAVEPOINT missing_delete;
SET LOCAL ROLE portion_delete_157_user;
DO $assert$
BEGIN
    BEGIN
        -- Middle split physically uses UPDATE/INSERT, but semantic DELETE privilege is mandatory.
        UPDATE item__for_portion_of_valid SET id = NULL, valid_from = '2024-03-01', valid_until = '2024-06-01' WHERE id = 1;
        RAISE EXCEPTION 'Expected base DELETE privilege failure' USING ERRCODE = 'P0002';
    EXCEPTION WHEN insufficient_privilege THEN NULL;
    END;
END $assert$;
RESET ROLE;
DO $assert$ BEGIN IF (SELECT count(*) FROM item) <> 4 THEN RAISE EXCEPTION 'Permission failure changed rows'; END IF; END $assert$;
\echo PASS middle split requires base DELETE privilege
ROLLBACK TO missing_delete;
GRANT DELETE ON item TO portion_delete_157_user;
-- Discard scratch tables created by the owner before exercising a different invoker.
DO $scratch$
DECLARE scratch record;
BEGIN
    FOR scratch IN SELECT c.relname FROM pg_class AS c WHERE c.relnamespace = pg_my_temp_schema() AND c.relkind = 'r' LOOP
        EXECUTE format('DROP TABLE IF EXISTS pg_temp.%I CASCADE', scratch.relname);
    END LOOP;
END $scratch$;
SAVEPOINT invoker;
SET LOCAL ROLE portion_delete_157_user;
DO $assert$
DECLARE affected bigint;
BEGIN
    UPDATE item__for_portion_of_valid SET id = NULL, valid_from = '2024-03-01', valid_until = '2024-06-01' WHERE id = 2;
    GET DIAGNOSTICS affected = ROW_COUNT;
    IF affected <> 0 THEN RAISE EXCEPTION 'Invisible base row affected: %', affected; END IF;
END $assert$;
UPDATE item__for_portion_of_valid SET part = NULL, valid_from = '2024-03-01', valid_until = '2024-06-01' WHERE id = 1;
RESET ROLE;
DO $assert$ BEGIN PERFORM check_item(ARRAY['1/7:a:A:[2024-01-01,2024-03-01)','1/7:a:A:[2024-06-01,2025-01-01)','1/7:b:B:[2025-01-01,2026-01-01)','1/7:c:C:[2026-01-01,infinity)','2/9:other:OTHER:[2024-01-01,infinity)']); END $assert$;
\echo PASS invoker RLS visible split and invisible row protection
ROLLBACK TO invoker;
-- Physical UPDATE/INSERT policies govern a middle split, not a DELETE-only policy.
ALTER POLICY insertable ON item WITH CHECK (false);
SAVEPOINT denied_policy;
SET LOCAL ROLE portion_delete_157_user;
DO $assert$
BEGIN
    BEGIN
        UPDATE item__for_portion_of_valid SET id = NULL, valid_from = '2024-03-01', valid_until = '2024-06-01' WHERE id = 1;
        RAISE EXCEPTION 'Expected INSERT RLS policy rejection' USING ERRCODE = 'P0002';
    EXCEPTION WHEN insufficient_privilege THEN NULL;
    END;
END $assert$;
RESET ROLE;
DO $assert$ BEGIN PERFORM check_item(ARRAY['1/7:a:A:[2024-01-01,2025-01-01)','1/7:b:B:[2025-01-01,2026-01-01)','1/7:c:C:[2026-01-01,infinity)','2/9:other:OTHER:[2024-01-01,infinity)']); END $assert$;
\echo PASS physical INSERT policy denial rolls back middle split atomically
ROLLBACK TO denied_policy;
-- A native temporal FK must reject removal of covered parent time.
CREATE TABLE child (id int, part int, valid_range daterange,
    FOREIGN KEY (id, part, PERIOD valid_range) REFERENCES item (id, part, PERIOD valid_range) DEFERRABLE);
INSERT INTO child VALUES (1,7,'[2024-01-01,2025-01-01)');
DO $assert$
BEGIN
    BEGIN
        UPDATE item__for_portion_of_valid SET id = NULL, valid_from = '2024-03-01', valid_until = '2024-06-01' WHERE id = 1;
        SET CONSTRAINTS ALL IMMEDIATE;
        RAISE EXCEPTION 'Expected temporal FK failure' USING ERRCODE = 'P0002';
    EXCEPTION WHEN foreign_key_violation THEN NULL;
    END;
    PERFORM check_item(ARRAY['1/7:a:A:[2024-01-01,2025-01-01)','1/7:b:B:[2025-01-01,2026-01-01)','1/7:c:C:[2026-01-01,infinity)','2/9:other:OTHER:[2024-01-01,infinity)']);
END $assert$;
\echo PASS temporal FK failure preserves original rows atomically
DROP TABLE child;
-- A regular child refers to existence anywhere in history, not coverage of every instant.
CREATE TABLE regular_child (child_id int PRIMARY KEY, parent_id int, parent_part int);
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'regular_child'::regclass,
    fk_column_names => ARRAY['parent_id','parent_part']::name[],
    pk_table_oid => 'item'::regclass,
    pk_column_names => ARRAY['id','part']::name[],
    delete_action => 'CASCADE'
) AS ignored \gset
INSERT INTO regular_child VALUES (10,1,7),(20,2,9);
UPDATE item__for_portion_of_valid SET id = NULL, valid_from = '2024-03-01', valid_until = '2024-06-01' WHERE id = 1;
SET CONSTRAINTS ALL IMMEDIATE;
DO $assert$ BEGIN
    IF (SELECT array_agg(child_id ORDER BY child_id) FROM regular_child) IS DISTINCT FROM ARRAY[10,20] THEN
        RAISE EXCEPTION 'Partial history removal cascaded regular child';
    END IF;
    PERFORM check_item(ARRAY['1/7:a:A:[2024-01-01,2024-03-01)','1/7:a:A:[2024-06-01,2025-01-01)','1/7:b:B:[2025-01-01,2026-01-01)','1/7:c:C:[2026-01-01,infinity)','2/9:other:OTHER:[2024-01-01,infinity)']);
END $assert$;
\echo PASS partial history deletion retains regular CASCADE child
UPDATE item__for_portion_of_valid SET part = NULL WHERE id = 1;
SET CONSTRAINTS ALL IMMEDIATE;
DO $assert$ BEGIN
    IF (SELECT array_agg(child_id ORDER BY child_id) FROM regular_child) IS DISTINCT FROM ARRAY[20] THEN
        RAISE EXCEPTION 'Full history removal did not cascade exactly its regular child';
    END IF;
    PERFORM check_item(ARRAY['2/9:other:OTHER:[2024-01-01,infinity)']);
END $assert$;
\echo PASS full history deletion cascades only matching regular child
ROLLBACK;
\i sql/include/test_teardown.sql
\echo PASS STATBUS-611 delete protocol
