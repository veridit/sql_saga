\i sql/include/test_setup.sql

/*
 * Two regular foreign keys referencing the same unique key share one generated helper function.
 * Sharing must also work when the helper's schema or name needs quoting (the referenced table lives
 * in a schema with a space, its name and key column contain spaces and capitals).
 * Both FKs must be added, use the same helper, enforce both sides, survive dropping one of them,
 * and check_integrity() must pass.
 */
\set SHOW_CONTEXT never
\set VERBOSITY default
SET client_min_messages = warning;

CREATE SCHEMA "Shared Q";
SET search_path = "Shared Q", public;

CREATE TABLE "Shared Q"."Par T" ("Key Id" int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('"Shared Q"."Par T"', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('"Shared Q"."Par T"', ARRAY['Key Id']::name[], 'valid', key_type => 'natural');
INSERT INTO "Shared Q"."Par T" VALUES (1, '[2020-01-01,2030-01-01)'), (2, '[2020-01-01,2030-01-01)'), (3, '[2020-01-01,2030-01-01)');

CREATE TABLE "Shared Q".c1 (id int PRIMARY KEY, par_id int);
CREATE TABLE "Shared Q".c2 (id int PRIMARY KEY, owner_id int);
SELECT sql_saga.add_regular_foreign_key('"Shared Q".c1', ARRAY['par_id']::name[], 'Par T_Key Id_valid');
SELECT sql_saga.add_regular_foreign_key('"Shared Q".c2', ARRAY['owner_id']::name[], 'Par T_Key Id_valid');

/*
 * Historical fixture: converts a regular foreign key just added by add_regular_foreign_key (1.3 shape: a
 * referencing-side constraint trigger, no helper) back to the shape released sql_saga 1.0-1.2 built and that a
 * not yet migrated installation still has: the helper <uk schema>.<uk table>_<key columns>_exists(<child column
 * types>) with the canonical body, SECURITY DEFINER, search_path = pg_catalog, owned by the referenced table owner,
 * executable by PUBLIC and shared by every regular FK of the key; the child CHECK <fk_check_constraint> calling it;
 * the catalog row naming the helper. Same construction as test 128. sql_saga's event triggers are kept out of it.
 */
CREATE FUNCTION "Shared Q".legacy_regular_fk(fk_name name) RETURNS void LANGUAGE plpgsql AS $legacy_regular_fk$
DECLARE
    r record;
BEGIN
    PERFORM set_config('session_replication_role', 'replica', true);
    SELECT format('%I.%I', fk.table_schema, fk.table_name)::regclass AS child,
           format('%I.%I', uk.table_schema, uk.table_name)::regclass AS parent,
           fk.fk_check_constraint,
           format('%I.%I', uk.table_schema, sql_saga.__internal_make_name(ARRAY[uk.table_name] || ARRAY(
               SELECT c FROM unnest(uk.column_names) WITH ORDINALITY AS u (c, o) WHERE c <> e.range_column_name ORDER BY o)
               || ARRAY['exists']::name[])) AS helper,
           (SELECT string_agg(format_type(a.atttypid, a.atttypmod), ', ' ORDER BY u.o)
            FROM unnest(fk.column_names) WITH ORDINALITY AS u (n, o)
            JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (format('%I.%I', fk.table_schema, fk.table_name)::regclass, u.n)) AS types,
           (SELECT string_agg(quote_ident(n), ', ' ORDER BY o) FROM unnest(fk.column_names) WITH ORDINALITY AS u (n, o)) AS args,
           (SELECT rl.rolname FROM pg_catalog.pg_class AS pc JOIN pg_catalog.pg_roles AS rl ON rl.oid = pc.relowner
            WHERE pc.oid = format('%I.%I', uk.table_schema, uk.table_name)::regclass) AS owner
    INTO r
    FROM sql_saga.foreign_keys AS fk
    JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
    JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
    WHERE fk.foreign_key_name = fk_name AND fk.type = 'regular_to_temporal';
    -- Released 1.0-1.2 shared one helper between all regular FKs referencing the same key.
    IF to_regprocedure(format('%s(%s)', r.helper, r.types)) IS NULL THEN
        EXECUTE format('CREATE FUNCTION %s(%s) RETURNS boolean LANGUAGE sql STABLE STRICT SECURITY DEFINER SET search_path = pg_catalog AS %L',
                       r.helper, r.types, sql_saga.__internal_regular_fk_helper_body(fk_name));
        EXECUTE format('ALTER FUNCTION %s(%s) OWNER TO %I', r.helper, r.types, r.owner);
        EXECUTE format('GRANT EXECUTE ON FUNCTION %s(%s) TO PUBLIC', r.helper, r.types);
    END IF;
    EXECUTE format('DROP TRIGGER %I ON %s', r.fk_check_constraint, r.child);
    EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I CHECK (%s(%s))', r.child, r.fk_check_constraint, r.helper, r.args);
    UPDATE sql_saga.foreign_keys AS fk
    -- As released: format('%s(%s)', <schema-qualified %I.%I name>, <child column types>).
    SET fk_helper_function = format('%s(%s)', r.helper, r.types)
    WHERE fk.foreign_key_name = fk_name;
    PERFORM set_config('session_replication_role', 'origin', true);
END;
$legacy_regular_fk$;

-- Since 1.3 add_regular_foreign_key creates no helper; the sharing of the released helper is what this
-- test covers, so both foreign keys are put in the released 1.0-1.2 shape.
SELECT "Shared Q".legacy_regular_fk('c1_par_id_fkey');
SELECT "Shared Q".legacy_regular_fk('c2_owner_id_fkey');
DROP FUNCTION "Shared Q".legacy_regular_fk(name);

SELECT foreign_key_name, fk_helper_function FROM sql_saga.foreign_keys WHERE table_schema = 'Shared Q' ORDER BY 1;
SELECT p.oid::regprocedure AS helper FROM pg_catalog.pg_proc AS p WHERE p.pronamespace = '"Shared Q"'::regnamespace;
SELECT sql_saga.check_integrity();

-- c1 references key 1 only, c2 references key 2 only, key 3 is unreferenced, so a surviving sibling FK
-- cannot mask a broken one.
INSERT INTO "Shared Q".c1 VALUES (1, 1);
INSERT INTO "Shared Q".c2 VALUES (1, 2);
BEGIN;
INSERT INTO "Shared Q".c1 VALUES (2, 99);
ROLLBACK;
BEGIN;
DELETE FROM "Shared Q"."Par T" WHERE "Key Id" = 1;
ROLLBACK;
BEGIN;
INSERT INTO "Shared Q".c2 VALUES (2, 99);
ROLLBACK;
BEGIN;
DELETE FROM "Shared Q"."Par T" WHERE "Key Id" = 2;
ROLLBACK;
BEGIN;
DELETE FROM "Shared Q"."Par T" WHERE "Key Id" = 3;
ROLLBACK;

-- Dropping one FK keeps the shared helper for the other; dropping both removes it.
BEGIN;
SELECT sql_saga.drop_foreign_key('"Shared Q".c1', ARRAY['par_id']::name[]);
SELECT p.oid::regprocedure AS helper FROM pg_catalog.pg_proc AS p WHERE p.pronamespace = '"Shared Q"'::regnamespace;
SELECT sql_saga.check_integrity();
-- The surviving FK c2 still enforces both sides.
SAVEPOINT c2_missing_parent;
INSERT INTO "Shared Q".c2 VALUES (3, 99);
ROLLBACK TO SAVEPOINT c2_missing_parent;
SAVEPOINT c2_referenced_parent;
DELETE FROM "Shared Q"."Par T" WHERE "Key Id" = 2;
ROLLBACK TO SAVEPOINT c2_referenced_parent;
-- Key 1 is no longer referenced once c1's FK is gone.
SAVEPOINT c1_parent_free;
DELETE FROM "Shared Q"."Par T" WHERE "Key Id" = 1;
ROLLBACK TO SAVEPOINT c1_parent_free;
SELECT sql_saga.drop_foreign_key('"Shared Q".c2', ARRAY['owner_id']::name[]);
SELECT count(*) AS helpers_left FROM pg_catalog.pg_proc AS p WHERE p.pronamespace = '"Shared Q"'::regnamespace;
ROLLBACK;

SELECT sql_saga.check_integrity();
SELECT 'integrity checked' AS done;

-- The 1.3 shape with the same quoting: a referencing-side constraint trigger, no helper, both sides enforced.
BEGIN;
CREATE TABLE "Shared Q"."C 3" (id int PRIMARY KEY, "Par Ref" int);
SELECT sql_saga.add_regular_foreign_key('"Shared Q"."C 3"', ARRAY['Par Ref']::name[], 'Par T_Key Id_valid') AS c3_fk \gset
SELECT fk.fk_check_constraint, fk.fk_helper_function IS NULL AS no_helper, t.tgfoid::regprocedure AS trigger_function
FROM sql_saga.foreign_keys AS fk
JOIN pg_catalog.pg_trigger AS t ON t.tgrelid = '"Shared Q"."C 3"'::regclass AND t.tgname = fk.fk_check_constraint
WHERE fk.foreign_key_name = :'c3_fk';
INSERT INTO "Shared Q"."C 3" VALUES (1, 3);
SAVEPOINT c3_missing_parent;
INSERT INTO "Shared Q"."C 3" VALUES (2, 99);
ROLLBACK TO SAVEPOINT c3_missing_parent;
SAVEPOINT c3_referenced_parent;
DELETE FROM "Shared Q"."Par T" WHERE "Key Id" = 3;
ROLLBACK TO SAVEPOINT c3_referenced_parent;
SELECT sql_saga.check_integrity();
ROLLBACK;

SET client_min_messages = warning;
RESET search_path;
DROP SCHEMA "Shared Q" CASCADE;
\i sql/include/test_teardown.sql
RESET client_min_messages;
