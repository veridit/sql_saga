\i sql/include/test_setup.sql

/*
 * XOR unique keys (add_unique_key ... mutually_exclusive_columns => ...) whose column order is not
 * alphabetical, in the shapes StatBus uses (tmp/STATBUS_REPORT_xor_key_order.md):
 *   activity         (type, legal_unit_id, establishment_id)
 *   location         (type, establishment_id, legal_unit_id)
 *   contact          (legal_unit_id, establishment_id)
 *   person_for_unit  (person_id, person_role_id, legal_unit_id, establishment_id)
 *
 * sql_saga.unique_keys.column_names must stay in the order given to add_unique_key, which is the order of
 * the native constraint, across every DDL that does not rename a key column: ALTER TABLE ADD COLUMN,
 * CREATE INDEX, COMMENT, ADD CONSTRAINT, and the DDL sql_saga itself runs in add_foreign_key. A real
 * rename of a key column replaces that one name in place (in column_names and in
 * mutually_exclusive_columns) and keeps the order. check_integrity() passes after every step, and foreign
 * keys from and to the XOR key tables keep enforcing.
 */
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;

CREATE SCHEMA xko;
SET search_path = xko, public;

-- Catalog order versus the order of the native constraint (conkey without the range column).
CREATE VIEW xko.key_order AS
SELECT uk.unique_key_name, uk.column_names AS catalog_columns,
       (SELECT array_agg(a.attname ORDER BY u.ord)
        FROM unnest(c.conkey) WITH ORDINALITY AS u (attnum, ord)
        JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attnum) = (c.conrelid, u.attnum)
        WHERE a.attname <> e.range_column_name) AS constraint_columns,
       uk.mutually_exclusive_columns AS xor_columns
FROM sql_saga.unique_keys AS uk
JOIN sql_saga.era AS e ON (e.table_schema, e.table_name, e.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
JOIN pg_catalog.pg_constraint AS c
  ON c.conrelid = format('%I.%I', uk.table_schema, uk.table_name)::regclass AND c.conname = uk.unique_constraint
WHERE uk.table_schema = 'xko' AND uk.mutually_exclusive_columns IS NOT NULL
ORDER BY uk.unique_key_name;

-- check_integrity(), reported as one line so that a failure does not hide the steps after it.
CREATE FUNCTION xko.integrity(step text) RETURNS text LANGUAGE plpgsql AS $integrity$
BEGIN
    PERFORM sql_saga.check_integrity();
    RETURN step || ': ok';
EXCEPTION WHEN OTHERS THEN
    RETURN step || ': ' || SQLERRM;
END;
$integrity$;

-- Runs one statement with every deferrable constraint checked immediately, always rolled back, and
-- reports only whether it was accepted or refused, so this test does not depend on how a foreign key is
-- enforced (CHECK, trigger or native constraint) or on the wording of its error.
CREATE FUNCTION xko.probe(statement text) RETURNS text LANGUAGE plpgsql AS $probe$
BEGIN
    BEGIN
        SET CONSTRAINTS ALL IMMEDIATE;
        EXECUTE statement;
        RAISE EXCEPTION USING ERRCODE = 'P0004', MESSAGE = 'accepted';
    EXCEPTION
        WHEN assert_failure THEN RETURN 'accepted';
        WHEN integrity_constraint_violation THEN RETURN 'refused';
    END;
END;
$probe$;

CREATE TABLE xko.legal_unit (id int NOT NULL, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('xko.legal_unit', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('xko.legal_unit', ARRAY['id'], key_type => 'primary', unique_key_name => 'legal_unit_id_valid');
CREATE TABLE xko.establishment (id int NOT NULL, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('xko.establishment', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('xko.establishment', ARRAY['id'], key_type => 'primary', unique_key_name => 'establishment_id_valid');

CREATE TABLE xko.activity (id int NOT NULL, type text NOT NULL, legal_unit_id int, establishment_id int, valid_range daterange NOT NULL);
CREATE TABLE xko.location (id int NOT NULL, type text NOT NULL, establishment_id int, legal_unit_id int, valid_range daterange NOT NULL);
CREATE TABLE xko.contact (id int NOT NULL, legal_unit_id int, establishment_id int, valid_range daterange NOT NULL);
CREATE TABLE xko.person_for_unit (id int NOT NULL, person_id int NOT NULL, person_role_id int NOT NULL,
                                  legal_unit_id int, establishment_id int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('xko.activity', 'valid_range', 'valid');
SELECT sql_saga.add_era('xko.location', 'valid_range', 'valid');
SELECT sql_saga.add_era('xko.contact', 'valid_range', 'valid');
SELECT sql_saga.add_era('xko.person_for_unit', 'valid_range', 'valid');

\echo '--- Right after add_unique_key: catalog order equals constraint order'
SELECT sql_saga.add_unique_key(table_oid => 'xko.activity'::regclass, key_type => 'natural',
    column_names => ARRAY['type', 'legal_unit_id', 'establishment_id'],
    mutually_exclusive_columns => ARRAY['legal_unit_id', 'establishment_id'],
    unique_key_name => 'activity_type_establishment_id_valid');
SELECT sql_saga.add_unique_key(table_oid => 'xko.location'::regclass, key_type => 'natural',
    column_names => ARRAY['type', 'establishment_id', 'legal_unit_id'],
    mutually_exclusive_columns => ARRAY['establishment_id', 'legal_unit_id'],
    unique_key_name => 'location_type_establishment_id_valid');
SELECT sql_saga.add_unique_key(table_oid => 'xko.contact'::regclass, key_type => 'natural',
    column_names => ARRAY['legal_unit_id', 'establishment_id'],
    mutually_exclusive_columns => ARRAY['legal_unit_id', 'establishment_id'],
    unique_key_name => 'contact_natural_key_valid');
SELECT sql_saga.add_unique_key(table_oid => 'xko.person_for_unit'::regclass, key_type => 'natural',
    column_names => ARRAY['person_id', 'person_role_id', 'legal_unit_id', 'establishment_id'],
    mutually_exclusive_columns => ARRAY['legal_unit_id', 'establishment_id'],
    unique_key_name => 'person_for_unit_natural_key_valid');
TABLE xko.key_order;
SELECT xko.integrity('after add_unique_key');

\echo '--- add_foreign_key from the XOR tables (sql_saga runs DDL of its own)'
SELECT sql_saga.add_foreign_key('xko.activity', ARRAY['legal_unit_id'], 'xko.legal_unit', ARRAY['id']);
SELECT sql_saga.add_foreign_key('xko.activity', ARRAY['establishment_id'], 'xko.establishment', ARRAY['id']);
SELECT sql_saga.add_foreign_key('xko.person_for_unit', ARRAY['legal_unit_id'], 'xko.legal_unit', ARRAY['id']);
TABLE xko.key_order;
SELECT xko.integrity('after add_foreign_key');

\echo '--- Unrelated DDL: ALTER TABLE ADD COLUMN'
ALTER TABLE xko.activity ADD COLUMN note text;
TABLE xko.key_order;
SELECT xko.integrity('after ADD COLUMN');

\echo '--- Unrelated DDL: CREATE INDEX (the alphabetical lookup index StatBus creates)'
CREATE INDEX activity_lookup_idx ON xko.activity (establishment_id, legal_unit_id, type);
TABLE xko.key_order;
SELECT xko.integrity('after CREATE INDEX');

\echo '--- Unrelated DDL: COMMENT'
COMMENT ON TABLE xko.location IS 'where a unit is';
TABLE xko.key_order;
SELECT xko.integrity('after COMMENT');

\echo '--- Unrelated DDL: ALTER TABLE ADD CONSTRAINT'
ALTER TABLE xko.activity ADD CONSTRAINT "One and only one statistical unit id must be set"
    CHECK (num_nonnulls(establishment_id, legal_unit_id) = 1);
ALTER TABLE xko.contact ADD CONSTRAINT contact_one_unit CHECK (num_nonnulls(legal_unit_id, establishment_id) = 1);
TABLE xko.key_order;
SELECT xko.integrity('after ADD CONSTRAINT');

\echo '--- Unrelated DDL on another table only'
CREATE TABLE xko.unrelated (id int);
DROP TABLE xko.unrelated;
TABLE xko.key_order;
SELECT xko.integrity('after unrelated CREATE/DROP TABLE');

\echo '--- Foreign keys referencing the XOR key person_for_unit_natural_key_valid (columns paired by position)'
CREATE TABLE xko.person_ref (id int PRIMARY KEY, person_id int, person_role_id int, legal_unit_id int, establishment_id int);
SELECT sql_saga.add_regular_foreign_key('xko.person_ref',
    ARRAY['person_id', 'person_role_id', 'legal_unit_id', 'establishment_id'], 'person_for_unit_natural_key_valid',
    foreign_key_name => 'person_ref_fkey', fk_check_constraint => 'person_ref_check');
CREATE TABLE xko.person_tref (id int, person_id int, person_role_id int, legal_unit_id int, establishment_id int,
                              valid_range daterange NOT NULL);
SELECT sql_saga.add_era('xko.person_tref', 'valid_range', 'valid');
SELECT sql_saga.add_temporal_foreign_key('xko.person_tref',
    ARRAY['person_id', 'person_role_id', 'legal_unit_id', 'establishment_id'], 'valid', 'person_for_unit_natural_key_valid',
    foreign_key_name => 'person_tref_fkey');
SELECT fk.foreign_key_name, fk.type, fk.column_names, uk.column_names AS referenced_columns
FROM sql_saga.foreign_keys AS fk JOIN sql_saga.unique_keys AS uk USING (unique_key_name)
WHERE fk.table_schema = 'xko' AND fk.unique_key_name = 'person_for_unit_natural_key_valid'
ORDER BY 1;
TABLE xko.key_order;
SELECT xko.integrity('after foreign keys to the XOR key');

INSERT INTO xko.legal_unit VALUES (1, '[2020-01-01,infinity)'), (2, '[2020-01-01,infinity)');
INSERT INTO xko.establishment VALUES (7, '[2020-01-01,infinity)');
-- Key rows (person, role, legal unit, establishment). sql_saga does not require the XOR columns to be
-- exclusive (a num_nonnulls CHECK is the user's own), so all four are set here.
--   1 = (10, 20, 1, 7) referenced by person_ref only
--   2 = (11, 20, 2, 7) not referenced (yet)
--   3 = (12, 20, 2, 7) referenced by person_tref only
INSERT INTO xko.person_for_unit VALUES (1, 10, 20, 1, 7, '[2020-01-01,infinity)'),
                                       (2, 11, 20, 2, 7, '[2020-01-01,infinity)'),
                                       (3, 12, 20, 2, 7, '[2020-01-01,infinity)');
INSERT INTO xko.activity (id, type, legal_unit_id, establishment_id, valid_range) VALUES (1, 'primary', 1, NULL, '[2021-01-01,infinity)');

\echo '--- Valid references are accepted'
INSERT INTO xko.person_ref VALUES (1, 10, 20, 1, 7);
INSERT INTO xko.person_tref VALUES (1, 12, 20, 2, 7, '[2021-01-01,2022-01-01)');
SELECT count(*) AS person_ref_rows FROM xko.person_ref;
SELECT count(*) AS person_tref_rows FROM xko.person_tref;

\echo '--- Invalid references are refused, valid ones accepted'
SELECT xko.probe($$INSERT INTO xko.person_ref VALUES (2, 10, 20, 2, 7)$$) AS regular_fk_missing_key;
SELECT xko.probe($$INSERT INTO xko.person_ref VALUES (2, 11, 20, 2, 7)$$) AS regular_fk_existing_key;
SELECT xko.probe($$INSERT INTO xko.person_tref VALUES (2, 10, 20, 2, 7, '[2021-01-01,2022-01-01)')$$) AS temporal_fk_missing_key;
SELECT xko.probe($$INSERT INTO xko.person_tref VALUES (2, 11, 20, 2, 7, '[2021-01-01,2022-01-01)')$$) AS temporal_fk_existing_key;
SELECT xko.probe($$INSERT INTO xko.activity (id, type, legal_unit_id, valid_range) VALUES (2, 'primary', 99, '[2021-01-01,infinity)')$$) AS activity_missing_legal_unit;
SELECT xko.probe($$INSERT INTO xko.activity (id, type, legal_unit_id, valid_range) VALUES (2, 'secondary', 2, '[2021-01-01,infinity)')$$) AS activity_existing_legal_unit;
SELECT xko.probe($$INSERT INTO xko.activity (id, type, legal_unit_id, valid_range) VALUES (2, 'primary', 1, '[2022-01-01,infinity)')$$) AS activity_duplicate_xor_key;

\echo '--- Deleting a referenced key row is refused, an unreferenced one is not'
SELECT xko.probe($$DELETE FROM xko.person_for_unit WHERE id = 1$$) AS delete_key_of_regular_fk;
SELECT xko.probe($$DELETE FROM xko.person_for_unit WHERE id = 3$$) AS delete_key_of_temporal_fk;
SELECT xko.probe($$DELETE FROM xko.person_for_unit WHERE id = 2$$) AS delete_unreferenced_key;
SELECT xko.probe($$UPDATE xko.person_for_unit SET person_role_id = 21 WHERE id = 1$$) AS update_key_of_regular_fk;

\echo '--- A real rename of a key column: name replaced in place, order kept'
ALTER TABLE xko.activity RENAME COLUMN legal_unit_id TO legal_unit_ident;
ALTER TABLE xko.person_for_unit RENAME COLUMN person_role_id TO role_id;
TABLE xko.key_order;
SELECT xko.integrity('after RENAME COLUMN');

\echo '--- Foreign keys still work after the rename'
SELECT fk.foreign_key_name, fk.type, fk.column_names, uk.column_names AS referenced_columns
FROM sql_saga.foreign_keys AS fk JOIN sql_saga.unique_keys AS uk USING (unique_key_name)
WHERE fk.table_schema = 'xko' AND fk.unique_key_name = 'person_for_unit_natural_key_valid'
ORDER BY 1;
INSERT INTO xko.person_ref VALUES (3, 11, 20, 2, 7);
SELECT count(*) AS person_ref_rows FROM xko.person_ref;
SELECT xko.probe($$INSERT INTO xko.person_ref VALUES (4, 11, 21, 2, 7)$$) AS regular_fk_missing_key;
SELECT xko.probe($$INSERT INTO xko.person_tref VALUES (2, 11, 21, 2, 7, '[2021-01-01,2022-01-01)')$$) AS temporal_fk_missing_key;
SELECT xko.probe($$DELETE FROM xko.person_for_unit WHERE id = 2$$) AS delete_key_of_regular_fk;
SELECT xko.probe($$DELETE FROM xko.person_for_unit WHERE id = 3$$) AS delete_key_of_temporal_fk;
SELECT xko.probe($$INSERT INTO xko.activity (id, type, legal_unit_ident, valid_range) VALUES (2, 'primary', 99, '[2021-01-01,infinity)')$$) AS activity_missing_legal_unit;
SELECT xko.probe($$INSERT INTO xko.activity (id, type, legal_unit_ident, valid_range) VALUES (2, 'primary', 1, '[2022-01-01,infinity)')$$) AS activity_duplicate_xor_key;

\echo '--- Unrelated DDL after the rename'
ALTER TABLE xko.person_for_unit ADD COLUMN note text;
TABLE xko.key_order;
SELECT xko.integrity('after ADD COLUMN following the rename');

SET client_min_messages = warning;
RESET search_path;
DROP SCHEMA xko CASCADE;
\i sql/include/test_teardown.sql
RESET client_min_messages;
