\i sql/include/test_setup.sql

/*
 * Regular (non-temporal) foreign keys to a temporal table must survive pg_dump + pg_restore whatever the
 * order in which pg_restore loads the tables' data.
 *
 * Up to 1.2 the referencing side of a regular foreign key is a CHECK constraint calling a helper
 * function that reads the referenced table. pg_dump emits a CHECK constraint inline with its table, so
 * it is active while the data is loaded: when the child's data is loaded before the parent's (a child
 * named "activity" sorts before its parent "legal_unit"), even a serial or --single-transaction restore
 * fails with "violates check constraint" (tmp/fable_audit_v13_brief.md, defect B).
 *
 * This test states both contracts:
 *   enforcement   a missing parent key is refused on INSERT and on UPDATE of the child, NULL in a key
 *                 column is accepted (MATCH SIMPLE), composite keys compare every column, deleting or
 *                 changing a referenced parent key is refused, an unreferenced parent row can be
 *                 deleted, and check_integrity() passes; for children that sort before and after the
 *                 parent alike;
 *   restore safety no CHECK constraint on a child table calls a function that reads the parent table,
 *                 and a child row loaded before its parent row in one transaction (deferred
 *                 constraints, as a restore loads data before it checks) is accepted once the parent
 *                 row is there.
 * Every probe is rolled back and reports only accepted/refused, so the test does not depend on how the
 * referencing side is enforced or on the wording of its error.
 */
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;

CREATE SCHEMA rfo;
SET search_path = rfo, public;

-- One statement with every deferrable constraint checked immediately, rolled back.
CREATE FUNCTION rfo.probe(statement text) RETURNS text LANGUAGE plpgsql AS $probe$
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

-- The parent: a temporal table with a primary key (id) and a composite natural key (ident_type, ident).
CREATE TABLE rfo.legal_unit (id int NOT NULL, ident_type text NOT NULL, ident text NOT NULL,
                             valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('rfo.legal_unit', 'valid_range', 'valid',
    valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('rfo.legal_unit', ARRAY['id'], key_type => 'primary', unique_key_name => 'legal_unit_id_valid');
SELECT sql_saga.add_unique_key('rfo.legal_unit', ARRAY['ident_type', 'ident'], key_type => 'natural',
    unique_key_name => 'legal_unit_ident_valid');

-- Children: "activity" and "contact" sort before "legal_unit", "stat" sorts after it.
CREATE TABLE rfo.activity (id int PRIMARY KEY, legal_unit_id int);
CREATE TABLE rfo.stat (id int PRIMARY KEY, legal_unit_id int);
CREATE TABLE rfo.contact (id int PRIMARY KEY, ident_type text, ident text);
SELECT sql_saga.add_foreign_key('rfo.activity', ARRAY['legal_unit_id'], 'rfo.legal_unit', ARRAY['id']);
SELECT sql_saga.add_foreign_key('rfo.stat', ARRAY['legal_unit_id'], 'rfo.legal_unit', ARRAY['id']);
SELECT sql_saga.add_regular_foreign_key('rfo.contact', ARRAY['ident_type', 'ident'], 'legal_unit_ident_valid');
SELECT foreign_key_name, type, table_name, column_names, unique_key_name
FROM sql_saga.foreign_keys WHERE table_schema = 'rfo' ORDER BY foreign_key_name;

INSERT INTO rfo.legal_unit (id, ident_type, ident, valid_from, valid_until) VALUES
    (1, 'tax', '100', '2020-01-01', '2022-01-01'),
    (1, 'tax', '100', '2022-01-01', 'infinity'),
    (2, 'tax', '200', '2020-01-01', 'infinity'),
    (3, 'tax', '300', '2020-01-01', 'infinity'),
    (4, 'tax', '400', '2020-01-01', 'infinity');
-- legal unit 1 <- activity, 2 <- stat, (tax, 300) <- contact; 4 is not referenced.
INSERT INTO rfo.activity VALUES (1, 1), (2, NULL);
INSERT INTO rfo.stat VALUES (1, 2);
INSERT INTO rfo.contact VALUES (1, 'tax', '300'), (2, NULL, '999'), (3, 'tax', NULL);
SELECT sql_saga.check_integrity();

\echo '--- Restore safety: no CHECK constraint on a child table calls a function that reads the parent'
SELECT fk.foreign_key_name, fk.table_name,
       (SELECT count(*)
        FROM pg_catalog.pg_constraint AS c
        JOIN pg_catalog.pg_depend AS d
          ON (d.classid, d.objid, d.refclassid) = ('pg_catalog.pg_constraint'::regclass, c.oid, 'pg_catalog.pg_proc'::regclass)
        JOIN pg_catalog.pg_proc AS p ON p.oid = d.refobjid
        WHERE c.conrelid = format('%I.%I', fk.table_schema, fk.table_name)::regclass
          AND c.contype = 'c'
          AND p.pronamespace <> 'pg_catalog'::regnamespace
          AND p.prosrc ~ format('\m%s\M', uk.table_name)) AS checks_reading_parent
FROM sql_saga.foreign_keys AS fk
JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
WHERE fk.table_schema = 'rfo' AND fk.type = 'regular_to_temporal'
ORDER BY fk.foreign_key_name;

-- And no CHECK constraint in this schema calls a function outside pg_catalog at all (this test creates none).
SELECT c.conrelid::regclass AS table_name, c.conname, pg_catalog.pg_get_constraintdef(c.oid) AS definition
FROM pg_catalog.pg_constraint AS c
JOIN pg_catalog.pg_depend AS d
  ON (d.classid, d.objid, d.refclassid) = ('pg_catalog.pg_constraint'::regclass, c.oid, 'pg_catalog.pg_proc'::regclass)
JOIN pg_catalog.pg_proc AS p ON p.oid = d.refobjid
WHERE c.connamespace = 'rfo'::regnamespace AND c.contype = 'c' AND p.pronamespace <> 'pg_catalog'::regnamespace
ORDER BY 1, 2;

\echo '--- Restore safety: a child row loaded before its parent row is accepted once the parent is there'
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
INSERT INTO rfo.activity VALUES (10, 5);
INSERT INTO rfo.stat VALUES (10, 5);
INSERT INTO rfo.contact VALUES (10, 'tax', '500');
INSERT INTO rfo.legal_unit (id, ident_type, ident, valid_from, valid_until) VALUES (5, 'tax', '500', '2020-01-01', 'infinity');
SET CONSTRAINTS ALL IMMEDIATE;
SELECT (SELECT count(*) FROM rfo.activity WHERE id = 10) AS activity_rows,
       (SELECT count(*) FROM rfo.stat WHERE id = 10) AS stat_rows,
       (SELECT count(*) FROM rfo.contact WHERE id = 10) AS contact_rows;
ROLLBACK;
-- The same load without the parent is refused when the constraints are checked.
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
INSERT INTO rfo.activity VALUES (10, 5);
SELECT rfo.probe($$SET CONSTRAINTS ALL IMMEDIATE$$) AS child_without_parent_at_check;
ROLLBACK;

\echo '--- Enforcement on the referencing side (activity and contact sort before legal_unit, stat after)'
SELECT rfo.probe($$INSERT INTO rfo.activity VALUES (3, 99)$$) AS activity_missing_parent;
SELECT rfo.probe($$INSERT INTO rfo.activity VALUES (3, 4)$$) AS activity_existing_parent;
SELECT rfo.probe($$INSERT INTO rfo.activity VALUES (3, NULL)$$) AS activity_null;
SELECT rfo.probe($$UPDATE rfo.activity SET legal_unit_id = 99 WHERE id = 1$$) AS activity_update_to_missing;
SELECT rfo.probe($$UPDATE rfo.activity SET legal_unit_id = 4 WHERE id = 2$$) AS activity_update_null_to_existing;
SELECT rfo.probe($$INSERT INTO rfo.stat VALUES (2, 99)$$) AS stat_missing_parent;
SELECT rfo.probe($$INSERT INTO rfo.stat VALUES (2, 4)$$) AS stat_existing_parent;
SELECT rfo.probe($$INSERT INTO rfo.stat VALUES (2, NULL)$$) AS stat_null;
SELECT rfo.probe($$UPDATE rfo.stat SET legal_unit_id = 99 WHERE id = 1$$) AS stat_update_to_missing;
SELECT rfo.probe($$INSERT INTO rfo.contact VALUES (4, 'tax', '999')$$) AS contact_missing_parent;
SELECT rfo.probe($$INSERT INTO rfo.contact VALUES (4, 'vat', '100')$$) AS contact_mixed_columns;
SELECT rfo.probe($$INSERT INTO rfo.contact VALUES (4, 'tax', '400')$$) AS contact_existing_parent;
SELECT rfo.probe($$INSERT INTO rfo.contact VALUES (4, NULL, NULL)$$) AS contact_all_null;
SELECT rfo.probe($$INSERT INTO rfo.contact VALUES (4, 'vat', NULL)$$) AS contact_partly_null;
SELECT rfo.probe($$UPDATE rfo.contact SET ident = '999' WHERE id = 1$$) AS contact_update_to_missing;
-- An UPDATE that does not touch the key columns of a row whose key exists is accepted.
SELECT rfo.probe($$UPDATE rfo.activity SET id = 5 WHERE id = 1$$) AS activity_update_other_column;

\echo '--- Enforcement on the referenced side'
SELECT rfo.probe($$DELETE FROM rfo.legal_unit WHERE id = 1$$) AS delete_parent_of_activity;
SELECT rfo.probe($$DELETE FROM rfo.legal_unit WHERE id = 2$$) AS delete_parent_of_stat;
SELECT rfo.probe($$DELETE FROM rfo.legal_unit WHERE id = 3$$) AS delete_parent_of_contact;
SELECT rfo.probe($$UPDATE rfo.legal_unit SET id = 9 WHERE id = 1$$) AS update_parent_key_of_activity;
SELECT rfo.probe($$UPDATE rfo.legal_unit SET ident = '301' WHERE id = 3$$) AS update_parent_key_of_contact;
SELECT rfo.probe($$DELETE FROM rfo.legal_unit WHERE id = 4$$) AS delete_unreferenced_parent;

\echo '--- check_integrity'
SELECT sql_saga.check_integrity();

SET client_min_messages = warning;
RESET search_path;
DROP SCHEMA rfo CASCADE;
\i sql/include/test_teardown.sql
RESET client_min_messages;
