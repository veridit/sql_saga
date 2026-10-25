\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * A regular (non-temporal) foreign key to an XOR natural key (mutually_exclusive_columns), StatBus's shape (1.4).
 *
 * An XOR key is a natural key over columns of which exactly one is set (legal_unit_id XOR establishment_id),
 * unique with NULLS NOT DISTINCT. A regular foreign key to it pairs every key column; MATCH SIMPLE accepts a child
 * row with any key column NULL, MATCH FULL refuses a partly NULL one, as native foreign keys do.
 *
 * Part 1  The child check: a present pair is accepted, an absent pair refused, partly NULL keys per match type.
 * Part 2  As native: under MATCH SIMPLE a child row whose key has a NULL column (every XOR reference has one) is
 *         neither checked nor cascaded. PostgreSQL does the same for a native foreign key to a UNIQUE NULLS NOT
 *         DISTINCT key. A regular foreign key to an XOR key therefore protects nothing; reference the single
 *         identifier (legal_unit.id, establishment.id) instead, as StatBus does.
 * Part 3  Integrity holds.
 */

BEGIN;
CREATE SCHEMA rxk;
SET search_path = rxk, public;

CREATE FUNCTION rxk.outcome(statement text) RETURNS text LANGUAGE plpgsql AS $outcome$
BEGIN
    BEGIN
        EXECUTE statement;
        RETURN 'ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN SQLSTATE || ': ' || SQLERRM;
    END;
END;
$outcome$;

CREATE TABLE rxk.activity (id int NOT NULL, lu_id int, es_id int, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('rxk.activity', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('rxk.activity', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_unique_key('rxk.activity', ARRAY['lu_id', 'es_id'], key_type => 'natural',
    mutually_exclusive_columns => ARRAY['lu_id', 'es_id'], unique_key_name => 'activity_unit_xor');
INSERT INTO rxk.activity (id, lu_id, es_id, valid_from, valid_until) VALUES
    (1, 10, NULL, '2020-01-01', '2022-01-01'),
    (1, 10, NULL, '2022-01-01', 'infinity'),
    (2, NULL, 20, '2020-01-01', 'infinity'),
    (3, 30, NULL, '2020-01-01', 'infinity');

CREATE TABLE rxk.note (id int PRIMARY KEY, lu_id int, es_id int);
SELECT sql_saga.add_regular_foreign_key('rxk.note', ARRAY['lu_id', 'es_id'], 'activity_unit_xor',
    delete_action => 'CASCADE', foreign_key_name => 'note_xor_fk');
CREATE TABLE rxk.tag (id int PRIMARY KEY, lu_id int, es_id int);
SELECT sql_saga.add_regular_foreign_key('rxk.tag', ARRAY['lu_id', 'es_id'], 'activity_unit_xor',
    match_type => 'FULL', foreign_key_name => 'tag_xor_fk');

CREATE FUNCTION rxk.ids(rel regclass) RETURNS text LANGUAGE plpgsql AS $ids$
DECLARE result text;
BEGIN
    EXECUTE format('SELECT coalesce(string_agg(id::text, '','' ORDER BY id), ''none'') FROM %s', rel) INTO result;
    RETURN result;
END;
$ids$;

\o
\echo '--- part 1: the child check'
SELECT 'MATCH SIMPLE, (lu 10, NULL) present | ' || rxk.outcome($$INSERT INTO rxk.note VALUES (1, 10, NULL)$$);
SELECT 'MATCH SIMPLE, (NULL, es 20) present | ' || rxk.outcome($$INSERT INTO rxk.note VALUES (2, NULL, 20)$$);
SELECT 'MATCH SIMPLE, (lu 30, NULL) present | ' || rxk.outcome($$INSERT INTO rxk.note VALUES (3, 30, NULL)$$);
SELECT 'MATCH SIMPLE, (lu 99, NULL) is partly NULL, accepted as natively | ' || rxk.outcome($$INSERT INTO rxk.note VALUES (4, 99, NULL)$$);
SELECT 'MATCH SIMPLE, (lu 10, es 20) absent | ' || rxk.outcome($$INSERT INTO rxk.note VALUES (5, 10, 20)$$);
SELECT 'MATCH FULL, (lu 10, NULL) partly NULL | ' || rxk.outcome($$INSERT INTO rxk.tag VALUES (1, 10, NULL)$$);
SELECT 'MATCH FULL, (NULL, NULL) | ' || rxk.outcome($$INSERT INTO rxk.tag VALUES (2, NULL, NULL)$$);
SELECT 'notes | ' || rxk.ids('rxk.note');

\echo '--- part 2: MATCH SIMPLE ignores keys with a NULL column, as natively'
SAVEPOINT p;
DELETE FROM rxk.activity WHERE lu_id = 10 AND valid_from = '2020-01-01';
SELECT 'one of two rows of (lu 10) deleted, notes | ' || rxk.ids('rxk.note');
DELETE FROM rxk.activity WHERE lu_id = 10;
SELECT 'last row of (lu 10) deleted, notes | ' || rxk.ids('rxk.note');
DELETE FROM rxk.activity WHERE es_id = 20;
SELECT 'last row of (es 20) deleted, notes | ' || rxk.ids('rxk.note');
ROLLBACK TO SAVEPOINT p;

\echo '--- part 3: integrity'
SELECT 'check_integrity | ' || rxk.outcome($$SELECT sql_saga.check_integrity()$$);

ROLLBACK;
\i sql/include/test_teardown.sql
