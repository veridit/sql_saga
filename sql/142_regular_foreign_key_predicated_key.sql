\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * Regular (non-temporal) to temporal foreign keys against a predicated unique key (1.4, review finding F5).
 *
 * A predicated key is unique only among the rows that satisfy its predicate. The regular foreign key looked the
 * key up in every row, so a child could reference a key that exists only outside the predicate, and deleting the
 * last row inside the predicate neither refused (NO ACTION) nor cascaded (CASCADE). A temporal foreign key to a
 * predicated key is refused (PostgreSQL finds no unique constraint to reference), and so is a regular one now:
 *
 * Part 1  add_regular_foreign_key against a predicated key is refused (0A000), for NO ACTION and CASCADE, and
 *         nothing is created.
 * Part 2  add_foreign_key by columns still picks only primary and natural keys.
 */

BEGIN;
CREATE SCHEMA rpk;
SET search_path = rpk, public;

CREATE FUNCTION rpk.outcome(statement text) RETURNS text LANGUAGE plpgsql AS $outcome$
BEGIN
    BEGIN
        EXECUTE statement;
        RETURN 'ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN SQLSTATE || ': ' || SQLERRM;
    END;
END;
$outcome$;

CREATE TABLE rpk.unit (id int NOT NULL, code text, active boolean NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('rpk.unit', 'valid_range', 'valid',
    valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('rpk.unit', ARRAY['code'], key_type => 'predicated', predicate => 'active',
    unique_key_name => 'unit_active_code');
CREATE TABLE rpk.note (unit_code text);

\o
\echo '--- part 1: a regular foreign key cannot reference a predicated key'
SELECT 'NO ACTION | ' || rpk.outcome($$SELECT sql_saga.add_regular_foreign_key('rpk.note', ARRAY['unit_code'], 'unit_active_code')$$);
SELECT 'CASCADE | ' || rpk.outcome($$SELECT sql_saga.add_regular_foreign_key('rpk.note', ARRAY['unit_code'], 'unit_active_code', delete_action => 'CASCADE')$$);
SELECT 'catalogued foreign keys | ' || count(*) FROM sql_saga.foreign_keys;
SELECT 'triggers on note and unit | ' || count(*) FROM pg_trigger WHERE tgrelid IN ('rpk.note'::regclass, 'rpk.unit'::regclass) AND tgname LIKE '%unit_code%';

\echo '--- part 2: add_foreign_key by columns finds no primary or natural key on code'
SELECT 'by columns | ' || rpk.outcome($$SELECT sql_saga.add_foreign_key('rpk.note', ARRAY['unit_code'], 'rpk.unit', ARRAY['code'])$$);

ROLLBACK;
\i sql/include/test_teardown.sql
