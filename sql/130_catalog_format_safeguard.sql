\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * Catalog format safeguard (since 1.3; the format is '1.4' from 1.4, whose foreign_keys gained uk_truncate_trigger).
 * sql_saga.era carries catalog_format text, with no default, under the named CHECK "restore old sql_saga dumps per
 * doc/upgrade.md" (catalog_format IS NOT NULL AND catalog_format = '1.4'): a plain (catalog_format = '1.4') is NULL
 * for a NULL value and would pass an older dump. The value is the sql_saga
 * release that introduced the current catalog layout. Every path that inserts era rows writes it, so a dump of
 * an older catalog (no such column in its COPY list) restored into this install leaves it NULL and the restore
 * fails at the first era row, naming the doc; a newer-format dump into an older install fails the same way.
 */

BEGIN;
CREATE SCHEMA cfs;
SET search_path = cfs, public;

-- Run a statement, rolled back: 'accepted', or the SQLSTATE and violated constraint of the error.
CREATE FUNCTION cfs.outcome(statement text) RETURNS text LANGUAGE plpgsql AS $outcome$
DECLARE
    v_constraint text;
BEGIN
    BEGIN
        EXECUTE statement;
        RAISE EXCEPTION USING ERRCODE = 'P0004', MESSAGE = 'accepted';
    EXCEPTION
        WHEN assert_failure THEN RETURN 'accepted';
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS v_constraint = CONSTRAINT_NAME;
            RETURN 'refused ' || SQLSTATE || coalesce(' "' || nullif(v_constraint, '') || '"', '');
    END;
END;
$outcome$;

CREATE FUNCTION cfs.integrity() RETURNS text LANGUAGE plpgsql AS $integrity$
BEGIN
    BEGIN
        PERFORM sql_saga.check_integrity();
        RETURN 'ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN SQLSTATE || ': ' || SQLERRM;
    END;
END;
$integrity$;

CREATE TABLE cfs.unit (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
CREATE TABLE cfs.versioned (id int PRIMARY KEY, name text);
CREATE TABLE cfs.bare (id int NOT NULL, valid_range daterange NOT NULL);

\o
\echo '--- fresh install'
SELECT 'check_integrity | ' || cfs.integrity();

\echo '--- the column and the constraint'
SELECT 'catalog_format column: type, default | '
    || coalesce((SELECT format_type(a.atttypid, a.atttypmod) || ', ' || CASE WHEN a.atthasdef THEN 'has default' ELSE 'no default' END
                 FROM pg_catalog.pg_attribute AS a
                 WHERE a.attrelid = 'sql_saga.era'::regclass AND a.attname = 'catalog_format' AND NOT a.attisdropped),
                'missing');
SELECT 'constraint "restore old sql_saga dumps per doc/upgrade.md" | '
    || coalesce((SELECT pg_catalog.pg_get_constraintdef(c.oid)
                 FROM pg_catalog.pg_constraint AS c
                 WHERE c.conrelid = 'sql_saga.era'::regclass AND c.conname = 'restore old sql_saga dumps per doc/upgrade.md'),
                'missing');

\echo '--- every path that inserts era rows writes the current format'
\o /dev/null
SELECT sql_saga.add_era('cfs.unit'::regclass, 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_system_versioning('cfs.versioned');
\o
SELECT 'era ' || e.table_name || '.' || e.era_name || ' catalog_format | '
    || coalesce((to_jsonb(e) ->> 'catalog_format'), 'NULL')
FROM sql_saga.era AS e
WHERE e.table_schema = 'cfs'
ORDER BY e.table_name, e.era_name;
SELECT 'check_integrity after add_era and add_system_versioning | ' || cfs.integrity();

\echo '--- a direct era row must state the current format'
SELECT 'INSERT without catalog_format (an older dump) | ' || cfs.outcome($$
    INSERT INTO sql_saga.era (table_schema, table_name, era_name, range_column_name, range_type, multirange_type, range_subtype, range_subtype_category)
    VALUES ('cfs', 'bare', 'valid', 'valid_range', 'daterange', 'datemultirange', 'date', 'D')
$$);
SELECT 'INSERT with catalog_format NULL | ' || cfs.outcome($$
    INSERT INTO sql_saga.era (table_schema, table_name, era_name, range_column_name, range_type, multirange_type, range_subtype, range_subtype_category, catalog_format)
    VALUES ('cfs', 'bare', 'valid', 'valid_range', 'daterange', 'datemultirange', 'date', 'D', NULL)
$$);
SELECT 'INSERT with catalog_format ''1.2'' | ' || cfs.outcome($$
    INSERT INTO sql_saga.era (table_schema, table_name, era_name, range_column_name, range_type, multirange_type, range_subtype, range_subtype_category, catalog_format)
    VALUES ('cfs', 'bare', 'valid', 'valid_range', 'daterange', 'datemultirange', 'date', 'D', '1.2')
$$);
SELECT 'INSERT with catalog_format ''1.3'' (the previous layout) | ' || cfs.outcome($$
    INSERT INTO sql_saga.era (table_schema, table_name, era_name, range_column_name, range_type, multirange_type, range_subtype, range_subtype_category, catalog_format)
    VALUES ('cfs', 'bare', 'valid', 'valid_range', 'daterange', 'datemultirange', 'date', 'D', '1.3')
$$);
SELECT 'INSERT with catalog_format ''1.5'' (a newer layout) | ' || cfs.outcome($$
    INSERT INTO sql_saga.era (table_schema, table_name, era_name, range_column_name, range_type, multirange_type, range_subtype, range_subtype_category, catalog_format)
    VALUES ('cfs', 'bare', 'valid', 'valid_range', 'daterange', 'datemultirange', 'date', 'D', '1.5')
$$);
SELECT 'INSERT with catalog_format ''1.4'' | ' || cfs.outcome($$
    INSERT INTO sql_saga.era (table_schema, table_name, era_name, range_column_name, range_type, multirange_type, range_subtype, range_subtype_category, catalog_format)
    VALUES ('cfs', 'bare', 'valid', 'valid_range', 'daterange', 'datemultirange', 'date', 'D', '1.4')
$$);
SELECT 'UPDATE an existing era row to catalog_format NULL | ' || cfs.outcome($$
    UPDATE sql_saga.era SET catalog_format = NULL WHERE table_schema = 'cfs' AND table_name = 'unit'
$$);

\echo '--- final'
SELECT 'check_integrity | ' || cfs.integrity();

\o /dev/null
ROLLBACK;
\i sql/include/test_teardown.sql
