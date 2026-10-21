\i sql/include/test_setup.sql

/*
 * add_current_view(current_func_name => ...) accepts an unqualified zero-argument function, resolved by
 * add_current_view itself (search_path sql_saga, pg_catalog, public), as the released tag-upgrade fixture
 * does with 'drift_today()' in public. The caller's search_path does not select it.
 * The view binds that exact function. Every later rebuild of the view (ADD COLUMN, DROP COLUMN,
 * ALTER COLUMN TYPE on the base) must keep calling the SAME function, whatever search_path the rebuild runs
 * under, never a same-named function that happens to resolve elsewhere, and never fail to find it.
 */
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;

CREATE SCHEMA cvb;
CREATE SCHEMA cvb_decoy;

-- The intended clock: settable, so the test is deterministic.
SET cvb.today = '2024-06-01';
CREATE FUNCTION public.cvb_today() RETURNS date LANGUAGE sql STABLE AS $$ SELECT current_setting('cvb.today')::date $$;
-- A same-named decoy first on the caller's search_path. If anything binds to it, the current view is empty.
CREATE FUNCTION cvb_decoy.cvb_today() RETURNS date LANGUAGE sql STABLE AS $$ SELECT '1900-01-01'::date $$;

CREATE FUNCTION cvb.integrity(step text) RETURNS text LANGUAGE plpgsql AS $integrity$
BEGIN
    PERFORM sql_saga.check_integrity();
    RETURN step || ': ok';
EXCEPTION WHEN OTHERS THEN
    RETURN step || ': ' || SQLERRM;
END;
$integrity$;

-- Which cvb_today() the view calls, from the exact dependency PostgreSQL records.
CREATE FUNCTION cvb.bound_function() RETURNS text LANGUAGE sql STABLE BEGIN ATOMIC
    SELECT string_agg(p.oid::regprocedure::text || ' in ' || n.nspname, ', ' ORDER BY n.nspname)
    FROM pg_rewrite AS r
    JOIN pg_depend AS d ON d.classid = 'pg_rewrite'::regclass AND d.objid = r.oid AND d.refclassid = 'pg_proc'::regclass
    JOIN pg_proc AS p ON p.oid = d.refobjid
    JOIN pg_namespace AS n ON n.oid = p.pronamespace
    WHERE r.ev_class = to_regclass('cvb.thing__current_valid') AND p.proname = 'cvb_today';
END;

CREATE TABLE cvb.thing (id int NOT NULL, name text, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('cvb.thing'::regclass, 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('cvb.thing'::regclass, ARRAY['id'], key_type => 'primary');
INSERT INTO cvb.thing (id, name, valid_from, valid_until) VALUES (1, 'one', '2024-01-01', 'infinity');

-- Register the clock unqualified, with the decoy first on the caller's search_path throughout.
SET search_path = cvb_decoy, cvb, public;
SELECT sql_saga.add_current_view('cvb.thing'::regclass, current_func_name => 'cvb_today()');
SELECT cvb.bound_function() AS bound_after_create;
SELECT id, name FROM cvb.thing__current_valid ORDER BY id;

\echo '--- ADD COLUMN rebuilds the view on the same function'
ALTER TABLE cvb.thing ADD COLUMN note text;
SELECT cvb.bound_function() AS bound_after_add;
SELECT id, name, note FROM cvb.thing__current_valid ORDER BY id;
SELECT cvb.integrity('after ADD COLUMN');

SET cvb.today = '2024-07-01';
UPDATE cvb.thing__current_valid SET note = 'noted' WHERE id = 1;
SELECT id, name, note, valid_from, valid_until FROM cvb.thing ORDER BY id, valid_from;

\echo '--- ALTER COLUMN TYPE rebuilds the view on the same function'
ALTER TABLE cvb.thing ALTER COLUMN note TYPE varchar(20);
SELECT cvb.bound_function() AS bound_after_type;
SELECT id, name, note FROM cvb.thing__current_valid ORDER BY id;
SELECT cvb.integrity('after ALTER COLUMN TYPE');

SET cvb.today = '2024-07-15';
UPDATE cvb.thing__current_valid SET note = 'retyped' WHERE id = 1;
SELECT id, name, note, valid_from, valid_until FROM cvb.thing ORDER BY id, valid_from;

\echo '--- DROP COLUMN rebuilds the view on the same function'
ALTER TABLE cvb.thing DROP COLUMN note;
SELECT cvb.bound_function() AS bound_after_drop;
SELECT id, name FROM cvb.thing__current_valid ORDER BY id;
SELECT cvb.integrity('after DROP COLUMN');

SET cvb.today = '2024-08-01';
UPDATE cvb.thing__current_valid SET name = 'uno' WHERE id = 1;
SELECT id, name, valid_from, valid_until FROM cvb.thing ORDER BY id, valid_from;

RESET search_path;
DROP SCHEMA cvb CASCADE;
DROP FUNCTION public.cvb_today();
DROP SCHEMA cvb_decoy CASCADE;

\i sql/include/test_teardown.sql
