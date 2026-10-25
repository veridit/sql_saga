\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * A role that may create objects in public cannot run its code with the extension owner's rights (1.4, review
 * round 2, R8).
 *
 * sql_saga's SECURITY DEFINER functions had public on their search_path. A role with CREATE on public defined an
 * operator public.@>(name[], name[]); an exact argument-type match beats pg_catalog's anyarray @>, whatever the
 * order of the path, so add_unique_key(..., mutually_exclusive_columns) ran that role's code as the extension
 * owner (a superuser). The same holds for any unqualified function or operator the workers use.
 *
 * Part 1  With shadowing operators and functions in public, every management function used on the attacker's own
 *         tables runs no attacker code with superuser rights.
 * Part 2  The unqualified clock function of add_current_view still resolves as released (sql_saga, pg_catalog,
 *         public), and the view calls it with the reader's rights.
 */

BEGIN;
CREATE ROLE hj_attacker;
GRANT CREATE, USAGE ON SCHEMA public TO hj_attacker;
GRANT USAGE ON SCHEMA sql_saga TO hj_attacker;
CREATE SCHEMA hj AUTHORIZATION hj_attacker;
CREATE TABLE public.hj_superuser_runs (what text);
GRANT INSERT ON public.hj_superuser_runs TO PUBLIC;

SET LOCAL ROLE hj_attacker;
-- Records every call made with superuser rights.
CREATE FUNCTION public.hj_mark(what text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF (SELECT r.rolsuper FROM pg_catalog.pg_roles AS r WHERE r.rolname = current_user) THEN
        INSERT INTO public.hj_superuser_runs VALUES (what);
    END IF;
END $$;
CREATE FUNCTION public.hj_contains(a name[], b name[]) RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN PERFORM public.hj_mark('@>(name[],name[])'); RETURN a::text[] @> b::text[]; END $$;
CREATE OPERATOR public.@> (LEFTARG = name[], RIGHTARG = name[], FUNCTION = public.hj_contains);
CREATE FUNCTION public.hj_overlaps(a name[], b name[]) RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN PERFORM public.hj_mark('&&(name[],name[])'); RETURN a::text[] && b::text[]; END $$;
CREATE OPERATOR public.&& (LEFTARG = name[], RIGHTARG = name[], FUNCTION = public.hj_overlaps);
CREATE FUNCTION public.hj_name_eq(a name, b name) RETURNS boolean LANGUAGE plpgsql AS $$
BEGIN PERFORM public.hj_mark('=(name,name)'); RETURN a::text = b::text; END $$;
CREATE OPERATOR public.= (LEFTARG = name, RIGHTARG = name, FUNCTION = public.hj_name_eq);
CREATE FUNCTION public.quote_ident(t name) RETURNS text LANGUAGE plpgsql AS $$
BEGIN PERFORM public.hj_mark('quote_ident(name)'); RETURN pg_catalog.quote_ident(t::text); END $$;
CREATE FUNCTION public.hj_today() RETURNS date LANGUAGE sql STABLE AS $$ SELECT current_date $$;

CREATE TABLE hj.unit (id int NOT NULL, a int, b int, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('hj.unit', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('hj.unit', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_unique_key('hj.unit', ARRAY['a', 'b'], key_type => 'natural', mutually_exclusive_columns => ARRAY['a', 'b'], unique_key_name => 'hj_xor');
SELECT sql_saga.set_era_ephemeral_columns('hj.unit', 'valid', ARRAY['valid_from']);
CREATE TABLE hj.note (unit_id int);
SELECT sql_saga.add_foreign_key('hj.note', ARRAY['unit_id'], 'hj.unit', ARRAY['id'], delete_action => 'CASCADE');
SELECT sql_saga.add_for_portion_of_view('hj.unit');
SELECT sql_saga.add_current_view('hj.unit', current_func_name => 'hj_today()');
CREATE TABLE hj.sv (id int NOT NULL, note text);
SELECT sql_saga.add_system_versioning('hj.sv');
SELECT sql_saga.set_system_time_era_excluded_columns('hj.sv', ARRAY['note']);
SELECT sql_saga.rebuild_system_versioning_views('hj.sv');
SELECT sql_saga.sync_history_policies('hj.sv');
INSERT INTO hj.unit (id, a, valid_from, valid_until) VALUES (1, 1, '2020-01-01', 'infinity');
INSERT INTO hj.note VALUES (1);
DELETE FROM hj.unit;
SELECT sql_saga.drop_current_view('hj.unit');
SELECT sql_saga.drop_for_portion_of_view('hj.unit');
SELECT sql_saga.drop_foreign_key_by_name('hj.note', 'note_unit_id_fkey');
SELECT sql_saga.drop_unique_key_by_name('hj.unit', 'hj_xor');
SELECT sql_saga.drop_system_versioning('hj.sv');
SELECT sql_saga.drop_era('hj.unit', 'valid', 'CASCADE', true);
RESET ROLE;

\o
\echo '--- part 1: attacker code run with superuser rights'
SELECT 'calls | ' || coalesce(string_agg(DISTINCT what, ', '), 'none') FROM public.hj_superuser_runs;

\echo '--- part 2: the clock of a current view'
SELECT 'clock bound to | ' || n.nspname || '.' || p.proname
FROM pg_catalog.pg_proc AS p JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
WHERE p.proname = 'hj_today';

ROLLBACK;
\i sql/include/test_teardown.sql
