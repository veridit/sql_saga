\i sql/include/test_setup.sql

/*
 * pg_restore --clean into a database that already holds sql_saga families (tmp/design_restore_clean.md).
 *
 * pg_restore --clean drops every archived object, dependents first and each base table last, before it
 * recreates them. sql_saga's drop protection must let that teardown through, and only that:
 *   - the teardown is recognized by the session pg_restore sets up (check_function_bodies = off,
 *     row_security = off), a pg_restore session (application_name = 'pg_restore', or the explicit
 *     opt-in sql_saga.clean_restore = on for a restore script run through psql), and a top-level DROP;
 *   - anything else, even with the same settings, keeps today's protection.
 * The teardown statements below are the exact ones pg_restore --clean emits for this schema.
 */
\set SHOW_CONTEXT never
\set VERBOSITY default
SET client_min_messages = warning;

-- A fixed installation token makes the managed history policy names deterministic.
UPDATE sql_saga.installation SET token = 'test0001';

-- Every kind of family: era with synchronized columns, unique keys, FOR PORTION OF and current views,
-- a native temporal foreign key, a regular-to-temporal foreign key (triggers on p), a versioned table
-- with a base RLS policy (copied to its history), and a versioned table whose custom history and view
-- names sort before the base table (so pg_restore drops the base table before its history).
CREATE TABLE p (id int, valid_range daterange NOT NULL, valid_from date, valid_until date, x text);
SELECT sql_saga.add_era('p', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('p', ARRAY['id'], key_type => 'primary', unique_key_name => 'p_id_valid');
SELECT sql_saga.add_for_portion_of_view('p', 'valid');
SELECT sql_saga.add_current_view('p');
CREATE TABLE c (id int, p_id int, valid_range daterange NOT NULL, x text);
SELECT sql_saga.add_era('c', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('c', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_foreign_key('c', ARRAY['p_id'], 'p', ARRAY['id']);
CREATE TABLE r (id int PRIMARY KEY, p_id int);
SELECT sql_saga.add_regular_foreign_key('r', ARRAY['p_id'], 'p_id_valid');
CREATE TABLE b (id int PRIMARY KEY, tenant text);
SELECT sql_saga.add_system_versioning('b');
ALTER TABLE b ENABLE ROW LEVEL SECURITY;
CREATE POLICY b_tenant ON b FOR SELECT USING (tenant = current_user);
CREATE TABLE zbase (id int PRIMARY KEY, x text);
SELECT sql_saga.add_system_versioning('zbase', history_table_name => 'aa_hist', view_name => 'ab_view');
INSERT INTO p (id, valid_range, x) VALUES (1, '[2020-01-01,infinity)', 'a');
INSERT INTO c VALUES (1, 1, '[2021-01-01,infinity)', 'c');
INSERT INTO r VALUES (1, 1);
INSERT INTO b VALUES (1, 'a');
UPDATE b SET tenant = 'b';

-- What is left of the families: sql_saga catalog rows, relations, policies and template functions.
CREATE TEMP VIEW family_state AS
    SELECT 'catalog sql_saga.' || t.catalog AS item, t.n::text AS state
    FROM (SELECT 'era' AS catalog, count(*) AS n FROM sql_saga.era
          UNION ALL SELECT 'unique_keys', count(*) FROM sql_saga.unique_keys
          UNION ALL SELECT 'foreign_keys', count(*) FROM sql_saga.foreign_keys
          UNION ALL SELECT 'system_time_era', count(*) FROM sql_saga.system_time_era
          UNION ALL SELECT 'system_versioning', count(*) FROM sql_saga.system_versioning
          UNION ALL SELECT 'updatable_view', count(*) FROM sql_saga.updatable_view) AS t
    UNION ALL
    -- Only this test's relations: the regression database may hold other tests' tables.
    SELECT 'relations of the families', coalesce(string_agg(c.relname, ', ' ORDER BY c.relname), '')
    FROM pg_catalog.pg_class AS c
    WHERE c.relnamespace = 'public'::regnamespace AND c.relkind IN ('r', 'v')
      AND c.relname ~ '^(p|c|r|b|zbase|aa_hist|ab_view)($|_|__)'
    UNION ALL
    SELECT 'policies', coalesce(string_agg(format('%s.%s', p.polrelid::regclass, p.polname), ', ' ORDER BY p.polrelid::regclass::text, p.polname), '')
    FROM pg_catalog.pg_policy AS p
    UNION ALL
    SELECT 'functions in public', coalesce(string_agg(pr.proname, ', ' ORDER BY pr.proname), '')
    FROM pg_catalog.pg_proc AS pr
    WHERE pr.pronamespace = 'public'::regnamespace
      AND NOT EXISTS (SELECT FROM pg_catalog.pg_depend AS d
                      WHERE (d.classid, d.objid, d.deptype) = ('pg_catalog.pg_proc'::regclass, pr.oid, 'e'))
    UNION ALL
    SELECT 'template sync functions', coalesce(string_agg(pr.proname, ', ' ORDER BY pr.proname), '')
    FROM pg_catalog.pg_proc AS pr
    WHERE pr.pronamespace = 'sql_saga'::regnamespace AND pr.proname LIKE '%template_sync'
    UNION ALL
    SELECT 'event triggers enabled', string_agg(e.evtname || '=' || e.evtenabled::text, ', ' ORDER BY e.evtname)
    FROM pg_catalog.pg_event_trigger AS e
    WHERE e.evtname LIKE 'sql_saga%';
RESET client_min_messages;

TABLE family_state;

\echo '=== A: accidental drops in a session with pg_restore''s settings, but not a pg_restore session: still refused'
BEGIN;
SET LOCAL check_function_bodies = off;
SET LOCAL row_security = off;
SAVEPOINT a;
DROP TRIGGER IF EXISTS b_truncate ON public.b; -- fail
ROLLBACK TO SAVEPOINT a;
DROP FUNCTION IF EXISTS public.b__as_of(timestamp with time zone); -- fail
ROLLBACK TO SAVEPOINT a;
DROP POLICY IF EXISTS sql_saga_history_reader_test0001 ON public.b_history; -- fail
ROLLBACK TO SAVEPOINT a;
DROP POLICY IF EXISTS sql_saga_5d18f5f440ffed66 ON public.b_history; -- fail
ROLLBACK TO SAVEPOINT a;
DROP VIEW IF EXISTS public.p__for_portion_of_valid; -- fail
ROLLBACK TO SAVEPOINT a;
DROP TRIGGER IF EXISTS current_valid ON public.p__current_valid; -- fail
ROLLBACK TO SAVEPOINT a;
DROP TRIGGER IF EXISTS r_p_id_fkey_uk_update ON public.p; -- fail
ROLLBACK TO SAVEPOINT a;
ALTER TABLE IF EXISTS ONLY public.c DROP CONSTRAINT IF EXISTS c_pkey; -- fail
ROLLBACK TO SAVEPOINT a;
ROLLBACK;

\echo '=== B: the opt-in alone, in a normal session: still refused'
BEGIN;
SET LOCAL sql_saga.clean_restore = on;
SAVEPOINT b;
DROP TRIGGER IF EXISTS b_truncate ON public.b; -- fail
ROLLBACK TO SAVEPOINT b;
DROP VIEW IF EXISTS public.p__current_valid; -- fail
ROLLBACK TO SAVEPOINT b;
ROLLBACK;

\echo '=== B: pg_restore''s application_name alone, in a normal session: still refused'
BEGIN;
SET LOCAL application_name = 'pg_restore';
SAVEPOINT b;
DROP TRIGGER IF EXISTS b_truncate ON public.b; -- fail
ROLLBACK TO SAVEPOINT b;
ROLLBACK;

\echo '=== D: in a clean-restore session, statements that are not part of the teardown keep every check'
BEGIN;
SET LOCAL check_function_bodies = off;
SET LOCAL row_security = off;
SET LOCAL sql_saga.clean_restore = on;
SAVEPOINT d;
ALTER TABLE b_history DISABLE ROW LEVEL SECURITY; -- fail
ROLLBACK TO SAVEPOINT d;
ALTER TRIGGER r_p_id_fkey_uk_update ON p RENAME TO r_p_id_fkey_uk_update_renamed; -- fail
ROLLBACK TO SAVEPOINT d;
ROLLBACK;

\echo '=== E: renaming a regular foreign key''s trigger is refused'
BEGIN;
SAVEPOINT e;
ALTER TRIGGER r_p_id_fkey_uk_update ON p RENAME TO r_p_id_fkey_uk_update_renamed; -- fail
ROLLBACK TO SAVEPOINT e;
ALTER TRIGGER r_p_id_fkey_uk_delete ON p RENAME TO r_p_id_fkey_uk_delete_renamed; -- fail
ROLLBACK TO SAVEPOINT e;
ROLLBACK;

-- F: the teardown drops the managed history policies as soon as any non-policy object of the family
-- goes, including an ordinary user trigger or constraint on the versioned base table (pg_restore drops
-- those before the base table). A normal session dropping the same objects leaves the policies alone.
CREATE TEMP VIEW b_history_policies AS
    SELECT count(*) AS managed_history_policies
    FROM pg_catalog.pg_policy AS p
    WHERE p.polrelid = 'public.b_history'::regclass
      AND sql_saga.__internal_is_managed_history_policy(p.polname);

\echo '=== F1: normal session, an ordinary user trigger and an ordinary user constraint on a versioned base: policies kept'
BEGIN;
CREATE FUNCTION public.b_user_trigger() RETURNS trigger LANGUAGE plpgsql AS $b_user_trigger$ BEGIN RETURN NEW; END; $b_user_trigger$;
CREATE TRIGGER b_user BEFORE INSERT ON public.b FOR EACH ROW EXECUTE FUNCTION public.b_user_trigger();
ALTER TABLE public.b ADD CONSTRAINT b_user_check CHECK (id > 0);
TABLE b_history_policies;
DROP TRIGGER b_user ON public.b;
TABLE b_history_policies;
ALTER TABLE ONLY public.b DROP CONSTRAINT b_user_check;
TABLE b_history_policies;
ROLLBACK;

\echo '=== F2: teardown, DROP TRIGGER of an ordinary user trigger on a versioned base: managed history policies dropped'
BEGIN;
CREATE FUNCTION public.b_user_trigger() RETURNS trigger LANGUAGE plpgsql AS $b_user_trigger$ BEGIN RETURN NEW; END; $b_user_trigger$;
CREATE TRIGGER b_user BEFORE INSERT ON public.b FOR EACH ROW EXECUTE FUNCTION public.b_user_trigger();
TABLE b_history_policies;
SET LOCAL check_function_bodies = off;
SET LOCAL row_security = off;
SET LOCAL application_name = 'pg_restore';
DROP TRIGGER b_user ON public.b;
TABLE b_history_policies;
ROLLBACK;

\echo '=== F3: teardown, DROP CONSTRAINT of an ordinary user constraint on a versioned base: managed history policies dropped'
BEGIN;
ALTER TABLE public.b ADD CONSTRAINT b_user_check CHECK (id > 0);
TABLE b_history_policies;
SET LOCAL check_function_bodies = off;
SET LOCAL row_security = off;
SET LOCAL sql_saga.clean_restore = on;
ALTER TABLE ONLY public.b DROP CONSTRAINT b_user_check;
TABLE b_history_policies;
ROLLBACK;

DROP VIEW b_history_policies;

TABLE family_state;

-- C: the teardown succeeds. It is replayed inside a transaction and rolled back, so the families
-- are still there afterwards. What sql_saga drops itself (the managed history policies carrying this
-- installation's token, which the dump cannot name) is reported at DEBUG, not shown here; the state
-- after the teardown shows that nothing of any family is left behind.
\echo '=== C1: pg_restore --clean --if-exists of a dump from another installation, run as a script through psql with the opt-in'
-- The dump's managed history policy names carry the other installation's token: those drops are no-ops.
BEGIN;
SET LOCAL check_function_bodies = off;
SET LOCAL row_security = off;
SET LOCAL sql_saga.clean_restore = on;
SET LOCAL client_min_messages = warning;
DROP POLICY IF EXISTS sql_saga_history_reader_dump0001 ON public.b_history;
DROP POLICY IF EXISTS sql_saga_history_reader_dump0001 ON public.aa_hist;
DROP POLICY IF EXISTS sql_saga_history_base_select_dump0001 ON public.b_history;
DROP POLICY IF EXISTS sql_saga_history_base_select_dump0001 ON public.aa_hist;
DROP POLICY IF EXISTS sql_saga_0123456789abcdef ON public.b_history;
DROP POLICY IF EXISTS b_tenant ON public.b;
ALTER TABLE IF EXISTS ONLY public.c DROP CONSTRAINT IF EXISTS c_p_id_valid;
DROP TRIGGER IF EXISTS zbase_truncate ON public.zbase;
DROP TRIGGER IF EXISTS zbase_system_time_write_history ON public.zbase;
DROP TRIGGER IF EXISTS zbase_system_time_generated_always ON public.zbase;
DROP TRIGGER IF EXISTS r_p_id_fkey_uk_update ON public.p;
DROP TRIGGER IF EXISTS r_p_id_fkey_uk_delete ON public.p;
DROP TRIGGER IF EXISTS r_p_id_check ON public.r;
DROP TRIGGER IF EXISTS p_valid_sync_temporal_trg ON public.p;
DROP TRIGGER IF EXISTS for_portion_of_valid ON public.p__for_portion_of_valid;
DROP TRIGGER IF EXISTS current_valid ON public.p__current_valid;
DROP TRIGGER IF EXISTS b_truncate ON public.b;
DROP TRIGGER IF EXISTS b_system_time_write_history ON public.b;
DROP TRIGGER IF EXISTS b_system_time_generated_always ON public.b;
DROP INDEX IF EXISTS public.r_p_id_idx;
DROP INDEX IF EXISTS public.p_valid_range_gist_idx;
DROP INDEX IF EXISTS public.p_id_valid_range_idx;
DROP INDEX IF EXISTS public.p_id_idx;
DROP INDEX IF EXISTS public.c_valid_range_gist_idx;
DROP INDEX IF EXISTS public.c_p_id_valid_gist_idx;
DROP INDEX IF EXISTS public.c_id_valid_range_idx;
DROP INDEX IF EXISTS public.c_id_idx;
ALTER TABLE IF EXISTS ONLY public.zbase DROP CONSTRAINT IF EXISTS zbase_pkey;
ALTER TABLE IF EXISTS ONLY public.r DROP CONSTRAINT IF EXISTS r_pkey;
ALTER TABLE IF EXISTS ONLY public.p DROP CONSTRAINT IF EXISTS p_pkey;
ALTER TABLE IF EXISTS ONLY public.c DROP CONSTRAINT IF EXISTS c_pkey;
ALTER TABLE IF EXISTS ONLY public.b DROP CONSTRAINT IF EXISTS b_pkey;
DROP TABLE IF EXISTS public.r;
DROP VIEW IF EXISTS public.p__for_portion_of_valid;
DROP VIEW IF EXISTS public.p__current_valid;
DROP TABLE IF EXISTS public.p;
DROP TABLE IF EXISTS public.c;
DROP FUNCTION IF EXISTS sql_saga.public_p_valid_template_sync();
DROP FUNCTION IF EXISTS public.zbase__from_to(timestamp with time zone, timestamp with time zone);
DROP FUNCTION IF EXISTS public.zbase__between_symmetric(timestamp with time zone, timestamp with time zone);
DROP FUNCTION IF EXISTS public.zbase__between(timestamp with time zone, timestamp with time zone);
DROP FUNCTION IF EXISTS public.zbase__as_of(timestamp with time zone);
DROP VIEW IF EXISTS public.ab_view;
DROP VIEW IF EXISTS public.aa_hist_gated;
DROP TABLE IF EXISTS public.zbase;
DROP TABLE IF EXISTS public.aa_hist;
DROP FUNCTION IF EXISTS public.b__from_to(timestamp with time zone, timestamp with time zone);
DROP FUNCTION IF EXISTS public.b__between_symmetric(timestamp with time zone, timestamp with time zone);
DROP FUNCTION IF EXISTS public.b__between(timestamp with time zone, timestamp with time zone);
DROP FUNCTION IF EXISTS public.b__as_of(timestamp with time zone);
DROP VIEW IF EXISTS public.b_with_history;
DROP VIEW IF EXISTS public.b_history_gated;
DROP TABLE IF EXISTS public.b_history;
DROP TABLE IF EXISTS public.b;
RESET client_min_messages;
TABLE family_state;
ROLLBACK;

\echo '=== C2: pg_restore --clean (without --if-exists) of a dump of this very database, by pg_restore'
BEGIN;
SET LOCAL check_function_bodies = off;
SET LOCAL row_security = off;
SET LOCAL application_name = 'pg_restore';
DROP POLICY sql_saga_history_reader_test0001 ON public.b_history;
DROP POLICY sql_saga_history_reader_test0001 ON public.aa_hist;
DROP POLICY sql_saga_history_base_select_test0001 ON public.b_history;
DROP POLICY sql_saga_history_base_select_test0001 ON public.aa_hist;
DROP POLICY sql_saga_5d18f5f440ffed66 ON public.b_history;
DROP POLICY b_tenant ON public.b;
ALTER TABLE ONLY public.c DROP CONSTRAINT c_p_id_valid;
DROP TRIGGER zbase_truncate ON public.zbase;
DROP TRIGGER zbase_system_time_write_history ON public.zbase;
DROP TRIGGER zbase_system_time_generated_always ON public.zbase;
DROP TRIGGER r_p_id_fkey_uk_update ON public.p;
DROP TRIGGER r_p_id_fkey_uk_delete ON public.p;
DROP TRIGGER r_p_id_check ON public.r;
DROP TRIGGER p_valid_sync_temporal_trg ON public.p;
DROP TRIGGER for_portion_of_valid ON public.p__for_portion_of_valid;
DROP TRIGGER current_valid ON public.p__current_valid;
DROP TRIGGER b_truncate ON public.b;
DROP TRIGGER b_system_time_write_history ON public.b;
DROP TRIGGER b_system_time_generated_always ON public.b;
DROP INDEX public.r_p_id_idx;
DROP INDEX public.p_valid_range_gist_idx;
DROP INDEX public.p_id_valid_range_idx;
DROP INDEX public.p_id_idx;
DROP INDEX public.c_valid_range_gist_idx;
DROP INDEX public.c_p_id_valid_gist_idx;
DROP INDEX public.c_id_valid_range_idx;
DROP INDEX public.c_id_idx;
ALTER TABLE ONLY public.zbase DROP CONSTRAINT zbase_pkey;
ALTER TABLE ONLY public.r DROP CONSTRAINT r_pkey;
ALTER TABLE ONLY public.p DROP CONSTRAINT p_pkey;
ALTER TABLE ONLY public.c DROP CONSTRAINT c_pkey;
ALTER TABLE ONLY public.b DROP CONSTRAINT b_pkey;
DROP TABLE public.r;
DROP VIEW public.p__for_portion_of_valid;
DROP VIEW public.p__current_valid;
DROP TABLE public.p;
DROP TABLE public.c;
DROP FUNCTION sql_saga.public_p_valid_template_sync();
DROP FUNCTION public.zbase__from_to(timestamp with time zone, timestamp with time zone);
DROP FUNCTION public.zbase__between_symmetric(timestamp with time zone, timestamp with time zone);
DROP FUNCTION public.zbase__between(timestamp with time zone, timestamp with time zone);
DROP FUNCTION public.zbase__as_of(timestamp with time zone);
DROP VIEW public.ab_view;
DROP VIEW public.aa_hist_gated;
DROP TABLE public.zbase;
DROP TABLE public.aa_hist;
DROP FUNCTION public.b__from_to(timestamp with time zone, timestamp with time zone);
DROP FUNCTION public.b__between_symmetric(timestamp with time zone, timestamp with time zone);
DROP FUNCTION public.b__between(timestamp with time zone, timestamp with time zone);
DROP FUNCTION public.b__as_of(timestamp with time zone);
DROP VIEW public.b_with_history;
DROP VIEW public.b_history_gated;
DROP TABLE public.b_history;
DROP TABLE public.b;
TABLE family_state;
ROLLBACK;

TABLE family_state;

\echo '=== C3: identifiers that need quoting (spaces, an embedded double quote) in pg_restore''s ALTER TABLE ... DROP CONSTRAINT'
SET client_min_messages = warning;
CREATE SCHEMA "my schema";
CREATE TABLE "my schema"."My Table" (id int, valid_range daterange NOT NULL, x text);
SELECT sql_saga.add_era('"my schema"."My Table"', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('"my schema"."My Table"', ARRAY['id'], key_type => 'primary', unique_key_name => 'my_table_id_valid');
SELECT sql_saga.add_for_portion_of_view('"my schema"."My Table"', 'valid');
CREATE TABLE "my schema"."Ref ""T"" able" (id int PRIMARY KEY, p_id int);
SELECT sql_saga.add_regular_foreign_key('"my schema"."Ref ""T"" able"', ARRAY['p_id'], 'my_table_id_valid');
SELECT sql_saga.add_system_versioning('"my schema"."Ref ""T"" able"');
RESET client_min_messages;
CREATE TEMP VIEW quoted_family_state AS
    SELECT (SELECT count(*) FROM sql_saga.era WHERE table_schema = 'my schema') AS eras,
           (SELECT count(*) FROM sql_saga.system_versioning WHERE table_schema = 'my schema') AS versioned,
           (SELECT count(*) FROM pg_catalog.pg_class WHERE relnamespace = '"my schema"'::regnamespace AND relkind IN ('r', 'v')) AS relations,
           (SELECT count(*) FROM pg_catalog.pg_policy AS p JOIN pg_catalog.pg_class AS c ON c.oid = p.polrelid
             WHERE c.relnamespace = '"my schema"'::regnamespace) AS policies;
TABLE quoted_family_state;
BEGIN;
SET LOCAL check_function_bodies = off;
SET LOCAL row_security = off;
SET LOCAL application_name = 'pg_restore';
DROP POLICY sql_saga_history_reader_test0001 ON "my schema"."Ref ""T"" able_history";
DROP POLICY sql_saga_history_base_select_test0001 ON "my schema"."Ref ""T"" able_history";
DROP TRIGGER for_portion_of_valid ON "my schema"."My Table__for_portion_of_valid";
DROP TRIGGER "Ref ""T"" able_truncate" ON "my schema"."Ref ""T"" able";
DROP TRIGGER "Ref ""T"" able_system_time_write_history" ON "my schema"."Ref ""T"" able";
DROP TRIGGER "Ref ""T"" able_system_time_generated_always" ON "my schema"."Ref ""T"" able";
DROP TRIGGER "Ref ""T"" able_p_id_fkey_uk_update" ON "my schema"."My Table";
DROP TRIGGER "Ref ""T"" able_p_id_fkey_uk_delete" ON "my schema"."My Table";
DROP TRIGGER "Ref ""T"" able_p_id_check" ON "my schema"."Ref ""T"" able";
DROP INDEX "my schema"."Ref ""T"" able_p_id_idx";
DROP INDEX "my schema"."My Table_valid_range_gist_idx";
DROP INDEX "my schema"."My Table_id_valid_range_idx";
DROP INDEX "my schema"."My Table_id_idx";
ALTER TABLE ONLY "my schema"."Ref ""T"" able" DROP CONSTRAINT "Ref ""T"" able_pkey";
ALTER TABLE ONLY "my schema"."My Table" DROP CONSTRAINT "My Table_pkey";
DROP VIEW "my schema"."My Table__for_portion_of_valid";
DROP TABLE "my schema"."My Table";
DROP FUNCTION "my schema"."Ref ""T"" able__from_to"(timestamp with time zone, timestamp with time zone);
DROP FUNCTION "my schema"."Ref ""T"" able__between_symmetric"(timestamp with time zone, timestamp with time zone);
DROP FUNCTION "my schema"."Ref ""T"" able__between"(timestamp with time zone, timestamp with time zone);
DROP FUNCTION "my schema"."Ref ""T"" able__as_of"(timestamp with time zone);
DROP VIEW "my schema"."Ref ""T"" able_with_history";
DROP VIEW "my schema"."Ref ""T"" able_history_gated";
DROP TABLE "my schema"."Ref ""T"" able_history";
DROP TABLE "my schema"."Ref ""T"" able";
TABLE quoted_family_state;
ROLLBACK;
TABLE quoted_family_state;
DROP VIEW quoted_family_state;
SET client_min_messages = warning;
DROP SCHEMA "my schema" CASCADE;
RESET client_min_messages;

DROP VIEW family_state;

-- The extension teardown's cascade inventory is not part of what this test pins.
SET client_min_messages = warning;
\i sql/include/test_teardown.sql
RESET client_min_messages;
