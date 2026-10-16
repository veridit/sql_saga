\i sql/include/test_setup.sql

/*
 * Moving a system-versioned table, with its history, to another schema (or renaming it).
 *
 * A managed table cannot be renamed or moved directly (058). The supported sequence, named by the
 * refusal message:
 *   1. sql_saga.drop_system_versioning(table, cleanup => false)  -- keeps the history table and rows
 *   2. sql_saga.drop_system_time_era(table, cleanup => false)     -- keeps the system_valid_range column
 *   3. move the base table and its history table into the same schema
 *   4. sql_saga.add_system_versioning(moved table, history_table_name => ..., history_table_must_exist => true)
 *
 * history_table_must_exist => true adopts the existing history table, and fails without changing
 * anything if it is missing, still in the old schema only, of another shape, already the history table
 * of another family, or has an era. Adopting an existing table is always reported by a NOTICE with its
 * row count. Creating a new history table (the default) stays silent.
 *
 * Not inside one transaction: history rows are written per transaction.
 */
\set SHOW_CONTEXT never
\set VERBOSITY default
SET client_min_messages = warning;

CREATE SCHEMA mv_old;
CREATE SCHEMA mv_new;

-- Everything one add_system_versioning call could change: the base table, every table carrying the
-- history name in either schema (so an old-schema candidate is covered too), and their sql_saga metadata.
-- History rows are listed without their system time ranges (wall-clock values); history_fingerprint
-- below compares complete contents.
CREATE FUNCTION mv_new.family_state(base_schema name, base_name name, history_name name)
RETURNS TABLE (ord int, item text, state text) LANGUAGE plpgsql STABLE AS $family_state$
DECLARE
    base_oid regclass := to_regclass(format('%I.%I', base_schema, base_name));
    h record;
    n int := 0;
BEGIN
    RETURN QUERY
    SELECT 1, 'base system_time era', coalesce((
        SELECT string_agg(format('%s(%s %s)', e.era_name, e.range_column_name, e.range_type), ',' ORDER BY e.era_name)
        FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name) = (base_schema, base_name)), '')
    UNION ALL
    SELECT 2, 'base system_time_era', coalesce((
        SELECT format('%s,%s,%s,%s snapshot=%s', ste.generated_always_trigger, ste.write_history_trigger,
                      ste.truncate_trigger, ste.infinity_check_constraint, ste.table_columns_snapshot)
        FROM sql_saga.system_time_era AS ste WHERE (ste.table_schema, ste.table_name) = (base_schema, base_name)), '')
    UNION ALL
    SELECT 3, 'base system_versioning', coalesce((
        SELECT format('history=%s.%s view=%s.%s', sv.history_schema_name, sv.history_table_name, sv.view_schema_name, sv.view_table_name)
        FROM sql_saga.system_versioning AS sv WHERE (sv.table_schema, sv.table_name) = (base_schema, base_name)), '')
    UNION ALL
    SELECT 4, 'base columns', (
        SELECT string_agg(format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)), ',' ORDER BY a.attnum)
        FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = base_oid AND a.attnum > 0 AND NOT a.attisdropped)
    UNION ALL
    SELECT 5, 'base triggers', coalesce((
        SELECT string_agg(t.tgname, ',' ORDER BY t.tgname)
        FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = base_oid AND NOT t.tgisinternal), '')
    UNION ALL
    SELECT 6, 'base constraints', coalesce((
        SELECT string_agg(c.conname, ',' ORDER BY c.conname)
        FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = base_oid), '')
    UNION ALL
    SELECT 7, 'relations in ' || base_schema || ' starting with ' || base_name, coalesce((
        SELECT string_agg(c.relname, ',' ORDER BY c.relname)
        FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS ns ON ns.oid = c.relnamespace
        WHERE ns.nspname = base_schema AND c.relkind IN ('r', 'v') AND starts_with(c.relname, base_name)), '')
    UNION ALL
    SELECT 8, 'functions in ' || base_schema || ' starting with ' || base_name, coalesce((
        SELECT string_agg(p.proname, ',' ORDER BY p.proname)
        FROM pg_catalog.pg_proc AS p JOIN pg_catalog.pg_namespace AS ns ON ns.oid = p.pronamespace
        WHERE ns.nspname = base_schema AND starts_with(p.proname, base_name)), '')
    UNION ALL
    SELECT 9, 'tables named ' || history_name, coalesce((
        SELECT string_agg(format('%s.%s', ns.nspname, c.relname), ',' ORDER BY ns.nspname)
        FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS ns ON ns.oid = c.relnamespace
        WHERE c.relname = history_name AND ns.nspname IN ('mv_old', 'mv_new')), '')
    ORDER BY 1;

    FOR h IN
        SELECT ns.nspname AS hs, c.oid AS hoid
        FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS ns ON ns.oid = c.relnamespace
        WHERE c.relname = history_name AND ns.nspname IN ('mv_old', 'mv_new')
        ORDER BY ns.nspname
    LOOP
        n := n + 1;
        RETURN QUERY
        SELECT 10 * n + 1, h.hs || '.' || history_name || ' columns', (
            SELECT string_agg(format('%s %s', a.attname, format_type(a.atttypid, a.atttypmod)), ',' ORDER BY a.attnum)
            FROM pg_catalog.pg_attribute AS a WHERE a.attrelid = h.hoid AND a.attnum > 0 AND NOT a.attisdropped)
        UNION ALL
        SELECT 10 * n + 2, h.hs || '.' || history_name || ' triggers', coalesce((
            SELECT string_agg(t.tgname, ',' ORDER BY t.tgname)
            FROM pg_catalog.pg_trigger AS t WHERE t.tgrelid = h.hoid AND NOT t.tgisinternal), '')
        UNION ALL
        SELECT 10 * n + 3, h.hs || '.' || history_name || ' constraints', coalesce((
            SELECT string_agg(c.conname, ',' ORDER BY c.conname)
            FROM pg_catalog.pg_constraint AS c WHERE c.conrelid = h.hoid), '')
        UNION ALL
        SELECT 10 * n + 4, h.hs || '.' || history_name || ' row security', (
            SELECT format('enabled=%s policies=%s', c.relrowsecurity, (SELECT count(*) FROM pg_catalog.pg_policy AS p WHERE p.polrelid = h.hoid))
            FROM pg_catalog.pg_class AS c WHERE c.oid = h.hoid)
        UNION ALL
        SELECT 10 * n + 5, h.hs || '.' || history_name || ' eras', coalesce((
            SELECT string_agg(format('%s(%s %s)', e.era_name, e.range_column_name, e.range_type), ',' ORDER BY e.era_name)
            FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name) = (h.hs, history_name)), '')
        UNION ALL
        SELECT 10 * n + 6, h.hs || '.' || history_name || ' system_time_era', coalesce((
            SELECT ste.era_name::text FROM sql_saga.system_time_era AS ste
            WHERE (ste.table_schema, ste.table_name) = (h.hs, history_name)), '')
        UNION ALL
        SELECT 10 * n + 7, h.hs || '.' || history_name || ' history of', coalesce((
            SELECT string_agg(format('%s.%s', sv.table_schema, sv.table_name), ',')
            FROM sql_saga.system_versioning AS sv WHERE (sv.history_schema_name, sv.history_table_name) = (h.hs, history_name)), '');
        RETURN QUERY EXECUTE format($q$
            SELECT %1$s, %2$L, coalesce(string_agg((to_jsonb(h) - 'system_valid_range')::text, ' ' ORDER BY (to_jsonb(h) - 'system_valid_range')::text), '')
            FROM %3$s AS h$q$, 10 * n + 8, h.hs || '.' || history_name || ' rows', h.hoid::regclass);
    END LOOP;
END;
$family_state$;

-- Complete contents of a table, system time ranges included, for before/after comparison.
CREATE FUNCTION mv_new.history_fingerprint(table_oid regclass) RETURNS text LANGUAGE plpgsql STABLE AS $history_fingerprint$
DECLARE
    result text;
BEGIN
    EXECUTE format('SELECT md5(coalesce(string_agg(to_jsonb(h)::text, %L ORDER BY to_jsonb(h)::text), %L)) FROM %s AS h', ' ', '', table_oid)
    INTO result;
    RETURN result;
END;
$history_fingerprint$;

CREATE TABLE mv_old.t (id int PRIMARY KEY, v text);
SELECT sql_saga.add_system_versioning('mv_old.t');
RESET client_min_messages;

INSERT INTO mv_old.t VALUES (1, 'a');
SELECT clock_timestamp() AS ts_a \gset
UPDATE mv_old.t SET v = 'b';
SELECT clock_timestamp() AS ts_b \gset
UPDATE mv_old.t SET v = 'c';
SELECT count(*) AS history_rows_before_move FROM mv_old.t_history;
SELECT mv_new.history_fingerprint('mv_old.t_history') AS t_history_before_move \gset

\echo '=== 1: a direct move or rename is refused, and the message names the safe sequence'
ALTER TABLE mv_old.t SET SCHEMA mv_new;
ALTER TABLE mv_old.t RENAME TO t2;
ALTER TABLE mv_old.t_history SET SCHEMA mv_new;

\echo '=== 2: round trip: drop management keeping the data, move both tables, adopt the history'
SELECT sql_saga.drop_system_versioning('mv_old.t', cleanup => false);
SELECT sql_saga.drop_system_time_era('mv_old.t', cleanup => false);
ALTER TABLE mv_old.t SET SCHEMA mv_new;
ALTER TABLE mv_old.t_history SET SCHEMA mv_new;
SELECT sql_saga.add_system_versioning('mv_new.t', history_table_name => 't_history', history_table_must_exist => true);
SELECT * FROM mv_new.family_state('mv_new', 't', 't_history') ORDER BY ord;
SELECT table_schema, table_name, history_schema_name, history_table_name, view_schema_name, view_table_name, func_as_of
FROM sql_saga.system_versioning ORDER BY table_schema, table_name;
SELECT count(*) AS history_rows_after_move FROM mv_new.t_history;
SELECT mv_new.history_fingerprint('mv_new.t_history') = :'t_history_before_move' AS history_contents_preserved_by_move;

\echo '=== 2b: a new change is recorded after re-adding, and old and new versions are all queryable'
UPDATE mv_new.t SET v = 'd';
SELECT count(*) AS history_rows_after_update FROM mv_new.t_history;
SELECT v FROM mv_new.t_with_history ORDER BY lower(system_valid_range);
SELECT v AS as_of_a FROM mv_new.t__as_of(:'ts_a');
SELECT v AS as_of_b FROM mv_new.t__as_of(:'ts_b');
SELECT v AS as_of_now FROM mv_new.t__as_of(now());
SELECT count(*) AS between_a_and_now FROM mv_new.t__between(:'ts_a', now());
SELECT sql_saga.check_integrity();
SELECT 'integrity checked' AS done;

\echo '=== 3: refusals with history_table_must_exist => true: nothing is created or changed'
-- Each case shows family_state (base, every table with the history name, their metadata and rows)
-- before and after, plus a fingerprint of the complete history contents where a history table exists.

\echo '--- 3a: the history table is missing'
CREATE TABLE mv_new.u (id int PRIMARY KEY, v text);
SELECT * FROM mv_new.family_state('mv_new', 'u', 'u_history') ORDER BY ord;
SELECT sql_saga.add_system_versioning('mv_new.u', history_table_must_exist => true);
SELECT * FROM mv_new.family_state('mv_new', 'u', 'u_history') ORDER BY ord;

\echo '--- 3b: the history table is still in the old schema only'
SET client_min_messages = warning;
CREATE TABLE mv_old.w (id int PRIMARY KEY, v text);
SELECT sql_saga.add_system_versioning('mv_old.w');
INSERT INTO mv_old.w VALUES (1, 'w1');
UPDATE mv_old.w SET v = 'w2';
UPDATE mv_old.w SET v = 'w3';
SELECT sql_saga.drop_system_versioning('mv_old.w', cleanup => false);
SELECT sql_saga.drop_system_time_era('mv_old.w', cleanup => false);
ALTER TABLE mv_old.w SET SCHEMA mv_new;
RESET client_min_messages;
SELECT * FROM mv_new.family_state('mv_new', 'w', 'w_history') ORDER BY ord;
SELECT mv_new.history_fingerprint('mv_old.w_history') AS w_history_before \gset
SELECT sql_saga.add_system_versioning('mv_new.w', history_table_must_exist => true);
SELECT * FROM mv_new.family_state('mv_new', 'w', 'w_history') ORDER BY ord;
SELECT mv_new.history_fingerprint('mv_old.w_history') = :'w_history_before' AS old_schema_history_contents_unchanged;

\echo '--- 3c: the history table has another shape'
-- The shape is checked before any DDL: a ddl_command_start event trigger counts every DDL command
-- into a sequence, which a failed transaction does not roll back.
CREATE SEQUENCE mv_new.ddl_count;
CREATE FUNCTION mv_new.count_ddl() RETURNS event_trigger LANGUAGE plpgsql AS $count_ddl$
BEGIN
    PERFORM nextval('mv_new.ddl_count');
END;
$count_ddl$;
CREATE TABLE mv_new.s (id int PRIMARY KEY, v text, system_valid_range tstzrange NOT NULL DEFAULT tstzrange('-infinity', 'infinity'));
CREATE TABLE mv_new.s_history (id int, system_valid_range tstzrange NOT NULL);
INSERT INTO mv_new.s_history VALUES (1, tstzrange('2019-01-01', '2020-01-01')), (2, tstzrange('2020-01-01', '2021-01-01'));
SELECT * FROM mv_new.family_state('mv_new', 's', 's_history') ORDER BY ord;
SELECT mv_new.history_fingerprint('mv_new.s_history') AS s_history_before \gset
CREATE EVENT TRIGGER mv_count_ddl ON ddl_command_start EXECUTE FUNCTION mv_new.count_ddl();
-- Positive control: one real DDL command under the trigger is counted.
CREATE TABLE mv_new.ddl_control ();
SELECT is_called AS ddl_control_counted FROM mv_new.ddl_count;
SELECT setval('mv_new.ddl_count', 1, false) AS ddl_counter_reset;
SELECT sql_saga.add_system_versioning('mv_new.s', history_table_must_exist => true);
DROP EVENT TRIGGER mv_count_ddl;
SELECT is_called AS ddl_ran_before_shape_refusal FROM mv_new.ddl_count;
SELECT * FROM mv_new.family_state('mv_new', 's', 's_history') ORDER BY ord;
SELECT mv_new.history_fingerprint('mv_new.s_history') = :'s_history_before' AS other_shape_history_contents_unchanged;

\echo '--- 3c2: the history table lacks the system time column the base table would get'
-- The comparison is against the base table as it would be versioned, with system_valid_range added.
CREATE TABLE mv_new.r (id int PRIMARY KEY, v text);
CREATE TABLE mv_new.r_history (id int, v text);
INSERT INTO mv_new.r_history VALUES (1, 'r1');
SELECT * FROM mv_new.family_state('mv_new', 'r', 'r_history') ORDER BY ord;
SELECT setval('mv_new.ddl_count', 1, false) AS ddl_counter_reset;
CREATE EVENT TRIGGER mv_count_ddl ON ddl_command_start EXECUTE FUNCTION mv_new.count_ddl();
SELECT sql_saga.add_system_versioning('mv_new.r', history_table_must_exist => true);
DROP EVENT TRIGGER mv_count_ddl;
SELECT is_called AS ddl_ran_before_shape_refusal FROM mv_new.ddl_count;
SELECT * FROM mv_new.family_state('mv_new', 'r', 'r_history') ORDER BY ord;

\echo '--- 3d: the history table already belongs to another family'
CREATE TABLE mv_new.x (id int PRIMARY KEY, v text, system_valid_range tstzrange NOT NULL DEFAULT tstzrange('-infinity', 'infinity'));
SELECT * FROM mv_new.family_state('mv_new', 'x', 't_history') ORDER BY ord;
SELECT * FROM mv_new.family_state('mv_new', 't', 't_history') ORDER BY ord;
SELECT mv_new.history_fingerprint('mv_new.t_history') AS t_history_before \gset
SELECT sql_saga.add_system_versioning('mv_new.x', history_table_name => 't_history', history_table_must_exist => true);
SELECT * FROM mv_new.family_state('mv_new', 'x', 't_history') ORDER BY ord;
-- The other family is untouched: its catalog rows, history rows, and history queries.
SELECT * FROM mv_new.family_state('mv_new', 't', 't_history') ORDER BY ord;
SELECT mv_new.history_fingerprint('mv_new.t_history') = :'t_history_before' AS other_family_history_contents_unchanged;
SELECT table_schema, table_name, history_schema_name, history_table_name
FROM sql_saga.system_versioning ORDER BY table_schema, table_name;
SELECT v AS other_family_as_of_a FROM mv_new.t__as_of(:'ts_a');
SELECT v AS other_family_as_of_b FROM mv_new.t__as_of(:'ts_b');
SELECT v FROM mv_new.t_with_history ORDER BY lower(system_valid_range);

\echo '--- 3e: the history table has an era'
SET client_min_messages = warning;
CREATE TABLE mv_new.e (id int PRIMARY KEY, v text, valid_range daterange NOT NULL,
    system_valid_range tstzrange NOT NULL DEFAULT tstzrange('-infinity', 'infinity'));
CREATE TABLE mv_new.e_history (id int, v text, valid_range daterange NOT NULL, system_valid_range tstzrange NOT NULL);
SELECT sql_saga.add_era('mv_new.e_history', 'valid_range', synchronize_columns => false);
RESET client_min_messages;
INSERT INTO mv_new.e_history VALUES (1, 'e1', '[2019-01-01,2020-01-01)', tstzrange('2019-01-01', '2020-01-01'));
SELECT * FROM mv_new.family_state('mv_new', 'e', 'e_history') ORDER BY ord;
SELECT to_jsonb(e) AS e_history_era_before FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name) = ('mv_new', 'e_history') \gset
SELECT mv_new.history_fingerprint('mv_new.e_history') AS e_history_before \gset
SELECT sql_saga.add_system_versioning('mv_new.e', history_table_must_exist => true);
SELECT * FROM mv_new.family_state('mv_new', 'e', 'e_history') ORDER BY ord;
SELECT (SELECT to_jsonb(e) FROM sql_saga.era AS e WHERE (e.table_schema, e.table_name) = ('mv_new', 'e_history')) = :'e_history_era_before'::jsonb AS history_era_row_unchanged;
SELECT mv_new.history_fingerprint('mv_new.e_history') = :'e_history_before' AS era_history_contents_unchanged;

\echo '=== 4: the default still adopts an existing history table, and says so; creating one stays silent'
CREATE TABLE mv_new.f (id int PRIMARY KEY, v text,
    system_valid_range tstzrange NOT NULL DEFAULT tstzrange('-infinity', 'infinity'));
CREATE TABLE mv_new.f_history (id int, v text, system_valid_range tstzrange NOT NULL);
INSERT INTO mv_new.f_history VALUES (1, 'older', tstzrange('2019-01-01', '2020-01-01')), (1, 'old', tstzrange('2020-01-01', '2021-01-01'));
SELECT sql_saga.add_system_versioning('mv_new.f');
SELECT table_schema, table_name, history_schema_name, history_table_name
FROM sql_saga.system_versioning WHERE table_name = 'f';
CREATE TABLE mv_new.g (id int PRIMARY KEY, v text);
SELECT sql_saga.add_system_versioning('mv_new.g');
SELECT table_schema, table_name, history_schema_name, history_table_name
FROM sql_saga.system_versioning WHERE table_name = 'g';
SELECT sql_saga.check_integrity();
SELECT 'integrity checked' AS done;

SET client_min_messages = warning;
DROP SCHEMA mv_new CASCADE;
DROP SCHEMA mv_old CASCADE;
\i sql/include/test_teardown.sql
RESET client_min_messages;
