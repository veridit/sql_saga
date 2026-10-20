\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * Foreign key column pairing (tmp/fable_audit_v13.md W11, C2, C3, C4; P1 items 6 and 7).
 *
 * A foreign key pairs its i-th child column with the i-th referenced column. sql_saga must pair them as the
 * user gave them, whatever plan the server picks and whatever order the catalog lists the key in:
 *
 *   Part 1 (W11). add_regular_foreign_key builds the child-side check and the helper signature from
 *     fk_column_names with a string_agg that has no ORDER BY, over a join on pg_attribute. When that join is
 *     a merge join sorted by name, the check passes the child columns alphabetically to a helper whose
 *     $1..$4 are in key order: valid rows are refused, and a mixed-type key cannot even be added. The plan is
 *     the planner's choice, so the same scenario runs twice, under planner settings that force the sorting
 *     merge join and under settings that force a nested loop (no sort), with identical expectations.
 *   Part 2 (C4). add_foreign_key finds the referenced key only when pk_column_names equals the catalog list
 *     exactly; any order of the same columns must be found, pairing child and referenced columns by position
 *     as given. The catalog stores the child columns in the referenced key's order; drop_foreign_key finds the
 *     foreign key by its child columns in that order or in the order the user gave (as a set), and refuses an
 *     ambiguous match. Renames of a permuted foreign key's columns keep it working.
 *   Part 3 (C2/C3, item 7). A foreign key created against a key whose catalog list sql_saga 1.0/1.1 had sorted
 *     (the user was forced to pass the sorted list, the child side followed the user's order) pairs child
 *     columns with the wrong referenced columns. A fresh 1.3 cannot create one, so it is built by the narrowest
 *     damage that reproduces the state an updated 1.0/1.1 database is in after its key order was restored.
 *     check_integrity() must name the mispairing, the repair (caveated: it pairs by position, which the user
 *     must confirm) and how to find affected rows; the scan and the repair are then run verbatim.
 *   Part 4. The same with quoted names containing a double quote and an apostrophe.
 *
 * Enforcement is observed through probe(), which runs one statement with every deferrable constraint checked
 * immediately, always rolls it back, and reports accepted / refused (any integrity_constraint_violation) /
 * error, so this test does not depend on how a foreign key is enforced (CHECK, trigger or native constraint).
 * Parent-side probes always use a parent referenced by exactly one child, so one foreign key cannot mask
 * another, and the reference they protect is inserted (and checked) first.
 */

CREATE SCHEMA fkp;
SET search_path = fkp, public;

CREATE FUNCTION fkp.probe(statement text) RETURNS text LANGUAGE plpgsql AS $probe$
BEGIN
    BEGIN
        SET CONSTRAINTS ALL IMMEDIATE;
        EXECUTE statement;
        SET CONSTRAINTS ALL IMMEDIATE;
        RAISE EXCEPTION USING ERRCODE = 'P0004', MESSAGE = 'accepted';
    EXCEPTION
        WHEN assert_failure THEN RETURN 'accepted';
        WHEN integrity_constraint_violation THEN RETURN 'refused';
        WHEN OTHERS THEN RETURN 'error: ' || SQLERRM;
    END;
END;
$probe$;

-- Runs statements and keeps their effect; reports ok, or the error (the statements are then rolled back).
CREATE FUNCTION fkp.run(statement text) RETURNS text LANGUAGE plpgsql AS $run$
BEGIN
    EXECUTE statement;
    RETURN 'ok';
EXCEPTION WHEN OTHERS THEN
    RETURN 'error: ' || SQLERRM;
END;
$run$;

-- Runs statements and always rolls them back; reports ok or the error.
CREATE FUNCTION fkp.attempt(statement text) RETURNS text LANGUAGE plpgsql AS $attempt$
BEGIN
    EXECUTE statement;
    RAISE EXCEPTION USING ERRCODE = 'P0004', MESSAGE = 'ok';
EXCEPTION
    WHEN assert_failure THEN RETURN 'ok';
    WHEN OTHERS THEN RETURN 'error: ' || SQLERRM;
END;
$attempt$;

-- Runs statements and keeps their effect if they succeed; reports 'applied', or 'refused' (whatever the error,
-- whose wording is not specified here) after which nothing of the statements is left behind.
CREATE FUNCTION fkp.run_or_refuse(statement text) RETURNS text LANGUAGE plpgsql AS $run_or_refuse$
BEGIN
    EXECUTE statement;
    RETURN 'applied';
EXCEPTION WHEN OTHERS THEN
    RETURN 'refused';
END;
$run_or_refuse$;

-- The managed constraints of a foreign key as they are: the native constraint (temporal) or the child CHECK
-- (regular, if the design still has one), each with its validation state and definition.
CREATE FUNCTION fkp.fk_objects(fk_name name) RETURNS text LANGUAGE sql AS $fk_objects$
    SELECT coalesce(string_agg(format('%s %s %s', c.contype, c.convalidated, pg_catalog.pg_get_constraintdef(c.oid)), '; ' ORDER BY c.conname COLLATE "C"), 'none')
    FROM sql_saga.foreign_keys AS fk
    JOIN pg_catalog.pg_constraint AS c
      ON c.conrelid = format('%I.%I', fk.table_schema, fk.table_name)::regclass
     AND c.conname IN (fk.foreign_key_name, fk.fk_check_constraint)
    WHERE fk.foreign_key_name = fk_name;
$fk_objects$;

CREATE FUNCTION fkp.integrity() RETURNS text LANGUAGE plpgsql AS $integrity$
DECLARE
    message text;
    hint text;
BEGIN
    PERFORM sql_saga.check_integrity();
    RETURN 'ok';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS message = MESSAGE_TEXT, hint = PG_EXCEPTION_HINT;
    RETURN 'message: ' || message || CASE WHEN hint <> '' THEN E'\nhint: ' || hint ELSE '' END;
END;
$integrity$;

-- Whether the released add_regular_foreign_key's name join, for this child table and these columns, is
-- planned under the current settings as a join that sorts the names.
CREATE FUNCTION fkp.name_join_sorts_by_name(child regclass, columns name[]) RETURNS boolean LANGUAGE plpgsql AS $name_join_sorts_by_name$
DECLARE
    line text;
    sorted boolean := false;
BEGIN
    FOR line IN EXECUTE format($$
        EXPLAIN SELECT string_agg(format_type(a.atttypid, a.atttypmod), ', '), string_agg(quote_ident(n.name), ', ')
        FROM unnest(%L::name[]) WITH ORDINALITY AS n (name, ordinality)
        LEFT JOIN pg_catalog.pg_attribute AS a ON (a.attrelid, a.attname) = (%s::oid, n.name)$$, columns, child::oid)
    LOOP
        sorted := sorted OR line LIKE '%Sort Key: n.name%';
    END LOOP;
    RETURN sorted;
END;
$name_join_sorts_by_name$;

-- The child-side CHECK of a regular foreign key, if the design still has one, calls the helper with the
-- child columns in the order of foreign_keys.column_names.
CREATE FUNCTION fkp.check_arguments_in_key_order(fk_name name) RETURNS text LANGUAGE sql AS $check_arguments_in_key_order$
    SELECT CASE WHEN count(fk.foreign_key_name) = 0 THEN 'no such foreign key'
                ELSE coalesce(bool_and(pg_catalog.pg_get_constraintdef(c.oid) LIKE
                                       '%(' || (SELECT string_agg(quote_ident(n), ', ' ORDER BY o) FROM unnest(fk.column_names) WITH ORDINALITY AS u (n, o)) || '))'),
                              true)::text END
    FROM sql_saga.foreign_keys AS fk
    LEFT JOIN pg_catalog.pg_constraint AS c
      ON c.conrelid = format('%I.%I', fk.table_schema, fk.table_name)::regclass AND c.conname = fk.fk_check_constraint AND c.contype = 'c'
    WHERE fk.foreign_key_name = fk_name;
$check_arguments_in_key_order$;

/* ---------------------------------------------------------------------------------------------------------
 * Part 1: W11. One scenario, run under two planner configurations. Every table name carries the suffix, so the
 * two runs do not share objects. Each parent is referenced by exactly one child.
 * --------------------------------------------------------------------------------------------------------- */
CREATE FUNCTION fkp.w11(sfx text) RETURNS SETOF text LANGUAGE plpgsql AS $w11$
DECLARE
    key text := $$ARRAY['person_id', 'person_role_id', 'legal_unit_id', 'establishment_id']$$;
    mkey text := $$ARRAY['unit_id', 'code', 'region_id', 'kind']$$;
BEGIN
    -- A 4-column int natural key, one parent for the regular child and one for the temporal child.
    -- Key rows: 1 = (10, 20, 30, 40) referenced, 2 = (11, 21, 31, 41) unreferenced.
    EXECUTE format($$
        CREATE TABLE fkp.%1$I (id int NOT NULL, person_id int NOT NULL, person_role_id int NOT NULL,
                               legal_unit_id int NOT NULL, establishment_id int NOT NULL, valid_range daterange NOT NULL);
        CREATE TABLE fkp.%2$I (LIKE fkp.%1$I);
        SELECT sql_saga.add_era('fkp.%1$I', 'valid_range', 'valid');
        SELECT sql_saga.add_era('fkp.%2$I', 'valid_range', 'valid');
        SELECT sql_saga.add_unique_key('fkp.%1$I', %3$s, key_type => 'natural', unique_key_name => %4$L);
        SELECT sql_saga.add_unique_key('fkp.%2$I', %3$s, key_type => 'natural', unique_key_name => %5$L);
        INSERT INTO fkp.%1$I VALUES (1, 10, 20, 30, 40, '[2020-01-01,infinity)'), (2, 11, 21, 31, 41, '[2020-01-01,infinity)');
        INSERT INTO fkp.%2$I TABLE fkp.%1$I;
        CREATE TABLE fkp.%6$I (id int PRIMARY KEY, name text, note text, created_at timestamptz, amount numeric, flag boolean,
                               person_id int, person_role_id int, legal_unit_id int, establishment_id int,
                               c1 text, c2 text, c3 text, c4 text, c5 text, c6 text, c7 text, c8 text);
        CREATE TABLE fkp.%7$I (id int, name text, note text, person_id int, person_role_id int, legal_unit_id int,
                               establishment_id int, valid_range daterange NOT NULL);
        SELECT sql_saga.add_era('fkp.%7$I', 'valid_range', 'valid');
        $$, 'par_r_' || sfx, 'par_t_' || sfx, key, 'par_r_' || sfx || '_key', 'par_t_' || sfx || '_key',
        'ref_' || sfx, 'tref_' || sfx);

    RETURN NEXT 'plan: name join sorts by name: '
        || fkp.name_join_sorts_by_name(format('fkp.%I', 'ref_' || sfx)::regclass, ARRAY['person_id', 'person_role_id', 'legal_unit_id', 'establishment_id']);
    RETURN NEXT 'add regular foreign key: ' || fkp.run(format($$SELECT sql_saga.add_foreign_key('fkp.%1$I', %2$s, 'fkp.%3$I', %2$s, foreign_key_name => %4$L)$$,
        'ref_' || sfx, key, 'par_r_' || sfx, 'ref_' || sfx || '_fkey'));
    RETURN NEXT 'add temporal foreign key: ' || fkp.run(format($$SELECT sql_saga.add_foreign_key('fkp.%1$I', %2$s, 'fkp.%3$I', %2$s, foreign_key_name => %4$L)$$,
        'tref_' || sfx, key, 'par_t_' || sfx, 'tref_' || sfx || '_fkey'));
    RETURN NEXT 'regular child check arguments in key order: ' || fkp.check_arguments_in_key_order(('ref_' || sfx || '_fkey')::name);
    RETURN NEXT 'integrity: ' || fkp.integrity();

    RETURN NEXT 'regular: existing key accepted: ' || fkp.probe(format($$INSERT INTO fkp.%I (id, person_id, person_role_id, legal_unit_id, establishment_id) VALUES (1, 10, 20, 30, 40)$$, 'ref_' || sfx));
    RETURN NEXT 'regular: missing key refused: ' || fkp.probe(format($$INSERT INTO fkp.%I (id, person_id, person_role_id, legal_unit_id, establishment_id) VALUES (1, 10, 20, 30, 41)$$, 'ref_' || sfx));
    -- (30, 40, 20, 10) is key row 1 read through the alphabetical column list.
    RETURN NEXT 'regular: key read alphabetically refused: ' || fkp.probe(format($$INSERT INTO fkp.%I (id, person_id, person_role_id, legal_unit_id, establishment_id) VALUES (1, 30, 40, 20, 10)$$, 'ref_' || sfx));
    RETURN NEXT 'regular: NULL column accepted (MATCH SIMPLE): ' || fkp.probe(format($$INSERT INTO fkp.%I (id, person_id, person_role_id, legal_unit_id, establishment_id) VALUES (1, 99, 99, 99, NULL)$$, 'ref_' || sfx));
    RETURN NEXT 'regular: reference to key row 1 inserted: ' || fkp.run(format($$INSERT INTO fkp.%I (id, person_id, person_role_id, legal_unit_id, establishment_id) VALUES (1, 10, 20, 30, 40)$$, 'ref_' || sfx));
    RETURN NEXT 'regular parent: delete referenced key row refused: ' || fkp.probe(format($$DELETE FROM fkp.%I WHERE id = 1$$, 'par_r_' || sfx));
    RETURN NEXT 'regular parent: update referenced key refused: ' || fkp.probe(format($$UPDATE fkp.%I SET person_role_id = 29 WHERE id = 1$$, 'par_r_' || sfx));
    RETURN NEXT 'regular parent: delete unreferenced key row accepted: ' || fkp.probe(format($$DELETE FROM fkp.%I WHERE id = 2$$, 'par_r_' || sfx));
    RETURN NEXT 'regular parent: update unreferenced key accepted: ' || fkp.probe(format($$UPDATE fkp.%I SET person_role_id = 29 WHERE id = 2$$, 'par_r_' || sfx));

    RETURN NEXT 'temporal: existing key accepted: ' || fkp.probe(format($$INSERT INTO fkp.%I (id, person_id, person_role_id, legal_unit_id, establishment_id, valid_range) VALUES (1, 10, 20, 30, 40, '[2021-01-01,2022-01-01)')$$, 'tref_' || sfx));
    RETURN NEXT 'temporal: missing key refused: ' || fkp.probe(format($$INSERT INTO fkp.%I (id, person_id, person_role_id, legal_unit_id, establishment_id, valid_range) VALUES (1, 10, 20, 30, 41, '[2021-01-01,2022-01-01)')$$, 'tref_' || sfx));
    RETURN NEXT 'temporal: key read alphabetically refused: ' || fkp.probe(format($$INSERT INTO fkp.%I (id, person_id, person_role_id, legal_unit_id, establishment_id, valid_range) VALUES (1, 30, 40, 20, 10, '[2021-01-01,2022-01-01)')$$, 'tref_' || sfx));
    RETURN NEXT 'temporal: reference to key row 1 inserted: ' || fkp.run(format($$INSERT INTO fkp.%I (id, person_id, person_role_id, legal_unit_id, establishment_id, valid_range) VALUES (1, 10, 20, 30, 40, '[2021-01-01,2022-01-01)')$$, 'tref_' || sfx));
    RETURN NEXT 'temporal parent: delete referenced key row refused: ' || fkp.probe(format($$DELETE FROM fkp.%I WHERE id = 1$$, 'par_t_' || sfx));
    RETURN NEXT 'temporal parent: update referenced key refused: ' || fkp.probe(format($$UPDATE fkp.%I SET person_role_id = 29 WHERE id = 1$$, 'par_t_' || sfx));
    RETURN NEXT 'temporal parent: delete unreferenced key row accepted: ' || fkp.probe(format($$DELETE FROM fkp.%I WHERE id = 2$$, 'par_t_' || sfx));
    RETURN NEXT 'temporal parent: update unreferenced key accepted: ' || fkp.probe(format($$UPDATE fkp.%I SET person_role_id = 29 WHERE id = 2$$, 'par_t_' || sfx));

    -- A mixed-type key (int, text, int, text): a mispaired argument list cannot even be created.
    -- Key rows: 1 = (1, 'a', 2, 'b') referenced, 2 = (3, 'c', 4, 'd') unreferenced.
    EXECUTE format($$
        CREATE TABLE fkp.%1$I (id int NOT NULL, unit_id int NOT NULL, code text NOT NULL, region_id int NOT NULL, kind text NOT NULL,
                               valid_range daterange NOT NULL);
        CREATE TABLE fkp.%2$I (LIKE fkp.%1$I);
        SELECT sql_saga.add_era('fkp.%1$I', 'valid_range', 'valid');
        SELECT sql_saga.add_era('fkp.%2$I', 'valid_range', 'valid');
        SELECT sql_saga.add_unique_key('fkp.%1$I', %3$s, key_type => 'natural', unique_key_name => %4$L);
        SELECT sql_saga.add_unique_key('fkp.%2$I', %3$s, key_type => 'natural', unique_key_name => %5$L);
        INSERT INTO fkp.%1$I VALUES (1, 1, 'a', 2, 'b', '[2020-01-01,infinity)'), (2, 3, 'c', 4, 'd', '[2020-01-01,infinity)');
        INSERT INTO fkp.%2$I TABLE fkp.%1$I;
        CREATE TABLE fkp.%6$I (id int PRIMARY KEY, unit_id int, code text, region_id int, kind text);
        CREATE TABLE fkp.%7$I (id int, unit_id int, code text, region_id int, kind text, valid_range daterange NOT NULL);
        SELECT sql_saga.add_era('fkp.%7$I', 'valid_range', 'valid');
        $$, 'mpar_r_' || sfx, 'mpar_t_' || sfx, mkey, 'mpar_r_' || sfx || '_key', 'mpar_t_' || sfx || '_key',
        'mref_' || sfx, 'mtref_' || sfx);
    RETURN NEXT 'plan (mixed): name join sorts by name: '
        || fkp.name_join_sorts_by_name(format('fkp.%I', 'mref_' || sfx)::regclass, ARRAY['unit_id', 'code', 'region_id', 'kind']);
    RETURN NEXT 'mixed: add regular foreign key: ' || fkp.run(format($$SELECT sql_saga.add_foreign_key('fkp.%1$I', %2$s, 'fkp.%3$I', %2$s, foreign_key_name => %4$L)$$,
        'mref_' || sfx, mkey, 'mpar_r_' || sfx, 'mref_' || sfx || '_fkey'));
    RETURN NEXT 'mixed: add temporal foreign key: ' || fkp.run(format($$SELECT sql_saga.add_foreign_key('fkp.%1$I', %2$s, 'fkp.%3$I', %2$s, foreign_key_name => %4$L)$$,
        'mtref_' || sfx, mkey, 'mpar_t_' || sfx, 'mtref_' || sfx || '_fkey'));
    RETURN NEXT 'mixed: regular child check arguments in key order: ' || fkp.check_arguments_in_key_order(('mref_' || sfx || '_fkey')::name);
    RETURN NEXT 'mixed: integrity: ' || fkp.integrity();
    RETURN NEXT 'mixed regular: existing key accepted: ' || fkp.probe(format($$INSERT INTO fkp.%I VALUES (1, 1, 'a', 2, 'b')$$, 'mref_' || sfx));
    RETURN NEXT 'mixed regular: missing key refused: ' || fkp.probe(format($$INSERT INTO fkp.%I VALUES (1, 1, 'a', 2, 'd')$$, 'mref_' || sfx));
    RETURN NEXT 'mixed regular: reference to key row 1 inserted: ' || fkp.run(format($$INSERT INTO fkp.%I VALUES (1, 1, 'a', 2, 'b')$$, 'mref_' || sfx));
    RETURN NEXT 'mixed regular parent: delete referenced key row refused: ' || fkp.probe(format($$DELETE FROM fkp.%I WHERE id = 1$$, 'mpar_r_' || sfx));
    RETURN NEXT 'mixed regular parent: update referenced key refused: ' || fkp.probe(format($$UPDATE fkp.%I SET code = 'z' WHERE id = 1$$, 'mpar_r_' || sfx));
    RETURN NEXT 'mixed regular parent: delete unreferenced key row accepted: ' || fkp.probe(format($$DELETE FROM fkp.%I WHERE id = 2$$, 'mpar_r_' || sfx));
    RETURN NEXT 'mixed regular parent: update unreferenced key accepted: ' || fkp.probe(format($$UPDATE fkp.%I SET code = 'z' WHERE id = 2$$, 'mpar_r_' || sfx));
    RETURN NEXT 'mixed temporal: existing key accepted: ' || fkp.probe(format($$INSERT INTO fkp.%I VALUES (1, 1, 'a', 2, 'b', '[2021-01-01,2022-01-01)')$$, 'mtref_' || sfx));
    RETURN NEXT 'mixed temporal: missing key refused: ' || fkp.probe(format($$INSERT INTO fkp.%I VALUES (1, 1, 'a', 2, 'd', '[2021-01-01,2022-01-01)')$$, 'mtref_' || sfx));
    RETURN NEXT 'mixed temporal: reference to key row 1 inserted: ' || fkp.run(format($$INSERT INTO fkp.%I VALUES (1, 1, 'a', 2, 'b', '[2021-01-01,2022-01-01)')$$, 'mtref_' || sfx));
    RETURN NEXT 'mixed temporal parent: delete referenced key row refused: ' || fkp.probe(format($$DELETE FROM fkp.%I WHERE id = 1$$, 'mpar_t_' || sfx));
    RETURN NEXT 'mixed temporal parent: update referenced key refused: ' || fkp.probe(format($$UPDATE fkp.%I SET code = 'z' WHERE id = 1$$, 'mpar_t_' || sfx));
    RETURN NEXT 'mixed temporal parent: delete unreferenced key row accepted: ' || fkp.probe(format($$DELETE FROM fkp.%I WHERE id = 2$$, 'mpar_t_' || sfx));
    RETURN NEXT 'mixed temporal parent: update unreferenced key accepted: ' || fkp.probe(format($$UPDATE fkp.%I SET code = 'z' WHERE id = 2$$, 'mpar_t_' || sfx));
END;
$w11$;
\o

\echo '--- part 1: planner forced to a merge join, which sorts the name join (the W11 shape)'
BEGIN;
SET LOCAL enable_hashjoin = off;
SET LOCAL enable_nestloop = off;
SET LOCAL enable_mergejoin = on;
SET LOCAL plan_cache_mode = force_custom_plan;
SELECT fkp.w11('sort');
COMMIT;

\echo '--- part 1: planner forced to a nested loop, which keeps the given order'
BEGIN;
SET LOCAL enable_hashjoin = off;
SET LOCAL enable_mergejoin = off;
SET LOCAL enable_nestloop = on;
SET LOCAL plan_cache_mode = force_custom_plan;
SELECT fkp.w11('loop');
COMMIT;

/* ---------------------------------------------------------------------------------------------------------
 * Part 2: C4. The referenced columns in another order than the catalog's; child columns pair with them by
 * position as given. The child tables use their own column names (p, r, l, e) so the pairing is visible in
 * the catalog. Separate parents for the regular and the temporal child.
 * --------------------------------------------------------------------------------------------------------- */
\o /dev/null
CREATE TABLE fkp.c4_par_r (id int NOT NULL, person_id int NOT NULL, person_role_id int NOT NULL,
                           legal_unit_id int NOT NULL, establishment_id int NOT NULL, valid_range daterange NOT NULL);
CREATE TABLE fkp.c4_par_t (LIKE fkp.c4_par_r);
CREATE TABLE fkp.c4_other (LIKE fkp.c4_par_r);
SELECT sql_saga.add_era('fkp.c4_par_r', 'valid_range', 'valid');
SELECT sql_saga.add_era('fkp.c4_par_t', 'valid_range', 'valid');
SELECT sql_saga.add_era('fkp.c4_other', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('fkp.c4_par_r', ARRAY['person_id', 'person_role_id', 'legal_unit_id', 'establishment_id'], key_type => 'natural', unique_key_name => 'c4_par_r_key');
SELECT sql_saga.add_unique_key('fkp.c4_par_t', ARRAY['person_id', 'person_role_id', 'legal_unit_id', 'establishment_id'], key_type => 'natural', unique_key_name => 'c4_par_t_key');
SELECT sql_saga.add_unique_key('fkp.c4_other', ARRAY['person_id', 'person_role_id', 'legal_unit_id', 'establishment_id'], key_type => 'natural', unique_key_name => 'c4_other_key');
-- Key rows: 1 = (12, 22, 32, 42) referenced, 2 = (13, 23, 33, 43) unreferenced.
INSERT INTO fkp.c4_par_r VALUES (1, 12, 22, 32, 42, '[2020-01-01,infinity)'), (2, 13, 23, 33, 43, '[2020-01-01,infinity)');
INSERT INTO fkp.c4_par_t TABLE fkp.c4_par_r;
INSERT INTO fkp.c4_other TABLE fkp.c4_par_r;
CREATE TABLE fkp.perm_ref (id int PRIMARY KEY, p int, r int, l int, e int);
CREATE TABLE fkp.perm_tref (id int, p int, r int, l int, e int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('fkp.perm_tref', 'valid_range', 'valid');
\o
\echo '--- part 2: referenced columns given in another order than the key'
SELECT 'add regular foreign key, referenced columns alphabetical: ' || fkp.run($$SELECT sql_saga.add_foreign_key('fkp.perm_ref',
    ARRAY['e', 'l', 'p', 'r'], 'fkp.c4_par_r', ARRAY['establishment_id', 'legal_unit_id', 'person_id', 'person_role_id'], foreign_key_name => 'perm_ref_fkey')$$);
SELECT 'add temporal foreign key, referenced columns reversed: ' || fkp.run($$SELECT sql_saga.add_foreign_key('fkp.perm_tref',
    ARRAY['e', 'l', 'r', 'p'], 'fkp.c4_par_t', ARRAY['establishment_id', 'legal_unit_id', 'person_role_id', 'person_id'], foreign_key_name => 'perm_tref_fkey')$$);
-- The catalog lists the child columns in the order of the referenced key, each still paired with its column.
SELECT fk.foreign_key_name || ': ' || fk.column_names::text || ' -> ' || uk.unique_key_name || ' ' || uk.column_names::text
FROM sql_saga.foreign_keys AS fk JOIN sql_saga.unique_keys AS uk USING (unique_key_name)
WHERE fk.foreign_key_name IN ('perm_ref_fkey', 'perm_tref_fkey') ORDER BY fk.foreign_key_name;
SELECT 'regular child check arguments in key order: ' || fkp.check_arguments_in_key_order('perm_ref_fkey');
SELECT 'integrity: ' || fkp.integrity();
SELECT 'regular: existing key accepted: ' || fkp.probe($$INSERT INTO fkp.perm_ref (id, p, r, l, e) VALUES (1, 12, 22, 32, 42)$$);
SELECT 'regular: missing key refused: ' || fkp.probe($$INSERT INTO fkp.perm_ref (id, p, r, l, e) VALUES (1, 12, 22, 32, 40)$$);
SELECT 'regular: key read in the given referenced order refused: ' || fkp.probe($$INSERT INTO fkp.perm_ref (id, p, r, l, e) VALUES (1, 42, 32, 12, 22)$$);
SELECT 'regular: reference to key row 1 inserted: ' || fkp.run($$INSERT INTO fkp.perm_ref (id, p, r, l, e) VALUES (1, 12, 22, 32, 42)$$);
SELECT 'regular parent: delete referenced key row refused: ' || fkp.probe($$DELETE FROM fkp.c4_par_r WHERE id = 1$$);
SELECT 'regular parent: update referenced key refused: ' || fkp.probe($$UPDATE fkp.c4_par_r SET person_role_id = 29 WHERE id = 1$$);
SELECT 'regular parent: delete unreferenced key row accepted: ' || fkp.probe($$DELETE FROM fkp.c4_par_r WHERE id = 2$$);
SELECT 'regular parent: update unreferenced key accepted: ' || fkp.probe($$UPDATE fkp.c4_par_r SET person_role_id = 29 WHERE id = 2$$);
SELECT 'temporal: existing key accepted: ' || fkp.probe($$INSERT INTO fkp.perm_tref (id, p, r, l, e, valid_range) VALUES (1, 12, 22, 32, 42, '[2021-01-01,2022-01-01)')$$);
SELECT 'temporal: missing key refused: ' || fkp.probe($$INSERT INTO fkp.perm_tref (id, p, r, l, e, valid_range) VALUES (1, 12, 22, 32, 40, '[2021-01-01,2022-01-01)')$$);
SELECT 'temporal: key read in the given referenced order refused: ' || fkp.probe($$INSERT INTO fkp.perm_tref (id, p, r, l, e, valid_range) VALUES (1, 42, 32, 22, 12, '[2021-01-01,2022-01-01)')$$);
SELECT 'temporal: reference to key row 1 inserted: ' || fkp.run($$INSERT INTO fkp.perm_tref (id, p, r, l, e, valid_range) VALUES (1, 12, 22, 32, 42, '[2021-01-01,2022-01-01)')$$);
SELECT 'temporal parent: delete referenced key row refused: ' || fkp.probe($$DELETE FROM fkp.c4_par_t WHERE id = 1$$);
SELECT 'temporal parent: update referenced key refused: ' || fkp.probe($$UPDATE fkp.c4_par_t SET person_role_id = 29 WHERE id = 1$$);
SELECT 'temporal parent: delete unreferenced key row accepted: ' || fkp.probe($$DELETE FROM fkp.c4_par_t WHERE id = 2$$);
SELECT 'temporal parent: update unreferenced key accepted: ' || fkp.probe($$UPDATE fkp.c4_par_t SET person_role_id = 29 WHERE id = 2$$);

\echo '--- part 2: drop_foreign_key finds a permuted foreign key by its child columns in key order or in the order given'
SELECT 'drop regular by catalogued (key) order: ' || fkp.attempt($$SELECT sql_saga.drop_foreign_key('fkp.perm_ref', ARRAY['p', 'r', 'l', 'e'])$$);
SELECT 'drop regular by the order given to add_foreign_key: ' || fkp.attempt($$SELECT sql_saga.drop_foreign_key('fkp.perm_ref', ARRAY['e', 'l', 'p', 'r'])$$);
SELECT 'drop temporal by catalogued (key) order: ' || fkp.attempt($$SELECT sql_saga.drop_foreign_key('fkp.perm_tref', ARRAY['p', 'r', 'l', 'e'])$$);
SELECT 'drop temporal by the order given to add_foreign_key: ' || fkp.attempt($$SELECT sql_saga.drop_foreign_key('fkp.perm_tref', ARRAY['e', 'l', 'r', 'p'])$$);
SELECT 'drop by a different column set: ' || fkp.attempt($$SELECT sql_saga.drop_foreign_key('fkp.perm_ref', ARRAY['p', 'r', 'l'])$$);
-- A second foreign key on the same child columns (to another parent) makes the column set ambiguous.
SELECT 'add a second regular foreign key on the same child columns: ' || fkp.run($$SELECT sql_saga.add_foreign_key('fkp.perm_ref',
    ARRAY['p', 'r', 'l', 'e'], 'fkp.c4_other', ARRAY['person_id', 'person_role_id', 'legal_unit_id', 'establishment_id'], foreign_key_name => 'perm_ref2_fkey')$$);
SELECT 'drop by the order given, ambiguous: ' || fkp.attempt($$SELECT sql_saga.drop_foreign_key('fkp.perm_ref', ARRAY['e', 'l', 'p', 'r'])$$);
SELECT 'drop by key order, ambiguous: ' || fkp.attempt($$SELECT sql_saga.drop_foreign_key('fkp.perm_ref', ARRAY['p', 'r', 'l', 'e'])$$);
SELECT 'drop by name instead: ' || fkp.attempt($$SELECT sql_saga.drop_foreign_key_by_name('fkp.perm_ref', 'perm_ref2_fkey')$$);
SELECT 'both regular foreign keys still there: ' || string_agg(foreign_key_name, ', ' ORDER BY foreign_key_name COLLATE "C")
FROM sql_saga.foreign_keys WHERE table_schema = 'fkp' AND table_name = 'perm_ref';

\echo '--- part 2: renames of a permuted foreign key (rolled back)'
BEGIN;
\o /dev/null
ALTER TABLE fkp.perm_ref RENAME COLUMN p TO person;
ALTER TABLE fkp.perm_tref RENAME COLUMN p TO person;
ALTER TABLE fkp.c4_par_r RENAME COLUMN person_role_id TO role_id;
ALTER TABLE fkp.c4_par_t RENAME COLUMN person_role_id TO role_id;
\o
SELECT fk.foreign_key_name || ': ' || fk.column_names::text || ' -> ' || uk.unique_key_name || ' ' || uk.column_names::text
FROM sql_saga.foreign_keys AS fk JOIN sql_saga.unique_keys AS uk USING (unique_key_name)
WHERE fk.foreign_key_name IN ('perm_ref_fkey', 'perm_tref_fkey') ORDER BY fk.foreign_key_name;
SELECT 'integrity after the renames: ' || fkp.integrity();
SELECT 'regular: existing key accepted after the renames: ' || fkp.probe($$INSERT INTO fkp.perm_ref (id, person, r, l, e) VALUES (2, 12, 22, 32, 42)$$);
SELECT 'regular: missing key refused after the renames: ' || fkp.probe($$INSERT INTO fkp.perm_ref (id, person, r, l, e) VALUES (2, 12, 22, 32, 40)$$);
SELECT 'regular parent: update referenced key refused after the renames: ' || fkp.probe($$UPDATE fkp.c4_par_r SET role_id = 29 WHERE id = 1$$);
SELECT 'temporal: existing key accepted after the renames: ' || fkp.probe($$INSERT INTO fkp.perm_tref (id, person, r, l, e, valid_range) VALUES (2, 12, 22, 32, 42, '[2022-01-01,2023-01-01)')$$);
SELECT 'temporal: missing key refused after the renames: ' || fkp.probe($$INSERT INTO fkp.perm_tref (id, person, r, l, e, valid_range) VALUES (2, 12, 22, 32, 40, '[2022-01-01,2023-01-01)')$$);
SELECT 'temporal parent: update referenced key refused after the renames: ' || fkp.probe($$UPDATE fkp.c4_par_t SET role_id = 29 WHERE id = 1$$);
ROLLBACK;

/* ---------------------------------------------------------------------------------------------------------
 * Damage: rebuild a regular and a temporal foreign key the way sql_saga 1.0/1.1 built them against a key
 * whose catalog list it had sorted (helper body and referenced-side trigger arguments from the sorted list;
 * the child CHECK, if any, with the child columns in the catalog order; the native foreign key REFERENCES
 * the sorted list), then restore the catalog list to the constraint order, as the update to 1.3 does.
 * sql_saga's event triggers are kept out of it (session_replication_role = replica, transaction-local).
 * --------------------------------------------------------------------------------------------------------- */
\o /dev/null
CREATE FUNCTION fkp.mispair(uk_name name, sorted_columns name[], regular_fk name, temporal_fk name) RETURNS void LANGUAGE plpgsql AS $mispair$
DECLARE
    original name[];
    r record;
BEGIN
    PERFORM set_config('session_replication_role', 'replica', true);
    SELECT uk.column_names INTO original FROM sql_saga.unique_keys AS uk WHERE uk.unique_key_name = uk_name;
    UPDATE sql_saga.unique_keys AS uk SET column_names = sorted_columns WHERE uk.unique_key_name = uk_name;

    PERFORM sql_saga.__internal_regenerate_regular_fk_helper(regular_fk);
    PERFORM sql_saga.__internal_recreate_regular_fk_uk_triggers(regular_fk);
    FOR r IN
        SELECT c.conrelid::regclass AS child, c.conname, pg_catalog.to_regprocedure(fk.fk_helper_function)::regproc AS helper,
               (SELECT string_agg(quote_ident(n), ', ' ORDER BY o) FROM unnest(fk.column_names) WITH ORDINALITY AS u (n, o)) AS args
        FROM sql_saga.foreign_keys AS fk
        JOIN pg_catalog.pg_constraint AS c
          ON c.conrelid = format('%I.%I', fk.table_schema, fk.table_name)::regclass AND c.conname = fk.fk_check_constraint AND c.contype = 'c'
        WHERE fk.foreign_key_name = regular_fk
    LOOP
        EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I, ADD CONSTRAINT %I CHECK (%s(%s))', r.child, r.conname, r.conname, r.helper, r.args);
    END LOOP;

    SELECT format('%I.%I', fk.table_schema, fk.table_name)::regclass AS child,
           (SELECT string_agg(quote_ident(n), ', ' ORDER BY o) FROM unnest(fk.column_names) WITH ORDINALITY AS u (n, o)) AS child_columns,
           fe.range_column_name AS child_range,
           format('%I.%I', uk.table_schema, uk.table_name)::regclass AS parent,
           (SELECT string_agg(quote_ident(n), ', ' ORDER BY o) FROM unnest(sorted_columns) WITH ORDINALITY AS u (n, o)) AS parent_columns,
           ue.range_column_name AS parent_range
    INTO r
    FROM sql_saga.foreign_keys AS fk
    JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
    JOIN sql_saga.era AS fe ON (fe.table_schema, fe.table_name, fe.era_name) = (fk.table_schema, fk.table_name, fk.fk_era_name)
    JOIN sql_saga.era AS ue ON (ue.table_schema, ue.table_name, ue.era_name) = (uk.table_schema, uk.table_name, uk.era_name)
    WHERE fk.foreign_key_name = temporal_fk;
    EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', r.child, temporal_fk);
    EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %I FOREIGN KEY (%s, PERIOD %I) REFERENCES %s (%s, PERIOD %I) DEFERRABLE',
                   r.child, temporal_fk, r.child_columns, r.child_range, r.parent, r.parent_columns, r.parent_range);

    UPDATE sql_saga.unique_keys AS uk SET column_names = original WHERE uk.unique_key_name = uk_name;
    PERFORM set_config('session_replication_role', 'origin', true);
END;
$mispair$;

-- Expected check_integrity reports, repairs and scans (hand-written specification), filled inside the rolled
-- back transactions of parts 3 and 4.
CREATE TABLE fkp.expected_report (fk name PRIMARY KEY, report text, repair text, scan text);

/* ---------------------------------------------------------------------------------------------------------
 * Part 3: an XOR key in the StatBus shape person_for_unit (person_id, person_role_id, legal_unit_id,
 * establishment_id), sorted by 1.0/1.1 to (establishment_id, legal_unit_id, person_id, person_role_id).
 * --------------------------------------------------------------------------------------------------------- */
CREATE TABLE fkp.person_for_unit (id int NOT NULL, person_id int NOT NULL, person_role_id int NOT NULL,
                                  legal_unit_id int, establishment_id int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('fkp.person_for_unit', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key(table_oid => 'fkp.person_for_unit'::regclass, key_type => 'natural',
    column_names => ARRAY['person_id', 'person_role_id', 'legal_unit_id', 'establishment_id'],
    mutually_exclusive_columns => ARRAY['legal_unit_id', 'establishment_id'],
    unique_key_name => 'person_for_unit_natural_key_valid');
-- 1 = (10, 20, 30, 40) for the regular child, 4 = (12, 22, 32, 42) for the temporal child, 3 = (11, 21, 31, 41)
-- unreferenced. No row (40, 30, 10, 20): a child row with those values is a reference to row 1 read through the
-- sorted list, and an orphan otherwise.
INSERT INTO fkp.person_for_unit VALUES (1, 10, 20, 30, 40, '[2020-01-01,infinity)'),
                                       (3, 11, 21, 31, 41, '[2020-01-01,infinity)'),
                                       (4, 12, 22, 32, 42, '[2020-01-01,infinity)');
CREATE TABLE fkp.person_ref (id int PRIMARY KEY, person_id int, person_role_id int, legal_unit_id int, establishment_id int);
CREATE TABLE fkp.person_tref (id int, person_id int, person_role_id int, legal_unit_id int, establishment_id int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('fkp.person_tref', 'valid_range', 'valid');
SELECT sql_saga.add_foreign_key('fkp.person_ref', ARRAY['person_id', 'person_role_id', 'legal_unit_id', 'establishment_id'],
    'fkp.person_for_unit', ARRAY['person_id', 'person_role_id', 'legal_unit_id', 'establishment_id'], foreign_key_name => 'person_ref_fkey');
SELECT sql_saga.add_foreign_key('fkp.person_tref', ARRAY['person_id', 'person_role_id', 'legal_unit_id', 'establishment_id'],
    'fkp.person_for_unit', ARRAY['person_id', 'person_role_id', 'legal_unit_id', 'establishment_id'], foreign_key_name => 'person_tref_fkey');
\o
\echo '--- part 3: correctly paired foreign keys to the XOR key'
SELECT 'integrity: ' || fkp.integrity();
\o /dev/null
-- Parts 3 and 4 each run in a transaction that is rolled back at the end, so the damage of one part (or a
-- repair that fails) cannot be reported by the other part's check_integrity.
BEGIN;
SELECT fkp.mispair('person_for_unit_natural_key_valid', ARRAY['establishment_id', 'legal_unit_id', 'person_id', 'person_role_id'],
                   'person_ref_fkey', 'person_tref_fkey');
\o
\echo '--- part 3: the mispaired foreign keys (the 1.0/1.1 state) enforce the wrong pairing'
SELECT 'regular: intended key refused: ' || fkp.probe($$INSERT INTO fkp.person_ref VALUES (1, 10, 20, 30, 40)$$);
SELECT 'temporal: intended key refused: ' || fkp.probe($$INSERT INTO fkp.person_tref VALUES (1, 10, 20, 30, 40, '[2021-01-01,2022-01-01)')$$);
-- (40, 30, 10, 20) exists only if read through the sorted list, as key row 1. Kept: data the damage let in.
SELECT 'regular: crossed key accepted and kept: ' || fkp.run($$INSERT INTO fkp.person_ref VALUES (9, 40, 30, 10, 20)$$);
SELECT 'temporal: crossed key accepted and kept: ' || fkp.run($$INSERT INTO fkp.person_tref VALUES (9, 40, 30, 10, 20, '[2021-01-01,2022-01-01)')$$);

\o /dev/null
\set ref_repair 'SELECT sql_saga.drop_foreign_key(''fkp.person_ref'', ARRAY[''person_id'', ''person_role_id'', ''legal_unit_id'', ''establishment_id'']); SELECT sql_saga.add_foreign_key(''fkp.person_ref'', ARRAY[''person_id'', ''person_role_id'', ''legal_unit_id'', ''establishment_id''], ''fkp.person_for_unit'', ARRAY[''person_id'', ''person_role_id'', ''legal_unit_id'', ''establishment_id''], foreign_key_name => ''person_ref_fkey'')'
\set ref_scan 'SELECT fk.* FROM fkp.person_ref AS fk WHERE fk.person_id IS NOT NULL AND fk.person_role_id IS NOT NULL AND fk.legal_unit_id IS NOT NULL AND fk.establishment_id IS NOT NULL AND NOT EXISTS (SELECT FROM fkp.person_for_unit AS uk WHERE uk.person_id = fk.person_id AND uk.person_role_id = fk.person_role_id AND uk.legal_unit_id = fk.legal_unit_id AND uk.establishment_id = fk.establishment_id)'
\set tref_repair 'SELECT sql_saga.drop_foreign_key(''fkp.person_tref'', ARRAY[''person_id'', ''person_role_id'', ''legal_unit_id'', ''establishment_id'']); SELECT sql_saga.add_foreign_key(''fkp.person_tref'', ARRAY[''person_id'', ''person_role_id'', ''legal_unit_id'', ''establishment_id''], ''fkp.person_for_unit'', ARRAY[''person_id'', ''person_role_id'', ''legal_unit_id'', ''establishment_id''], foreign_key_name => ''person_tref_fkey'')'
\set confirm 'The repair pairs the child columns with the unique key columns by position; confirm that this is the intended pairing first. Inspect and resolve affected rows before re-adding.'
INSERT INTO fkp.expected_report VALUES
('person_ref_fkey',
 'message: sql_saga: integrity violation for foreign key person_ref_fkey: pairs child column person_id with referenced column establishment_id, but unique key person_for_unit_natural_key_valid pairs it with referenced column person_id; repair with ' || :'ref_repair' || E'\n'
 || 'hint: ' || :'confirm' || ' The foreign key may have accepted rows that reference a missing key, and add_foreign_key refuses while they exist; list them with ' || :'ref_scan',
 :'ref_repair', :'ref_scan'),
('person_tref_fkey',
 'message: sql_saga: integrity violation for foreign key person_tref_fkey: pairs child column person_id with referenced column establishment_id, but unique key person_for_unit_natural_key_valid pairs it with referenced column person_id; repair with ' || :'tref_repair' || E'\n'
 || 'hint: ' || :'confirm' || ' The foreign key may have accepted rows that reference a missing key; add_foreign_key creates and validates the replacement native foreign key and refuses while they exist.',
 :'tref_repair', NULL);
\o

\echo '--- part 3: check_integrity names the mispaired regular foreign key, the caveated repair and the orphan scan'
SELECT CASE WHEN fkp.integrity() = report THEN 'reported exactly as expected' ELSE E'unexpected report:\n' || fkp.integrity() END
FROM fkp.expected_report WHERE fk = 'person_ref_fkey';
\echo '--- part 3: the orphan scan of the hint, run verbatim, lists the row the mispaired foreign key let in'
\o /dev/null
SELECT format('CREATE TEMP VIEW ref_scan AS %s', scan) FROM fkp.expected_report WHERE fk = 'person_ref_fkey' \gexec
\o
SELECT 'orphan: ' || s::text FROM pg_temp.ref_scan AS s;
\echo '--- part 3: the repair, run verbatim while the orphan exists, is refused and leaves the mispaired foreign key as it was'
\o /dev/null
CREATE TEMP TABLE objects_before AS SELECT fkp.fk_objects('person_ref_fkey') AS ref, fkp.fk_objects('person_tref_fkey') AS tref;
\o
SELECT 'repair person_ref_fkey with the orphan present: ' || fkp.run_or_refuse(repair) FROM fkp.expected_report WHERE fk = 'person_ref_fkey';
SELECT 'person_ref_fkey objects unchanged (no unvalidated constraint left behind): ' || (fkp.fk_objects('person_ref_fkey') = ref) FROM pg_temp.objects_before;
SELECT CASE WHEN fkp.integrity() = report THEN 'still reported exactly as expected' ELSE E'unexpected report:\n' || fkp.integrity() END
FROM fkp.expected_report WHERE fk = 'person_ref_fkey';
\o /dev/null
DELETE FROM fkp.person_ref AS r USING pg_temp.ref_scan AS s WHERE r.id = s.id;
\o
\echo '--- part 3: the repair, run verbatim once the orphan is resolved'
SELECT 'repair person_ref_fkey: ' || fkp.run(repair) FROM fkp.expected_report WHERE fk = 'person_ref_fkey';

\echo '--- part 3: check_integrity then names the mispaired temporal foreign key'
SELECT CASE WHEN fkp.integrity() = report THEN 'reported exactly as expected' ELSE E'unexpected report:\n' || fkp.integrity() END
FROM fkp.expected_report WHERE fk = 'person_tref_fkey';
SELECT 'repair person_tref_fkey with the orphan present: ' || fkp.run_or_refuse(repair) FROM fkp.expected_report WHERE fk = 'person_tref_fkey';
SELECT 'person_tref_fkey objects unchanged (no unvalidated constraint left behind): ' || (fkp.fk_objects('person_tref_fkey') = tref) FROM pg_temp.objects_before;
SELECT CASE WHEN fkp.integrity() = report THEN 'still reported exactly as expected' ELSE E'unexpected report:\n' || fkp.integrity() END
FROM fkp.expected_report WHERE fk = 'person_tref_fkey';
\o /dev/null
DELETE FROM fkp.person_tref WHERE id = 9;
\o
SELECT 'repair person_tref_fkey: ' || fkp.run(repair) FROM fkp.expected_report WHERE fk = 'person_tref_fkey';
SELECT 'integrity after both repairs: ' || fkp.integrity();

\echo '--- part 3: the repaired foreign keys pair as intended'
SELECT 'regular: intended key accepted: ' || fkp.probe($$INSERT INTO fkp.person_ref VALUES (1, 10, 20, 30, 40)$$);
SELECT 'regular: crossed key refused: ' || fkp.probe($$INSERT INTO fkp.person_ref VALUES (1, 40, 30, 10, 20)$$);
SELECT 'regular: reference to key row 1 inserted: ' || fkp.run($$INSERT INTO fkp.person_ref VALUES (1, 10, 20, 30, 40)$$);
SELECT 'temporal: intended key accepted: ' || fkp.probe($$INSERT INTO fkp.person_tref VALUES (1, 12, 22, 32, 42, '[2021-01-01,2022-01-01)')$$);
SELECT 'temporal: crossed key refused: ' || fkp.probe($$INSERT INTO fkp.person_tref VALUES (1, 40, 30, 10, 20, '[2021-01-01,2022-01-01)')$$);
SELECT 'temporal: reference to key row 4 inserted: ' || fkp.run($$INSERT INTO fkp.person_tref VALUES (1, 12, 22, 32, 42, '[2021-01-01,2022-01-01)')$$);
SELECT 'parent: delete key row referenced only by the regular child refused: ' || fkp.probe($$DELETE FROM fkp.person_for_unit WHERE id = 1$$);
SELECT 'parent: update key referenced only by the regular child refused: ' || fkp.probe($$UPDATE fkp.person_for_unit SET person_role_id = 29 WHERE id = 1$$);
SELECT 'parent: delete key row referenced only by the temporal child refused: ' || fkp.probe($$DELETE FROM fkp.person_for_unit WHERE id = 4$$);
SELECT 'parent: update key referenced only by the temporal child refused: ' || fkp.probe($$UPDATE fkp.person_for_unit SET person_role_id = 29 WHERE id = 4$$);
SELECT 'parent: delete unreferenced key row accepted: ' || fkp.probe($$DELETE FROM fkp.person_for_unit WHERE id = 3$$);
SELECT 'parent: update unreferenced key accepted: ' || fkp.probe($$UPDATE fkp.person_for_unit SET person_role_id = 29 WHERE id = 3$$);
SELECT 'integrity: ' || fkp.integrity();
ROLLBACK;

/* ---------------------------------------------------------------------------------------------------------
 * Part 4: the same with names that need quoting, including a double quote and an apostrophe: schema
 * odd "s'ch, parent Par'ent "T", XOR key ("ro'le", "Pers""on") sorted to ("Pers""on", "ro'le"), children
 * Chi'ld "R" (regular) and Chi'ld "T" (temporal) with columns my "role" and my 'person', foreign keys
 * fk "odd" 'r' and fk "odd" 't'. Parent row 1 = (ro'le 1, Pers"on 2); the crossed reference is (2, 1).
 * --------------------------------------------------------------------------------------------------------- */
\o /dev/null
CREATE SCHEMA "odd ""s'ch";
CREATE TABLE "odd ""s'ch"."Par'ent ""T""" (id int NOT NULL, "ro'le" int, "Pers""on" int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('"odd ""s''ch"."Par''ent ""T"""', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key(table_oid => '"odd ""s''ch"."Par''ent ""T"""', key_type => 'natural', column_names => ARRAY['ro''le', 'Pers"on'],
    mutually_exclusive_columns => ARRAY['ro''le', 'Pers"on'], unique_key_name => 'uk "odd" ''k''');
INSERT INTO "odd ""s'ch"."Par'ent ""T""" VALUES (1, 1, 2, '[2020-01-01,infinity)');
CREATE TABLE "odd ""s'ch"."Chi'ld ""R""" (id int PRIMARY KEY, "my ""role""" int, "my 'person'" int);
CREATE TABLE "odd ""s'ch"."Chi'ld ""T""" (id int, "my ""role""" int, "my 'person'" int, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('"odd ""s''ch"."Chi''ld ""T"""', 'valid_range', 'valid');
SELECT sql_saga.add_foreign_key('"odd ""s''ch"."Chi''ld ""R"""', ARRAY['my "role"', 'my ''person'''], '"odd ""s''ch"."Par''ent ""T"""', ARRAY['ro''le', 'Pers"on'], foreign_key_name => 'fk "odd" ''r''');
SELECT sql_saga.add_foreign_key('"odd ""s''ch"."Chi''ld ""T"""', ARRAY['my "role"', 'my ''person'''], '"odd ""s''ch"."Par''ent ""T"""', ARRAY['ro''le', 'Pers"on'], foreign_key_name => 'fk "odd" ''t''');
\o
\echo '--- part 4: correctly paired foreign keys with quoted names'
SELECT 'integrity: ' || fkp.integrity();
\o /dev/null
BEGIN;
SELECT fkp.mispair('uk "odd" ''k''', ARRAY['Pers"on', 'ro''le'], 'fk "odd" ''r''', 'fk "odd" ''t''');
INSERT INTO fkp.expected_report VALUES
('fk "odd" ''r''',
 $report$message: sql_saga: integrity violation for foreign key "fk ""odd"" 'r'": pairs child column "my ""role""" with referenced column "Pers""on", but unique key "uk ""odd"" 'k'" pairs it with referenced column "ro'le"; repair with SELECT sql_saga.drop_foreign_key('"odd ""s''ch"."Chi''ld ""R"""', ARRAY['my "role"', 'my ''person''']); SELECT sql_saga.add_foreign_key('"odd ""s''ch"."Chi''ld ""R"""', ARRAY['my "role"', 'my ''person'''], '"odd ""s''ch"."Par''ent ""T"""', ARRAY['ro''le', 'Pers"on'], foreign_key_name => 'fk "odd" ''r''')
hint: The repair pairs the child columns with the unique key columns by position; confirm that this is the intended pairing first. Inspect and resolve affected rows before re-adding. The foreign key may have accepted rows that reference a missing key, and add_foreign_key refuses while they exist; list them with SELECT fk.* FROM "odd ""s'ch"."Chi'ld ""R""" AS fk WHERE fk."my ""role""" IS NOT NULL AND fk."my 'person'" IS NOT NULL AND NOT EXISTS (SELECT FROM "odd ""s'ch"."Par'ent ""T""" AS uk WHERE uk."ro'le" = fk."my ""role""" AND uk."Pers""on" = fk."my 'person'")$report$,
 $repair$SELECT sql_saga.drop_foreign_key('"odd ""s''ch"."Chi''ld ""R"""', ARRAY['my "role"', 'my ''person''']); SELECT sql_saga.add_foreign_key('"odd ""s''ch"."Chi''ld ""R"""', ARRAY['my "role"', 'my ''person'''], '"odd ""s''ch"."Par''ent ""T"""', ARRAY['ro''le', 'Pers"on'], foreign_key_name => 'fk "odd" ''r''')$repair$,
 $scan$SELECT fk.* FROM "odd ""s'ch"."Chi'ld ""R""" AS fk WHERE fk."my ""role""" IS NOT NULL AND fk."my 'person'" IS NOT NULL AND NOT EXISTS (SELECT FROM "odd ""s'ch"."Par'ent ""T""" AS uk WHERE uk."ro'le" = fk."my ""role""" AND uk."Pers""on" = fk."my 'person'")$scan$),
('fk "odd" ''t''',
 $report$message: sql_saga: integrity violation for foreign key "fk ""odd"" 't'": pairs child column "my ""role""" with referenced column "Pers""on", but unique key "uk ""odd"" 'k'" pairs it with referenced column "ro'le"; repair with SELECT sql_saga.drop_foreign_key('"odd ""s''ch"."Chi''ld ""T"""', ARRAY['my "role"', 'my ''person''']); SELECT sql_saga.add_foreign_key('"odd ""s''ch"."Chi''ld ""T"""', ARRAY['my "role"', 'my ''person'''], '"odd ""s''ch"."Par''ent ""T"""', ARRAY['ro''le', 'Pers"on'], foreign_key_name => 'fk "odd" ''t''')
hint: The repair pairs the child columns with the unique key columns by position; confirm that this is the intended pairing first. Inspect and resolve affected rows before re-adding. The foreign key may have accepted rows that reference a missing key; add_foreign_key creates and validates the replacement native foreign key and refuses while they exist.$report$,
 $repair$SELECT sql_saga.drop_foreign_key('"odd ""s''ch"."Chi''ld ""T"""', ARRAY['my "role"', 'my ''person''']); SELECT sql_saga.add_foreign_key('"odd ""s''ch"."Chi''ld ""T"""', ARRAY['my "role"', 'my ''person'''], '"odd ""s''ch"."Par''ent ""T"""', ARRAY['ro''le', 'Pers"on'], foreign_key_name => 'fk "odd" ''t''')$repair$,
 NULL);
\o
\echo '--- part 4: the mispaired foreign keys with quoted names enforce the wrong pairing'
SELECT 'regular: intended key refused: ' || fkp.probe($$INSERT INTO "odd ""s'ch"."Chi'ld ""R""" VALUES (1, 1, 2)$$);
SELECT 'temporal: intended key refused: ' || fkp.probe($$INSERT INTO "odd ""s'ch"."Chi'ld ""T""" VALUES (1, 1, 2, '[2021-01-01,2022-01-01)')$$);
SELECT 'regular: crossed key accepted and kept: ' || fkp.run($$INSERT INTO "odd ""s'ch"."Chi'ld ""R""" VALUES (9, 2, 1)$$);
SELECT 'temporal: crossed key accepted and kept: ' || fkp.run($$INSERT INTO "odd ""s'ch"."Chi'ld ""T""" VALUES (9, 2, 1, '[2021-01-01,2022-01-01)')$$);
\echo '--- part 4: check_integrity reports the regular foreign key with correctly quoted repair and scan'
SELECT CASE WHEN fkp.integrity() = report THEN 'reported exactly as expected' ELSE E'unexpected report:\n' || fkp.integrity() END
FROM fkp.expected_report WHERE fk = 'fk "odd" ''r''';
\o /dev/null
SELECT format('CREATE TEMP VIEW odd_scan AS %s', scan) FROM fkp.expected_report WHERE fk = 'fk "odd" ''r''' \gexec
\o
SELECT 'orphan: ' || s::text FROM pg_temp.odd_scan AS s;
\o /dev/null
DELETE FROM "odd ""s'ch"."Chi'ld ""R""" AS r USING pg_temp.odd_scan AS s WHERE r.id = s.id;
\o
SELECT 'repair fk "odd" ''r'': ' || fkp.run(repair) FROM fkp.expected_report WHERE fk = 'fk "odd" ''r''';
\echo '--- part 4: then the temporal foreign key'
SELECT CASE WHEN fkp.integrity() = report THEN 'reported exactly as expected' ELSE E'unexpected report:\n' || fkp.integrity() END
FROM fkp.expected_report WHERE fk = 'fk "odd" ''t''';
\o /dev/null
DELETE FROM "odd ""s'ch"."Chi'ld ""T""" WHERE id = 9;
\o
SELECT 'repair fk "odd" ''t'': ' || fkp.run(repair) FROM fkp.expected_report WHERE fk = 'fk "odd" ''t''';
SELECT 'integrity after both repairs: ' || fkp.integrity();
SELECT 'regular: intended key accepted: ' || fkp.probe($$INSERT INTO "odd ""s'ch"."Chi'ld ""R""" VALUES (1, 1, 2)$$);
SELECT 'regular: crossed key refused: ' || fkp.probe($$INSERT INTO "odd ""s'ch"."Chi'ld ""R""" VALUES (1, 2, 1)$$);
SELECT 'temporal: intended key accepted: ' || fkp.probe($$INSERT INTO "odd ""s'ch"."Chi'ld ""T""" VALUES (1, 1, 2, '[2021-01-01,2022-01-01)')$$);
SELECT 'temporal: crossed key refused: ' || fkp.probe($$INSERT INTO "odd ""s'ch"."Chi'ld ""T""" VALUES (1, 2, 1, '[2021-01-01,2022-01-01)')$$);
SELECT 'integrity at the end: ' || fkp.integrity();
ROLLBACK;

\o /dev/null
RESET search_path;
DROP SCHEMA "odd ""s'ch" CASCADE;
DROP SCHEMA fkp CASCADE;
\i sql/include/test_teardown.sql
RESET client_min_messages;
