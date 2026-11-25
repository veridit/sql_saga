\set ECHO none
\set QUIET 1
SET client_min_messages = warning;
\i sql/include/test_setup.sql
-- A natural key holds one value per entity (hug 40nwvn). It may be NULL before it starts and after it is retired;
-- an identifier that changes is another natural key column, and the entity moves from one to the other by a row that
-- carries both (a handover). The schema enforces it with a guard per natural key column beside the 1.14 guard, and
-- temporal_merge refuses, per row, a natural key value other than the entity's. A merge writes a natural key value
-- only inside the source row's time: pieces outside it keep their stored value, also NULL. Both planners alike.
CREATE SCHEMA nk_166;
SET search_path TO nk_166, public;
SET datestyle TO ISO, YMD;
CREATE TABLE ho (id int NOT NULL, old_code text, new_code text, v text, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('ho', 'valid_range') AS ignored \gset
SELECT sql_saga.add_unique_key('ho', ARRAY['id'], key_type => 'primary') AS ignored \gset
SELECT sql_saga.add_unique_key('ho', ARRAY['old_code'], key_type => 'natural') AS ignored \gset
SELECT sql_saga.add_unique_key('ho', ARRAY['new_code'], key_type => 'natural') AS ignored \gset
\set ECHO all
\set QUIET 0
\set VERBOSITY terse

\echo '==== The schema: each natural key column is bound to the primary key both ways'
SELECT conname, pg_get_constraintdef(oid) AS definition
FROM pg_constraint WHERE conrelid = 'ho'::regclass AND contype = 'x' ORDER BY conname;
SELECT unique_key_name, pk_consistency_constraint_names FROM sql_saga.unique_keys
WHERE table_name = 'ho' ORDER BY unique_key_name;

\echo '-- one entity cannot hold two values of a natural key, at any time (refused):'
BEGIN;
SAVEPOINT two_values;
INSERT INTO ho VALUES (1, 'X', NULL, 'a', '[2020-01-01,2025-01-01)'), (1, 'Q', NULL, 'a', '[2025-01-01,2030-01-01)');
ROLLBACK TO SAVEPOINT two_values;
\echo '-- a value is NULL before the key starts and after it is retired (allowed):'
SAVEPOINT null_periods;
INSERT INTO ho VALUES (1, NULL, NULL, 'a', '[2018-01-01,2020-01-01)'), (1, 'X', NULL, 'a', '[2020-01-01,2025-01-01)'),
                      (1, NULL, 'Y', 'a', '[2025-01-01,2030-01-01)');
SELECT id, old_code, new_code, valid_range FROM ho ORDER BY valid_range;
ROLLBACK TO SAVEPOINT null_periods;
COMMIT;

CREATE TABLE src (row_id int, id int, old_code text, new_code text, v text, valid_range daterange);
CREATE TABLE grid_case (sid int, id int, old_code text, new_code text, v text, valid_range daterange, mode text);
INSERT INTO grid_case VALUES
    -- the handover: a row carrying the old key and the new one, from 2025
    (1, NULL, 'X', 'Y', 'a', '[2025-01-01,2030-01-01)', 'PATCH_FOR_PORTION_OF'),
    (2, NULL, 'X', 'Y', 'b', '[2025-01-01,2030-01-01)', 'PATCH_FOR_PORTION_OF'),
    (3, NULL, 'X', 'Y', 'b', '[2025-01-01,2030-01-01)', 'MERGE_ENTITY_PATCH'),
    (4, NULL, 'X', 'Y', 'b', '[2025-01-01,2030-01-01)', 'MERGE_ENTITY_REPLACE'),
    -- the new key given with the identity, for part of the time: written there only
    (5, 1, NULL, 'Y', 'b', '[2025-01-01,2026-01-01)', 'PATCH_FOR_PORTION_OF'),
    (6, 1, NULL, 'Y', 'b', '[2025-01-01,2026-01-01)', 'MERGE_ENTITY_PATCH'),
    (7, 1, NULL, 'Y', 'b', '[2025-01-01,2026-01-01)', 'MERGE_ENTITY_UPSERT'),
    -- the identity alone: natural keys untouched
    (8, 1, NULL, NULL, 'b', '[2024-01-01,2026-01-01)', 'PATCH_FOR_PORTION_OF'),
    (9, 1, NULL, NULL, 'b', '[2024-01-01,2026-01-01)', 'MERGE_ENTITY_UPSERT'),
    (10, 1, NULL, NULL, 'b', '[2024-01-01,2026-01-01)', 'MERGE_ENTITY_REPLACE'),
    -- another value of a key the entity holds: refused
    (11, 1, 'Q', NULL, 'b', '[2025-01-01,2026-01-01)', 'PATCH_FOR_PORTION_OF'),
    (12, 1, 'Q', NULL, 'b', '[2025-01-01,2026-01-01)', 'MERGE_ENTITY_PATCH'),
    -- a key value only the source gives, found by the other key: a handover for part of the time
    (13, NULL, 'X', 'Q', 'b', '[2025-01-01,2026-01-01)', 'MERGE_ENTITY_PATCH'),
    -- time before the stored key starts: the new piece takes the source's values, like any column
    (14, 1, NULL, NULL, 'b', '[2018-01-01,2021-01-01)', 'MERGE_ENTITY_PATCH'),
    (15, NULL, 'X', NULL, 'b', '[2018-01-01,2021-01-01)', 'MERGE_ENTITY_PATCH');

\echo '==== Target: id 1, old_code X, new_code NULL, v a, [2020,2030). One source row per case, both planners.'
CREATE TEMP TABLE grid_out (planner text, sid int, status text, error text, result text);
DO $grid$
DECLARE
    v_planner text;
    v_case record;
    v_status text;
    v_error text;
    v_result text;
BEGIN
    FOREACH v_planner IN ARRAY ARRAY['native', 'plpgsql'] LOOP
        FOR v_case IN SELECT * FROM grid_case ORDER BY sid LOOP
            BEGIN
                INSERT INTO ho VALUES (1, 'X', NULL, 'a', '[2020-01-01,2030-01-01)');
                INSERT INTO src VALUES (v_case.sid, v_case.id, v_case.old_code, v_case.new_code, v_case.v, v_case.valid_range);
                PERFORM set_config('sql_saga.temporal_merge.use_plpgsql_planner', (v_planner = 'plpgsql')::text, true);
                CALL sql_saga.temporal_merge(target_table => 'ho', source_table => 'src',
                                             mode => v_case.mode::sql_saga.temporal_merge_mode);
                SELECT f.status::text, f.error_message INTO v_status, v_error FROM pg_temp.temporal_merge_feedback AS f;
                SELECT string_agg(format('%s/%s/%s %s', COALESCE(h.old_code, '-'), COALESCE(h.new_code, '-'), h.v,
                                         h.valid_range), ' ' ORDER BY h.valid_range)
                INTO v_result FROM ho AS h;
                RAISE EXCEPTION 'undo the case';
            EXCEPTION WHEN OTHERS THEN
                IF SQLERRM <> 'undo the case' THEN
                    v_status := 'raised';
                    v_error := SQLERRM;
                    v_result := NULL;
                END IF;
            END;
            INSERT INTO grid_out VALUES (v_planner, v_case.sid, v_status, v_error, v_result);
        END LOOP;
    END LOOP;
END;
$grid$;

\echo '-- The native planner (the default):'
SELECT o.sid, c.mode, format('id=%s old=%s new=%s v=%s %s', COALESCE(c.id::text, '-'), COALESCE(c.old_code, '-'),
       COALESCE(c.new_code, '-'), c.v, c.valid_range) AS source, o.status, o.error, o.result
FROM grid_out AS o JOIN grid_case AS c USING (sid)
WHERE o.planner = 'native' ORDER BY o.sid;

\echo '-- The PL/pgSQL planner gives the same result for every case (no rows):'
SELECT n.sid, n.status AS native, p.status AS plpgsql, n.error AS native_error, p.error AS plpgsql_error,
       n.result AS native_result, p.result AS plpgsql_result
FROM grid_out AS n JOIN grid_out AS p USING (sid)
WHERE n.planner = 'native' AND p.planner = 'plpgsql'
  AND (n.status, n.error, n.result) IS DISTINCT FROM (p.status, p.error, p.result)
ORDER BY n.sid;

\echo '==== Two rows of one batch giving the entity two values of a key: both refused, nothing written'
BEGIN;
INSERT INTO ho VALUES (1, 'X', NULL, 'a', '[2020-01-01,2030-01-01)');
INSERT INTO src VALUES (1, 1, NULL, 'Y', 'b', '[2025-01-01,2026-01-01)'), (2, 1, NULL, 'Z', 'c', '[2027-01-01,2028-01-01)');
CALL sql_saga.temporal_merge(target_table => 'ho', source_table => 'src', mode => 'MERGE_ENTITY_PATCH');
SELECT source_row_id, status, error_message FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;
TABLE ho;
ROLLBACK;
BEGIN;
SET LOCAL sql_saga.temporal_merge.use_plpgsql_planner = true;
INSERT INTO ho VALUES (1, 'X', NULL, 'a', '[2020-01-01,2030-01-01)');
INSERT INTO src VALUES (1, 1, NULL, 'Y', 'b', '[2025-01-01,2026-01-01)'), (2, 1, NULL, 'Z', 'c', '[2027-01-01,2028-01-01)');
CALL sql_saga.temporal_merge(target_table => 'ho', source_table => 'src', mode => 'MERGE_ENTITY_PATCH');
SELECT source_row_id, status, error_message FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;
TABLE ho;
ROLLBACK;

\echo '==== An entity found by a key its earlier pieces do not hold: the planner sees the whole entity'
\echo '-- id 1: old_code X over [2020,2030), new_code W from 2025; the source names only W, from 2021. Both planners name'
\echo '-- one entity in the feedback, with its key values, also for the piece before W starts.'
BEGIN;
INSERT INTO ho VALUES (1, 'X', NULL, 'a', '[2020-01-01,2025-01-01)'), (1, 'X', 'W', 'a', '[2025-01-01,2030-01-01)');
INSERT INTO src VALUES (1, NULL, NULL, 'W', 'c', '[2021-01-01,2026-01-01)');
CALL sql_saga.temporal_merge(target_table => 'ho', source_table => 'src', mode => 'PATCH_FOR_PORTION_OF');
SELECT source_row_id, status, target_entity_keys, error_message FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;
TABLE ho ORDER BY valid_range;
ROLLBACK;
BEGIN;
SET LOCAL sql_saga.temporal_merge.use_plpgsql_planner = true;
INSERT INTO ho VALUES (1, 'X', NULL, 'a', '[2020-01-01,2025-01-01)'), (1, 'X', 'W', 'a', '[2025-01-01,2030-01-01)');
INSERT INTO src VALUES (1, NULL, NULL, 'W', 'c', '[2021-01-01,2026-01-01)');
CALL sql_saga.temporal_merge(target_table => 'ho', source_table => 'src', mode => 'PATCH_FOR_PORTION_OF');
SELECT source_row_id, status, target_entity_keys, error_message FROM pg_temp.temporal_merge_feedback ORDER BY source_row_id;
TABLE ho ORDER BY valid_range;
ROLLBACK;

\echo '==== check_integrity reports a natural key whose reverse guard is missing'
\echo '-- the state 1.14 left (the reverse guard did not exist), bypassing the event triggers as a restore would:'
BEGIN;
SET LOCAL session_replication_role = replica;
ALTER TABLE ho DROP CONSTRAINT ho_new_code_valid_nk_consistency_excl;
UPDATE sql_saga.unique_keys SET pk_consistency_constraint_names = '{ho_new_code_valid_pk_consistency_excl}'
WHERE unique_key_name = 'ho_new_code_valid';
RESET session_replication_role;
SAVEPOINT report;
SELECT sql_saga.check_integrity();
ROLLBACK TO SAVEPOINT report;
ROLLBACK;

\echo '==== An XOR natural key (lu or es): both planners give the same writes and feedback'
\echo '-- id 1 lu 10, id 2 es 10, id 3 lu 20 from 2025. Rows: by lu, by es, by lu over time before lu 20 starts, a new'
\echo '-- entity by es, an identity whose es belongs to another entity (ambiguous), the identity alone, and rows giving'
\echo '-- both lu and es (hug bjd8v7): each gets its own ERROR, the rest of the batch is applied, the table keeps its CHECK.'
CREATE TABLE xk (id int NOT NULL DEFAULT 100, lu int, es int, v text, valid_range daterange NOT NULL, CHECK (num_nonnulls(lu, es) <= 1));
SELECT sql_saga.add_era('xk', 'valid_range') AS ignored \gset
SELECT sql_saga.add_unique_key('xk', ARRAY['id'], key_type => 'primary') AS ignored \gset
SELECT sql_saga.add_unique_key('xk', ARRAY['lu', 'es'], key_type => 'natural', mutually_exclusive_columns => ARRAY['lu', 'es']) AS ignored \gset
CREATE TABLE xk_src (row_id int, id int, lu int, es int, v text, valid_range daterange);
INSERT INTO xk_src VALUES (1, NULL, 10, NULL, 'lu10', '[2022-01-01,2023-01-01)'), (2, NULL, NULL, 10, 'es10', '[2022-01-01,2023-01-01)'),
    (3, NULL, 20, NULL, 'lu20', '[2021-01-01,2026-01-01)'), (4, NULL, NULL, 30, 'new', '[2022-01-01,2023-01-01)'),
    (5, 1, NULL, 10, 'conflict', '[2024-01-01,2025-01-01)'), (6, 2, NULL, NULL, 'byid', '[2026-01-01,2027-01-01)'),
    (7, NULL, 10, 10, 'both', '[2022-01-01,2023-01-01)'), (8, NULL, 40, 40, 'both new', '[2022-01-01,2023-01-01)'),
    (9, 3, 20, 50, 'both by id', '[2026-01-01,2027-01-01)');
CREATE TABLE xk_out (planner text, feedback jsonb, result jsonb);
DO $xk$
DECLARE
    v_planner text;
    v_feedback jsonb;
    v_result jsonb;
BEGIN
    FOREACH v_planner IN ARRAY ARRAY['native', 'plpgsql'] LOOP
        BEGIN
            INSERT INTO xk VALUES (1, 10, NULL, 'a', '[2020-01-01,2030-01-01)'), (2, NULL, 10, 'a', '[2020-01-01,2030-01-01)'),
                                  (3, NULL, NULL, 'a', '[2020-01-01,2025-01-01)'), (3, 20, NULL, 'a', '[2025-01-01,2030-01-01)');
            PERFORM set_config('sql_saga.temporal_merge.use_plpgsql_planner', (v_planner = 'plpgsql')::text, true);
            CALL sql_saga.temporal_merge(target_table => 'xk', source_table => 'xk_src', mode => 'MERGE_ENTITY_PATCH');
            SELECT jsonb_agg(jsonb_build_object('row', f.source_row_id, 'status', f.status, 'keys', f.target_entity_keys, 'error', f.error_message) ORDER BY f.source_row_id)
            INTO v_feedback FROM pg_temp.temporal_merge_feedback AS f;
            SELECT jsonb_agg(to_jsonb(x) ORDER BY x.id, x.valid_range) INTO v_result FROM xk AS x;
            RAISE EXCEPTION 'undo the case';
        EXCEPTION WHEN OTHERS THEN
            IF SQLERRM <> 'undo the case' THEN RAISE; END IF;
        END;
        INSERT INTO xk_out VALUES (v_planner, v_feedback, v_result);
    END LOOP;
END;
$xk$;
\echo '-- The native planner:'
SELECT (f->>'row')::int AS row_id, f->>'status' AS status, f->'keys' AS target_entity_keys, f->>'error' AS error
FROM xk_out AS o, jsonb_array_elements(o.feedback) AS f WHERE o.planner = 'native' ORDER BY 1;
SELECT x.id, x.lu, x.es, x.v, x.valid_range
FROM xk_out AS o, jsonb_populate_recordset(NULL::xk, o.result) AS x WHERE o.planner = 'native' ORDER BY x.id, x.valid_range;
\echo '-- The PL/pgSQL planner gives the same feedback and table:'
SELECT n.feedback = p.feedback AS same_feedback, n.result = p.result AS same_table
FROM xk_out AS n, xk_out AS p WHERE n.planner = 'native' AND p.planner = 'plpgsql';

\set ECHO none
\set QUIET 1
\set VERBOSITY default
DROP SCHEMA nk_166 CASCADE;
RESET search_path;
RESET datestyle;
\i sql/include/test_teardown.sql
