\i sql/include/test_setup.sql

BEGIN;
SET client_min_messages TO WARNING;

\echo '-- A source row names its entity by its identity, by a natural key, or by both (hug mfky60). Both planners resolve'
\echo '-- it by every key it names: keys that find different entities, or an identity that finds nothing while a natural'
\echo '-- key finds an entity, make the row an ERROR in its feedback, never a write to a guessed entity.'
CREATE TABLE tm (id int NOT NULL, code text, v text, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('tm', 'valid_range') AS ignored \gset
SELECT sql_saga.add_unique_key('tm', ARRAY['id'], key_type => 'primary') AS ignored \gset
SELECT sql_saga.add_unique_key('tm', ARRAY['code'], key_type => 'natural') AS ignored \gset

CREATE TEMP TABLE grid_case (sid int, id int, code text);
INSERT INTO grid_case VALUES
    (1, 1, NULL),     -- identity of entity 1 only
    (2, NULL, 'B'),   -- natural key of entity 2 only
    (3, 3, 'C'),      -- both, naming entity 3
    (4, 1, 'B'),      -- identity of entity 1, natural key of entity 2
    (5, 9, NULL),     -- a new identity
    (6, NULL, 'Z'),   -- a new natural key and no identity (id has no default)
    (7, 9, 'Z'),      -- a new identity and a new natural key
    (9, 9, 'B');      -- a new identity with the natural key of entity 2
CREATE TEMP TABLE grid_out (planner text, mode text, sid int, status text, written text, error text);

DO $grid$
DECLARE
    v_planner text;
    v_mode text;
    v_case record;
    v_status text;
    v_written text;
    v_error text;
BEGIN
    FOREACH v_planner IN ARRAY ARRAY['native', 'plpgsql'] LOOP
        FOREACH v_mode IN ARRAY ARRAY['PATCH_FOR_PORTION_OF', 'MERGE_ENTITY_PATCH', 'INSERT_NEW_ENTITIES'] LOOP
            FOR v_case IN SELECT * FROM grid_case ORDER BY sid LOOP
                BEGIN
                    INSERT INTO tm VALUES (1, 'A', 'a', '[2020-01-01,2030-01-01)'), (2, 'B', 'b', '[2020-01-01,2030-01-01)'),
                                          (3, 'C', 'c', '[2020-01-01,2030-01-01)');
                    CREATE TEMP TABLE src (row_id int, id int, code text, v text, valid_range daterange);
                    INSERT INTO src VALUES (v_case.sid, v_case.id, v_case.code, 'new', '[2025-01-01,2026-01-01)');
                    PERFORM set_config('sql_saga.temporal_merge.use_plpgsql_planner', (v_planner = 'plpgsql')::text, true);
                    CALL sql_saga.temporal_merge(target_table => 'tm', source_table => 'src', mode => v_mode::sql_saga.temporal_merge_mode);
                    SELECT f.status::text, f.error_message INTO v_status, v_error FROM pg_temp.temporal_merge_feedback AS f;
                    SELECT string_agg(format('%s/%s', t.id, t.code), ' ' ORDER BY t.id) INTO v_written FROM tm AS t WHERE t.v = 'new';
                    RAISE EXCEPTION 'undo the case';
                EXCEPTION WHEN OTHERS THEN
                    IF SQLERRM <> 'undo the case' THEN
                        v_status := 'raised';
                        v_written := NULL;
                        v_error := SQLERRM;
                    END IF;
                END;
                INSERT INTO grid_out VALUES (v_planner, v_mode, v_case.sid, v_status, v_written, v_error);
            END LOOP;
        END LOOP;
    END LOOP;
END;
$grid$;

\echo '-- The native planner (the default):'
SELECT o.mode, o.sid, format('id=%s code=%s', COALESCE(c.id::text, '-'), COALESCE(c.code, '-')) AS source,
       o.status, o.written, o.error
FROM grid_out AS o JOIN grid_case AS c USING (sid)
WHERE o.planner = 'native'
ORDER BY o.mode DESC, o.sid;

\echo '-- The PL/pgSQL planner gives the same result for every case (no rows):'
SELECT n.mode, n.sid, n.status AS native, p.status AS plpgsql, n.written AS native_written, p.written AS plpgsql_written,
       n.error AS native_error, p.error AS plpgsql_error
FROM grid_out AS n JOIN grid_out AS p USING (mode, sid)
WHERE n.planner = 'native' AND p.planner = 'plpgsql'
  AND (n.status, n.written, n.error) IS DISTINCT FROM (p.status, p.written, p.error)
ORDER BY n.mode, n.sid;

ROLLBACK;

\i sql/include/test_teardown.sql
