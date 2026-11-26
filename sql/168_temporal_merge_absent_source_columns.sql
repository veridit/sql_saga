\set ECHO none
\set QUIET 1
SET client_min_messages = warning;
\i sql/include/test_setup.sql
-- A column the source table does not have says nothing: it keeps the target's value in every mode. A column the source
-- has and sets to NULL is a value (UPSERT, REPLACE) or keeps the target's value (PATCH). The native planner is the
-- reference; the PL/pgSQL planner gives the same result (hug y7hbe9).
CREATE SCHEMA ac_168;
SET search_path TO ac_168, public;
SET datestyle TO ISO, YMD;
CREATE TABLE unit (id int NOT NULL, code text, name text, region text, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('unit', 'valid_range') AS ignored \gset
SELECT sql_saga.add_unique_key('unit', ARRAY['id'], key_type => 'primary') AS ignored \gset
SELECT sql_saga.add_unique_key('unit', ARRAY['code'], key_type => 'natural') AS ignored \gset
-- Entity 1: region North then South. Each source renames it over [2023,2027), which also covers new time [2026,2027).
-- no_region lacks a data column; no_code lacks the natural key column; null_region has both, region NULL.
CREATE TABLE no_region (row_id int, id int, code text, name text, valid_range daterange);
INSERT INTO no_region VALUES (1, 1, 'A', 'Alpha2', '[2023-01-01,2027-01-01)');
CREATE TABLE no_code (row_id int, id int, name text, region text, valid_range daterange);
INSERT INTO no_code VALUES (1, 1, 'Alpha2', 'East', '[2023-01-01,2027-01-01)');
CREATE TABLE null_region (row_id int, id int, code text, name text, region text, valid_range daterange);
INSERT INTO null_region VALUES (1, 1, 'A', 'Alpha2', NULL, '[2023-01-01,2027-01-01)');
CREATE TABLE result (planner text, source text, mode text, pieces text);
DO $grid$
DECLARE
    v_planner text;
    v_source text;
    v_mode text;
    v_delete text;
    v_pieces text;
BEGIN
    FOREACH v_planner IN ARRAY ARRAY['native', 'plpgsql'] LOOP
        FOREACH v_source IN ARRAY ARRAY['no_region', 'no_code', 'null_region'] LOOP
            FOREACH v_mode IN ARRAY ARRAY['MERGE_ENTITY_UPSERT', 'MERGE_ENTITY_PATCH', 'MERGE_ENTITY_REPLACE',
                                          'UPDATE_FOR_PORTION_OF', 'PATCH_FOR_PORTION_OF', 'REPLACE_FOR_PORTION_OF',
                                          'MERGE_ENTITY_UPSERT+DELETE_MISSING_TIMELINE', 'MERGE_ENTITY_PATCH+DELETE_MISSING_TIMELINE',
                                          'MERGE_ENTITY_REPLACE+DELETE_MISSING_TIMELINE'] LOOP
                v_delete := COALESCE(split_part(v_mode, '+', 2), '');
                BEGIN
                    INSERT INTO unit VALUES (1, 'A', 'Alpha', 'North', '[2020-01-01,2024-01-01)'),
                                            (1, 'A', 'Alpha', 'South', '[2024-01-01,2026-01-01)');
                    PERFORM set_config('sql_saga.temporal_merge.use_plpgsql_planner', (v_planner = 'plpgsql')::text, true);
                    CALL sql_saga.temporal_merge(target_table => 'unit', source_table => v_source::regclass,
                                                 mode => split_part(v_mode, '+', 1)::sql_saga.temporal_merge_mode,
                                                 delete_mode => COALESCE(NULLIF(v_delete, ''), 'NONE')::sql_saga.temporal_merge_delete_mode);
                    SELECT string_agg(format('%s/%s/%s %s', u.code, u.name, COALESCE(u.region, '-'), u.valid_range), ', ' ORDER BY u.valid_range)
                    INTO v_pieces FROM unit AS u;
                    RAISE EXCEPTION 'undo the case';
                EXCEPTION WHEN OTHERS THEN
                    IF SQLERRM <> 'undo the case' THEN v_pieces := 'raised: ' || SQLERRM; END IF;
                END;
                INSERT INTO result VALUES (v_planner, v_source, v_mode, v_pieces);
            END LOOP;
        END LOOP;
    END LOOP;
END;
$grid$;
\set ECHO all
\set QUIET 0
\set VERBOSITY terse

\echo '==== The native planner (the reference): code/name/region per piece'
\echo '-- no_region: the source has no region column, so every mode keeps North and South; the new time has none.'
\echo '-- no_code: the source has no code column; the entity keeps code A everywhere, the new time included.'
\echo '-- null_region: region is NULL in the source, a value for UPSERT and REPLACE, kept by PATCH.'
SELECT source, mode, pieces FROM result WHERE planner = 'native' ORDER BY source, mode;

\echo '==== The PL/pgSQL planner gives the same result for every case (no rows)'
SELECT n.source, n.mode, n.pieces AS native, p.pieces AS plpgsql
FROM result AS n JOIN result AS p USING (source, mode)
WHERE n.planner = 'native' AND p.planner = 'plpgsql' AND n.pieces IS DISTINCT FROM p.pieces
ORDER BY n.source, n.mode;

\set ECHO none
\set QUIET 1
\set VERBOSITY default
DROP SCHEMA ac_168 CASCADE;
RESET search_path;
RESET datestyle;
\i sql/include/test_teardown.sql
