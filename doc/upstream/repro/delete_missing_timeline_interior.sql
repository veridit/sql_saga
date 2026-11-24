-- Bug rtwrp6: DELETE_MISSING_TIMELINE keeps target time beside interior source edges.
-- Run: psql -X -d <throwaway *_jhf db with sql_saga> -f doc/upstream/repro/delete_missing_timeline_interior.sql
-- Expected for every variant: only the source's time remains for entity 1.
--   D: X [2020-01-01,2021-06-01)   correct on 1.14
--   E: X [2020-03-01,2020-06-01)   1.14 keeps a [2020-01-01,2020-03-01) and a [2020-06-01,2021-01-01)
--   F: X [2020-06-01,2022-06-01)   1.14 keeps a [2020-01-01,2020-06-01) and b [2022-06-01,2023-01-01)
\set QUIET on
SET client_min_messages = warning;
BEGIN;
CREATE TABLE g (id int, v text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('g', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until') \g /dev/null
INSERT INTO g (id, v, valid_from, valid_until) VALUES (1, 'a', '2020-01-01', '2021-01-01'), (1, 'b', '2022-01-01', '2023-01-01');
CREATE TEMP TABLE src (row_id int, id int, v text, valid_from date, valid_until date);
SAVEPOINT p;
-- D: source starts at a target boundary, ends in the gap (069 shape)
INSERT INTO src VALUES (1, 1, 'X', '2020-01-01', '2021-06-01');
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_REPLACE', delete_mode => 'DELETE_MISSING_TIMELINE');
SELECT 'D aligned start' AS variant, id, v, valid_from, valid_until FROM g ORDER BY valid_from;
ROLLBACK TO p;
-- E: source inside first slice only
INSERT INTO src VALUES (1, 1, 'X', '2020-03-01', '2020-06-01');
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_REPLACE', delete_mode => 'DELETE_MISSING_TIMELINE');
SELECT 'E inside one slice' AS variant, id, v, valid_from, valid_until FROM g ORDER BY valid_from;
ROLLBACK TO p;
-- F: source spans the gap, both ends inside slices (my case)
INSERT INTO src VALUES (1, 1, 'X', '2020-06-01', '2022-06-01');
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_REPLACE', delete_mode => 'DELETE_MISSING_TIMELINE');
SELECT 'F spans gap' AS variant, id, v, valid_from, valid_until FROM g ORDER BY valid_from;
TABLE pg_temp.temporal_merge_plan;
ROLLBACK;
