-- Bug: MERGE_ENTITY_UPSERT with DELETE_MISSING_ENTITIES does not delete entities absent from the source.
-- Run: psql -X -d <throwaway *_jhf db with sql_saga> -f doc/upstream/repro/delete_missing_entities_upsert.sql
-- Expected for both modes: entity 2 is deleted. 1.14: PATCH deletes it, UPSERT keeps it.
-- Cause: src/2k_temporal_merge_plan.sql scans the whole target only for mode IN ('MERGE_ENTITY_PATCH',
-- 'MERGE_ENTITY_REPLACE'); UPSERT falls into the source-entities-only scan, so it never sees entity 2.
\set QUIET on
SET client_min_messages = warning;
BEGIN;
CREATE TABLE g (id int, v text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('g', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until') \g /dev/null
INSERT INTO g (id, v, valid_from, valid_until) VALUES (1, 'a', '2020-01-01', '2021-01-01'), (2, 'z', '2020-01-01', '2021-01-01');
CREATE TEMP TABLE src (row_id int, id int, v text, valid_from date, valid_until date);
INSERT INTO src VALUES (1, 1, 'X', '2020-01-01', '2021-01-01');
SAVEPOINT p;
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_PATCH', delete_mode => 'DELETE_MISSING_ENTITIES');
SELECT 'PATCH' AS mode, id, v, valid_from, valid_until FROM g ORDER BY id, valid_from;
ROLLBACK TO p;
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_UPSERT', delete_mode => 'DELETE_MISSING_ENTITIES');
SELECT 'UPSERT' AS mode, id, v, valid_from, valid_until FROM g ORDER BY id, valid_from;
ROLLBACK;
