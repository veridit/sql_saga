-- MERGE_ENTITY_REPLACE / REPLACE_FOR_PORTION_OF with a target column the source does not have (hug: see task).
-- The PL/pgSQL planner sets it to NULL (replace: the source states every column); the native planner keeps the
-- target value, as UPSERT does. Run in a throwaway database with sql_saga 1.15.
SET client_min_messages = warning;
CREATE TABLE unit (id int NOT NULL, code text, name text, region text, valid_at daterange NOT NULL);
SELECT sql_saga.add_era('unit', 'valid_at', synchronize_columns => false);
SELECT sql_saga.add_unique_key('unit', ARRAY['id'], key_type => 'primary');
INSERT INTO unit VALUES (1, 'A', 'Alpha', 'North', '[2020-01-01,2024-01-01)'), (1, 'A', 'Alpha', 'South', '[2024-01-01,2026-01-01)');
CREATE TABLE src (row_id int, id int, name text, valid_at daterange);
INSERT INTO src VALUES (1, 1, 'Alpha2', '[2023-01-01,2027-01-01)');
BEGIN;
CALL sql_saga.temporal_merge(target_table => 'unit', source_table => 'src', mode => 'MERGE_ENTITY_REPLACE');
TABLE unit ORDER BY valid_at;  -- native: region North / South / NULL (new time) kept per piece
ROLLBACK;
BEGIN;
SET LOCAL sql_saga.temporal_merge.use_plpgsql_planner = true;
CALL sql_saga.temporal_merge(target_table => 'unit', source_table => 'src', mode => 'MERGE_ENTITY_REPLACE');
TABLE unit ORDER BY valid_at;  -- PL/pgSQL: one piece 2023-2027, region NULL
ROLLBACK;
