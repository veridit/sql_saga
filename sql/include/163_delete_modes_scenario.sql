-- Run once per planner by sql/163_temporal_merge_delete_modes.sql. Each block resets the target, merges, and shows the
-- target. Target: entity 1 'a' over [2020,2023), entity 2 'z' over [2020,2023).
-- rtwrp6: DELETE_MISSING_TIMELINE removes every part of a source entity's timeline the source does not cover,
-- wherever the source edges fall (inside a target row, on a boundary, across a gap).
\echo '-- DMT, source inside one target row: only the source time remains'
TRUNCATE g; INSERT INTO g (id, v, valid_from, valid_until) VALUES (1, 'a', '2020-01-01', '2023-01-01'), (2, 'z', '2020-01-01', '2023-01-01');
TRUNCATE src; INSERT INTO src VALUES (1, 1, 'X', '2021-01-01', '2022-01-01');
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_REPLACE', delete_mode => 'DELETE_MISSING_TIMELINE');
SELECT id, v, valid_from, valid_until FROM g ORDER BY id, valid_from;
\echo '-- DMT, source starts on a boundary and ends inside a row'
TRUNCATE g; INSERT INTO g (id, v, valid_from, valid_until) VALUES (1, 'a', '2020-01-01', '2021-01-01'), (1, 'b', '2021-01-01', '2023-01-01');
TRUNCATE src; INSERT INTO src VALUES (1, 1, 'X', '2021-01-01', '2022-01-01');
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_REPLACE', delete_mode => 'DELETE_MISSING_TIMELINE');
SELECT id, v, valid_from, valid_until FROM g ORDER BY id, valid_from;
\echo '-- DMT, source spans a target gap with both edges inside rows'
TRUNCATE g; INSERT INTO g (id, v, valid_from, valid_until) VALUES (1, 'a', '2020-01-01', '2021-01-01'), (1, 'b', '2022-01-01', '2023-01-01');
TRUNCATE src; INSERT INTO src VALUES (1, 1, 'X', '2020-06-01', '2022-06-01');
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_REPLACE', delete_mode => 'DELETE_MISSING_TIMELINE');
SELECT id, v, valid_from, valid_until FROM g ORDER BY id, valid_from;
\echo '-- DMT, two source rows with a hole between them: the hole is deleted too'
TRUNCATE g; INSERT INTO g (id, v, valid_from, valid_until) VALUES (1, 'a', '2020-01-01', '2023-01-01');
TRUNCATE src; INSERT INTO src VALUES (1, 1, 'X', '2020-01-01', '2021-01-01'), (2, 1, 'Y', '2022-01-01', '2023-01-01');
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_REPLACE', delete_mode => 'DELETE_MISSING_TIMELINE');
SELECT id, v, valid_from, valid_until FROM g ORDER BY id, valid_from;
\echo '-- DMT with UPSERT and PATCH, source equal to the target values inside a row: nothing outside the source survives'
TRUNCATE g; INSERT INTO g (id, v, valid_from, valid_until) VALUES (1, 'a', '2020-01-01', '2023-01-01');
TRUNCATE src; INSERT INTO src VALUES (1, 1, 'a', '2021-01-01', '2022-01-01');
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_UPSERT', delete_mode => 'DELETE_MISSING_TIMELINE');
SELECT id, v, valid_from, valid_until FROM g ORDER BY id, valid_from;
TRUNCATE g; INSERT INTO g (id, v, valid_from, valid_until) VALUES (1, 'a', '2020-01-01', '2023-01-01');
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_PATCH', delete_mode => 'DELETE_MISSING_TIMELINE');
SELECT id, v, valid_from, valid_until FROM g ORDER BY id, valid_from;
\echo '-- DMT leaves entities absent from the source alone; DMTE also deletes them'
TRUNCATE g; INSERT INTO g (id, v, valid_from, valid_until) VALUES (1, 'a', '2020-01-01', '2023-01-01'), (2, 'z', '2020-01-01', '2023-01-01');
TRUNCATE src; INSERT INTO src VALUES (1, 1, 'X', '2021-01-01', '2022-01-01');
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_PATCH', delete_mode => 'DELETE_MISSING_TIMELINE_AND_ENTITIES');
SELECT id, v, valid_from, valid_until FROM g ORDER BY id, valid_from;

-- 2hxkwx: DELETE_MISSING_ENTITIES deletes the entities absent from the source with every MERGE_ENTITY_* mode.
\echo '-- DME with UPSERT, PATCH and REPLACE: entity 2 is deleted, entity 1 keeps its untouched time'
TRUNCATE g; INSERT INTO g (id, v, valid_from, valid_until) VALUES (1, 'a', '2020-01-01', '2023-01-01'), (2, 'z', '2020-01-01', '2023-01-01');
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_UPSERT', delete_mode => 'DELETE_MISSING_ENTITIES');
SELECT id, v, valid_from, valid_until FROM g ORDER BY id, valid_from;
TRUNCATE g; INSERT INTO g (id, v, valid_from, valid_until) VALUES (1, 'a', '2020-01-01', '2023-01-01'), (2, 'z', '2020-01-01', '2023-01-01');
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_PATCH', delete_mode => 'DELETE_MISSING_ENTITIES');
SELECT id, v, valid_from, valid_until FROM g ORDER BY id, valid_from;
TRUNCATE g; INSERT INTO g (id, v, valid_from, valid_until) VALUES (1, 'a', '2020-01-01', '2023-01-01'), (2, 'z', '2020-01-01', '2023-01-01');
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_REPLACE', delete_mode => 'DELETE_MISSING_ENTITIES');
SELECT id, v, valid_from, valid_until FROM g ORDER BY id, valid_from;
\echo '-- DMTE with UPSERT: entity 2 deleted, entity 1 reduced to the source time'
TRUNCATE g; INSERT INTO g (id, v, valid_from, valid_until) VALUES (1, 'a', '2020-01-01', '2023-01-01'), (2, 'z', '2020-01-01', '2023-01-01');
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_UPSERT', delete_mode => 'DELETE_MISSING_TIMELINE_AND_ENTITIES');
SELECT id, v, valid_from, valid_until FROM g ORDER BY id, valid_from;

-- Delete modes apply to the MERGE_ENTITY_* modes only; any other mode refuses them, and nothing changes.
\echo '-- the other modes refuse a delete mode'
TRUNCATE g; INSERT INTO g (id, v, valid_from, valid_until) VALUES (1, 'a', '2020-01-01', '2023-01-01'), (2, 'z', '2020-01-01', '2023-01-01');
SAVEPOINT refuse;
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'INSERT_NEW_ENTITIES', delete_mode => 'DELETE_MISSING_TIMELINE');
ROLLBACK TO SAVEPOINT refuse;
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'UPDATE_FOR_PORTION_OF', delete_mode => 'DELETE_MISSING_ENTITIES');
ROLLBACK TO SAVEPOINT refuse;
CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => 'DELETE_FOR_PORTION_OF', delete_mode => 'DELETE_MISSING_TIMELINE_AND_ENTITIES');
ROLLBACK TO SAVEPOINT refuse;
SELECT id, v, valid_from, valid_until FROM g ORDER BY id, valid_from;
