-- The running example of doc/upstream/temporal-dml.md, executed with sql_saga's temporal_merge.
-- Every result table in that document is the output of this script (temporal_dml_examples.out).
-- Run in a throwaway database: psql -X -d DB -f temporal_dml_examples.sql
\set ON_ERROR_STOP 1
SET client_min_messages = warning;
SET datestyle = 'ISO';
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS sql_saga;
DROP SCHEMA IF EXISTS dml CASCADE;
CREATE SCHEMA dml;
SET search_path = dml, public;

-- unit: an entity with an identity (id), a natural key (code), two values and an audit column.
CREATE SEQUENCE unit_id_seq START 10;
CREATE TABLE unit (id int NOT NULL DEFAULT nextval('unit_id_seq'), code text, name text, region text, edit_by text,
                   valid_at daterange NOT NULL);
SELECT sql_saga.add_era('unit', 'valid_at', synchronize_columns => false, ephemeral_columns => ARRAY['edit_by']) AS ok \gset
SELECT sql_saga.add_unique_key('unit', ARRAY['id'], key_type => 'primary') AS ok \gset
SELECT sql_saga.add_unique_key('unit', ARRAY['code'], key_type => 'natural') AS ok \gset
CREATE TABLE import (row_id int, id int, code text, name text, region text, edit_by text, valid_at daterange);

-- The target before every example.
CREATE PROCEDURE reset() LANGUAGE sql AS $$
    TRUNCATE unit;
    SELECT setval('unit_id_seq', 10, false);
    INSERT INTO unit (id, code, name, region, edit_by, valid_at) VALUES
        (1, 'A', 'Alpha', 'North', 'load', '[2020-01-01,2024-01-01)'),
        (1, 'A', 'Alpha', 'South', 'load', '[2024-01-01,2026-01-01)'),
        (1, 'A', 'Alpha', 'South', 'load', '[2027-01-01,infinity)'),
        (2, 'B', 'Beta',  'North', 'load', '[2020-01-01,infinity)');
$$;
-- The import of most examples: entity 1 renamed over [2023,2027) with no region; a new entity C from 2025.
CREATE PROCEDURE import_default() LANGUAGE sql AS $$
    TRUNCATE import;
    INSERT INTO import VALUES
        (1, 1,    'A', 'Alpha2', NULL,   'imp', '[2023-01-01,2027-01-01)'),
        (2, NULL, 'C', 'Gamma',  'West', 'imp', '[2025-01-01,infinity)');
$$;
CREATE VIEW timeline AS
SELECT id, code, name, region, edit_by, lower(valid_at) AS valid_from, upper(valid_at) AS valid_until FROM unit;
-- After each call: the feedback per import row, and the timeline.
\set feedback 'SELECT source_row_id AS row_id, status, target_entity_keys AS entity FROM pg_temp.temporal_merge_feedback ORDER BY 1;'

\echo '==== The target timeline'
CALL reset();
TABLE timeline ORDER BY id, valid_from;
\echo '==== The import'
CALL import_default();
TABLE import ORDER BY row_id;

\echo '==== E1 MERGE_ENTITY_UPSERT: matched time updated (NULL is a value), gap and new entity inserted'
BEGIN; CALL reset(); CALL import_default();
CALL sql_saga.temporal_merge('unit', 'import', mode => 'MERGE_ENTITY_UPSERT');
:feedback TABLE timeline ORDER BY id, valid_from; ROLLBACK;

\echo '==== E2 MERGE_ENTITY_PATCH: as E1, but a NULL keeps the value at each point'
BEGIN; CALL reset(); CALL import_default();
CALL sql_saga.temporal_merge('unit', 'import', mode => 'MERGE_ENTITY_PATCH');
:feedback TABLE timeline ORDER BY id, valid_from; ROLLBACK;

\echo '==== E3 UPDATE_FOR_PORTION_OF: matched time only; the gap and the new entity are skipped'
BEGIN; CALL reset(); CALL import_default();
CALL sql_saga.temporal_merge('unit', 'import', mode => 'UPDATE_FOR_PORTION_OF');
:feedback TABLE timeline ORDER BY id, valid_from; ROLLBACK;

\echo '==== E4 PATCH_FOR_PORTION_OF'
BEGIN; CALL reset(); CALL import_default();
CALL sql_saga.temporal_merge('unit', 'import', mode => 'PATCH_FOR_PORTION_OF');
:feedback TABLE timeline ORDER BY id, valid_from; ROLLBACK;

\echo '==== E5 DELETE_FOR_PORTION_OF'
BEGIN; CALL reset(); CALL import_default();
CALL sql_saga.temporal_merge('unit', 'import', mode => 'DELETE_FOR_PORTION_OF');
:feedback TABLE timeline ORDER BY id, valid_from; ROLLBACK;

\echo '==== E6 INSERT_NEW_ENTITIES: only an entity the target does not have'
BEGIN; CALL reset(); CALL import_default();
CALL sql_saga.temporal_merge('unit', 'import', mode => 'INSERT_NEW_ENTITIES');
:feedback TABLE timeline ORDER BY id, valid_from; ROLLBACK;

\echo '==== E7 MERGE_ENTITY_PATCH + DELETE_MISSING_TIMELINE: the import states the whole timeline of entity 1'
BEGIN; CALL reset(); CALL import_default();
CALL sql_saga.temporal_merge('unit', 'import', mode => 'MERGE_ENTITY_PATCH', delete_mode => 'DELETE_MISSING_TIMELINE');
:feedback TABLE timeline ORDER BY id, valid_from; ROLLBACK;

\echo '==== E8 MERGE_ENTITY_PATCH + DELETE_MISSING_ENTITIES: entities the import does not name are removed'
BEGIN; CALL reset(); CALL import_default();
CALL sql_saga.temporal_merge('unit', 'import', mode => 'MERGE_ENTITY_PATCH', delete_mode => 'DELETE_MISSING_ENTITIES');
:feedback TABLE timeline ORDER BY id, valid_from; ROLLBACK;

\echo '==== E9 the whole timeline of entity 1: region West at every point (PATCH_FOR_PORTION_OF over all time)'
BEGIN; CALL reset(); TRUNCATE import;
INSERT INTO import VALUES (1, 1, 'A', NULL, 'West', 'imp', '[-infinity,infinity)');
CALL sql_saga.temporal_merge('unit', 'import', mode => 'PATCH_FOR_PORTION_OF');
TABLE timeline ORDER BY id, valid_from; ROLLBACK;

\echo '==== E10 an entity named by its natural key only'
BEGIN; CALL reset(); TRUNCATE import;
INSERT INTO import VALUES (1, NULL, 'B', NULL, 'East', 'imp', '[2022-01-01,2023-01-01)');
CALL sql_saga.temporal_merge('unit', 'import', mode => 'PATCH_FOR_PORTION_OF');
:feedback TABLE timeline ORDER BY id, valid_from; ROLLBACK;

\echo '==== E11 one new entity over two import rows: one generated id'
BEGIN; CALL reset(); TRUNCATE import;
INSERT INTO import VALUES (1, NULL, 'D', 'Delta', 'East', 'imp', '[2020-01-01,2022-01-01)'),
                          (2, NULL, 'D', 'Delta', 'West', 'imp', '[2022-01-01,infinity)');
CALL sql_saga.temporal_merge('unit', 'import', mode => 'MERGE_ENTITY_PATCH');
:feedback TABLE timeline ORDER BY id, valid_from; ROLLBACK;

\echo '==== E12 two import rows overlapping in time for one entity: the later row wins where they overlap'
BEGIN; CALL reset(); TRUNCATE import;
INSERT INTO import VALUES (1, 2, 'B', 'One', 'East', 'imp', '[2021-01-01,2023-01-01)'),
                          (2, 2, 'B', 'Two', NULL,   'imp', '[2022-01-01,2024-01-01)');
CALL sql_saga.temporal_merge('unit', 'import', mode => 'MERGE_ENTITY_PATCH');
TABLE timeline ORDER BY id, valid_from; ROLLBACK;

\echo '==== E13 an import identical to the target: nothing written'
BEGIN; CALL reset(); TRUNCATE import;
INSERT INTO import VALUES (1, 2, 'B', 'Beta', 'North', 'imp', '[2021-01-01,2022-01-01)');
CALL sql_saga.temporal_merge('unit', 'import', mode => 'MERGE_ENTITY_PATCH');
:feedback ROLLBACK;

\echo '==== E14 contradicting keys: identity 2 with the natural key of entity 1'
BEGIN; CALL reset(); TRUNCATE import;
INSERT INTO import VALUES (1, 2, 'A', 'Who', NULL, 'imp', '[2022-01-01,2023-01-01)');
CALL sql_saga.temporal_merge('unit', 'import', mode => 'MERGE_ENTITY_PATCH');
SELECT source_row_id AS row_id, status, error_message FROM pg_temp.temporal_merge_feedback;
ROLLBACK;

DROP SCHEMA dml CASCADE;
