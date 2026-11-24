\set QUIET on
SET client_min_messages = notice;
CREATE TABLE bl (id int NOT NULL, code text, v text, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('bl', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until') \g /dev/null
SELECT sql_saga.add_unique_key('bl', ARRAY['id'], key_type => 'primary') \g /dev/null
SELECT sql_saga.add_unique_key('bl', ARRAY['code'], key_type => 'natural') \g /dev/null
INSERT INTO bl (id, code, v, valid_from, valid_until) SELECT g, 'c'||g, 'v', '2020-01-01', '2030-01-01' FROM generate_series(1, 200000) g;
ANALYZE bl;
CREATE TEMP TABLE src (row_id int, id int, code text, v text, valid_from date, valid_until date);
INSERT INTO src SELECT g, g*7 % 200000 + 1, 'c'||(g*7 % 200000 + 1), 'x', '2025-01-01', '2026-01-01' FROM generate_series(1, 1000) g;
ANALYZE src;
SET sql_saga.log_step_timing = 'on';
BEGIN;
CALL sql_saga.temporal_merge('bl', 'src', ARRAY['id'], mode => 'MERGE_ENTITY_UPSERT');
ROLLBACK;
BEGIN;
CALL sql_saga.temporal_merge('bl', 'src', ARRAY['id'], mode => 'MERGE_ENTITY_UPSERT');
ROLLBACK;
