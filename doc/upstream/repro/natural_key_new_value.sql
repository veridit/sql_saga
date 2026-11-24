\set QUIET on
SET client_min_messages = warning;
CREATE TABLE tm2 (id int NOT NULL, code text, v text, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('tm2', 'valid_range') \g /dev/null
SELECT sql_saga.add_unique_key('tm2', ARRAY['id'], key_type => 'primary') \g /dev/null
SELECT sql_saga.add_unique_key('tm2', ARRAY['code'], key_type => 'natural') \g /dev/null
CREATE TEMP TABLE s2 (row_id int, id int, code text, v text, valid_range daterange);
CREATE PROCEDURE run2(p_planner text, p_id int, p_code text, p_mode text) LANGUAGE plpgsql AS $$
DECLARE r record;
BEGIN
  TRUNCATE tm2; TRUNCATE s2;
  INSERT INTO tm2 VALUES (1,'A','a','[2020-01-01,2030-01-01)');
  INSERT INTO s2 VALUES (1, p_id, p_code, 'new', '[2025-01-01,2026-01-01)');
  PERFORM set_config('sql_saga.temporal_merge.use_plpgsql_planner', (p_planner='plpgsql')::text, true);
  CALL sql_saga.temporal_merge(target_table => 'tm2', source_table => 's2', mode => p_mode::sql_saga.temporal_merge_mode);
  SELECT f.status, f.error_message INTO r FROM pg_temp.temporal_merge_feedback f;
  RAISE WARNING '% % id=% code=%: % % | %', p_planner, p_mode, p_id, p_code, r.status, r.error_message,
    (SELECT string_agg(format('%s/%s/%s %s', id, code, v, valid_range), '; ' ORDER BY valid_range) FROM tm2);
END $$;
BEGIN; CALL run2('native', 1, 'Z', 'PATCH_FOR_PORTION_OF'); ROLLBACK;
BEGIN; CALL run2('plpgsql', 1, 'Z', 'PATCH_FOR_PORTION_OF'); ROLLBACK;
BEGIN; CALL run2('native', 1, 'Z', 'MERGE_ENTITY_PATCH'); ROLLBACK;
BEGIN; CALL run2('plpgsql', 1, 'Z', 'MERGE_ENTITY_PATCH'); ROLLBACK;
