\set QUIET on
SET client_min_messages = warning;
BEGIN;
CREATE TABLE g (id int, v text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('g', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until') \g /dev/null
CREATE TEMP TABLE src (row_id int, id int, v text, valid_from date, valid_until date);
-- entity 1 in source (covers 2021 of [2020,2023)); entity 2 not in source; entity 3 new
INSERT INTO src VALUES (1, 1, 'X', '2021-01-01', '2022-01-01'), (2, 3, 'N', '2021-01-01', '2022-01-01');
CREATE TEMP TABLE grid (planner text, mode text, delete_mode text, result text);
DO $$
DECLARE m text; d text; p text; r text;
BEGIN
  FOREACH p IN ARRAY ARRAY['native','plpgsql'] LOOP
  FOREACH m IN ARRAY enum_range(NULL::sql_saga.temporal_merge_mode)::text[] LOOP
  FOREACH d IN ARRAY enum_range(NULL::sql_saga.temporal_merge_delete_mode)::text[] LOOP
    PERFORM set_config('sql_saga.temporal_merge.use_plpgsql_planner', (p = 'plpgsql')::text, true);
    TRUNCATE g;
    INSERT INTO g (id, v, valid_from, valid_until) VALUES (1, 'a', '2020-01-01', '2023-01-01'), (2, 'z', '2020-01-01', '2023-01-01');
    BEGIN
      CALL sql_saga.temporal_merge(target_table => 'g', source_table => 'src', primary_identity_columns => ARRAY['id'], mode => m::sql_saga.temporal_merge_mode, delete_mode => d::sql_saga.temporal_merge_delete_mode);
      SELECT string_agg(format('%s:%s[%s,%s)', id, v, extract(year from valid_from), extract(year from valid_until)), ' ' ORDER BY id, valid_from) INTO r FROM g;
    EXCEPTION WHEN OTHERS THEN r := 'ERROR ' || SQLERRM;
    END;
    INSERT INTO grid VALUES (p, m, d, r);
  END LOOP; END LOOP; END LOOP;
END $$;
SELECT mode, delete_mode, max(result) FILTER (WHERE planner='native') AS native,
       CASE WHEN max(result) FILTER (WHERE planner='native') = max(result) FILTER (WHERE planner='plpgsql') THEN 'same' ELSE max(result) FILTER (WHERE planner='plpgsql') END AS plpgsql
FROM grid GROUP BY 1,2 ORDER BY 1,2;
ROLLBACK;
