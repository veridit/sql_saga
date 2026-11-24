\set QUIET on
SET client_min_messages = warning;
CREATE TABLE tv (id int NOT NULL, code text, v text, valid_range daterange NOT NULL);
SELECT sql_saga.add_era('tv', 'valid_range') \g /dev/null
SELECT sql_saga.add_unique_key('tv', ARRAY['id'], key_type => 'primary') \g /dev/null
SELECT sql_saga.add_unique_key('tv', ARRAY['code'], key_type => 'natural') \g /dev/null
CREATE TABLE tv_out (planner text, mode text, src text, status text, tbl text);
CREATE PROCEDURE tv_run() LANGUAGE plpgsql AS $p$
DECLARE pl text; m text; c record; st text; tb text;
BEGIN
 FOREACH pl IN ARRAY ARRAY['native','plpgsql'] LOOP
  FOREACH m IN ARRAY ARRAY['PATCH_FOR_PORTION_OF','MERGE_ENTITY_PATCH','MERGE_ENTITY_REPLACE'] LOOP
   FOR c IN SELECT * FROM (VALUES (1,NULL::text),(NULL,'A'),(NULL,'Q'),(1,'A'),(1,'Q'),(1,'Z')) v(id,code) LOOP
    BEGIN
     DELETE FROM tv;
     INSERT INTO tv VALUES (1,'A','a','[2020-01-01,2025-01-01)'),(1,'Q','a','[2025-01-01,2030-01-01)');
     DROP TABLE IF EXISTS s; CREATE TEMP TABLE s (row_id int, id int, code text, v text, valid_range daterange);
     INSERT INTO s VALUES (1, c.id, c.code, 'new', '[2024-01-01,2026-01-01)');
     PERFORM set_config('sql_saga.temporal_merge.use_plpgsql_planner', (pl='plpgsql')::text, true);
     CALL sql_saga.temporal_merge(target_table => 'tv', source_table => 's', mode => m::sql_saga.temporal_merge_mode);
     SELECT f.status::text INTO st FROM pg_temp.temporal_merge_feedback f;
     SELECT string_agg(format('%s/%s %s-%s', t.code, t.v, to_char(lower(t.valid_range),'YY'), to_char(upper(t.valid_range),'YY')), ' ' ORDER BY t.valid_range) INTO tb FROM tv t;
     RAISE EXCEPTION 'rollback';
    EXCEPTION WHEN OTHERS THEN
     IF SQLERRM <> 'rollback' THEN st := 'EXC '||left(SQLERRM,50); tb := NULL; END IF;
    END;
    INSERT INTO tv_out VALUES (pl, m, format('id=%s code=%s', coalesce(c.id::text,'-'), coalesce(c.code,'-')), st, tb);
   END LOOP;
  END LOOP;
 END LOOP;
END $p$;
CALL tv_run();
SELECT n.mode, n.src, n.status, n.tbl AS native, CASE WHEN p.tbl IS DISTINCT FROM n.tbl OR p.status<>n.status THEN p.status||' '||coalesce(p.tbl,'') ELSE '=' END AS plpgsql
FROM tv_out n JOIN tv_out p USING (mode, src) WHERE n.planner='native' AND p.planner='plpgsql' ORDER BY n.mode DESC, n.src;
