\set QUIET on
SET client_min_messages = warning;
CREATE TABLE IF NOT EXISTS tm (id int NOT NULL, code text, v text, valid_range daterange NOT NULL);
DO $$ BEGIN IF NOT EXISTS (SELECT FROM sql_saga.era WHERE table_name='tm') THEN
 PERFORM sql_saga.add_era('tm', 'valid_range');
 PERFORM sql_saga.add_unique_key('tm', ARRAY['id'], key_type => 'primary');
 PERFORM sql_saga.add_unique_key('tm', ARRAY['code'], key_type => 'natural'); END IF; END $$;
CREATE TABLE IF NOT EXISTS grid_out (planner text, mode text, sid int, src text, status text, err text, tbl text);
TRUNCATE grid_out;
CREATE OR REPLACE PROCEDURE run_grid() LANGUAGE plpgsql AS $p$
DECLARE pl text; m text; c record; st text; er text; tb text;
BEGIN
 FOR pl IN SELECT unnest(ARRAY['native','plpgsql']) LOOP
  FOR m IN SELECT unnest(ARRAY['PATCH_FOR_PORTION_OF','MERGE_ENTITY_PATCH','INSERT_NEW_ENTITIES']) LOOP
   FOR c IN SELECT * FROM (VALUES (1,1,NULL::text),(2,NULL,'B'),(3,3,'C'),(4,1,'B'),(5,9,NULL),(6,NULL,'Z'),(7,9,'Z'),(8,1,'Z'),(9,9,'B')) v(sid,id,code) LOOP
    BEGIN
     TRUNCATE tm;
     INSERT INTO tm VALUES (1,'A','a','[2020-01-01,2030-01-01)'),(2,'B','b','[2020-01-01,2030-01-01)'),(3,'C','c','[2020-01-01,2030-01-01)');
     DROP TABLE IF EXISTS s; CREATE TEMP TABLE s (row_id int, id int, code text, v text, valid_range daterange);
     INSERT INTO s VALUES (c.sid, c.id, c.code, 'new', '[2025-01-01,2026-01-01)');
     PERFORM set_config('sql_saga.temporal_merge.use_plpgsql_planner', (pl='plpgsql')::text, true);
     CALL sql_saga.temporal_merge(target_table => 'tm', source_table => 's', mode => m::sql_saga.temporal_merge_mode);
     SELECT f.status::text, f.error_message INTO st, er FROM pg_temp.temporal_merge_feedback f;
     SELECT string_agg(format('%s/%s', t.id, t.code), ' ' ORDER BY t.id, t.code) INTO tb FROM tm t WHERE t.v='new';
     RAISE EXCEPTION 'rollback';
    EXCEPTION WHEN OTHERS THEN
     IF SQLERRM <> 'rollback' THEN st := 'EXC'; er := left(SQLERRM, 70); tb := NULL; END IF;
    END;
    INSERT INTO grid_out VALUES (pl, m, c.sid, format('id=%s code=%s', coalesce(c.id::text,'-'), coalesce(c.code,'-')), st, left(er,70), tb);
   END LOOP;
  END LOOP;
 END LOOP;
END $p$;
CALL run_grid();
SELECT n.mode, n.src, n.status||coalesce(' '||n.tbl,'') native, p.status||coalesce(' '||p.tbl,'') plpgsql, CASE WHEN (n.status,n.tbl) IS DISTINCT FROM (p.status,p.tbl) THEN '<<' END d, coalesce(n.err,'')||' | '||coalesce(p.err,'') errs
FROM grid_out n JOIN grid_out p USING (mode, sid) WHERE n.planner='native' AND p.planner='plpgsql' ORDER BY mode, sid;
