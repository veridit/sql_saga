# Two temporal_merge calls that name the same entities by different kinds of key, one by identity and one by natural
# key, must run one after the other, not deadlock (review of 1.15, finding B1). Each source names one entity by its
# identity and the other by its natural key, crosswise, so the row locks of the two calls would cross. A third session
# holds a row lock on entity 1 so that both calls reach their row locks before either finishes.

setup
{
  CREATE TABLE kk (id int NOT NULL, ident text, name text NOT NULL, valid_range daterange NOT NULL);
  SELECT sql_saga.add_era('kk', 'valid_range');
  SELECT sql_saga.add_unique_key('kk', ARRAY['id'], key_type => 'primary');
  SELECT sql_saga.add_unique_key('kk', ARRAY['ident'], key_type => 'natural');
  INSERT INTO kk VALUES (1, 'A', 'x', '[2020-01-01,2030-01-01)'), (2, 'B', 'x', '[2020-01-01,2030-01-01)');
}

teardown
{
  SELECT sql_saga.drop_unique_key('kk', ARRAY['ident']);
  SELECT sql_saga.drop_unique_key('kk', ARRAY['id']);
  SELECT sql_saga.drop_era('kk');
  DROP TABLE kk;
}

session holder
step hlock { BEGIN; SELECT id FROM kk WHERE id = 1 FOR UPDATE; }
step hc { COMMIT; }

session sa
setup { BEGIN ISOLATION LEVEL READ COMMITTED; SET client_min_messages = warning; }
# entity 2 by identity, entity 1 by natural key
step sa
{
  CREATE TEMP TABLE srca (row_id int, id int, ident text, name text, valid_range daterange) ON COMMIT DROP;
  INSERT INTO srca VALUES (1, 2, NULL, 'a', '[2020-01-01,2030-01-01)'), (2, NULL, 'A', 'a', '[2020-01-01,2030-01-01)');
  CALL sql_saga.temporal_merge(target_table => 'kk', source_table => 'srca', mode => 'MERGE_ENTITY_PATCH');
}
step sac { COMMIT; }

session sb
setup { BEGIN ISOLATION LEVEL READ COMMITTED; SET client_min_messages = warning; }
# entity 1 by identity, entity 2 by natural key
step sb
{
  CREATE TEMP TABLE srcb (row_id int, id int, ident text, name text, valid_range daterange) ON COMMIT DROP;
  INSERT INTO srcb VALUES (1, 1, NULL, 'b', '[2020-01-01,2030-01-01)'), (2, NULL, 'B', 'b', '[2020-01-01,2030-01-01)');
  CALL sql_saga.temporal_merge(target_table => 'kk', source_table => 'srcb', mode => 'MERGE_ENTITY_PATCH');
}
step sbc { COMMIT; }

session observer
setup { SET datestyle = 'ISO, YMD'; }
step result { SELECT id, ident, name, valid_range FROM kk ORDER BY id, valid_range; }

# sb waits for the holder; sa waits for sb (they share both entities). sb finishes first, then sa: both rows say a.
permutation hlock sb sa hc sbc sac result
