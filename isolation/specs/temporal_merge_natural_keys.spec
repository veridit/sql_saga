# temporal_merge with natural keys at READ COMMITTED (hug 40nwvn). A natural key column outside the identity holds one
# value per entity and may be NULL on some of its pieces (before the key starts, after it is retired), so a writer that
# names the entity by that key must lock and read the whole entity, not only the pieces that hold the key.

setup
{
  CREATE TABLE nk (id int NOT NULL, old_code text, new_code text, v text NOT NULL, valid_range daterange NOT NULL);
  SELECT sql_saga.add_era('nk', 'valid_range');
  SELECT sql_saga.add_unique_key('nk', ARRAY['id'], key_type => 'primary');
  SELECT sql_saga.add_unique_key('nk', ARRAY['old_code'], key_type => 'natural');
  SELECT sql_saga.add_unique_key('nk', ARRAY['new_code'], key_type => 'natural');
  INSERT INTO nk VALUES (1, 'X', NULL, 'a', '[2020-01-01,2025-01-01)'), (1, 'X', 'W', 'a', '[2025-01-01,2030-01-01)'),
                        (2, 'Y', NULL, 'a', '[2020-01-01,2030-01-01)');
}

teardown
{
  SELECT sql_saga.drop_unique_key('nk', ARRAY['new_code']);
  SELECT sql_saga.drop_unique_key('nk', ARRAY['old_code']);
  SELECT sql_saga.drop_unique_key('nk', ARRAY['id']);
  SELECT sql_saga.drop_era('nk');
  DROP TABLE nk;
}

session s1
setup { BEGIN ISOLATION LEVEL READ COMMITTED; SET client_min_messages = warning; }
# entity 1 by identity: v for 2021, a piece that does not hold new_code W
step s1id
{
  CREATE TEMP TABLE src1 (row_id int, id int, old_code text, new_code text, v text, valid_range daterange) ON COMMIT DROP;
  INSERT INTO src1 VALUES (1, 1, NULL, NULL, 's1', '[2021-01-01,2022-01-01)');
  CALL sql_saga.temporal_merge(target_table => 'nk', source_table => 'src1', mode => 'MERGE_ENTITY_PATCH');
}
# entity 2 gets new_code Q
step s1q
{
  CREATE TEMP TABLE src1 (row_id int, id int, old_code text, new_code text, v text, valid_range daterange) ON COMMIT DROP;
  INSERT INTO src1 VALUES (1, 2, NULL, 'Q', 's1', '[2022-01-01,2023-01-01)');
  CALL sql_saga.temporal_merge(target_table => 'nk', source_table => 'src1', mode => 'MERGE_ENTITY_PATCH');
}
step s1c { COMMIT; }

session s2
setup { BEGIN ISOLATION LEVEL READ COMMITTED; SET client_min_messages = warning; }
# entity 1 by new_code W only, over time where W is not yet stored
step s2w
{
  CREATE TEMP TABLE src2 (row_id int, id int, old_code text, new_code text, v text, valid_range daterange) ON COMMIT DROP;
  INSERT INTO src2 VALUES (1, NULL, NULL, 'W', 's2', '[2020-01-01,2026-01-01)');
  CALL sql_saga.temporal_merge(target_table => 'nk', source_table => 'src2', mode => 'PATCH_FOR_PORTION_OF');
}
# entity 2 by old_code, giving another new_code than s1
step s2z
{
  CREATE TEMP TABLE src2 (row_id int, id int, old_code text, new_code text, v text, valid_range daterange) ON COMMIT DROP;
  INSERT INTO src2 VALUES (1, NULL, 'Y', 'Z', 's2', '[2026-01-01,2027-01-01)');
  CALL sql_saga.temporal_merge(target_table => 'nk', source_table => 'src2', mode => 'MERGE_ENTITY_PATCH');
  SELECT status, error_message FROM pg_temp.temporal_merge_feedback;
}
step s2c { COMMIT; }

session s3
setup { SET datestyle = 'ISO, YMD'; }
step result { SELECT id, old_code, new_code, v, valid_range FROM nk ORDER BY id, valid_range; }

# The key writer waits for the identity writer of the same entity, then plans with its change: one piece 2020-2026.
permutation s1id s2w s1c s2c result
# The second writer giving entity 2 a second new_code value waits, then is refused per row; s1's Q stays.
permutation s1q s2z s1c s2c result
