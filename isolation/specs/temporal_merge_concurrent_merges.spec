# temporal_merge against temporal_merge at READ COMMITTED (hug 7rzgv5): the remaining races of the revert study, and
# merges of different entities, which must not wait for each other (concurrent batch imports stay parallel).

setup
{
  CREATE TABLE t (id int NOT NULL, price int NOT NULL, note text NOT NULL,
                  valid_range daterange NOT NULL, valid_from date, valid_until date);
  SELECT sql_saga.add_era('t', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
  SELECT sql_saga.add_unique_key('t', ARRAY['id'], key_type => 'primary');
  INSERT INTO t (id, price, note, valid_from, valid_until) VALUES (1, 0, 'orig', '2020-01-01', '2030-01-01'),
                                                                  (3, 0, 'orig', '2020-01-01', '2030-01-01');
}

teardown
{
  SELECT sql_saga.drop_unique_key('t', ARRAY['id']);
  SELECT sql_saga.drop_era('t');
  DROP TABLE t;
}

session s1
setup { BEGIN ISOLATION LEVEL READ COMMITTED; SET client_min_messages = warning; }
# portion delete of 2020 for entity 1 (Andres Freund's delete2 shape)
step s1del2020
{
  CREATE TEMP TABLE src1 (row_id int, id int, price int, note text, valid_from date, valid_until date) ON COMMIT DROP;
  INSERT INTO src1 (row_id, id, valid_from, valid_until) VALUES (1, 1, '2020-01-01', '2021-01-01');
  CALL sql_saga.temporal_merge(target_table => 't', source_table => 'src1', primary_identity_columns => ARRAY['id'], mode => 'DELETE_FOR_PORTION_OF');
}
# create entity 2
step s1new
{
  CREATE TEMP TABLE src1 (row_id int, id int, price int, note text, valid_from date, valid_until date) ON COMMIT DROP;
  INSERT INTO src1 VALUES (1, 2, 1, 'n1', '2020-01-01', '2025-01-01');
  CALL sql_saga.temporal_merge(target_table => 't', source_table => 'src1', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_PATCH');
}
# change entity 3
step s1e3
{
  CREATE TEMP TABLE src1 (row_id int, id int, price int, note text, valid_from date, valid_until date) ON COMMIT DROP;
  INSERT INTO src1 VALUES (1, 3, 9, 'e3', '2022-01-01', '2023-01-01');
  CALL sql_saga.temporal_merge(target_table => 't', source_table => 'src1', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_PATCH');
}
step s1c { COMMIT; }

session s2
setup { BEGIN ISOLATION LEVEL READ COMMITTED; SET client_min_messages = warning; }
# portion update of 2024 for entity 1
step s2m2024
{
  CREATE TEMP TABLE src2 (row_id int, id int, price int, note text, valid_from date, valid_until date) ON COMMIT DROP;
  INSERT INTO src2 (row_id, id, price, valid_from, valid_until) VALUES (1, 1, 2, '2024-01-01', '2025-01-01');
  CALL sql_saga.temporal_merge(target_table => 't', source_table => 'src2', primary_identity_columns => ARRAY['id'], mode => 'UPDATE_FOR_PORTION_OF');
}
# create entity 2 too, overlapping in time
step s2new
{
  CREATE TEMP TABLE src2 (row_id int, id int, price int, note text, valid_from date, valid_until date) ON COMMIT DROP;
  INSERT INTO src2 VALUES (1, 2, 2, 'n2', '2023-01-01', '2028-01-01');
  CALL sql_saga.temporal_merge(target_table => 't', source_table => 'src2', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_PATCH');
}
# change entity 1 only: a different entity from s1e3
step s2e1
{
  CREATE TEMP TABLE src2 (row_id int, id int, price int, note text, valid_from date, valid_until date) ON COMMIT DROP;
  INSERT INTO src2 VALUES (1, 1, 8, 'e1', '2022-01-01', '2023-01-01');
  CALL sql_saga.temporal_merge(target_table => 't', source_table => 'src2', primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_PATCH');
}
step s2c { COMMIT; }

session s3
setup { BEGIN ISOLATION LEVEL READ COMMITTED; SET client_min_messages = warning; }
# portion update of 2022 for entity 1
step s3m2022
{
  CREATE TEMP TABLE src3 (row_id int, id int, price int, valid_from date, valid_until date) ON COMMIT DROP;
  INSERT INTO src3 VALUES (1, 1, 3, '2022-01-01', '2023-01-01');
  CALL sql_saga.temporal_merge(target_table => 't', source_table => 'src3', primary_identity_columns => ARRAY['id'], mode => 'UPDATE_FOR_PORTION_OF');
}
step s3c { COMMIT; }

session s4
setup { SET datestyle = 'ISO, YMD'; }
step result { SELECT id, price, note, valid_from, valid_until FROM t ORDER BY id, valid_from; }

# d: a portion delete and two portion updates of entity 1, each waiting for the one before: all three apply.
permutation s1del2020 s2m2024 s1c s3m2022 s2c s3c result
# f: both sessions create entity 2; the second patches the first.
permutation s1new s2new s1c s2c s3c result
# Different entities: neither waits (s2e1 completes while s1 is open).
permutation s1e3 s2e1 s2c s1c s3c result
