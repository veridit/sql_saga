# temporal_merge against concurrent writers at READ COMMITTED (hug 7rzgv5, doc/upstream/for-portion-of-revert.md).
# Every permutation must end with the result of running the two transactions one after the other, without an error.
# Target: entity 1, price 0, note 'orig', valid 2020 to 2030.

setup
{
  CREATE TABLE t (id int NOT NULL, price int NOT NULL, note text NOT NULL,
                  valid_range daterange NOT NULL, valid_from date, valid_until date);
  SELECT sql_saga.add_era('t', 'valid_range', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
  SELECT sql_saga.add_unique_key('t', ARRAY['id'], key_type => 'primary');
  SELECT sql_saga.add_for_portion_of_view('t');
  INSERT INTO t (id, price, note, valid_from, valid_until) VALUES (1, 0, 'orig', '2020-01-01', '2030-01-01');
}

teardown
{
  SELECT sql_saga.drop_for_portion_of_view('t');
  SELECT sql_saga.drop_unique_key('t', ARRAY['id']);
  SELECT sql_saga.drop_era('t');
  DROP TABLE t;
}

session s1
setup { BEGIN ISOLATION LEVEL READ COMMITTED; SET client_min_messages = warning; }
# a plain UPDATE of another column
step s1note { UPDATE t SET note = 's1' WHERE id = 1; }
# a plain DELETE of the whole entity
step s1delete { DELETE FROM t WHERE id = 1; }
# a portion update of 2022
step s1m2022
{
  CREATE TEMP TABLE src1 (row_id int, id int, price int, valid_from date, valid_until date) ON COMMIT DROP;
  INSERT INTO src1 VALUES (1, 1, 1, '2022-01-01', '2023-01-01');
  CALL sql_saga.temporal_merge(target_table => 't', source_table => 'src1', primary_identity_columns => ARRAY['id'], mode => 'UPDATE_FOR_PORTION_OF');
}
step s1c { COMMIT; }

session s2
setup { BEGIN ISOLATION LEVEL READ COMMITTED; SET client_min_messages = warning; }
# a portion update of 2025
step s2m2025
{
  CREATE TEMP TABLE src2 (row_id int, id int, price int, valid_from date, valid_until date) ON COMMIT DROP;
  INSERT INTO src2 VALUES (1, 1, 2, '2025-01-01', '2026-01-01');
  CALL sql_saga.temporal_merge(target_table => 't', source_table => 'src2', primary_identity_columns => ARRAY['id'], mode => 'UPDATE_FOR_PORTION_OF');
}
# the same change through the for_portion_of view
step s2view { UPDATE t__for_portion_of_valid SET price = 2, valid_from = '2025-01-01', valid_until = '2026-01-01' WHERE id = 1; }
step s2c { COMMIT; }

session s3
setup { SET datestyle = 'ISO, YMD'; }
step result { SELECT id, price, note, valid_from, valid_until FROM t ORDER BY id, valid_from; }

# b: the note committed while the merge waits is kept on every piece.
permutation s1note s2m2025 s1c s2c result
# c: the entity deleted while the merge waits stays deleted.
permutation s1delete s2m2025 s1c s2c result
# e: as b, through the for_portion_of view.
permutation s1note s2view s1c s2c result
# a: two portion updates of disjoint slices both apply.
permutation s1m2022 s2m2025 s1c s2c result
