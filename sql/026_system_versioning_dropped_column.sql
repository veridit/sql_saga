\i sql/include/test_setup.sql

/*
 * A table whose column positions differ from its history table's.
 *
 * add_system_versioning creates the history table with CREATE TABLE ... (LIKE base), which does not
 * keep the slots of dropped columns. If the base had a column dropped BEFORE versioning, the two
 * tables number their columns differently. write_history converts each row to the history table by
 * column name, but then read the range column at the base table's position from the converted row:
 * the backend crashed with a segmentation fault (server restart) on the first UPDATE or DELETE.
 * History must be written correctly whatever the column positions are.
 */
SET client_min_messages = warning;
CREATE TABLE dc (id int PRIMARY KEY, dropped_before text, tenant text NOT NULL, v int);
ALTER TABLE dc DROP COLUMN dropped_before;
SELECT sql_saga.add_system_versioning('dc');
RESET client_min_messages;

INSERT INTO dc VALUES (1, 'a', 1), (2, 'b', 1);
UPDATE dc SET v = 2 WHERE id = 1;
DELETE FROM dc WHERE id = 2;

SELECT id, tenant, v, upper(system_valid_range) IS NOT NULL AS closed FROM dc_history ORDER BY id;
SELECT id, tenant, v FROM dc_with_history ORDER BY id, v;

SET client_min_messages = warning;
SELECT sql_saga.drop_system_versioning('dc');
DROP TABLE dc;
RESET client_min_messages;

\i sql/include/test_teardown.sql
