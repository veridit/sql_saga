\i sql/include/test_setup.sql

/*
 * P2: INSERT through a current view must work when the table's identifier is a temporal
 * PRIMARY KEY (id, <range> WITHOUT OVERLAPS) and the era has no valid_from/valid_until columns.
 *
 * add_current_view() takes the identifier from a single-column temporal unique key if one exists,
 * otherwise from the primary key. A temporal primary key includes the era's range column, so the
 * identifier passed to the view trigger was (id, valid_range), and temporal_merge then saw the range
 * column twice: ERROR column "valid_range" specified more than once.
 * (053 scenario 3 covers range-only eras only together with a separate single-column unique key.)
 *
 * Correct behaviour: the range column is never part of the entity identifier. INSERT and UPDATE
 * through the current view behave the same for a temporal primary key as for a separate unique key.
 */
\set VERBOSITY terse
SET client_min_messages = warning;
CREATE FUNCTION cv_test_now() RETURNS date LANGUAGE sql AS $$ SELECT '2024-02-29'::date $$;

-- Range-only era, identity from the temporal PRIMARY KEY only (the failing case)
CREATE TABLE cv_pk (id int, valid_range daterange NOT NULL, x text, PRIMARY KEY (id, valid_range WITHOUT OVERLAPS));
SELECT sql_saga.add_era('cv_pk', 'valid_range', 'valid');
SELECT sql_saga.add_current_view('cv_pk', 'valid', current_func_name => 'cv_test_now()');

-- Range-only era, identity from a separate temporal unique key (works today; control case)
CREATE TABLE cv_uk (id int, valid_range daterange NOT NULL, x text);
SELECT sql_saga.add_era('cv_uk', 'valid_range', 'valid');
SELECT sql_saga.add_unique_key('cv_uk', ARRAY['id'], 'valid');
SELECT sql_saga.add_current_view('cv_uk', 'valid', current_func_name => 'cv_test_now()');

INSERT INTO cv_pk VALUES (2, '[2023-01-01,infinity)', 'existing');
INSERT INTO cv_uk VALUES (2, '[2023-01-01,infinity)', 'existing');
RESET client_min_messages;

\echo '--- INSERT through the current view starts a new entity today'
INSERT INTO cv_pk__current_valid (id, x) VALUES (1, 'new');
INSERT INTO cv_uk__current_valid (id, x) VALUES (1, 'new');
SELECT 'cv_pk' AS tbl, id, x, valid_range FROM cv_pk
UNION ALL
SELECT 'cv_uk', id, x, valid_range FROM cv_uk
ORDER BY 1, 2, 4;

\echo '--- UPDATE through the current view closes the old version and opens a new one today'
UPDATE cv_pk__current_valid SET x = 'changed' WHERE id = 2;
UPDATE cv_uk__current_valid SET x = 'changed' WHERE id = 2;
SELECT 'cv_pk' AS tbl, id, x, valid_range FROM cv_pk
UNION ALL
SELECT 'cv_uk', id, x, valid_range FROM cv_uk
ORDER BY 1, 2, 4;

\echo '--- The two tables now hold the same history'
(SELECT id, x, valid_range FROM cv_pk EXCEPT SELECT id, x, valid_range FROM cv_uk)
UNION ALL
(SELECT id, x, valid_range FROM cv_uk EXCEPT SELECT id, x, valid_range FROM cv_pk);

/* Clean up */
SET client_min_messages = warning;
SELECT sql_saga.drop_current_view('cv_pk', 'valid');
SELECT sql_saga.drop_current_view('cv_uk', 'valid');
SELECT sql_saga.drop_unique_key('cv_uk', ARRAY['id'], 'valid');
SELECT sql_saga.drop_era('cv_pk', 'valid');
SELECT sql_saga.drop_era('cv_uk', 'valid');
DROP TABLE cv_pk, cv_uk;
DROP FUNCTION cv_test_now();
RESET client_min_messages;

\i sql/include/test_teardown.sql
