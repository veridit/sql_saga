\i sql/include/test_setup.sql

-- Test system versioning query functions
-- These functions allow querying historical states of the data:
--   tablename__as_of(timestamp) - Returns rows as they were at a specific point in time
--   tablename__between(ts1, ts2) - Returns rows with ranges overlapping [ts1, ts2]
--   tablename__between_symmetric(ts1, ts2) - Same as between but handles swapped args
--   tablename__from_to(ts1, ts2) - Returns rows overlapping half-open range [ts1, ts2)

CREATE TABLE sv_query_test (
    id serial PRIMARY KEY,
    name text NOT NULL,
    value int
);

SELECT sql_saga.add_system_versioning('sv_query_test');

-- Capture timestamp BEFORE insert (row won't exist yet at this point)
SELECT pg_sleep(0.05);
SELECT clock_timestamp() AS ts_before_insert \gset

SELECT pg_sleep(0.05);

\echo '--- Insert initial data ---'
INSERT INTO sv_query_test (name, value) VALUES ('item1', 100);
SELECT pg_sleep(0.05);

\echo '--- Record timestamp during first version (value=100) ---'
SELECT clock_timestamp() AS ts_during_v1 \gset

SELECT pg_sleep(0.05);

\echo '--- Update the value to 200 ---'
UPDATE sv_query_test SET value = 200 WHERE name = 'item1';
SELECT pg_sleep(0.05);

\echo '--- Record timestamp during second version (value=200) ---'
SELECT clock_timestamp() AS ts_during_v2 \gset

SELECT pg_sleep(0.05);

\echo '--- Update again to 300 ---'
UPDATE sv_query_test SET value = 300 WHERE name = 'item1';
SELECT pg_sleep(0.05);

\echo '--- Record timestamp during third version (value=300) ---'
SELECT clock_timestamp() AS ts_during_v3 \gset

SELECT pg_sleep(0.05);

\echo '--- Delete the row ---'
DELETE FROM sv_query_test WHERE name = 'item1';
SELECT pg_sleep(0.05);

\echo '--- Record timestamp after delete ---'
SELECT clock_timestamp() AS ts_after_delete \gset


-- Now test the query functions

\echo '\n=== SCENARIO 1: Current state (should be empty after delete) ==='
SELECT id, name, value FROM sv_query_test ORDER BY id;

\echo '\n=== SCENARIO 2: History table (should have all past versions) ==='
-- We can't show system_valid_range as it contains timestamps, but we can count and check values
SELECT id, name, value FROM sv_query_test_history ORDER BY value;

\echo '\n=== SCENARIO 3: _as_of function tests ==='

\echo '\n--- 3a: _as_of BEFORE insert (should be empty) ---'
SELECT id, name, value FROM sv_query_test__as_of(:'ts_before_insert'::timestamptz) ORDER BY id;

\echo '\n--- 3b: _as_of during v1 (should show value=100) ---'
SELECT id, name, value FROM sv_query_test__as_of(:'ts_during_v1'::timestamptz) ORDER BY id;

\echo '\n--- 3c: _as_of during v2 (should show value=200) ---'
SELECT id, name, value FROM sv_query_test__as_of(:'ts_during_v2'::timestamptz) ORDER BY id;

\echo '\n--- 3d: _as_of during v3 (should show value=300) ---'
SELECT id, name, value FROM sv_query_test__as_of(:'ts_during_v3'::timestamptz) ORDER BY id;

\echo '\n--- 3e: _as_of after delete (should be empty) ---'
SELECT id, name, value FROM sv_query_test__as_of(:'ts_after_delete'::timestamptz) ORDER BY id;

\echo '\n=== SCENARIO 4: _between function tests ==='

\echo '\n--- 4a: _between from before insert to during v1 (should show value=100) ---'
SELECT id, name, value FROM sv_query_test__between(:'ts_before_insert'::timestamptz, :'ts_during_v1'::timestamptz) ORDER BY value;

\echo '\n--- 4b: _between from v1 to v2 (should show values 100 and 200) ---'
SELECT id, name, value FROM sv_query_test__between(:'ts_during_v1'::timestamptz, :'ts_during_v2'::timestamptz) ORDER BY value;

\echo '\n--- 4c: _between from v1 to v3 (should show all 3 values) ---'
SELECT id, name, value FROM sv_query_test__between(:'ts_during_v1'::timestamptz, :'ts_during_v3'::timestamptz) ORDER BY value;

\echo '\n--- 4d: _between from v2 to after delete (should show values 200 and 300) ---'
SELECT id, name, value FROM sv_query_test__between(:'ts_during_v2'::timestamptz, :'ts_after_delete'::timestamptz) ORDER BY value;

\echo '\n=== SCENARIO 5: _between_symmetric function tests ==='

\echo '\n--- 5a: _between_symmetric with args in correct order ---'
SELECT id, name, value FROM sv_query_test__between_symmetric(:'ts_during_v1'::timestamptz, :'ts_during_v3'::timestamptz) ORDER BY value;

\echo '\n--- 5b: _between_symmetric with args SWAPPED (should give same result) ---'
SELECT id, name, value FROM sv_query_test__between_symmetric(:'ts_during_v3'::timestamptz, :'ts_during_v1'::timestamptz) ORDER BY value;

\echo '\n=== SCENARIO 6: _from_to function tests (half-open range [ts1, ts2)) ==='

\echo '\n--- 6a: _from_to from before insert to during v1 ---'
SELECT id, name, value FROM sv_query_test__from_to(:'ts_before_insert'::timestamptz, :'ts_during_v1'::timestamptz) ORDER BY value;

\echo '\n--- 6b: _from_to from v1 to v2 ---'
SELECT id, name, value FROM sv_query_test__from_to(:'ts_during_v1'::timestamptz, :'ts_during_v2'::timestamptz) ORDER BY value;

\echo '\n--- 6c: _from_to from v1 to v3 ---'
SELECT id, name, value FROM sv_query_test__from_to(:'ts_during_v1'::timestamptz, :'ts_during_v3'::timestamptz) ORDER BY value;


\echo '\n=== SCENARIO 7: with_history view (combines current and historical data) ==='
-- After delete, current table is empty, so this shows only history
SELECT id, name, value FROM sv_query_test_with_history ORDER BY value;


-- Cleanup
SELECT sql_saga.drop_system_versioning('sv_query_test');
DROP TABLE sv_query_test;

\i sql/include/test_teardown.sql
