\i sql/include/test_setup.sql
\i sql/include/benchmark_fixture_system_simple.sql

SET ROLE TO sql_saga_unprivileged_user;

--------------------------------------------------------------------------------
-- TEST: Fixture System Functionality
--
-- This test verifies the fixture system works correctly:
-- 1. Generate fixtures
-- 2. Load fixtures
-- 3. Verify data integrity
-- 4. Test fixture management functions
--------------------------------------------------------------------------------

\echo '--- Testing Fixture System Basic Functionality ---'

-- Clean up any existing test fixtures
SELECT sql_saga_fixtures.delete_fixture('test_temporal_merge_100') WHERE sql_saga_fixtures.fixture_exists('test_temporal_merge_100');
SELECT sql_saga_fixtures.delete_fixture('test_etl_100') WHERE sql_saga_fixtures.fixture_exists('test_etl_100');

-- Test 1: Generate temporal_merge fixture
\echo 'Test 1: Generating temporal_merge fixture (100 entities)'
SELECT sql_saga_fixtures.generate_temporal_merge_fixture('test_temporal_merge_100', 100, false, true);

-- Test 2: Generate ETL fixture  
\echo 'Test 2: Generating ETL fixture (100 entities)'
SELECT sql_saga_fixtures.generate_etl_fixture('test_etl_100', 100, 3, 5, false);

-- Test 3: List fixtures
\echo 'Test 3: List generated fixtures'
SELECT fixture_name, fixture_type, entities, total_rows
FROM sql_saga_fixtures.list_fixtures()
WHERE fixture_name LIKE 'test_%'
ORDER BY fixture_name;

-- Test 4: Check fixture existence
\echo 'Test 4: Check fixture existence'
SELECT 
    'test_temporal_merge_100' as fixture_name,
    sql_saga_fixtures.fixture_exists('test_temporal_merge_100') as exists;
SELECT 
    'test_etl_100' as fixture_name,
    sql_saga_fixtures.fixture_exists('test_etl_100') as exists;
SELECT 
    'nonexistent_fixture' as fixture_name,
    sql_saga_fixtures.fixture_exists('nonexistent_fixture') as exists;

-- Test 5: Get fixture info
\echo 'Test 5: Get fixture detailed info'
SELECT sql_saga_fixtures.get_fixture_info('test_temporal_merge_100');

-- Test 6: Load temporal_merge fixture
\echo 'Test 6: Loading temporal_merge fixture'

CREATE TABLE test_legal_unit_tm (
    id INTEGER NOT NULL,
    valid_range daterange NOT NULL,
    name varchar NOT NULL
);

CREATE TABLE test_establishment_tm (
    id INTEGER NOT NULL,
    valid_range daterange NOT NULL,
    legal_unit_id INTEGER NOT NULL,
    postal_place TEXT NOT NULL
);

SELECT sql_saga_fixtures.load_temporal_merge_fixture('test_temporal_merge_100', 'public');

-- Verify loaded data
SELECT 'legal_unit_tm loaded' as test, COUNT(*) as count FROM test_legal_unit_tm;
SELECT 'establishment_tm loaded' as test, COUNT(*) as count FROM test_establishment_tm;

-- Test data integrity
SELECT 'Data integrity check' as test,
       CASE WHEN COUNT(*) = 100 THEN 'PASS' ELSE 'FAIL' END as result
FROM test_legal_unit_tm;

SELECT 'FK integrity check' as test,
       CASE WHEN COUNT(*) = 100 THEN 'PASS' ELSE 'FAIL' END as result  
FROM test_establishment_tm e
JOIN test_legal_unit_tm l ON e.legal_unit_id = l.id;

-- Test 7: Load ETL fixture
\echo 'Test 7: Loading ETL fixture'

CREATE TABLE test_data_table (
    row_id bigint NOT NULL,
    batch int NOT NULL,
    identity_correlation int NOT NULL,
    legal_unit_id INTEGER,
    location_id INTEGER,
    merge_statuses jsonb,
    merge_errors jsonb,
    comment text,
    tax_ident text,
    lu_name text,
    physical_address text,
    postal_address text,
    activity_code text,
    employees numeric,
    turnover numeric,
    valid_from date NOT NULL,
    valid_until date NOT NULL
);

SELECT sql_saga_fixtures.load_etl_fixture('test_etl_100', 'test_data_table'::regclass);

-- Verify ETL data
SELECT 'ETL data loaded' as test, COUNT(*) as count FROM test_data_table;
SELECT 'ETL data integrity' as test,
       CASE WHEN COUNT(*) = 1500 THEN 'PASS' ELSE 'FAIL' END as result -- 100 entities * 3 batches * 5 rows
FROM test_data_table;

-- Test 8: Fixture health check
\echo 'Test 8: Fixture health check'
SELECT * FROM benchmark_check_fixture_health()
WHERE fixture_name LIKE 'test_%';

-- Test 9: Update fixture usage stats
\echo 'Test 9: Fixture usage statistics after loading'
SELECT fixture_name, load_count, last_loaded_at IS NOT NULL as has_load_time
FROM sql_saga_fixtures.list_fixtures()
WHERE fixture_name LIKE 'test_%'
ORDER BY fixture_name;

-- Test 10: Test auto-generation functionality
\echo 'Test 10: Testing auto-generation'
SELECT sql_saga_fixtures.ensure_temporal_merge_fixture('test_auto_50', 50, true, false) as auto_generated;
SELECT fixture_name, entities FROM sql_saga_fixtures.list_fixtures() WHERE fixture_name = 'test_auto_50';

-- Test 11: Test prevent auto-generation
\echo 'Test 11: Testing prevent auto-generation'
SELECT sql_saga_fixtures.ensure_temporal_merge_fixture('test_no_auto_25', 25, false, false) as should_be_false;

-- Test 12: Cleanup test fixtures
\echo 'Test 12: Cleanup test fixtures'
SELECT sql_saga_fixtures.delete_fixture('test_temporal_merge_100');
SELECT sql_saga_fixtures.delete_fixture('test_etl_100');  
SELECT sql_saga_fixtures.delete_fixture('test_auto_50');

-- Verify cleanup
SELECT 'Fixtures after cleanup' as test, COUNT(*) as remaining_test_fixtures
FROM sql_saga_fixtures.list_fixtures()
WHERE fixture_name LIKE 'test_%';

-- Cleanup test tables
DROP TABLE test_legal_unit_tm;
DROP TABLE test_establishment_tm;
DROP TABLE test_data_table;

\echo 'All fixture system tests completed successfully!'

\i sql/include/test_teardown.sql