\i sql/include/test_setup.sql

BEGIN;
\echo '----------------------------------------------------------------------------'
\echo 'Test: Two-Level Cache for temporal_merge'
\echo 'This test verifies the L2 (persistent) cache behavior across transactions.'
\echo '----------------------------------------------------------------------------'

CREATE SCHEMA l2cache;

-- Create a target table with era
CREATE TABLE l2cache.target (id int NOT NULL, valid_range daterange, valid_from date, valid_until date, value text);
SELECT sql_saga.add_era('l2cache.target', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'l2cache.target'::regclass, column_names => ARRAY['id'], key_type => 'primary');

-- Insert initial data
INSERT INTO l2cache.target (id, valid_from, valid_until, value)
VALUES (1, '2023-01-01', '2024-01-01', 'A');

\echo '--- Clear any existing L2 cache entries for our test table ---'
CALL sql_saga.temporal_merge_invalidate_cache('l2cache.target'::regclass);

\echo '--- Verify L2 cache is empty for our target ---'
SELECT count(*) AS l2_cache_entries_before
FROM sql_saga.temporal_merge_cache
WHERE cache_key LIKE 'l2cache.target:%';

--------------------------------------------------------------------------------
-- SCENARIO 1: First call populates L2 cache
--------------------------------------------------------------------------------
\echo '\n--- Scenario 1: First call populates L2 cache ---'

CREATE TEMP TABLE l2cache_source1 (row_id int, id int NOT NULL, valid_from date, valid_until date, value text);
INSERT INTO l2cache_source1 VALUES (1, 1, '2023-06-01', '2023-07-01', 'B');

-- This call should populate the L2 cache
CALL sql_saga.temporal_merge(
    target_table => 'l2cache.target'::regclass,
    source_table => 'l2cache_source1'::regclass,
    primary_identity_columns => '{id}'::text[],
    mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
    era_name => 'valid'
);

\echo '--- Verify L2 cache has an entry after first call ---'
SELECT count(*) AS l2_cache_entries_after_first_call
FROM sql_saga.temporal_merge_cache
WHERE cache_key LIKE 'l2cache.target:%';

\echo '--- Verify the cached SQL has the placeholder ---'
SELECT 
    (plan_sqls->0->>'sql') LIKE '%{{SOURCE_TABLE}}%' AS first_step_has_placeholder
FROM sql_saga.temporal_merge_cache
WHERE cache_key LIKE 'l2cache.target:%';

COMMIT;

--------------------------------------------------------------------------------
-- SCENARIO 2: Second transaction reuses L2 cache with different source table
--------------------------------------------------------------------------------
BEGIN;
\echo '\n--- Scenario 2: Second transaction reuses L2 cache ---'

-- Create a different temp table with SAME schema
CREATE TEMP TABLE l2cache_source2 (row_id int, id int NOT NULL, valid_from date, valid_until date, value text);
INSERT INTO l2cache_source2 VALUES (1, 1, '2023-08-01', '2023-09-01', 'C');

\echo '--- Check L2 cache stats before second call ---'
SELECT use_count AS l2_use_count_before
FROM sql_saga.temporal_merge_cache
WHERE cache_key LIKE 'l2cache.target:%';

-- This call should reuse the L2 cache (different source table, same schema)
CALL sql_saga.temporal_merge(
    target_table => 'l2cache.target'::regclass,
    source_table => 'l2cache_source2'::regclass,
    primary_identity_columns => '{id}'::text[],
    mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
    era_name => 'valid'
);

\echo '--- Verify L2 cache use_count incremented (L2 hit) ---'
SELECT use_count AS l2_use_count_after
FROM sql_saga.temporal_merge_cache
WHERE cache_key LIKE 'l2cache.target:%';

\echo '--- Verify the merge worked correctly ---'
SELECT id, valid_from, valid_until, value 
FROM l2cache.target 
ORDER BY id, valid_from;

COMMIT;

--------------------------------------------------------------------------------
-- SCENARIO 3: Cache invalidation on ALTER TABLE
--------------------------------------------------------------------------------
BEGIN;
\echo '\n--- Scenario 3: Cache invalidation on ALTER TABLE ---'

\echo '--- L2 cache entry exists before ALTER ---'
SELECT count(*) AS l2_cache_before_alter
FROM sql_saga.temporal_merge_cache
WHERE cache_key LIKE 'l2cache.target:%';

-- ALTER TABLE should trigger cache invalidation
ALTER TABLE l2cache.target ADD COLUMN extra_col text;

\echo '--- L2 cache entry should be deleted after ALTER ---'
SELECT count(*) AS l2_cache_after_alter
FROM sql_saga.temporal_merge_cache
WHERE cache_key LIKE 'l2cache.target:%';

COMMIT;

--------------------------------------------------------------------------------
-- SCENARIO 4: Cache invalidation with incompatible source schema
--------------------------------------------------------------------------------
BEGIN;
\echo '\n--- Scenario 4: Cache invalidation with incompatible source schema ---'

-- First, rebuild the cache
CREATE TEMP TABLE l2cache_source3 (row_id int, id int NOT NULL, valid_from date, valid_until date, value text, extra_col text);
INSERT INTO l2cache_source3 VALUES (1, 1, '2023-10-01', '2023-11-01', 'D', 'extra');

CALL sql_saga.temporal_merge(
    target_table => 'l2cache.target'::regclass,
    source_table => 'l2cache_source3'::regclass,
    primary_identity_columns => '{id}'::text[],
    mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
    era_name => 'valid'
);

SELECT count(*) AS l2_cache_after_rebuild, source_columns_hash AS hash_after_rebuild
FROM sql_saga.temporal_merge_cache
WHERE cache_key LIKE 'l2cache.target:%'
GROUP BY source_columns_hash;

COMMIT;

BEGIN;
-- Now use a source table with different column types (should invalidate)
CREATE TEMP TABLE l2cache_source4 (row_id int, id bigint NOT NULL, valid_from date, valid_until date, value text, extra_col text);
INSERT INTO l2cache_source4 VALUES (1, 1, '2023-12-01', '2024-01-01', 'E', 'extra');

\echo '--- Using source with different column types (id is bigint instead of int) ---'
\echo '--- This should trigger L2 cache invalidation due to hash mismatch ---'

-- This should detect the hash mismatch and rebuild
CALL sql_saga.temporal_merge(
    target_table => 'l2cache.target'::regclass,
    source_table => 'l2cache_source4'::regclass,
    primary_identity_columns => '{id}'::text[],
    mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
    era_name => 'valid'
);

SELECT count(*) AS l2_cache_count, source_columns_hash AS new_hash
FROM sql_saga.temporal_merge_cache
WHERE cache_key LIKE 'l2cache.target:%'
GROUP BY source_columns_hash;

\echo '--- Verify the merge still worked correctly despite cache rebuild ---'
SELECT id, valid_from, valid_until, value 
FROM l2cache.target 
ORDER BY id, valid_from;

COMMIT;

--------------------------------------------------------------------------------
-- SCENARIO 5: Cache statistics
--------------------------------------------------------------------------------
\echo '\n--- Scenario 5: Cache statistics ---'
SELECT total_entries, total_size_bytes > 0 AS has_size, most_used_count, least_used_count 
FROM sql_saga.temporal_merge_cache_stats();

--------------------------------------------------------------------------------
-- Cleanup
--------------------------------------------------------------------------------
CALL sql_saga.temporal_merge_invalidate_cache('l2cache.target'::regclass);
DROP TABLE l2cache.target CASCADE;
DROP SCHEMA l2cache CASCADE;

\i sql/include/test_teardown.sql
