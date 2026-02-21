\i sql/include/test_setup.sql

\echo '----------------------------------------------------------------------------'
\echo 'Test: Native Planner Per-Connection Cache'
\echo 'Validates cache hits/misses when different source tables (different OIDs)'
\echo 'have identical column structure — the StatBus batch loading pattern.'
\echo '----------------------------------------------------------------------------'

CREATE SCHEMA ncache;

-- Create first target table with era
CREATE TABLE ncache.target1 (id int NOT NULL, valid_range daterange, valid_from date, valid_until date, value text);
SELECT sql_saga.add_era('ncache.target1', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'ncache.target1'::regclass, column_names => ARRAY['id'], key_type => 'primary');

-- Insert initial data into target1
INSERT INTO ncache.target1 (id, valid_from, valid_until, value)
VALUES (1, '2023-01-01', '2024-01-01', 'A');

-- Reset native cache to start clean
SELECT sql_saga.temporal_merge_native_cache_reset();

--------------------------------------------------------------------------------
-- SCENARIO 1: First call = MISS
--------------------------------------------------------------------------------
\echo '\n--- Scenario 1: First call = MISS ---'

CREATE TEMP TABLE ncache_s1 (row_id int, id int NOT NULL, valid_from date, valid_until date, value text);
INSERT INTO ncache_s1 VALUES (1, 1, '2023-06-01', '2023-07-01', 'B');

CALL sql_saga.temporal_merge(
    target_table => 'ncache.target1'::regclass,
    source_table => 'ncache_s1'::regclass,
    primary_identity_columns => '{id}'::text[],
    mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
    era_name => 'valid'
);

\echo '--- Cache stats after first call ---'
SELECT stat_name, stat_value FROM sql_saga.temporal_merge_native_cache_stats()
WHERE stat_name IN ('planner_cache_entries', 'cache_hits', 'cache_misses')
ORDER BY stat_name;

\echo '--- Verify merge correctness ---'
SELECT id, valid_from, valid_until, value FROM ncache.target1 ORDER BY id, valid_from;

--------------------------------------------------------------------------------
-- SCENARIO 2: Same columns, different source OID = HIT
--------------------------------------------------------------------------------
\echo '\n--- Scenario 2: Same columns, different source OID = HIT ---'

CREATE TEMP TABLE ncache_s2 (row_id int, id int NOT NULL, valid_from date, valid_until date, value text);
INSERT INTO ncache_s2 VALUES (1, 1, '2023-08-01', '2023-09-01', 'C');

CALL sql_saga.temporal_merge(
    target_table => 'ncache.target1'::regclass,
    source_table => 'ncache_s2'::regclass,
    primary_identity_columns => '{id}'::text[],
    mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
    era_name => 'valid'
);

\echo '--- Cache stats after second call ---'
SELECT stat_name, stat_value FROM sql_saga.temporal_merge_native_cache_stats()
WHERE stat_name IN ('planner_cache_entries', 'cache_hits', 'cache_misses')
ORDER BY stat_name;

\echo '--- Verify merge correctness ---'
SELECT id, valid_from, valid_until, value FROM ncache.target1 ORDER BY id, valid_from;

--------------------------------------------------------------------------------
-- SCENARIO 3: Third source = HIT again
--------------------------------------------------------------------------------
\echo '\n--- Scenario 3: Third source = HIT again ---'

CREATE TEMP TABLE ncache_s3 (row_id int, id int NOT NULL, valid_from date, valid_until date, value text);
INSERT INTO ncache_s3 VALUES (1, 1, '2023-10-01', '2023-11-01', 'D');

CALL sql_saga.temporal_merge(
    target_table => 'ncache.target1'::regclass,
    source_table => 'ncache_s3'::regclass,
    primary_identity_columns => '{id}'::text[],
    mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
    era_name => 'valid'
);

\echo '--- Cache stats after third call ---'
SELECT stat_name, stat_value FROM sql_saga.temporal_merge_native_cache_stats()
WHERE stat_name IN ('planner_cache_entries', 'cache_hits', 'cache_misses')
ORDER BY stat_name;

\echo '--- Verify merge correctness ---'
SELECT id, valid_from, valid_until, value FROM ncache.target1 ORDER BY id, valid_from;

--------------------------------------------------------------------------------
-- SCENARIO 4: Different target = second cache entry
--------------------------------------------------------------------------------
\echo '\n--- Scenario 4: Different target = second cache entry ---'

CREATE TABLE ncache.target2 (id int NOT NULL, valid_range daterange, valid_from date, valid_until date, value text);
SELECT sql_saga.add_era('ncache.target2', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'ncache.target2'::regclass, column_names => ARRAY['id'], key_type => 'primary');

INSERT INTO ncache.target2 (id, valid_from, valid_until, value)
VALUES (1, '2023-01-01', '2024-01-01', 'X');

CREATE TEMP TABLE ncache_s4 (row_id int, id int NOT NULL, valid_from date, valid_until date, value text);
INSERT INTO ncache_s4 VALUES (1, 1, '2023-03-01', '2023-04-01', 'Y');

CALL sql_saga.temporal_merge(
    target_table => 'ncache.target2'::regclass,
    source_table => 'ncache_s4'::regclass,
    primary_identity_columns => '{id}'::text[],
    mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
    era_name => 'valid'
);

\echo '--- Cache stats after different target ---'
SELECT stat_name, stat_value FROM sql_saga.temporal_merge_native_cache_stats()
WHERE stat_name IN ('planner_cache_entries', 'cache_hits', 'cache_misses')
ORDER BY stat_name;

\echo '--- Verify merge correctness ---'
SELECT id, valid_from, valid_until, value FROM ncache.target2 ORDER BY id, valid_from;

--------------------------------------------------------------------------------
-- SCENARIO 5: Alternating targets = all HITs
--------------------------------------------------------------------------------
\echo '\n--- Scenario 5: Alternating targets = all HITs ---'

CREATE TEMP TABLE ncache_s5a (row_id int, id int NOT NULL, valid_from date, valid_until date, value text);
INSERT INTO ncache_s5a VALUES (1, 1, '2023-11-01', '2023-12-01', 'E');

CALL sql_saga.temporal_merge(
    target_table => 'ncache.target1'::regclass,
    source_table => 'ncache_s5a'::regclass,
    primary_identity_columns => '{id}'::text[],
    mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
    era_name => 'valid'
);

CREATE TEMP TABLE ncache_s5b (row_id int, id int NOT NULL, valid_from date, valid_until date, value text);
INSERT INTO ncache_s5b VALUES (1, 1, '2023-05-01', '2023-06-01', 'Z');

CALL sql_saga.temporal_merge(
    target_table => 'ncache.target2'::regclass,
    source_table => 'ncache_s5b'::regclass,
    primary_identity_columns => '{id}'::text[],
    mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
    era_name => 'valid'
);

\echo '--- Cache stats after alternating targets ---'
SELECT stat_name, stat_value FROM sql_saga.temporal_merge_native_cache_stats()
WHERE stat_name IN ('planner_cache_entries', 'cache_hits', 'cache_misses')
ORDER BY stat_name;

--------------------------------------------------------------------------------
-- SCENARIO 6: Source schema change = invalidation + rebuild
--------------------------------------------------------------------------------
\echo '\n--- Scenario 6: Source schema change = invalidation + rebuild ---'

-- Source with different column type: id is bigint instead of int
CREATE TEMP TABLE ncache_s6 (row_id int, id bigint NOT NULL, valid_from date, valid_until date, value text);
INSERT INTO ncache_s6 VALUES (1, 1, '2023-12-01', '2024-01-01', 'F');

CALL sql_saga.temporal_merge(
    target_table => 'ncache.target1'::regclass,
    source_table => 'ncache_s6'::regclass,
    primary_identity_columns => '{id}'::text[],
    mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
    era_name => 'valid'
);

\echo '--- Cache stats after schema change ---'
SELECT stat_name, stat_value FROM sql_saga.temporal_merge_native_cache_stats()
WHERE stat_name IN ('planner_cache_entries', 'cache_hits', 'cache_misses')
ORDER BY stat_name;

\echo '--- Verify merge correctness ---'
SELECT id, valid_from, valid_until, value FROM ncache.target1 ORDER BY id, valid_from;

--------------------------------------------------------------------------------
-- SCENARIO 7: L2 cache empty (native doesn't use it)
--------------------------------------------------------------------------------
\echo '\n--- Scenario 7: L2 cache empty (native planner does not use it) ---'

SELECT count(*) AS l2_cache_entries
FROM sql_saga.temporal_merge_cache;

--------------------------------------------------------------------------------
-- Cleanup
--------------------------------------------------------------------------------
DROP TABLE ncache.target1 CASCADE;
DROP TABLE ncache.target2 CASCADE;
DROP SCHEMA ncache CASCADE;

\i sql/include/test_teardown.sql
