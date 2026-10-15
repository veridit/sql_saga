\i sql/include/test_setup.sql

\echo '----------------------------------------------------------------------------'
\echo 'Test: sql_saga_native is an internal library, not an installable extension'
\echo '----------------------------------------------------------------------------'

\echo '--- Only sql_saga is offered as an extension ---'
SELECT name FROM pg_available_extensions WHERE name LIKE 'sql_saga%' ORDER BY name;
SELECT count(*) AS native_extension_versions
FROM pg_available_extension_versions WHERE name = 'sql_saga_native';

\echo '--- CREATE EXTENSION sql_saga_native must fail ---'
\set VERBOSITY terse
CREATE EXTENSION sql_saga_native;
\set VERBOSITY default

\echo '--- The native functions belong to the sql_saga extension and bind to $libdir/sql_saga_native ---'
SELECT p.proname, p.probin, e.extname
FROM pg_proc AS p
LEFT JOIN pg_depend AS d ON d.classid = 'pg_proc'::regclass AND d.objid = p.oid AND d.deptype = 'e'
LEFT JOIN pg_extension AS e ON e.oid = d.refobjid
WHERE p.probin = 'sql_saga_native'
ORDER BY p.proname;

\echo '--- The installed library was built from the same sql_saga version ---'
SELECT sql_saga.native_library_version() = e.extversion AS library_matches_extension
FROM pg_extension AS e WHERE e.extname = 'sql_saga';

\echo '--- The native planner works through sql_saga ---'
BEGIN;
CREATE TABLE native_pkg_target (id int NOT NULL, valid_range daterange NOT NULL, value text);
SELECT sql_saga.add_era('native_pkg_target', 'valid_range');
SELECT sql_saga.add_unique_key(table_oid => 'native_pkg_target'::regclass, column_names => ARRAY['id'], key_type => 'primary');
INSERT INTO native_pkg_target VALUES (1, '[2024-01-01,2025-01-01)', 'A');

CREATE TEMP TABLE native_pkg_source (row_id int, id int NOT NULL, valid_range daterange NOT NULL, value text) ON COMMIT DROP;
INSERT INTO native_pkg_source VALUES (1, 1, '[2024-06-01,2024-09-01)', 'B');

SET LOCAL sql_saga.temporal_merge.use_plpgsql_planner = false;
SELECT sql_saga.temporal_merge_native_cache_reset();
CALL sql_saga.temporal_merge(
    target_table => 'native_pkg_target'::regclass,
    source_table => 'native_pkg_source'::regclass,
    primary_identity_columns => ARRAY['id'],
    mode => 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
    era_name => 'valid'
);
SELECT id, valid_range, value FROM native_pkg_target ORDER BY id, valid_range;

\echo '--- The planner that ran was the native one (its cache recorded the call) ---'
SELECT stat_value AS native_cache_misses
FROM sql_saga.temporal_merge_native_cache_stats() WHERE stat_name = 'cache_misses';

\echo '--- The loaded module identifies itself (PG18 magic block name/version) ---'
SELECT module_name, version = (SELECT extversion FROM pg_extension WHERE extname = 'sql_saga') AS version_matches_sql_saga
FROM pg_get_loaded_modules() WHERE file_name LIKE 'sql_saga_native%';
ROLLBACK;

\i sql/include/test_teardown.sql
