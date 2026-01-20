-- =============================================================================
-- Test Suite: temporal_merge Internals Dump
--
-- Description:
--   This test creates a comprehensive dump of ALL intermediate temp tables
--   created during temporal_merge planning. It serves as a correctness
--   verification for the temporal_merge algorithm by capturing the state
--   at each processing stage.
--
-- Purpose:
--   1. Enshrine the correctness of the temporal_merge algorithm
--   2. Detect unexpected changes to intermediate processing
--   3. Document the data flow through all 30 temp tables
--   4. Aid debugging by showing exact intermediate state
-- =============================================================================

\i sql/include/test_setup.sql

-- ============================================================================
-- Setup: Create test tables
-- ============================================================================
\echo '=== SETUP ==='

CREATE TABLE internals_target (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    value INTEGER,
    valid_range DATERANGE NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE NOT NULL,
    edit_comment TEXT
);

SELECT sql_saga.add_era('internals_target', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('internals_target', ARRAY['id'], 'valid');

CREATE TABLE internals_source (
    row_id SERIAL PRIMARY KEY,
    id INTEGER,
    name TEXT,
    value INTEGER,
    valid_from DATE,
    valid_until DATE,
    edit_comment TEXT
);

-- Initial target state
INSERT INTO internals_target (id, name, value, valid_range, edit_comment) VALUES
    (1, 'Original', 100, '[2024-01-01,2024-12-31)', 'initial load'),
    (2, 'Second', 200, '[2024-06-01,2024-12-31)', 'initial load');

-- Source updates
INSERT INTO internals_source (id, name, value, valid_from, valid_until, edit_comment) VALUES
    (1, 'Updated', 150, '2024-03-01', '2024-09-01', 'Q2-Q3 update'),
    (3, 'New', 300, '2024-01-01', '2024-12-31', 'new entity');

\echo ''
\echo '=== Initial State ==='
TABLE internals_target;
TABLE internals_source;

-- ============================================================================
-- Execute temporal_merge_plan and dump all temp tables
-- ============================================================================
\echo ''
\echo '=== Execute temporal_merge_plan ==='

BEGIN;

SELECT COUNT(*) AS plan_row_count FROM sql_saga.temporal_merge_plan(
    target_table => 'internals_target'::regclass,
    source_table => 'internals_source'::regclass,
    mode => 'MERGE_ENTITY_UPSERT',
    era_name => 'valid',
    identity_columns => ARRAY['id'],
    row_id_column => 'row_id',
    ephemeral_columns => ARRAY['edit_comment'],
    p_log_trace => false
);

-- ============================================================================
-- DUMP ALL TEMP TABLES (in processing order)
-- ============================================================================

\echo ''
\echo '--- 1. source_initial ---'
TABLE pg_temp.source_initial;

\echo ''
\echo '--- 2. source_with_eclipsed_flag ---'
TABLE pg_temp.source_with_eclipsed_flag;

\echo ''
\echo '--- 3. target_rows ---'
TABLE pg_temp.target_rows;

\echo ''
\echo '--- 4. source_rows_with_matches ---'
TABLE pg_temp.source_rows_with_matches;

\echo ''
\echo '--- 5. source_rows_with_aggregates ---'
TABLE pg_temp.source_rows_with_aggregates;

\echo ''
\echo '--- 6. source_rows_with_discovery ---'
TABLE pg_temp.source_rows_with_discovery;

\echo ''
\echo '--- 7. source_rows ---'
TABLE pg_temp.source_rows;

\echo ''
\echo '--- 8. source_rows_with_new_flag ---'
TABLE pg_temp.source_rows_with_new_flag;

\echo ''
\echo '--- 9. source_rows_with_nk_json ---'
TABLE pg_temp.source_rows_with_nk_json;

\echo ''
\echo '--- 10. source_rows_with_canonical_key ---'
TABLE pg_temp.source_rows_with_canonical_key;

\echo ''
\echo '--- 11. source_rows_with_early_feedback ---'
TABLE pg_temp.source_rows_with_early_feedback;

\echo ''
\echo '--- 12. active_source_rows ---'
TABLE pg_temp.active_source_rows;

\echo ''
\echo '--- 13. all_rows ---'
TABLE pg_temp.all_rows;

\echo ''
\echo '--- 14. time_points_raw ---'
TABLE pg_temp.time_points_raw;

\echo ''
\echo '--- 15. time_points_unified ---'
TABLE pg_temp.time_points_unified;

\echo ''
\echo '--- 16. time_points_with_unified_ids ---'
TABLE pg_temp.time_points_with_unified_ids;

\echo ''
\echo '--- 17. time_points ---'
TABLE pg_temp.time_points;

\echo ''
\echo '--- 18. atomic_segments ---'
TABLE pg_temp.atomic_segments;

\echo ''
\echo '--- 19. existing_segments_with_target ---'
TABLE pg_temp.existing_segments_with_target;

\echo ''
\echo '--- 20. new_segments_no_target (if exists) ---'
SELECT CASE WHEN to_regclass('pg_temp.new_segments_no_target') IS NOT NULL 
    THEN 'exists' ELSE 'not created' END AS table_status;

\echo ''
\echo '--- 21. resolved_atomic_segments_with_payloads ---'
TABLE pg_temp.resolved_atomic_segments_with_payloads;

\echo ''
\echo '--- 22. resolved_atomic_segments_with_propagated_ids ---'
TABLE pg_temp.resolved_atomic_segments_with_propagated_ids;

\echo ''
\echo '--- 23. resolved_atomic_segments ---'
TABLE pg_temp.resolved_atomic_segments;

\echo ''
\echo '--- 24. island_group ---'
TABLE pg_temp.island_group;

\echo ''
\echo '--- 25. coalesced_final_segments ---'
TABLE pg_temp.coalesced_final_segments;

\echo ''
\echo '--- 26. diff ---'
TABLE pg_temp.diff;

\echo ''
\echo '--- 27. diff_ranked ---'
TABLE pg_temp.diff_ranked;

\echo ''
\echo '--- 28. plan_with_op ---'
TABLE pg_temp.plan_with_op;

\echo ''
\echo '--- 29. plan ---'
TABLE pg_temp.plan;

ROLLBACK;

-- ============================================================================
-- Cleanup
-- ============================================================================
\echo ''
\echo '=== Cleanup ==='

DROP TABLE internals_source;
DROP TABLE internals_target CASCADE;

\i sql/include/test_teardown.sql
