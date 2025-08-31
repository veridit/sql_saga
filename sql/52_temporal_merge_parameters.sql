\i sql/include/test_setup.sql

BEGIN;

-- This test reproduces a bug where temporal_merge_plan generates
-- a syntactically incorrect query when called from a trigger.

CREATE TABLE tm_bug_target (
    id int,
    value text,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('tm_bug_target', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('tm_bug_target', ARRAY['id']);

CREATE TEMP TABLE tm_bug_source (
    row_id int,
    id int,
    value text,
    valid_from date,
    valid_until date
);

INSERT INTO tm_bug_source VALUES (1, 1, 'new value', '2024-01-01', 'infinity');

-- This call mimics a pure-insert scenario and should fail, then succeed after the fix.
-- Using an empty p_id_columns array is the key to reproducing the bug.
CALL sql_saga.temporal_merge(
    p_target_table      := 'tm_bug_target'::regclass,
    p_source_table      := 'tm_bug_source'::regclass,
    p_id_columns        := '{}'::text[],
    p_ephemeral_columns := '{}'::text[],
    p_mode              := 'insert_only'::sql_saga.temporal_merge_mode
);

-- Verify the merge was successful
TABLE tm_bug_target;

ROLLBACK;
\i sql/include/test_teardown.sql

-- TODO: Add more tests for parameter edge cases.
--
-- Future Scenarios to Test:
--
-- 1. Non-default `p_source_row_id_column`:
--    - Create a source table where the unique row identifier is named something
--      other than 'row_id' (e.g., 'source_pk').
--    - Call temporal_merge, passing 'source_pk' to `p_source_row_id_column`.
--    - Verify that feedback is correctly joined back to the source table.
--
-- 2. `p_founding_id_column` behavior:
--    - Scenario 2a (New Entity): Create a source table with multiple rows that share
--      the same `founding_id` but have different data and contiguous time periods.
--      Verify that they are correctly merged into a single new entity with
--      multiple historical slices.
--    - Scenario 2b (Existing Entity): Create a target entity. Create a source table
--      with a `founding_id` and data that updates the existing entity. Verify
--      that `founding_id` is correctly ignored and the operation proceeds as a
--      normal update.
--
-- 3. Table structure variations:
--    - Scenario 3a (Target table with no data columns): Test a merge into a target
--      table that only has ID columns and temporal columns.
--    - Scenario 3b (Source table with extra columns): Test a merge where the source
--      table has additional columns not present in the target. These should be
--      safely ignored.
--
-- 4. Parameter combinations:
--    - Scenario 4a: Test `p_update_source_with_assigned_entity_ids = true` in
--      conjunction with a multi-row `p_founding_id_column` scenario to ensure
--      the generated surrogate key is correctly back-filled to all source rows
--      sharing the founding_id.
