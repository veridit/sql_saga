\i sql/include/test_setup.sql

SET ROLE TO sql_saga_unprivileged_user;

CREATE TABLE excl (
    id int primary key,
    value text NOT NULL,
    null_value integer,
    flap text NOT NULL
);

SELECT sql_saga.add_system_versioning('excl');

-- Initially, no columns are excluded.
INSERT INTO excl (id, value, flap) VALUES (1, 'initial', 'A');
-- This update should generate a history row.
UPDATE excl SET value = 'updated value' WHERE id = 1;
-- This update should also generate a history row.
UPDATE excl SET flap = 'B' WHERE id = 1;
-- This update changes a NULL to a non-NULL value, should generate history.
-- The OLD row, where null_value is still NULL, is written to history.
UPDATE excl SET null_value = 100 WHERE id = 1;

-- Check history. Should have 3 rows from the 3 updates.
-- The third row has a NULL in null_value because it's the state BEFORE the update.
SELECT id, value, null_value, flap FROM excl_history ORDER BY system_valid_from;

-- Now, set 'flap' as an excluded column. This function will be created next.
SELECT sql_saga.set_system_time_era_excluded_columns('excl', '{flap}');
-- Check that the metadata was updated.
SELECT excluded_column_names FROM sql_saga.system_time_era WHERE table_name = 'excl';

-- This update should NOT generate a history row.
UPDATE excl SET flap = 'C' WHERE id = 1;

-- Check history. Should still have only 3 rows.
SELECT id, value, null_value, flap FROM excl_history ORDER BY system_valid_from;

-- This update SHOULD generate a history row.
-- The OLD row now has null_value = 100 and flap = 'C' (from the un-historized update).
-- This is the row that will be written to history.
UPDATE excl SET value = 'another update' WHERE id = 1;

-- Check history. Should now have 4 rows.
-- The new fourth row shows the state before the final update.
SELECT id, value, null_value, flap FROM excl_history ORDER BY system_valid_from;

-- Test that excluding a non-existent column fails.
SELECT sql_saga.set_system_time_era_excluded_columns('excl', '{nonexistent}');
-- Test that excluding a system column fails.
SELECT sql_saga.set_system_time_era_excluded_columns('excl', '{xmin}');

\i sql/include/test_teardown.sql
