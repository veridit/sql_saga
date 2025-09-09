\i sql/include/test_setup.sql

BEGIN;

\echo '--- Test: add_era validation for p_synchronize_range_column ---'

CREATE TABLE public.test_range_validation (
    id int,
    valid_from date,
    valid_until date,
    -- This is intentionally NOT a range type to trigger the bug.
    invalid_range_col date
);

\echo 'Attempting to add era with an invalid range column type (should fail)'
-- This should fail because 'invalid_range_col' is a DATE, not a range type.
SELECT sql_saga.add_era(
    'public.test_range_validation'::regclass,
    p_synchronize_range_column := 'invalid_range_col'
);

ROLLBACK;

\i sql/include/test_teardown.sql
