\i sql/include/test_setup.sql

BEGIN;

SET ROLE TO sql_saga_unprivileged_user;

-- =============================================================================
-- Test: set_era_ephemeral_columns
-- =============================================================================

\echo '--- Setup: Create table with era ---'

CREATE TABLE public.ephemeral_test (
    id integer,
    name text,
    value numeric,
    edit_at timestamptz,
    edit_by text,
    valid_range daterange NOT NULL
);

SELECT sql_saga.add_era('public.ephemeral_test', 'valid_range');

-- =============================================================================
\echo '--- Test 1: Set ephemeral columns and verify in sql_saga.era ---'
-- =============================================================================

SELECT sql_saga.set_era_ephemeral_columns(
    table_class => 'public.ephemeral_test',
    era_name => 'valid',
    ephemeral_columns => ARRAY['edit_at', 'edit_by']::name[]
);

SELECT ephemeral_columns
FROM sql_saga.era
WHERE table_name = 'ephemeral_test' AND era_name = 'valid';

-- =============================================================================
\echo '--- Test 2: Clear ephemeral columns by setting to NULL ---'
-- =============================================================================

SELECT sql_saga.set_era_ephemeral_columns(
    table_class => 'public.ephemeral_test',
    era_name => 'valid',
    ephemeral_columns => NULL
);

SELECT ephemeral_columns IS NULL AS is_cleared
FROM sql_saga.era
WHERE table_name = 'ephemeral_test' AND era_name = 'valid';

-- =============================================================================
\echo '--- Test 3: Error - non-existent column ---'
-- =============================================================================

SAVEPOINT test3;

SELECT sql_saga.set_era_ephemeral_columns(
    table_class => 'public.ephemeral_test',
    era_name => 'valid',
    ephemeral_columns => ARRAY['no_such_column']::name[]
);

ROLLBACK TO test3;

-- =============================================================================
\echo '--- Test 4: Error - system column as ephemeral ---'
-- =============================================================================

SAVEPOINT test4;

SELECT sql_saga.set_era_ephemeral_columns(
    table_class => 'public.ephemeral_test',
    era_name => 'valid',
    ephemeral_columns => ARRAY['xmin']::name[]
);

ROLLBACK TO test4;

-- =============================================================================
\echo '--- Test 5: Error - non-existent era ---'
-- =============================================================================

SAVEPOINT test5;

SELECT sql_saga.set_era_ephemeral_columns(
    table_class => 'public.ephemeral_test',
    era_name => 'nonexistent',
    ephemeral_columns => ARRAY['edit_at']::name[]
);

ROLLBACK TO test5;

ROLLBACK;

\i sql/include/test_teardown.sql
