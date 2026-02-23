\i sql/include/test_setup.sql

BEGIN;

SET ROLE TO sql_saga_unprivileged_user;

-- =============================================================================
-- Test: sql_saga.first() aggregate
-- =============================================================================

-- =============================================================================
\echo '--- Test 1: Basic - first value ordered by id ---'
-- =============================================================================

SELECT sql_saga.first(val ORDER BY id)
FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val);

-- =============================================================================
\echo '--- Test 2: Empty input returns NULL ---'
-- =============================================================================

SELECT sql_saga.first(val ORDER BY id)
FROM (SELECT 1 AS val, 1 AS id WHERE false) AS t;

-- =============================================================================
\echo '--- Test 3: GROUP BY with multiple groups ---'
-- =============================================================================

CREATE TEMP TABLE first_test_data (
    grp text,
    sort_key integer,
    val text
);

INSERT INTO first_test_data (grp, sort_key, val) VALUES
    ('alpha', 3, 'third'),
    ('alpha', 1, 'first'),
    ('alpha', 2, 'second'),
    ('beta', 2, 'two'),
    ('beta', 1, 'one'),
    ('beta', 3, 'three');

SELECT grp, sql_saga.first(val ORDER BY sort_key)
FROM first_test_data
GROUP BY grp
ORDER BY grp;

-- =============================================================================
\echo '--- Test 4: Different types - integer ---'
-- =============================================================================

SELECT sql_saga.first(val ORDER BY id)
FROM (VALUES (1, 100), (2, 200), (3, 300)) AS t(id, val);

-- =============================================================================
\echo '--- Test 4b: Different types - date ---'
-- =============================================================================

SELECT sql_saga.first(val ORDER BY id)
FROM (VALUES (1, '2024-01-01'::date), (2, '2024-06-15'::date), (3, '2024-12-31'::date)) AS t(id, val);

-- =============================================================================
\echo '--- Test 5: NULL handling - first value is NULL ---'
-- =============================================================================

SELECT sql_saga.first(val ORDER BY id)
FROM (VALUES (1, NULL::text), (2, 'b'), (3, 'c')) AS t(id, val);

-- =============================================================================
\echo '--- Test 5b: NULL handling - non-first values are NULL ---'
-- =============================================================================

SELECT sql_saga.first(val ORDER BY id)
FROM (VALUES (1, 'a'), (2, NULL::text), (3, NULL::text)) AS t(id, val);

-- =============================================================================
\echo '--- Test 6: Single row ---'
-- =============================================================================

SELECT sql_saga.first(val ORDER BY id)
FROM (VALUES (1, 'only')) AS t(id, val);

ROLLBACK;

\i sql/include/test_teardown.sql
