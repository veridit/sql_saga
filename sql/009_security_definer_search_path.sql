\i sql/include/test_setup.sql

BEGIN;

-- =============================================================================
-- Test: Every SECURITY DEFINER function must have SET search_path
-- =============================================================================
-- This test guards against regressions where a new SECURITY DEFINER function
-- is added without a safe search_path. Without SET search_path, a SECURITY
-- DEFINER function executes with the caller's search_path, which could allow
-- schema-spoofing attacks.

\echo '--- Verify all SECURITY DEFINER functions have search_path ---'

-- This query finds SECURITY DEFINER functions in the sql_saga schema that
-- do NOT have a search_path in their proconfig (SET options).
SELECT p.proname AS missing_search_path
FROM pg_catalog.pg_proc AS p
JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
WHERE n.nspname = 'sql_saga'
  AND p.prosecdef = true
  AND (
    p.proconfig IS NULL
    OR NOT EXISTS (
      SELECT FROM unnest(p.proconfig) AS c(setting)
      WHERE c.setting LIKE 'search_path=%'
    )
  )
ORDER BY p.proname;

ROLLBACK;

\i sql/include/test_teardown.sql
