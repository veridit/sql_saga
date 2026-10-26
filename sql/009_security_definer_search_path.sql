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

SELECT count(*) = 0 AS all_definers_have_search_path
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
  );

\echo '--- Verify no SECURITY DEFINER function has a schema other roles may write on its search_path ---'

-- A SECURITY DEFINER function runs as the extension owner. Every schema on its search_path must be one only that
-- owner (a superuser) controls, or a role that may CREATE there shadows an operator or function with its own code,
-- run with the extension owner's rights (1.4, review round 2, R8). Allowed: sql_saga, pg_catalog, pg_temp (last).
SELECT p.proname AS writable_schema_on_search_path, c.setting
FROM pg_catalog.pg_proc AS p
JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
CROSS JOIN LATERAL unnest(p.proconfig) AS c(setting)
WHERE n.nspname = 'sql_saga'
  AND p.prosecdef
  AND c.setting LIKE 'search_path=%'
  AND EXISTS (
      SELECT FROM unnest(string_to_array(substr(c.setting, length('search_path=') + 1), ',')) AS s(schema_name)
      WHERE btrim(s.schema_name) NOT IN ('sql_saga', 'pg_catalog', 'pg_temp'))
ORDER BY p.proname;

SELECT count(*) = 0 AS no_writable_schema_on_search_path
FROM pg_catalog.pg_proc AS p
JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
CROSS JOIN LATERAL unnest(p.proconfig) AS c(setting)
WHERE n.nspname = 'sql_saga'
  AND p.prosecdef
  AND c.setting LIKE 'search_path=%'
  AND EXISTS (
      SELECT FROM unnest(string_to_array(substr(c.setting, length('search_path=') + 1), ',')) AS s(schema_name)
      WHERE btrim(s.schema_name) NOT IN ('sql_saga', 'pg_catalog', 'pg_temp'));

SELECT count(*) = 0 AS pg_temp_is_last
FROM pg_catalog.pg_proc AS p
JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
CROSS JOIN LATERAL unnest(p.proconfig) AS c(setting)
CROSS JOIN LATERAL (
    SELECT string_to_array(substr(c.setting, length('search_path=') + 1), ',') AS parts
) AS path
WHERE n.nspname = 'sql_saga'
  AND p.prosecdef
  AND c.setting LIKE 'search_path=%'
  AND (
      btrim(path.parts[cardinality(path.parts)]) <> 'pg_temp'
      OR EXISTS (
          SELECT FROM unnest(path.parts) WITH ORDINALITY AS s(schema_name, n)
          WHERE s.n < cardinality(path.parts) AND btrim(s.schema_name) = 'pg_temp'
      )
  );

ROLLBACK;

\i sql/include/test_teardown.sql
