--
-- test_teardown.sql
--
-- Common teardown for regression tests. This script drops the unprivileged
-- user role created by test_setup.sql.
--
-- It is important to reset the role first, in case a test fails and
-- leaves the session role set to the user that is about to be dropped.
RESET ROLE;

-- Revoke any privileges held by the test user and drop any objects they own.
-- This is necessary before the role can be dropped.
DROP OWNED BY sql_saga_unprivileged_user;

DROP ROLE IF EXISTS sql_saga_unprivileged_user;
