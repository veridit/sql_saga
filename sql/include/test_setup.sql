--
-- test_setup.sql
--
-- Common setup for regression tests that need to be self-contained.
-- This script creates the extension, a user role, and grants permissions.
--
CREATE EXTENSION IF NOT EXISTS btree_gist;
CREATE EXTENSION IF NOT EXISTS sql_saga CASCADE;

DO $$
BEGIN
    CREATE ROLE sql_saga_unprivileged_user;
EXCEPTION WHEN duplicate_object THEN
END
$$;

GRANT USAGE ON SCHEMA sql_saga TO sql_saga_unprivileged_user;
GRANT SELECT ON ALL TABLES IN SCHEMA sql_saga TO sql_saga_unprivileged_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA sql_saga TO sql_saga_unprivileged_user;

/*
 * Allow the unprivileged user to create tables in the public schema.
 * This is required for tests that create their own tables.
 * PG 15+ restricts this by default.
 */
GRANT CREATE ON SCHEMA public TO PUBLIC;
