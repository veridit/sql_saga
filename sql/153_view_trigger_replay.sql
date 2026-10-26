\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/* Saved user-trigger definitions are replayed as the DDL caller, not with extension-owner EXECUTE rights. */
CREATE ROLE vt_owner;
CREATE ROLE vt_other;
CREATE SCHEMA vt AUTHORIZATION vt_owner;
CREATE SCHEMA vt_other AUTHORIZATION vt_other;
GRANT USAGE ON SCHEMA sql_saga TO vt_owner;
GRANT USAGE ON SCHEMA vt_other TO vt_owner;

CREATE FUNCTION vt.attempt(statement text) RETURNS text LANGUAGE plpgsql AS $attempt$
BEGIN
    BEGIN
        EXECUTE statement;
        RETURN 'ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN SQLSTATE || ': ' || SQLERRM;
    END;
END;
$attempt$;
GRANT EXECUTE ON FUNCTION vt.attempt(text) TO PUBLIC;

CREATE FUNCTION vt_other.user_trigger() RETURNS trigger LANGUAGE plpgsql AS $user_trigger$
BEGIN
    RETURN NEW;
END;
$user_trigger$;

SET ROLE vt_owner;
CREATE TABLE vt.base (id int NOT NULL, extra text, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('vt.base', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('vt.base', ARRAY['id'], key_type => 'primary');
SELECT sql_saga.add_for_portion_of_view('vt.base');
CREATE TRIGGER user_instead INSTEAD OF INSERT ON vt.base__for_portion_of_valid
    FOR EACH ROW EXECUTE FUNCTION vt_other.user_trigger();
RESET ROLE;

-- The trigger was valid when created. Revoking the function's PUBLIC EXECUTE must make a later replay fail
-- just as native CREATE TRIGGER by this owner would. The whole ALTER is atomic.
REVOKE EXECUTE ON FUNCTION vt_other.user_trigger() FROM PUBLIC;
SET ROLE vt_owner;
\o
SELECT 'replay without function EXECUTE | ' || vt.attempt('ALTER TABLE vt.base DROP COLUMN extra');
SELECT 'column and view preserved after refusal | ' || (
    EXISTS (SELECT 1 FROM pg_catalog.pg_attribute AS a
            WHERE a.attrelid = 'vt.base'::regclass AND a.attname = 'extra' AND NOT a.attisdropped)
    AND to_regclass('vt.base__for_portion_of_valid') IS NOT NULL)::text;

\o /dev/null
RESET ROLE;
GRANT EXECUTE ON FUNCTION vt_other.user_trigger() TO vt_owner;
SET ROLE vt_owner;
\o
SELECT 'replay with function EXECUTE | ' || vt.attempt('ALTER TABLE vt.base DROP COLUMN extra');
SELECT 'column dropped after grant | ' || (NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_attribute AS a
    WHERE a.attrelid = 'vt.base'::regclass AND a.attname = 'extra' AND NOT a.attisdropped))::text;

\o /dev/null
RESET ROLE;
DROP SCHEMA vt CASCADE;
DROP SCHEMA vt_other CASCADE;
DROP OWNED BY vt_owner, vt_other;
DROP ROLE vt_owner, vt_other;
\i sql/include/test_teardown.sql
