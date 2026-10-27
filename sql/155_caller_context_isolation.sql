\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/* User code executed while a management worker validates caller SQL must not inherit the worker's authority. */
CREATE ROLE cx_victim;
CREATE ROLE cx_attacker;
CREATE SCHEMA cx AUTHORIZATION cx_victim;
CREATE SCHEMA cx_u AUTHORIZATION cx_attacker;
GRANT USAGE ON SCHEMA sql_saga, cx, cx_u TO cx_attacker;

CREATE FUNCTION cx.attempt(statement text) RETURNS text LANGUAGE plpgsql AS $attempt$
BEGIN
    BEGIN
        EXECUTE statement;
        RETURN 'ok';
    EXCEPTION WHEN OTHERS THEN
        RETURN SQLSTATE || ': ' || SQLERRM;
    END;
END;
$attempt$;
GRANT EXECUTE ON FUNCTION cx.attempt(text) TO PUBLIC;

SET ROLE cx_victim;
CREATE TABLE cx.victim_t (id int, secret text);
INSERT INTO cx.victim_t VALUES (1, 'do not delete');
RESET ROLE;

SET ROLE cx_attacker;
CREATE TABLE cx_u.t (id int, valid_range daterange, valid_from date, valid_until date);
INSERT INTO cx_u.t VALUES (1, daterange('2020-01-01', 'infinity'), '2020-01-01', 'infinity');
SELECT sql_saga.add_era('cx_u.t', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
CREATE TABLE cx_u.other (id int, valid_range daterange, valid_from date, valid_until date);
CREATE TABLE cx_u.stolen (v text);
CREATE FUNCTION cx_u.leak(v text) RETURNS int LANGUAGE plpgsql SECURITY DEFINER AS $leak$
BEGIN
    INSERT INTO cx_u.stolen VALUES (v);
    RETURN 1;
END;
$leak$;

/* IMMUTABLE is required for an index predicate; the malicious side effect is the point of the test. */
CREATE FUNCTION cx_u.pwn_truncate(id int) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $pwn_truncate$
BEGIN
    PERFORM sql_saga.__internal_truncate_as_owner('cx.victim_t'::regclass);
    RETURN true;
END;
$pwn_truncate$;
CREATE FUNCTION cx_u.pwn_worker(id int) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $pwn_worker$
BEGIN
    PERFORM sql_saga.__api_add_era('cx_u.other', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
    RETURN true;
END;
$pwn_worker$;
CREATE FUNCTION cx_u.pwn_read(id int) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS $pwn_read$
BEGIN
    PERFORM sql_saga.__internal_regular_fk_initial_check(
        'SELECT false', 'SELECT cx_u.leak(secret) FROM cx.victim_t', 'SELECT 1 WHERE $1 = 1',
        'cx.victim_t'::regclass, 'cx.victim_t'::regclass, ARRAY['id'], ARRAY['id']);
    RETURN true;
END;
$pwn_read$;
RESET ROLE;

GRANT EXECUTE ON FUNCTION sql_saga.__internal_truncate_as_owner(regclass) TO cx_attacker;
GRANT EXECUTE ON FUNCTION sql_saga.__api_add_era(regclass, name, name, boolean, name, name, name, boolean, boolean, boolean, regtype, name, name[]) TO cx_attacker;
GRANT EXECUTE ON FUNCTION sql_saga.__internal_regular_fk_initial_check(text, text, text, regclass, regclass, name[], name[]) TO cx_attacker;

SET ROLE cx_attacker;
\o
SELECT 'nested TRUNCATE through predicate | ' || cx.attempt($$SELECT sql_saga.add_unique_key('cx_u.t', ARRAY['id'], key_type => 'predicated', predicate => 'cx_u.pwn_truncate(id)')$$);
SELECT 'nested API worker through predicate | ' || cx.attempt($$SELECT sql_saga.add_unique_key('cx_u.t', ARRAY['id'], key_type => 'predicated', predicate => 'cx_u.pwn_worker(id)')$$);
SELECT 'nested FK validator through predicate | ' || cx.attempt($$SELECT sql_saga.add_unique_key('cx_u.t', ARRAY['id'], key_type => 'predicated', predicate => 'cx_u.pwn_read(id)')$$);

\o /dev/null
RESET ROLE;
\o
SELECT 'victim rows and catalogs preserved | ' || (
    (SELECT count(*) FROM cx.victim_t) = 1
    AND NOT EXISTS (SELECT FROM sql_saga.era WHERE table_schema = 'cx_u' AND table_name = 'other')
    AND NOT EXISTS (SELECT FROM sql_saga.unique_keys WHERE table_schema = 'cx_u' AND table_name = 't')
    AND (SELECT count(*) FROM cx_u.stolen) = 0)::text;

\o /dev/null
SET ROLE cx_attacker;
SELECT sql_saga.add_unique_key('cx_u.t', ARRAY['id'], key_type => 'predicated', predicate => 'id > 0', unique_key_name => 'good_pred');
\o
SELECT 'public predicated API positive control | ' || EXISTS (SELECT FROM sql_saga.unique_keys WHERE unique_key_name = 'good_pred')::text;

\o /dev/null
RESET ROLE;
DROP SCHEMA cx_u CASCADE;
DROP SCHEMA cx CASCADE;
DROP OWNED BY cx_attacker, cx_victim;
DROP ROLE cx_attacker, cx_victim;
\i sql/include/test_teardown.sql
