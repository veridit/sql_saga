\set ECHO none
\i sql/include/test_setup.sql
\pset format unaligned
\pset tuples_only on
\set SHOW_CONTEXT never
\set VERBOSITY terse
SET client_min_messages = warning;
\o /dev/null

/*
 * Release gate (1.4): sql_saga allows and refuses exactly what PostgreSQL does, for every kind of user.
 *
 * Every scenario runs twice on identical data: once against sql_saga's object, once against the native equivalent.
 * Each line prints both outcomes and whether they are equal; the expected output has "same" on every line, and every
 * deliberate difference is listed at the end and in the README.
 *
 * A. Management functions against the DDL they stand for (add_era/add_unique_key/add_foreign_key against
 *    ALTER TABLE ADD COLUMN / ADD CONSTRAINT UNIQUE / ADD FOREIGN KEY), for: superuser, owner, inheriting member,
 *    NOINHERIT member, NOINHERIT member after SET ROLE, GRANT ALL non-owner, SELECT-only, no rights, pg_maintain,
 *    SECURITY DEFINER wrapper owned by the owner (called by another role), wrapper owned by another role (called by
 *    the owner). A foreign key also needs REFERENCES on the referenced table.
 * B. Writes against a regular foreign key and a native foreign key with ON DELETE CASCADE, side by side: child
 *    INSERT of a present and an absent key, parent DELETE (cascade), parent TRUNCATE, by roles with and without
 *    rights on the other table, with row-level security (also FORCE) on parent and child, and with column
 *    privileges only.
 */

CREATE ROLE pp_owner;
CREATE ROLE pp_member IN ROLE pp_owner;
CREATE ROLE pp_noinherit NOINHERIT IN ROLE pp_owner;
CREATE ROLE pp_allprivs;
CREATE ROLE pp_reader;
CREATE ROLE pp_nobody;
CREATE ROLE pp_maint IN ROLE pg_maintain;
CREATE ROLE pp_writer;
CREATE ROLE pp_colwriter;
GRANT USAGE ON SCHEMA sql_saga TO pp_owner, pp_member, pp_noinherit, pp_allprivs, pp_reader, pp_nobody, pp_maint, pp_writer, pp_colwriter;
CREATE SCHEMA par AUTHORIZATION pp_owner;
GRANT USAGE ON SCHEMA par TO pp_member, pp_noinherit, pp_allprivs, pp_reader, pp_nobody, pp_maint, pp_writer, pp_colwriter;
GRANT CREATE ON SCHEMA par TO pp_allprivs;

-- Runs a statement as a role in a subtransaction and reports the outcome; never changes anything.
CREATE FUNCTION par.try(r text, stmt text) RETURNS text LANGUAGE plpgsql AS $try$
DECLARE
    res text;
BEGIN
    BEGIN
        IF r <> 'superuser' THEN
            EXECUTE format('SET LOCAL ROLE %I', r);
        END IF;
        EXECUTE stmt;
        res := 'allowed';
        RAISE EXCEPTION USING ERRCODE = 'P0099';
    EXCEPTION
        WHEN SQLSTATE 'P0099' THEN NULL;
        WHEN OTHERS THEN res := 'refused ' || SQLSTATE;
    END;
    RESET ROLE;
    RETURN res;
END;
$try$;
-- The same, keeping the change (for data checks), in the caller's transaction.
CREATE FUNCTION par.run(r text, stmt text) RETURNS text LANGUAGE plpgsql AS $run$
DECLARE
    res text;
    n bigint;
BEGIN
    BEGIN
        IF r <> 'superuser' THEN
            EXECUTE format('SET LOCAL ROLE %I', r);
        END IF;
        EXECUTE stmt;
        GET DIAGNOSTICS n = ROW_COUNT;
        res := 'allowed (' || n || ' rows)';
    EXCEPTION
        WHEN OTHERS THEN res := 'refused ' || SQLSTATE;
    END;
    RESET ROLE;
    RETURN res;
END;
$run$;
CREATE FUNCTION par.cmp(label text, saga text, native text) RETURNS text LANGUAGE sql AS $cmp$
    SELECT label || ' | ' || CASE WHEN saga = native THEN 'same: ' || saga ELSE 'DIFFERENT: sql_saga ' || saga || ', native ' || native END;
$cmp$;

SET ROLE pp_owner;
-- A: one table per management function, pairs for sql_saga and native.
CREATE TABLE par.s_era (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
CREATE TABLE par.n_era (id int NOT NULL);
CREATE TABLE par.s_key (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('par.s_key', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
CREATE TABLE par.n_key (id int NOT NULL, valid_range daterange NOT NULL);
-- Parents for foreign keys: temporal (sql_saga) and plain (native).
CREATE TABLE par.s_parent (id int NOT NULL, valid_range daterange NOT NULL, valid_from date, valid_until date);
SELECT sql_saga.add_era('par.s_parent', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key('par.s_parent', ARRAY['id'], key_type => 'primary', unique_key_name => 's_parent_pk');
CREATE TABLE par.n_parent (id int PRIMARY KEY);
GRANT ALL ON ALL TABLES IN SCHEMA par TO pp_allprivs;
GRANT SELECT ON ALL TABLES IN SCHEMA par TO pp_reader, pp_maint;
RESET ROLE;

-- SECURITY DEFINER wrappers: owned by the table owner, and owned by another role.
CREATE FUNCTION par.owner_wrapper(stmt text) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN EXECUTE stmt; END $$;
ALTER FUNCTION par.owner_wrapper(text) OWNER TO pp_owner;
CREATE FUNCTION par.other_wrapper(stmt text) RETURNS void LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN EXECUTE stmt; END $$;
ALTER FUNCTION par.other_wrapper(text) OWNER TO pp_allprivs;
GRANT EXECUTE ON FUNCTION par.owner_wrapper(text), par.other_wrapper(text) TO PUBLIC;

-- Callers: (label, role, how). how = direct | setrole:<role> | owner_wrapper | other_wrapper.
CREATE TEMP TABLE caller (n int, label text, r text, how text);
INSERT INTO caller VALUES
    (1, 'superuser', 'superuser', 'direct'),
    (2, 'owner', 'pp_owner', 'direct'),
    (3, 'inheriting member of owner', 'pp_member', 'direct'),
    (4, 'NOINHERIT member, no SET ROLE', 'pp_noinherit', 'direct'),
    (5, 'NOINHERIT member, SET ROLE owner', 'pp_noinherit', 'setrole:pp_owner'),
    (6, 'GRANT ALL, not owner', 'pp_allprivs', 'direct'),
    (7, 'SELECT only', 'pp_reader', 'direct'),
    (8, 'no rights', 'pp_nobody', 'direct'),
    (9, 'pg_maintain member', 'pp_maint', 'direct'),
    (10, 'wrapper owned by owner, called by no-rights role', 'pp_nobody', 'owner_wrapper'),
    (11, 'wrapper owned by non-owner, called by owner', 'pp_owner', 'other_wrapper');
GRANT SELECT ON caller TO PUBLIC;

-- Wraps a statement for a caller.
CREATE FUNCTION par.as_caller(how text, stmt text) RETURNS text LANGUAGE sql AS $as_caller$
    SELECT CASE
        WHEN how = 'direct' THEN stmt
        WHEN how LIKE 'setrole:%' THEN format('SET LOCAL ROLE %I; %s', substr(how, 9), stmt)
        WHEN how = 'owner_wrapper' THEN format('SELECT par.owner_wrapper(%L)', stmt)
        WHEN how = 'other_wrapper' THEN format('SELECT par.other_wrapper(%L)', stmt)
    END;
$as_caller$;

\o
\echo '--- A: management functions against the DDL they stand for'
SELECT par.cmp(c.label || ': add_era vs ALTER TABLE ADD COLUMN',
    par.try(c.r, par.as_caller(c.how, $$SELECT sql_saga.add_era('par.s_era', 'valid_range', 'valid', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until')$$)),
    par.try(c.r, par.as_caller(c.how, $$ALTER TABLE par.n_era ADD COLUMN valid_range daterange$$)))
FROM caller AS c ORDER BY c.n;
SELECT par.cmp(c.label || ': add_unique_key vs ADD CONSTRAINT UNIQUE',
    par.try(c.r, par.as_caller(c.how, $$SELECT sql_saga.add_unique_key('par.s_key', ARRAY['id'], key_type => 'natural')$$)),
    par.try(c.r, par.as_caller(c.how, $$ALTER TABLE par.n_key ADD UNIQUE (id, valid_range WITHOUT OVERLAPS)$$)))
FROM caller AS c ORDER BY c.n;

-- System versioning (review round 3, Q1): add_system_versioning stands for ALTER TABLE <base> (the era column and
-- triggers) plus CREATE TABLE <history> (LIKE <base>) in the base table's schema, given to the base owner; adopting an
-- existing history stands for ALTER TABLE <history> OWNER TO <base owner> instead of the CREATE.
\o /dev/null
SET ROLE pp_owner;
CREATE TABLE par.s_sv (id int);
CREATE TABLE par.n_sv (id int);
CREATE TABLE par.s_sv_adopt (id int);
RESET ROLE;
-- An existing table owned by another role, named as the history, against the native takeover of it.
CREATE TABLE par.foreign_history (id int, system_valid_range tstzrange);
ALTER TABLE par.foreign_history OWNER TO pp_allprivs;
GRANT ALL ON par.s_sv, par.n_sv, par.s_sv_adopt TO pp_allprivs;
\o
SELECT par.cmp(c.label || ': add_system_versioning vs CREATE TABLE (LIKE) in the schema',
    par.try(c.r, par.as_caller(c.how, $$SELECT sql_saga.add_system_versioning('par.s_sv')$$)),
    par.try(c.r, par.as_caller(c.how, $$ALTER TABLE par.n_sv ADD COLUMN system_valid_range tstzrange; CREATE TABLE par.n_sv_history (LIKE par.n_sv); ALTER TABLE par.n_sv_history OWNER TO pp_owner$$)))
FROM caller AS c ORDER BY c.n;
SELECT par.cmp(c.label || ': adopting another role''s history vs ALTER TABLE OWNER TO',
    par.try(c.r, par.as_caller(c.how, $$SELECT sql_saga.add_system_versioning('par.s_sv_adopt', history_table_name => 'foreign_history')$$)),
    par.try(c.r, par.as_caller(c.how, $$ALTER TABLE par.n_sv ADD COLUMN adopt_probe int; ALTER TABLE par.foreign_history OWNER TO pp_owner$$)))
FROM caller AS c ORDER BY c.n;

-- Foreign keys: the caller's own child table (so ownership of the child holds), parent owned by pp_owner.
\o /dev/null
CREATE TABLE par.s_child_all (id int, parent_id int);
CREATE TABLE par.n_child_all (id int, parent_id int);
ALTER TABLE par.s_child_all OWNER TO pp_allprivs;
ALTER TABLE par.n_child_all OWNER TO pp_allprivs;
\o
SELECT par.cmp('child owner without REFERENCES on the parent: add_foreign_key vs ADD FOREIGN KEY',
    par.try('pp_allprivs', $$SELECT sql_saga.add_foreign_key('par.s_child_all', ARRAY['parent_id'], 'par.s_parent', ARRAY['id'])$$),
    par.try('pp_allprivs', $$ALTER TABLE par.n_child_all ADD FOREIGN KEY (parent_id) REFERENCES par.n_parent (id)$$));
\o /dev/null
REVOKE REFERENCES ON par.s_parent, par.n_parent FROM pp_allprivs;
\o
SELECT par.cmp('child owner, REFERENCES revoked: add_foreign_key vs ADD FOREIGN KEY',
    par.try('pp_allprivs', $$SELECT sql_saga.add_foreign_key('par.s_child_all', ARRAY['parent_id'], 'par.s_parent', ARRAY['id'])$$),
    par.try('pp_allprivs', $$ALTER TABLE par.n_child_all ADD FOREIGN KEY (parent_id) REFERENCES par.n_parent (id)$$));
SELECT par.cmp('parent owner, child owned by another role: add_foreign_key vs ADD FOREIGN KEY',
    par.try('pp_owner', $$SELECT sql_saga.add_foreign_key('par.s_child_all', ARRAY['parent_id'], 'par.s_parent', ARRAY['id'])$$),
    par.try('pp_owner', $$ALTER TABLE par.n_child_all ADD FOREIGN KEY (parent_id) REFERENCES par.n_parent (id)$$));

\echo '--- B: writes against a regular foreign key and a native one, side by side'
\o /dev/null
SET ROLE pp_owner;
CREATE TABLE par.s_child (id int PRIMARY KEY, parent_id int, secret text);
CREATE TABLE par.n_child (id int PRIMARY KEY, parent_id int, secret text);
SELECT sql_saga.add_foreign_key('par.s_child', ARRAY['parent_id'], 'par.s_parent', ARRAY['id'], delete_action => 'CASCADE');
ALTER TABLE par.n_child ADD FOREIGN KEY (parent_id) REFERENCES par.n_parent (id) ON DELETE CASCADE;
INSERT INTO par.s_parent (id, valid_from, valid_until) SELECT g, '2020-01-01', 'infinity' FROM generate_series(1, 6) AS g;
INSERT INTO par.n_parent SELECT generate_series(1, 6);
INSERT INTO par.s_child VALUES (1, 1, 'x'), (2, 2, 'x'), (3, 3, 'x');
INSERT INTO par.n_child VALUES (1, 1, 'x'), (2, 2, 'x'), (3, 3, 'x');
-- pp_writer: writes on each table, no rights on the other one.
GRANT SELECT, INSERT, DELETE, TRUNCATE ON par.s_parent, par.n_parent TO pp_writer;
-- pp_colwriter: INSERT on the child's columns only, nothing on the parent.
GRANT INSERT (id, parent_id) ON par.s_child, par.n_child TO pp_colwriter;
-- Row-level security on both parents and children: the policies hide everything from non-owners.
ALTER TABLE par.s_parent ENABLE ROW LEVEL SECURITY;
ALTER TABLE par.n_parent ENABLE ROW LEVEL SECURITY;
ALTER TABLE par.s_child ENABLE ROW LEVEL SECURITY;
ALTER TABLE par.n_child ENABLE ROW LEVEL SECURITY;
CREATE POLICY nothing ON par.s_parent USING (false) WITH CHECK (true);
CREATE POLICY nothing ON par.n_parent USING (false) WITH CHECK (true);
CREATE POLICY ins ON par.s_child FOR INSERT WITH CHECK (true);
CREATE POLICY ins ON par.n_child FOR INSERT WITH CHECK (true);
CREATE POLICY del ON par.s_parent FOR DELETE USING (true);
CREATE POLICY del ON par.n_parent FOR DELETE USING (true);
RESET ROLE;
\o
SELECT par.cmp('column-privilege writer, child insert of a present key (parent unreadable, RLS hides it)',
    par.try('pp_colwriter', $$INSERT INTO par.s_child (id, parent_id) VALUES (10, 4)$$),
    par.try('pp_colwriter', $$INSERT INTO par.n_child (id, parent_id) VALUES (10, 4)$$));
SELECT par.cmp('column-privilege writer, child insert of an absent key',
    par.try('pp_colwriter', $$INSERT INTO par.s_child (id, parent_id) VALUES (11, 99)$$),
    par.try('pp_colwriter', $$INSERT INTO par.n_child (id, parent_id) VALUES (11, 99)$$));
SELECT par.cmp('parent writer without child rights, delete a referenced parent (cascade, RLS on the child)',
    par.try('pp_writer', $$DELETE FROM par.s_parent WHERE id = 1$$),
    par.try('pp_writer', $$DELETE FROM par.n_parent WHERE id = 1$$));
SELECT par.cmp('parent writer, TRUNCATE the referenced parent alone',
    par.try('pp_writer', $$TRUNCATE par.s_parent$$),
    par.try('pp_writer', $$TRUNCATE par.n_parent$$));
\o /dev/null
ALTER TABLE par.s_parent FORCE ROW LEVEL SECURITY;
ALTER TABLE par.n_parent FORCE ROW LEVEL SECURITY;
ALTER TABLE par.s_child FORCE ROW LEVEL SECURITY;
ALTER TABLE par.n_child FORCE ROW LEVEL SECURITY;
\o
SELECT par.cmp('FORCE RLS, owner inserts a child of a present key (policy hides the parent from the owner too)',
    par.try('pp_owner', $$INSERT INTO par.s_child (id, parent_id) VALUES (12, 5)$$),
    par.try('pp_owner', $$INSERT INTO par.n_child (id, parent_id) VALUES (12, 5)$$));
SELECT par.cmp('FORCE RLS, parent writer deletes an invisible parent (no row matches, as natively)',
    par.try('pp_writer', $$DELETE FROM par.s_parent WHERE id = 2$$),
    par.try('pp_writer', $$DELETE FROM par.n_parent WHERE id = 2$$));
-- A policy that lets the writer see and delete parent 3: the delete happens, and the cascade reaches a FORCE RLS
-- child that the writer cannot see at all. Native referential actions bypass row-level security; so must sql_saga.
\o /dev/null
CREATE POLICY see3 ON par.s_parent FOR SELECT USING (id = 3);
CREATE POLICY see3 ON par.n_parent FOR SELECT USING (id = 3);
\o
-- par.run's subtransaction is kept (only the outer transaction would roll it back), so count inside the same
-- function call: the counts below are taken by the superuser after both deletes in this statement's snapshot.
SELECT 'FORCE RLS, visible parent 3 deleted, cascade into an invisible child | sql_saga ' ||
    par.run('pp_writer', $$DELETE FROM par.s_parent WHERE id = 3$$) || ', native ' || par.run('pp_writer', $$DELETE FROM par.n_parent WHERE id = 3$$);
SELECT 'after the cascade | parent 3 left: sql_saga ' || (SELECT count(*) FROM par.s_parent WHERE id = 3) || ', native ' || (SELECT count(*) FROM par.n_parent WHERE id = 3)
    || ' | child 3 left: sql_saga ' || (SELECT count(*) FROM par.s_child WHERE id = 3) || ', native ' || (SELECT count(*) FROM par.n_child WHERE id = 3);

\echo '--- deliberate differences (README)'
SELECT 'TRUNCATE of a parent with no referencing child row | ' ||
    par.run('superuser', $$DELETE FROM par.s_child; DELETE FROM par.n_child$$) || ' then sql_saga ' ||
    par.try('superuser', $$TRUNCATE par.s_parent$$) || ', native ' || par.try('superuser', $$TRUNCATE par.n_parent$$);

\o /dev/null
RESET ROLE;
DROP SCHEMA par CASCADE;
DROP OWNED BY pp_member, pp_noinherit, pp_reader, pp_nobody, pp_maint, pp_writer, pp_colwriter, pp_allprivs, pp_owner;
DROP ROLE pp_member, pp_noinherit, pp_reader, pp_nobody, pp_maint, pp_writer, pp_colwriter;
DROP ROLE pp_allprivs, pp_owner;
\i sql/include/test_teardown.sql
