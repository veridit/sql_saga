\i sql/include/test_setup.sql

BEGIN;

/* Run tests as unprivileged user */
SET ROLE TO sql_saga_unprivileged_user;

/* DDL on unrelated tables should not be affected */
CREATE TABLE unrelated();
DROP TABLE unrelated;

/* Make sure nobody drops the objects we keep track of in our catalogs. */
CREATE TYPE integerrange AS RANGE (SUBTYPE = integer);
CREATE TABLE dp (
    id bigint,
    s integer,
    e integer,
    x boolean
);

/* era */
SELECT sql_saga.add_era('dp', 's', 'e', 'p', 'integerrange');
SAVEPOINT s1;
ALTER TABLE dp DROP COLUMN s; -- fails
ROLLBACK TO SAVEPOINT s1;
SAVEPOINT s2;
ALTER TABLE dp ALTER COLUMN s TYPE text; -- fails
ROLLBACK TO SAVEPOINT s2;
SAVEPOINT s3;
DROP TYPE integerrange; -- fails
ROLLBACK TO SAVEPOINT s3;

/* api */
ALTER TABLE dp ADD CONSTRAINT dp_pkey PRIMARY KEY (id);
SELECT sql_saga.add_for_portion_of_view('dp', 'p');
SAVEPOINT s4;
DROP VIEW dp__for_portion_of_p;
ROLLBACK TO SAVEPOINT s4;
SAVEPOINT s5;
DROP TRIGGER for_portion_of_p ON dp__for_portion_of_p;
ROLLBACK TO SAVEPOINT s5;
SAVEPOINT s6;
ALTER TABLE dp DROP CONSTRAINT dp_pkey;
ROLLBACK TO SAVEPOINT s6;
SELECT sql_saga.drop_for_portion_of_view('dp', 'p');
ALTER TABLE dp DROP CONSTRAINT dp_pkey;

/* unique_keys */
ALTER TABLE dp
    ADD CONSTRAINT u UNIQUE (id, s, e) DEFERRABLE,
    ADD CONSTRAINT x EXCLUDE USING gist (id WITH =, integerrange(s, e) WITH &&)  DEFERRABLE;
SELECT sql_saga.add_unique_key('dp', ARRAY['id'], 'p', 'k', 'u', 'x');
SAVEPOINT s7;
ALTER TABLE dp DROP CONSTRAINT u; -- fails
ROLLBACK TO SAVEPOINT s7;
SAVEPOINT s8;
ALTER TABLE dp DROP CONSTRAINT x; -- fails
ROLLBACK TO SAVEPOINT s8;
SAVEPOINT s9;
ALTER TABLE dp DROP CONSTRAINT dp_p_check; -- fails
ROLLBACK TO SAVEPOINT s9;

/* foreign_keys */
CREATE TABLE dp_ref (LIKE dp);
SELECT sql_saga.add_era('dp_ref', 's', 'e', 'p', 'integerrange');
SELECT sql_saga.add_foreign_key('dp_ref', ARRAY['id'], 'p', 'k', foreign_key_name => 'f');
SAVEPOINT s10;
DROP TRIGGER f_fk_insert ON dp_ref; -- fails
ROLLBACK TO SAVEPOINT s10;
SAVEPOINT s11;
DROP TRIGGER f_fk_update ON dp_ref; -- fails
ROLLBACK TO SAVEPOINT s11;
SAVEPOINT s12;
DROP TRIGGER f_uk_update ON dp; -- fails
ROLLBACK TO SAVEPOINT s12;
SAVEPOINT s13;
DROP TRIGGER f_uk_delete ON dp; -- fails
ROLLBACK TO SAVEPOINT s13;
SELECT sql_saga.drop_foreign_key('dp_ref', ARRAY['id'], 'p');
DROP TABLE dp_ref;

SELECT sql_saga.drop_unique_key('dp', ARRAY['id'], 'p');
SELECT sql_saga.drop_era('dp', 'p');
DROP TABLE dp;
DROP TYPE integerrange;

ROLLBACK;

\i sql/include/test_teardown.sql
