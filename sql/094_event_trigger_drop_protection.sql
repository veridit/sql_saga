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
    v integerrange NOT NULL,
    s integer,
    e integer,
    x boolean
);

/* era */
SELECT sql_saga.add_era('dp', 'v', 'p', valid_from_column_name => 's', valid_until_column_name => 'e');
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

CREATE TABLE dp_current (
    id bigint,
    v daterange NOT NULL,
    s date NOT NULL,
    e date NOT NULL
);
ALTER TABLE dp_current ADD CONSTRAINT dp_current_pkey PRIMARY KEY (id, v WITHOUT OVERLAPS);
SELECT sql_saga.add_era('dp_current', 'v', 'p', valid_from_column_name => 's', valid_until_column_name => 'e');
SELECT sql_saga.add_current_view('dp_current', 'p');
SAVEPOINT s_current_view;
DROP VIEW dp_current__current_p; -- fails
ROLLBACK TO SAVEPOINT s_current_view;
SAVEPOINT s_current_trigger;
DROP TRIGGER current_p ON dp_current__current_p; -- fails
ROLLBACK TO SAVEPOINT s_current_trigger;
SAVEPOINT s_current_pkey;
ALTER TABLE dp_current DROP CONSTRAINT dp_current_pkey; -- fails
ROLLBACK TO SAVEPOINT s_current_pkey;
SELECT sql_saga.drop_current_view('dp_current', 'p');
ALTER TABLE dp_current DROP CONSTRAINT dp_current_pkey;
SELECT sql_saga.drop_era('dp_current', 'p');
DROP TABLE dp_current;

/* unique_keys */
ALTER TABLE dp
    ADD CONSTRAINT u UNIQUE (id, v WITHOUT OVERLAPS);
SELECT sql_saga.add_unique_key(
    table_oid => 'dp'::regclass,
    column_names => ARRAY['id'],
    era_name => 'p',
    unique_key_name => 'k',
    unique_constraint => 'u'
);
SAVEPOINT s7;
ALTER TABLE dp DROP CONSTRAINT u; -- fails
ROLLBACK TO SAVEPOINT s7;
SAVEPOINT s9;
ALTER TABLE dp DROP CONSTRAINT dp_p_check; -- fails (bounds check constraint)
ROLLBACK TO SAVEPOINT s9;

SAVEPOINT s10;
/* unique_keys - pk_consistency */
CREATE TABLE dp_pk_consistency (
    id int,
    nk text,
    v daterange NOT NULL,
    s date,
    e date
);
SELECT sql_saga.add_era('dp_pk_consistency', 'v', 'p', valid_from_column_name => 's', valid_until_column_name => 'e');
ALTER TABLE dp_pk_consistency ADD PRIMARY KEY (id);
SELECT sql_saga.add_unique_key(
    table_oid => 'dp_pk_consistency',
    column_names => ARRAY['nk'],
    era_name => 'p',
    key_type => 'natural',
    unique_key_name => 'dp_pk_consistency_nk_valid'
);

SAVEPOINT expect_error;
ALTER TABLE dp_pk_consistency DROP CONSTRAINT dp_pk_consistency_nk_valid_pk_consistency_excl; -- fails
ROLLBACK TO SAVEPOINT expect_error;

SELECT sql_saga.drop_unique_key('dp_pk_consistency', ARRAY['nk'], 'p');
SELECT sql_saga.drop_era('dp_pk_consistency', 'p');
ROLLBACK TO SAVEPOINT s10;


SAVEPOINT s11;
/* foreign_keys */
CREATE TABLE dp_ref (LIKE dp);
SELECT sql_saga.add_era('dp_ref', 'v', 'p', valid_from_column_name => 's', valid_until_column_name => 'e');
SELECT sql_saga.add_temporal_foreign_key('dp_ref', ARRAY['id'], 'p', 'k', foreign_key_name => 'f');
-- Note: In the new API with native PostgreSQL 18 temporal FKs, there are no FK triggers to test dropping.
SELECT sql_saga.drop_foreign_key('dp_ref', ARRAY['id'], 'p');
DROP TABLE dp_ref;

SELECT sql_saga.drop_unique_key('dp', ARRAY['id'], 'p');
SELECT sql_saga.drop_era('dp', 'p');
ROLLBACK TO SAVEPOINT s11;

SAVEPOINT system_versioning_tests;
/* system_versioning - test drop protection for system-versioned tables */
CREATE TABLE dp_sysver (id int PRIMARY KEY, name text);
SELECT sql_saga.add_system_versioning('dp_sysver');

-- Test that we can't drop the history table directly
SAVEPOINT sv_history_table;
DROP TABLE dp_sysver_history; -- should fail
ROLLBACK TO SAVEPOINT sv_history_table;

-- Test that we can't drop the history view directly
SAVEPOINT sv_history_view;
DROP VIEW dp_sysver_with_history; -- should fail
ROLLBACK TO SAVEPOINT sv_history_view;

-- Test that we can't drop the generated_always trigger
SAVEPOINT sv_gen_trigger;
DROP TRIGGER dp_sysver_system_time_generated_always ON dp_sysver; -- should fail
ROLLBACK TO SAVEPOINT sv_gen_trigger;

-- Test that we can't drop the write_history trigger
SAVEPOINT sv_write_trigger;
DROP TRIGGER dp_sysver_system_time_write_history ON dp_sysver; -- should fail
ROLLBACK TO SAVEPOINT sv_write_trigger;

-- Test that we can't drop the truncate trigger
SAVEPOINT sv_truncate_trigger;
DROP TRIGGER dp_sysver_truncate ON dp_sysver; -- should fail
ROLLBACK TO SAVEPOINT sv_truncate_trigger;

-- Test that we can't drop the infinity check constraint
SAVEPOINT sv_infinity_check;
ALTER TABLE dp_sysver DROP CONSTRAINT dp_sysver_system_valid_range_infinity_check; -- should fail
ROLLBACK TO SAVEPOINT sv_infinity_check;

-- Test proper cleanup works
SELECT sql_saga.drop_system_versioning('dp_sysver');
DROP TABLE dp_sysver;
ROLLBACK TO SAVEPOINT system_versioning_tests;

ROLLBACK;

\i sql/include/test_teardown.sql
