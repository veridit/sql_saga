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

/* periods */
SELECT sql_saga.add_era('dp', 's', 'e', 'p', 'integerrange');
DROP TYPE integerrange;

/* api */
ALTER TABLE dp ADD CONSTRAINT dp_pkey PRIMARY KEY (id);
SELECT sql_saga.add_api('dp', 'p');
DROP VIEW dp__for_portion_of_p;
DROP TRIGGER for_portion_of_p ON dp__for_portion_of_p;
ALTER TABLE dp DROP CONSTRAINT dp_pkey;
SELECT sql_saga.drop_api('dp', 'p');
ALTER TABLE dp DROP CONSTRAINT dp_pkey;

/* unique_keys */
ALTER TABLE dp
    ADD CONSTRAINT u UNIQUE (id, s, e) DEFERRABLE,
    ADD CONSTRAINT x EXCLUDE USING gist (id WITH =, integerrange(s, e) WITH &&)  DEFERRABLE;
SELECT sql_saga.add_unique_key('dp', ARRAY['id'], 'p', 'k', 'u', 'x');
ALTER TABLE dp DROP CONSTRAINT u; -- fails
ALTER TABLE dp DROP CONSTRAINT x; -- fails
ALTER TABLE dp DROP CONSTRAINT dp_p_check; -- fails

/* foreign_keys */
CREATE TABLE dp_ref (LIKE dp);
SELECT sql_saga.add_era('dp_ref', 's', 'e', 'p', 'integerrange');
SELECT sql_saga.add_foreign_key('dp_ref', ARRAY['id'], 'p', 'k', foreign_key_name => 'f');
DROP TRIGGER f_fk_insert ON dp_ref; -- fails
DROP TRIGGER f_fk_update ON dp_ref; -- fails
DROP TRIGGER f_uk_update ON dp; -- fails
DROP TRIGGER f_uk_delete ON dp; -- fails
SELECT sql_saga.drop_foreign_key('dp_ref', 'f');
DROP TABLE dp_ref;

DROP TABLE dp;
DROP TYPE integerrange;
