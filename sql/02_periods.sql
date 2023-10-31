/* Run tests as unprivileged user */
SET ROLE TO sql_saga_unprivileged_user;

/* Basic period definitions with dates */
CREATE TABLE basic (val text, s date, e date);
TABLE sql_saga.era;
SELECT sql_saga.add_era('basic', 's', 'e', 'bp');
TABLE sql_saga.era;
SELECT sql_saga.drop_era('basic', 'bp');
TABLE sql_saga.era;
SELECT sql_saga.add_era('basic', 's', 'e', 'bp', bounds_check_constraint => 'c');
TABLE sql_saga.era;
SELECT sql_saga.drop_era('basic', 'bp', purge => true);
TABLE sql_saga.era;
SELECT sql_saga.add_era('basic', 's', 'e', 'bp');
TABLE sql_saga.era;
/* Test constraints */
INSERT INTO basic (val, s, e) VALUES ('x', null, null); --fail
INSERT INTO basic (val, s, e) VALUES ('x', '3000-01-01', null); --fail
INSERT INTO basic (val, s, e) VALUES ('x', null, '1000-01-01'); --fail
INSERT INTO basic (val, s, e) VALUES ('x', '3000-01-01', '1000-01-01'); --fail
INSERT INTO basic (val, s, e) VALUES ('x', '1000-01-01', '3000-01-01'); --success
TABLE basic;
/* Test dropping the whole thing */
DROP TABLE basic;
TABLE sql_saga.era;

