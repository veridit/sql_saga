\i sql/include/test_setup.sql

BEGIN;

/* Run tests as unprivileged user */
SET ROLE TO sql_saga_unprivileged_user;

/* DDL on unrelated tables should not be affected */
CREATE UNLOGGED TABLE unrelated();
ALTER TABLE unrelated SET LOGGED;
ALTER TABLE unrelated SET UNLOGGED;
DROP TABLE unrelated;

/* Ensure tables with periods are persistent */
CREATE UNLOGGED TABLE log (id bigint, valid_range daterange NOT NULL, valid_from date, valid_until date);
SAVEPOINT s1;
SELECT sql_saga.add_era('log', 'valid_range', 'p', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until'); -- fails
ROLLBACK TO SAVEPOINT s1;
ALTER TABLE log SET LOGGED;
SELECT sql_saga.add_era('log', 'valid_range', 'p', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until'); -- passes
SAVEPOINT s2;
ALTER TABLE log SET UNLOGGED; -- fails
ROLLBACK TO SAVEPOINT s2;
DROP TABLE log;

ROLLBACK;

\i sql/include/test_teardown.sql
