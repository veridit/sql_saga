/* Run tests as unprivileged user */
SET ROLE TO sql_saga_unprivileged_user;

/* DDL on unrelated tables should not be affected */
CREATE UNLOGGED TABLE unrelated();
ALTER TABLE unrelated SET LOGGED;
ALTER TABLE unrelated SET UNLOGGED;
DROP TABLE unrelated;

/* Ensure tables with periods are persistent */
CREATE UNLOGGED TABLE log (id bigint, valid_from date, valid_until date);
SELECT sql_saga.add_era('log', 'valid_from', 'valid_until', 'p'); -- fails
ALTER TABLE log SET LOGGED;
SELECT sql_saga.add_era('log', 'valid_from', 'valid_until', 'p'); -- passes
ALTER TABLE log SET UNLOGGED; -- fails
DROP TABLE log;
