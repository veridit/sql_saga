\i sql/include/test_setup.sql

BEGIN;

/* Run tests as unprivileged user */
SET ROLE TO sql_saga_unprivileged_user;

/*
 * Create a sequence to test non-serial primary keys.  This actually tests
 * things like uuid primary keys, but makes for reproducible test cases.
 */
CREATE SEQUENCE pricing_seq;

CREATE TABLE pricing (id1 bigserial,
                      id2 bigint PRIMARY KEY DEFAULT nextval('pricing_seq'),
                      id3 bigint GENERATED ALWAYS AS IDENTITY,
                      id4 bigint GENERATED ALWAYS AS (id1 + id2) STORED,
                      product text, quantity_from integer, quantity_until integer, price numeric);
SELECT sql_saga.add_era('pricing', 'quantity_from', 'quantity_until', 'quantities');
SELECT sql_saga.add_api('pricing', 'quantities');
TABLE sql_saga.api_view;
/* Test UPDATE FOR PORTION */
INSERT INTO pricing (product, quantity_from, quantity_until, price) VALUES ('Trinket', 2, 21, 200);
TABLE pricing ORDER BY quantity_from;
-- UPDATE fully preceding
UPDATE pricing__for_portion_of_quantities SET quantity_from = 1, quantity_until = 2, price = 0;
TABLE pricing ORDER BY quantity_from;
-- UPDATE fully succeeding
UPDATE pricing__for_portion_of_quantities SET quantity_from = 31, quantity_until = 51, price = 0;
TABLE pricing ORDER BY quantity_from;
-- UPDATE fully surrounding
UPDATE pricing__for_portion_of_quantities SET quantity_from = 1, quantity_until = 101, price = 100;
TABLE pricing ORDER BY quantity_from;
-- UPDATE portion
UPDATE pricing__for_portion_of_quantities SET quantity_from = 11, quantity_until = 21, price = 80;
TABLE pricing ORDER BY quantity_from;
-- UPDATE portion of multiple rows
UPDATE pricing__for_portion_of_quantities SET quantity_from = 5, quantity_until = 15, price = 90;
TABLE pricing ORDER BY quantity_from;
-- If we drop the period (without CASCADE) then the FOR PORTION views should be
-- dropped, too.
SELECT sql_saga.drop_era('pricing', 'quantities');
TABLE sql_saga.api_view;
-- Add it back to test the drop_for_portion_view function
SELECT sql_saga.add_era('pricing', 'quantity_from', 'quantity_until', 'quantities');
SELECT sql_saga.add_api('pricing', 'quantities');
-- We can't drop the the table without first dropping the FOR PORTION views
-- because Postgres will complain about dependant objects (our views) before we
-- get a chance to clean them up.
SAVEPOINT expect_fail;
DROP TABLE pricing;
ROLLBACK TO SAVEPOINT expect_fail;
SELECT sql_saga.drop_api('pricing', NULL);
TABLE sql_saga.api_view;
SELECT sql_saga.drop_era('pricing', 'quantities');
DROP TABLE pricing;
DROP SEQUENCE pricing_seq;

/* Types without btree must be excluded, too */
CREATE TABLE bt (
    id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    pt point,   -- something without btree
    t text,     -- something with btree
    valid_from integer,
    valid_until integer
);
SELECT sql_saga.add_era('bt', 'valid_from', 'valid_until', 'p');
SELECT sql_saga.add_api('bt', 'p');

INSERT INTO bt (pt, t, valid_from, valid_until) VALUES ('(0, 0)', 'sample', 10, 41);
TABLE bt ORDER BY valid_from, valid_until;
UPDATE bt__for_portion_of_p SET t = 'simple', valid_from = 21, valid_until = 31;
TABLE bt ORDER BY valid_from, valid_until;

SELECT sql_saga.drop_api('bt', 'p');
SELECT sql_saga.drop_era('bt', 'p');
DROP TABLE bt;

ROLLBACK;

\i sql/include/test_teardown.sql
