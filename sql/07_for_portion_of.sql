SELECT setting::integer < 120000 AS pre_12
FROM pg_settings WHERE name = 'server_version_num';

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
                      product text, min_quantity integer, max_quantity integer, price numeric);
CREATE TABLE pricing (id1 bigserial,
                      id2 bigint PRIMARY KEY DEFAULT nextval('pricing_seq'),
                      id3 bigint GENERATED ALWAYS AS IDENTITY,
                      product text, min_quantity integer, max_quantity integer, price numeric);
CREATE TABLE pricing (id1 bigserial,
                      id2 bigint PRIMARY KEY DEFAULT nextval('pricing_seq'),
                      product text, min_quantity integer, max_quantity integer, price numeric);
SELECT sql_saga.add_era('pricing', 'min_quantity', 'max_quantity', 'quantities');
SELECT sql_saga.add_api('pricing', 'quantities');
TABLE sql_saga.api_view;
/* Test UPDATE FOR PORTION */
INSERT INTO pricing (product, min_quantity, max_quantity, price) VALUES ('Trinket', 1, 20, 200);
TABLE pricing ORDER BY min_quantity;
-- UPDATE fully preceding
UPDATE pricing__for_portion_of_quantities SET min_quantity = 0, max_quantity = 1, price = 0;
TABLE pricing ORDER BY min_quantity;
-- UPDATE fully succeeding
UPDATE pricing__for_portion_of_quantities SET min_quantity = 30, max_quantity = 50, price = 0;
TABLE pricing ORDER BY min_quantity;
-- UPDATE fully surrounding
UPDATE pricing__for_portion_of_quantities SET min_quantity = 0, max_quantity = 100, price = 100;
TABLE pricing ORDER BY min_quantity;
-- UPDATE portion
UPDATE pricing__for_portion_of_quantities SET min_quantity = 10, max_quantity = 20, price = 80;
TABLE pricing ORDER BY min_quantity;
-- UPDATE portion of multiple rows
UPDATE pricing__for_portion_of_quantities SET min_quantity = 5, max_quantity = 15, price = 90;
TABLE pricing ORDER BY min_quantity;
-- If we drop the period (without CASCADE) then the FOR PORTION views should be
-- dropped, too.
SELECT sql_saga.drop_era('pricing', 'quantities');
TABLE sql_saga.api_view;
-- Add it back to test the drop_for_portion_view function
SELECT sql_saga.add_era('pricing', 'min_quantity', 'max_quantity', 'quantities');
SELECT sql_saga.add_api('pricing', 'quantities');
-- We can't drop the the table without first dropping the FOR PORTION views
-- because Postgres will complain about dependant objects (our views) before we
-- get a chance to clean them up.
DROP TABLE pricing;
SELECT sql_saga.drop_api('pricing', NULL);
TABLE sql_saga.api_view;
DROP TABLE pricing;
DROP SEQUENCE pricing_seq;

/* Types without btree must be excluded, too */
-- v10+
CREATE TABLE bt (
    id integer PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    pt point,   -- something without btree
    t text,     -- something with btree
    s integer,
    e integer
);
-- pre v10
CREATE TABLE bt (
    id serial PRIMARY KEY,
    pt point,   -- something without btree
    t text,     -- something with btree
    s integer,
    e integer
);
SELECT sql_saga.add_era('bt', 's', 'e', 'p');
SELECT sql_saga.add_api('bt', 'p');

INSERT INTO bt (pt, t, s, e) VALUES ('(0, 0)', 'sample', 10, 40);
TABLE bt ORDER BY s, e;
UPDATE bt__for_portion_of_p SET t = 'simple', s = 20, e = 30;
TABLE bt ORDER BY s, e;

SELECT sql_saga.drop_api('bt', 'p');
DROP TABLE bt;
