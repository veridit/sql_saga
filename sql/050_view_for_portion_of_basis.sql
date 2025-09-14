\i sql/include/test_setup.sql

BEGIN;

CREATE TABLE products (
    id int,
    name text,
    price numeric,
    valid_from date,
    valid_until date
);

-- Register era and unique key
SELECT sql_saga.add_era('products', 'valid_from', 'valid_until');
SELECT sql_saga.add_unique_key('products', ARRAY['id']);

-- Populate with some historical data
INSERT INTO products VALUES
(1, 'Laptop', 1200, '2023-01-01', '2024-01-01'),
(1, 'Laptop', 1150, '2024-01-01', 'infinity'),
(2, 'Mouse', 25, '2023-05-01', 'infinity');

TABLE products ORDER BY id, valid_from;

-- Test API Lifecycle
-- ==================

-- Add the for_portion_of view
SELECT sql_saga.add_for_portion_of_view('products'::regclass);
\d products__for_portion_of_valid
TABLE sql_saga.updatable_view;

-- Test DML Semantics
-- ==================

-- Test SELECT
-- Should show the complete history, same as the base table.
CREATE VIEW products_view_select_for_portion_of AS
TABLE products__for_portion_of_valid ORDER BY id, valid_from;
TABLE products_view_select_for_portion_of;

-- Test INSERT (should be disallowed)
SAVEPOINT insert_should_fail;
INSERT INTO products__for_portion_of_valid (id, name, price, valid_from, valid_until)
VALUES (3, 'Keyboard', 75, '2023-01-01', 'infinity');
ROLLBACK TO insert_should_fail;

-- The table should be unchanged.
TABLE products ORDER BY id, valid_from;
TABLE products_view_select_for_portion_of;


-- Test simple UPDATE (Historical correction, should be disallowed)
SAVEPOINT simple_update_should_fail;
UPDATE products__for_portion_of_valid SET price = 1250 WHERE id = 1 AND valid_from = '2023-01-01';
ROLLBACK TO simple_update_should_fail;

-- Price should be unchanged.
TABLE products ORDER BY id, valid_from;
TABLE products_view_select_for_portion_of;


-- Test UPDATE (applying a change to a portion of the timeline)
-- Apply a price change to Laptop for a 3-month period.
-- This should split the second historical record into three distinct parts.
UPDATE products__for_portion_of_valid
SET
    price = 1175,
    valid_from = '2024-03-01', -- Parameter for start of change
    valid_until = '2024-06-01'  -- Parameter for end of change
WHERE id = 1;

-- The timeline for Laptop should now have 4 distinct periods.
TABLE products ORDER BY id, valid_from;
TABLE products_view_select_for_portion_of;


-- Test UPDATE (spanning multiple historical records)
-- This should shorten two records and insert a new one in the middle.
-- To get a clean state, we delete and re-insert product 1's history.
DELETE FROM products WHERE id = 1;
INSERT INTO products VALUES
(1, 'Laptop', 1200, '2023-01-01', '2024-01-01'),
(1, 'Laptop', 1150, '2024-01-01', 'infinity');
TABLE products ORDER BY id, valid_from;

UPDATE products__for_portion_of_valid
SET
    price = 1180,
    valid_from = '2023-10-01',
    valid_until = '2024-04-01'
WHERE id = 1;

TABLE products ORDER BY id, valid_from;
TABLE products_view_select_for_portion_of;

-- Test UPDATE (fully surrounding all existing records for an entity)
-- This runs on the result of the previous test and should replace all
-- historical records for Laptop with a single new one.
UPDATE products__for_portion_of_valid
SET
    price = 1300,
    valid_from = '2022-01-01',
    valid_until = 'infinity'
WHERE id = 1;

TABLE products ORDER BY id, valid_from;
TABLE products_view_select_for_portion_of;


-- Test DELETE (should be disallowed)
-- Delete the Mouse record
SAVEPOINT delete_should_fail;
DELETE FROM products__for_portion_of_valid WHERE id = 2;
ROLLBACK TO delete_should_fail;

-- The record for the mouse should still be present
TABLE products ORDER BY id, valid_from;
TABLE products_view_select_for_portion_of;


-- Drop the dependent view first
DROP VIEW products_view_select_for_portion_of;

-- Drop the view
SELECT sql_saga.drop_for_portion_of_view('products'::regclass);
TABLE sql_saga.updatable_view;

-- The view should no longer exist. This will fail if it does.
SAVEPOINT view_is_gone;
SELECT * FROM products__for_portion_of_valid;
ROLLBACK TO view_is_gone;

ROLLBACK;

\i sql/include/test_teardown.sql
