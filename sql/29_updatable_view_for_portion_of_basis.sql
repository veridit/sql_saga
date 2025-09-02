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

-- Test INSERT (Direct historical insert)
INSERT INTO products__for_portion_of_valid (id, name, price, valid_from, valid_until)
VALUES (3, 'Keyboard', 75, '2023-01-01', 'infinity');

-- The new record should be visible in both the base table and the view
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

-- Test DELETE (Hard historical delete)
-- Delete the Mouse record
DELETE FROM products__for_portion_of_valid WHERE id = 2;

-- The record for the mouse should be gone completely
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
