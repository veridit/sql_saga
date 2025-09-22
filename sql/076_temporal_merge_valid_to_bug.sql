\i sql/include/test_setup.sql

BEGIN;

SET datestyle TO 'ISO, DMY';

\echo '--- SETUP: Create temporal table with valid_to synchronization ---'
CREATE TABLE public.my_temporal_table (
    id INT NOT NULL,
    name TEXT,
    death_date DATE,
    valid_from DATE NOT NULL,
    valid_until DATE,
    valid_to DATE
);
SELECT sql_saga.add_era('public.my_temporal_table', synchronize_valid_to_column => 'valid_to');
SELECT sql_saga.add_unique_key(table_oid => 'public.my_temporal_table'::regclass, column_names => ARRAY['id'], unique_key_name => 'my_temporal_table_id_valid');


\echo '--- SOURCE DATA: Two rows for one entity, second represents a death ---'
CREATE TEMP TABLE source_data (
    id INT NOT NULL,
    name TEXT,
    death_date DATE,
    valid_from DATE NOT NULL,
    valid_to DATE,
    founding_row_id INT,
    data_row_id INT,
    valid_until DATE
) ON COMMIT DROP;

INSERT INTO source_data (data_row_id, founding_row_id, id, name, valid_from, valid_to, death_date) VALUES
-- This first row represents the original state. Its valid_to is the final death date of the entity.
(1, 1, 101, 'Unit A v1', '2010-01-01', '2012-12-31', NULL),
-- This second row represents the death. valid_from and valid_to are the death date.
-- Crucially, the valid_to from the first row ('2012-12-31') is inconsistent for the
-- time slice that sql_saga will generate from it ('2010-01-01' to '2012-12-30').
(2, 1, 101, 'Unit A v1', '2012-12-31', '2012-12-31', '2012-12-31');

-- Mimic the import process deriving valid_until from valid_to
UPDATE source_data SET valid_until = valid_to + INTERVAL '1 day';

\echo 'Initial source data:'
TABLE source_data;


\echo '--- VERIFY FIX (MERGE_ENTITY_REPLACE): Call temporal_merge, which should now succeed ---'
-- This call should now succeed. The planner will correctly calculate the
-- `valid_to` for the shortened time slice, preventing a constraint violation.
CALL sql_saga.temporal_merge(
    target_table => 'public.my_temporal_table',
    source_table => 'source_data',
    identity_columns => ARRAY['id'],
    ephemeral_columns => '{}',
    mode => 'MERGE_ENTITY_REPLACE',
    founding_id_column => 'founding_row_id',
    row_id_column => 'data_row_id'
);

\echo '--- VERIFICATION: Final state of my_temporal_table ---'
-- The target table should now contain two correct historical slices.
TABLE public.my_temporal_table;


ROLLBACK;

--
-- Test DELETE_FOR_PORTION_OF to ensure the fix is generic
--
BEGIN;
SET datestyle TO 'ISO, DMY';

\echo '--- SETUP: Recreate table and initial data for DELETE_FOR_PORTION_OF test ---'
CREATE TABLE public.my_temporal_table (
    id INT NOT NULL,
    name TEXT,
    valid_from DATE NOT NULL,
    valid_until DATE,
    valid_to DATE
);
SELECT sql_saga.add_era('public.my_temporal_table', synchronize_valid_to_column => 'valid_to');
SELECT sql_saga.add_unique_key(table_oid => 'public.my_temporal_table'::regclass, column_names => ARRAY['id']);

-- Insert one long-lived entity.
INSERT INTO public.my_temporal_table (id, name, valid_from, valid_until)
VALUES (101, 'Unit A', '2010-01-01', '2020-01-01');

\echo 'Initial target data:'
TABLE public.my_temporal_table;

\echo '--- SOURCE DATA: A single row to delete a portion from the middle ---'
CREATE TEMP TABLE source_data_delete (
    id INT NOT NULL,
    valid_from DATE NOT NULL,
    valid_until DATE,
    data_row_id INT
) ON COMMIT DROP;

INSERT INTO source_data_delete (data_row_id, id, valid_from, valid_until) VALUES
(1, 101, '2012-01-01', '2013-01-01');

\echo 'Source data for deletion:'
TABLE source_data_delete;

\echo '--- VERIFY FIX (DELETE_FOR_PORTION_OF): Call temporal_merge ---'
CALL sql_saga.temporal_merge(
    target_table => 'public.my_temporal_table',
    source_table => 'source_data_delete',
    identity_columns => ARRAY['id'],
    ephemeral_columns => '{}',
    mode => 'DELETE_FOR_PORTION_OF',
    row_id_column => 'data_row_id'
);

\echo '--- VERIFICATION: Final state of my_temporal_table ---'
-- The target table should now be split into two slices around the deleted portion,
-- with valid_to correctly calculated for both. The first slice is shortened,
-- so its valid_to must be recalculated.
TABLE public.my_temporal_table;


ROLLBACK;


\i sql/include/test_teardown.sql
