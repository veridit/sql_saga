\i sql/include/test_setup.sql

SELECT $$
----------------------------------------------------------------------------
Test: `temporal_merge` with Updatable View for Minimal-Copying ETL
Concept:
  This test demonstrates an advanced ETL architecture that minimizes data
  copying and simplifies calling code by leveraging updatable views and a
  procedural, batch-driven approach. The example shows a multi-step process
  where dependent entities (locations, statistics) are created after their
  parent (legal unit), with generated IDs being written back to the master
  data table at each step.
The Problem:
  ETL procedures often prepare data in a temporary table, call
  `temporal_merge`, and then have a separate, boilerplate step to join
  the `temporal_merge_feedback` table back to the main ETL data table
  to update it with generated IDs, statuses, and error messages. This becomes
  especially complex when processing dependent entities (e.g. locations that
  require a `legal_unit_id`) within the same batch, and across multiple batches.
The Solution:
  Instead of a monolithic script, the ETL logic is encapsulated in idempotent
  procedures that operate on a specific batch of source rows, identified by
  their `row_id`s. This test uses updatable TEMP VIEWs over the main data
  table to avoid data copying. When combined with `temporal_merge`'s
  `p_update_source_with_identity` parameter, this allows for a
  powerful pattern of "ID back-propagation".
The Mechanism: "Intra-Step and Inter-Batch ID Propagation"
  This test demonstrates a robust "merge -> back-propagate -> merge" pattern
  for handling dependent entities, and shows how this pattern can be applied
  sequentially across multiple batches of data.
  - **Intra-Step Propagation (within a batch):**
    1. A procedure calls `temporal_merge` for a parent entity (`legal_unit`).
       The key parameter `p_update_source_with_identity` is set to
       `true`, which writes the newly generated `legal_unit_id` back into the
       master data table for the processed source rows.
    2. A "back-propagation" `UPDATE` fills this generated `legal_unit_id` into
       *all* other rows in the master data table that belong to the same
       conceptual entity (identified by `identity_seq`). This ensures foreign
       keys are available for dependent entities in the next step.
    3. Subsequent procedures call `temporal_merge` for each dependent entity
       (e.g., `location`, `stat_for_unit`). These calls succeed because the
       required foreign keys are now present in the source data.
  - **Inter-Batch Propagation (across batches):**
    The same set of idempotent procedures are called by a driver for each
    batch of data in sequence. This demonstrates how the same ETL logic can
    process multiple, independent business entities in a clean, stateful, and
    transactionally-safe way. A third batch is then processed that contains
    a series of staggered updates to an entity from a previous batch,
    demonstrating that the architecture correctly handles stateful,
    multi-faceted updates across batches.
----------------------------------------------------------------------------
$$ as doc;

-- 1. Setup: A realistic schema with dependent temporal tables.
CREATE SCHEMA etl;

BEGIN;

-- Stat Definition table (not temporal)
CREATE TABLE etl.stat_definition (id int primary key, code text unique);
INSERT INTO etl.stat_definition VALUES (1, 'employees'), (2, 'turnover');

-- Legal Unit table
CREATE TABLE etl.legal_unit (id serial, name text, valid_from date, valid_until date);
SELECT sql_saga.add_era('etl.legal_unit');
SELECT sql_saga.add_unique_key('etl.legal_unit', '{id}');

-- Location table
CREATE TABLE etl.location (id serial, legal_unit_id int, type text, address text, valid_from date, valid_until date);
SELECT sql_saga.add_era('etl.location');
SELECT sql_saga.add_unique_key('etl.location', '{id}');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'etl.location',
    fk_column_names => '{legal_unit_id}',
    fk_era_name => 'valid',
    unique_key_name => 'legal_unit_id_valid'
);

-- Stat For Unit table
CREATE TABLE etl.stat_for_unit (id serial, legal_unit_id int, stat_definition_id int, value int, valid_from date, valid_until date);
SELECT sql_saga.add_era('etl.stat_for_unit');
SELECT sql_saga.add_unique_key('etl.stat_for_unit', '{id}');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'etl.stat_for_unit',
    fk_column_names => '{legal_unit_id}',
    fk_era_name => 'valid',
    unique_key_name => 'legal_unit_id_valid'
);

-- The master ETL data table. In a real system, this would be a partitioned table.
CREATE TABLE etl.data_table (
    row_id int primary key,
    identity_seq int,
    -- Common temporal columns
    valid_from date,
    valid_until date,
    -- Source data for legal unit
    lu_name text,
    -- Source data for locations
    physical_address text,
    postal_address text,
    -- Source data for stats
    employees int,
    turnover int,
    -- Columns to be populated by the ETL process
    legal_unit_id int,
    physical_location_id int,
    postal_location_id int,
    employees_stat_id int,
    turnover_stat_id int,
    -- Feedback from temporal_merge
    merge_statuses jsonb,
    merge_errors jsonb
);

-- 2. Initial State: The data table has a sparse, staggered timeline for one new business.
--    All rows share the same identity_seq. NULLs indicate that an attribute is
--    either unknown or unchanged in that time slice.
INSERT INTO etl.data_table (row_id, identity_seq, lu_name, physical_address, postal_address, employees, turnover, valid_from, valid_until) VALUES
    -- Batch 1: NewCo AS
    (1, 1, 'NewCo AS',      '123 Business Rd', NULL,          NULL,  NULL, '2024-01-01', '2024-03-01'),
    (2, 1, NULL,            '123 Business Rd', 'PO Box 456',  10,    NULL, '2024-03-01', '2024-05-01'),
    (3, 1, 'RenamedCo AS',  '123 Business Rd', 'PO Box 456',  10, 50000, '2024-05-01', '2024-07-01'),
    (4, 1, 'RenamedCo AS',  '123 Business Rd', 'PO Box 789',  10, 55000, '2024-07-01', '2024-09-01'),
    (5, 1, 'RenamedCo AS',  '123 Business Rd', 'PO Box 789',  12, 60000, '2024-09-01', '2025-01-01'),
    -- Batch 2: SecondBiz Inc
    (6, 2, 'SecondBiz Inc', '456 Innovation Dr', NULL,              50, 250000, '2024-02-15', '2024-08-01'),
    (7, 2, NULL,            '789 Tech Pkwy',   'PO Box 999',        55, 300000, '2024-08-01', '2024-11-01'),
    (8, 2, 'SecondBiz Inc', '789 Tech Pkwy',   'PO Box 999',        60, 320000, '2024-11-01', '2025-01-01'),
    -- Batch 3: A staggered update to the entity from Batch 1, demonstrating cross-batch state.
    (9,  1, NULL,         NULL,              NULL,         15, 75000, '2025-01-01', '2025-03-01'),
    (10, 1, 'FinalCo AS', NULL,              NULL,         NULL, NULL, '2025-03-01', '2025-05-01'),
    (11, 1, NULL,         '1 New Street',    NULL,         NULL, NULL, '2025-05-01', '2025-07-01'),
    (12, 1, NULL,         NULL,              'PO Box 111', NULL, NULL, '2025-07-01', 'infinity');

\echo '--- ETL Data Table: Initial State (all generated IDs are NULL) ---'
TABLE etl.data_table ORDER BY identity_seq, row_id;

--------------------------------------------------------------------------------
\echo '--- Step 3: Define ETL Driver Procedures ---'
--------------------------------------------------------------------------------

CREATE PROCEDURE etl.process_legal_units(p_row_ids int[])
LANGUAGE plpgsql AS $procedure$
BEGIN
    -- Create a view for the legal unit batch
    EXECUTE format($$
        CREATE OR REPLACE TEMP VIEW source_view_lu AS
        SELECT
            row_id,
            identity_seq as founding_id, -- Use identity_seq as the founding_id for new entities
            legal_unit_id AS id,   -- Map the writeback column to 'id'
            lu_name AS name,
            merge_statuses,
            merge_errors,
            valid_from,
            valid_until
        FROM etl.data_table WHERE row_id = ANY(%L::int[]);
    $$, p_row_ids);

    -- Call temporal_merge. It will write the new legal_unit_id back to etl.data_table.
    CALL sql_saga.temporal_merge(
        p_target_table => 'etl.legal_unit',
        p_source_table => 'source_view_lu',
        p_identity_columns => '{id}',
        p_ephemeral_columns => '{}'::text[],
        p_mode => 'MERGE_ENTITY_PATCH',
        p_identity_correlation_column => 'founding_id',
        p_update_source_with_identity => true,
        p_update_source_with_feedback => true,
        p_feedback_status_column => 'merge_statuses',
        p_feedback_status_key => 'legal_unit',
        p_feedback_error_column => 'merge_errors',
        p_feedback_error_key => 'legal_unit'
    );

    -- Back-propagate the generated legal_unit_id to all rows sharing the same identity_seq
    UPDATE etl.data_table dt
    SET legal_unit_id = sub.legal_unit_id
    FROM (
        SELECT DISTINCT identity_seq, legal_unit_id
        FROM etl.data_table
        WHERE legal_unit_id IS NOT NULL AND row_id = ANY(p_row_ids)
    ) AS sub
    WHERE dt.identity_seq = sub.identity_seq AND dt.legal_unit_id IS NULL;
END;
$procedure$;

CREATE PROCEDURE etl.process_locations(p_row_ids int[])
LANGUAGE plpgsql AS $procedure$
BEGIN
    -- Create a view for the physical locations batch.
    EXECUTE format($$
        CREATE OR REPLACE TEMP VIEW source_view_loc_phys AS
        SELECT
            dt.row_id,
            dt.identity_seq as founding_id,
            dt.physical_location_id AS id,
            dt.legal_unit_id,
            'physical'::text as type,
            dt.physical_address as address,
            dt.merge_statuses,
            dt.merge_errors,
            dt.valid_from,
            dt.valid_until
        FROM etl.data_table dt
        WHERE dt.row_id = ANY(%L::int[]) AND dt.physical_address IS NOT NULL;
    $$, p_row_ids);

    -- Merge physical locations
    CALL sql_saga.temporal_merge(
        p_target_table => 'etl.location',
        p_source_table => 'source_view_loc_phys',
        p_identity_columns => '{id}',
        p_ephemeral_columns => '{}'::text[],
        p_mode => 'MERGE_ENTITY_PATCH',
        p_identity_correlation_column => 'founding_id',
        p_update_source_with_identity => true,
        p_update_source_with_feedback => true,
        p_feedback_status_column => 'merge_statuses',
        p_feedback_status_key => 'physical_location',
        p_feedback_error_column => 'merge_errors',
        p_feedback_error_key => 'physical_location'
    );

    -- Back-propagate the generated physical_location_id
    UPDATE etl.data_table dt
    SET physical_location_id = sub.physical_location_id
    FROM (
        SELECT DISTINCT identity_seq, physical_location_id
        FROM etl.data_table
        WHERE physical_location_id IS NOT NULL AND row_id = ANY(p_row_ids)
    ) AS sub
    WHERE dt.identity_seq = sub.identity_seq AND dt.physical_location_id IS NULL;

    -- Create a view for the postal locations batch.
    EXECUTE format($$
        CREATE OR REPLACE TEMP VIEW source_view_loc_post AS
        SELECT
            dt.row_id,
            dt.identity_seq as founding_id,
            dt.postal_location_id AS id,
            dt.legal_unit_id,
            'postal'::text as type,
            dt.postal_address as address,
            dt.merge_statuses,
            dt.merge_errors,
            dt.valid_from,
            dt.valid_until
        FROM etl.data_table dt
        WHERE dt.row_id = ANY(%L::int[]) AND dt.postal_address IS NOT NULL;
    $$, p_row_ids);

    -- Merge postal locations
    CALL sql_saga.temporal_merge(
        p_target_table => 'etl.location',
        p_source_table => 'source_view_loc_post',
        p_identity_columns => '{id}',
        p_ephemeral_columns => '{}'::text[],
        p_mode => 'MERGE_ENTITY_PATCH',
        p_identity_correlation_column => 'founding_id',
        p_update_source_with_identity => true,
        p_update_source_with_feedback => true,
        p_feedback_status_column => 'merge_statuses',
        p_feedback_status_key => 'postal_location',
        p_feedback_error_column => 'merge_errors',
        p_feedback_error_key => 'postal_location'
    );

    -- Back-propagate the generated postal_location_id
    UPDATE etl.data_table dt
    SET postal_location_id = sub.postal_location_id
    FROM (
        SELECT DISTINCT identity_seq, postal_location_id
        FROM etl.data_table
        WHERE postal_location_id IS NOT NULL AND row_id = ANY(p_row_ids)
    ) AS sub
    WHERE dt.identity_seq = sub.identity_seq AND dt.postal_location_id IS NULL;
END;
$procedure$;

CREATE PROCEDURE etl.process_stats(p_row_ids int[])
LANGUAGE plpgsql AS $procedure$
DECLARE
    v_stat_def RECORD;
    v_view_sql TEXT;
BEGIN
    FOR v_stat_def IN SELECT * FROM etl.stat_definition LOOP
        RAISE NOTICE 'Processing statistic: % for row_ids %', v_stat_def.code, p_row_ids;

        -- Dynamically create a view for the current statistic's batch
        v_view_sql := format(
            $SQL$
            CREATE OR REPLACE TEMP VIEW source_view_stat_%s AS
            SELECT
                dt.row_id,
                dt.identity_seq as founding_id,
                dt.%I AS id, -- Map the specific writeback ID column
                dt.legal_unit_id,
                %L as stat_definition_id,
                dt.%I as value, -- Map the specific source value column
                dt.merge_statuses,
                dt.merge_errors,
                dt.valid_from,
                dt.valid_until
            FROM etl.data_table dt
            WHERE dt.row_id = ANY(%L::int[]) AND dt.%I IS NOT NULL;
            $SQL$,
            v_stat_def.code, -- Suffix for unique view name
            format('%s_stat_id', v_stat_def.code), -- e.g., employees_stat_id
            v_stat_def.id,
            v_stat_def.code, -- e.g., employees
            p_row_ids,
            v_stat_def.code
        );
        EXECUTE v_view_sql;

        -- Call temporal_merge for the current statistic
        CALL sql_saga.temporal_merge(
            p_target_table => 'etl.stat_for_unit',
            p_source_table => format('source_view_stat_%s', v_stat_def.code)::regclass,
            p_identity_columns => '{id}'::text[],
            p_ephemeral_columns => '{}'::text[],
            p_mode => 'MERGE_ENTITY_PATCH',
            p_identity_correlation_column => 'founding_id',
            p_update_source_with_identity => true,
            p_update_source_with_feedback => true,
            p_feedback_status_column => 'merge_statuses',
            p_feedback_status_key => v_stat_def.code,
            p_feedback_error_column => 'merge_errors',
            p_feedback_error_key => v_stat_def.code
        );

        -- Back-propagate the generated stat ID for the current statistic
        EXECUTE format(
            $SQL$
            UPDATE etl.data_table dt
            SET %I = sub.id
            FROM (
                SELECT DISTINCT identity_seq, %I AS id
                FROM etl.data_table
                WHERE %I IS NOT NULL AND row_id = ANY(%L::int[])
            ) AS sub
            WHERE dt.identity_seq = sub.identity_seq AND dt.%I IS NULL;
            $SQL$,
            format('%s_stat_id', v_stat_def.code),
            format('%s_stat_id', v_stat_def.code),
            format('%s_stat_id', v_stat_def.code),
            p_row_ids,
            format('%s_stat_id', v_stat_def.code)
        );
    END LOOP;
END;
$procedure$;

--------------------------------------------------------------------------------
\echo '--- Step 4: Run ETL Driver for Batches ---'
--------------------------------------------------------------------------------
\echo '--- Processing Batch 1 (NewCo AS) ---'
CALL etl.process_legal_units('{1, 2, 3, 4, 5}');
CALL etl.process_locations('{1, 2, 3, 4, 5}');
CALL etl.process_stats('{1, 2, 3, 4, 5}');

\echo '--- ETL Data Table: After Batch 1 ---'
TABLE etl.data_table ORDER BY identity_seq, row_id;

\echo '--- Processing Batch 2 (SecondBiz Inc) ---'
CALL etl.process_legal_units('{6, 7, 8}');
CALL etl.process_locations('{6, 7, 8}');
CALL etl.process_stats('{6, 7, 8}');

\echo '--- ETL Data Table: After Batch 2 ---'
TABLE etl.data_table ORDER BY identity_seq, row_id;

\echo '--- Processing Batch 3 (Update to NewCo AS / RenamedCo AS) ---'
CALL etl.process_legal_units('{9, 10, 11, 12}');
CALL etl.process_locations('{9, 10, 11, 12}');
CALL etl.process_stats('{9, 10, 11, 12}');

\echo '--- ETL Data Table: Final State (all generated IDs are populated) ---'
TABLE etl.data_table ORDER BY identity_seq, row_id;

\echo '--- Final Target States ---'
\echo '--- legal_unit ---'
TABLE etl.legal_unit ORDER BY id, valid_from;
\echo '--- location ---'
TABLE etl.location ORDER BY id, valid_from;
\echo '--- stat_for_unit ---'
TABLE etl.stat_for_unit ORDER BY id, valid_from;

ROLLBACK;
\i sql/include/test_teardown.sql
