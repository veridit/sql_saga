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
  `update_source_with_identity` parameter, this allows for a
  powerful pattern of "ID back-propagation".
The Mechanism: "Intra-Step and Inter-Batch ID Propagation"
  This test demonstrates a robust "merge -> back-propagate -> merge" pattern
  for handling dependent entities, and shows how this pattern can be applied
  sequentially across multiple batches of data.
  - **Intra-Step Propagation (within a batch):**
    1. A procedure calls `temporal_merge` for a parent entity (`legal_unit`).
       The key parameter `update_source_with_identity` is set to
       `true`, which writes the newly generated `legal_unit_id` back into the
       master data table for the processed source rows.
    2. A "back-propagation" `UPDATE` fills this generated `legal_unit_id` into
       *all* other rows in the master data table that belong to the same
       conceptual entity (identified by `identity_correlation`). This ensures foreign
       keys are available for dependent entities in the next step.
    3. Subsequent procedures call `temporal_merge` for each dependent entity
       (e.g., `location`, `stat_for_unit`). These calls succeed because the
       required foreign keys are now present in the source data.
  - **Handling Different Data Models:**
    The example is extended to show several other common patterns:
    1. **Hybrid Key Strategy (Natural Key Lookup):** The `location` table
       demonstrates a powerful hybrid approach. `physical` locations have a
       stable identity (`identity_columns` = `id`), but are looked up using a
       natural key (`natural_identity_columns` = `legal_unit_id`, `type`). This shows
       how `temporal_merge` can use a business key to find an entity while
       correctly preserving its separate, stable primary key.
    2. **Non-Temporal Identifiers:** An `ident` table stores external
       identifiers (e.g., tax numbers) that are considered stable over
       time and reference a temporal entity. This is managed with a standard
       SQL `MERGE` statement.
    3. **Temporal Data with Natural Keys:** An `activity` table is temporal
       and uses a composite natural key without a surrogate `id`. This
       is managed with `temporal_merge` configured for natural keys and no
       identity writeback.
    4. **Ephemeral Metadata Columns:** A `comment` column is added to all
       temporal tables and populated from the source. It is passed to
       `temporal_merge` in the `ephemeral_columns` parameter. This ensures
       that changes to the comment update the existing historical record
       without creating a new one, correctly treating it as non-business metadata.
  - **Inter-Batch Propagation ("Current State" ETL Pattern):**
    The test simulates a common ETL pattern where incoming data represents
    the "current state" of an entity, valid from a certain point until
    further notice (`infinity`). Each new batch of data for an existing
    entity "cuts off" the previous timeline and establishes a new current
    state.
    The same set of idempotent procedures are called by a driver for each
    batch of data in sequence. This demonstrates how the same ETL logic can
    process multiple, independent business entities and their subsequent
    updates in a clean, stateful, and transactionally-safe way.
----------------------------------------------------------------------------
$$ as doc;

-- 1. Setup: A realistic schema with dependent temporal tables.
CREATE SCHEMA etl;

BEGIN;

-- Reference tables (not temporal)
CREATE TABLE etl.stat_definition (id int primary key, code text unique);
INSERT INTO etl.stat_definition VALUES (1, 'employees'), (2, 'turnover');

CREATE TABLE etl.ident_type (id int primary key, code text unique);
INSERT INTO etl.ident_type VALUES (1, 'tax'), (2, 'ssn');

CREATE TABLE etl.activity_type (id int primary key, code text unique);
INSERT INTO etl.activity_type VALUES (10, 'manufacturing'), (20, 'retail');

-- Legal Unit table
CREATE TABLE etl.legal_unit (id serial, name text, comment text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('etl.legal_unit', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'etl.legal_unit'::regclass, column_names => ARRAY['id'], key_type => 'primary', unique_key_name => 'legal_unit_id_valid');

-- External Identifiers table (not temporal)
CREATE TABLE etl.ident (
    id serial primary key,
    legal_unit_id int not null,
    ident_type_id int not null references etl.ident_type(id),
    ident_value text not null,
    UNIQUE (legal_unit_id, ident_type_id) -- An entity can only have one of each type
);
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'etl.ident'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'etl.legal_unit'::regclass,
    pk_column_names => ARRAY['id']
);

-- Location table
CREATE TABLE etl.location (id serial, legal_unit_id int, type text, address text, comment text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('etl.location', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'etl.location'::regclass, column_names => ARRAY['id'], key_type => 'primary', unique_key_name => 'location_id_valid');
-- A legal unit can only have one 'physical' address at any given time.
-- This is a predicated natural key.
SELECT sql_saga.add_unique_key(table_oid => 'etl.location'::regclass, column_names => ARRAY['legal_unit_id', 'type'], key_type => 'predicated', predicate => 'type = ''physical''', unique_key_name => 'location_legal_unit_id_type_valid');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'etl.location'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'etl.legal_unit'::regclass,
    pk_column_names => ARRAY['id']
);

-- Stat For Unit table
CREATE TABLE etl.stat_for_unit (id serial, legal_unit_id int, stat_definition_id int, value int, comment text, valid_range daterange, valid_from date, valid_until date);
SELECT sql_saga.add_era('etl.stat_for_unit', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'etl.stat_for_unit'::regclass, column_names => ARRAY['id'], key_type => 'primary', unique_key_name => 'stat_for_unit_id_valid');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'etl.stat_for_unit'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'etl.legal_unit'::regclass,
    pk_column_names => ARRAY['id']
);

-- Activity table (temporal, with a composite natural key)
CREATE TABLE etl.activity (
    legal_unit_id int,
    activity_type_id int,
    comment text,
    valid_range daterange,
    valid_from date,
    valid_until date
);
SELECT sql_saga.add_era('etl.activity', 'valid_range',
    valid_from_column_name => 'valid_from',
    valid_until_column_name => 'valid_until');
-- The "unique key" for a natural-key table is the set of natural key columns.
SELECT sql_saga.add_unique_key(table_oid => 'etl.activity'::regclass, column_names => ARRAY['legal_unit_id', 'activity_type_id'], key_type => 'primary', unique_key_name => 'activity_legal_unit_id_activity_type_id_valid');
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'etl.activity'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'etl.legal_unit'::regclass,
    pk_column_names => ARRAY['id']
);

-- The master ETL data table. In a real system, this would be a partitioned table.
CREATE TABLE etl.data_table (
    row_id int primary key,
    batch int not null,
    identity_correlation int,
    -- Common temporal columns
    valid_from date,
    valid_until date,
    -- Ephemeral metadata
    comment text,
    -- Source data for external identifiers
    tax_ident text,
    -- Source data for legal unit
    lu_name text,
    -- Source data for locations
    physical_address text,
    postal_address text,
    -- Source data for activities and stats
    activity_code text,
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

-- 2. Initial State: The data table has a sparse, staggered timeline for multiple businesses across multiple batches.
--    The identity_correlation identifies the *same* entity, so it must be returned on insert and backfilled by temporal_merge
--    and backfilled across batches by the caller.
--    NULLs indicate that an attribute is either unknown or unchanged in that time slice.
--    As the data comes in they all last until further notice, i.e. until infinity,
--    but, as new data appear for a target table, the old will get cut off.
INSERT INTO etl.data_table (row_id, batch, identity_correlation, comment, tax_ident, lu_name, physical_address, postal_address, activity_code, employees, turnover, valid_from, valid_until) VALUES
    -- Batch 1: NewCo AS
    (1, 1, 1, 'Initial load', 'TAX111', 'NewCo AS',      '123 Business Rd', NULL,           'manufacturing', NULL,   NULL, '2024-01-01', 'infinity'),
    (2, 1, 1, 'Postal address added', NULL,     NULL,            NULL,              'PO Box 456',   NULL,            10,     NULL, '2024-03-01', 'infinity'),
    (3, 1, 1, 'Company rename', 'TAX222', 'RenamedCo AS',  NULL,              NULL,           'retail',        NULL,  50000, '2024-05-01', 'infinity'),
    (4, 1, 1, 'Updated postal address', NULL,     NULL,            NULL,              'PO Box 789',   NULL,            NULL,  55000, '2024-07-01', 'infinity'),
    (5, 1, 1, 'Staff and turnover update', NULL,     NULL,            NULL,              NULL,           'manufacturing', 12,    60000, '2024-09-01', 'infinity'),
    -- Batch 2: SecondBiz Inc
    (6, 2, 2, 'Initial load', 'TAX333', 'SecondBiz Inc', '456 Innovation Dr', NULL,         'retail',        50,   250000, '2024-02-15', 'infinity'),
    (7, 2, 2, 'Address update', NULL,     NULL,            '789 Tech Pkwy',   'PO Box 999',   NULL,            55,   300000, '2024-08-01', 'infinity'),
    (8, 2, 2, 'Staff update', NULL,     'SecondBiz Inc', NULL,              NULL,           NULL,            60,   320000, '2024-11-01', 'infinity'),
    -- Batch 3: A staggered update to the entity from Batch 1, demonstrating cross-batch state.
    (9, 3,  1, 'Annual staff update', NULL,     NULL,            NULL,              NULL,           NULL,            15,    75000, '2025-01-01', 'infinity'),
    (10, 3, 1, 'Final rename', 'TAX444', 'FinalCo AS',    NULL,              NULL,           NULL,            NULL,   NULL, '2025-03-01', 'infinity'),
    (11, 3, 1, 'New physical address', NULL,     NULL,            '1 New Street',    NULL,           'retail',        NULL,   NULL, '2025-05-01', 'infinity'),
    (12, 3, 1, 'New postal address', NULL,     NULL,            NULL,              'PO Box 111',   NULL,            NULL,   NULL, '2025-07-01', 'infinity'),
    -- Batch 4: A batch of no-change operations.
    (13, 4, 1, 'No-op, filtered', NULL,     NULL,            NULL,              NULL,           NULL,            NULL,   NULL, '2025-07-01', 'infinity'),
    (14, 4, 1, 'No-op, identical', 'TAX444', 'FinalCo AS',    '1 New Street',    'PO Box 111',   'retail',        15,    75000, '2025-07-01', 'infinity');

\echo '--- ETL Data Table: Initial State (all generated IDs are NULL) ---'
TABLE etl.data_table ORDER BY identity_correlation, row_id;

--------------------------------------------------------------------------------
\echo '--- Step 3: Define ETL Driver Procedures ---'
--------------------------------------------------------------------------------

CREATE PROCEDURE etl.process_legal_units(p_batch_id int)
LANGUAGE plpgsql AS $procedure$
DECLARE
    v_rec RECORD;
BEGIN
    -- Create a view for the legal unit batch
    EXECUTE format($$
        CREATE OR REPLACE TEMP VIEW source_view_lu AS
        SELECT
            row_id,
            identity_correlation as founding_id, -- Use identity_correlation as the founding_id for new entities
            legal_unit_id AS id,   -- Map the writeback column to 'id'
            lu_name AS name,
            comment,
            merge_statuses,
            merge_errors,
            valid_from,
            valid_until
        FROM etl.data_table WHERE batch = %1$L AND lu_name IS NOT NULL;
    $$, p_batch_id /* %1$L */);

    -- Call temporal_merge. It will write the new legal_unit_id back to etl.data_table.
    CALL sql_saga.temporal_merge(
        target_table => 'etl.legal_unit',
        source_table => 'source_view_lu',
        primary_identity_columns => ARRAY['id'],
        ephemeral_columns => ARRAY['comment'],
        mode => 'MERGE_ENTITY_PATCH',
        founding_id_column => 'founding_id',
        update_source_with_identity => true,
        update_source_with_feedback => true,
        feedback_status_column => 'merge_statuses',
        feedback_status_key => 'legal_unit',
        feedback_error_column => 'merge_errors',
        feedback_error_key => 'legal_unit'
    );

    EXECUTE format('CREATE TEMP TABLE plan_lu_batch_%s AS SELECT * FROM pg_temp.temporal_merge_plan', p_batch_id);
    EXECUTE format('CREATE TEMP TABLE feedback_lu_batch_%s AS SELECT * FROM pg_temp.temporal_merge_feedback', p_batch_id);

    -- Intra-batch back-propagation: Fill the generated legal_unit_id into all
    -- other rows in this batch that belong to the same conceptual entity. This
    -- is necessary because subsequent ETL steps for dependent entities (like
    -- locations) need this foreign key to be present.
    UPDATE etl.data_table dt
    SET legal_unit_id = sub.legal_unit_id
    FROM (
        SELECT DISTINCT ON (identity_correlation)
            identity_correlation,
            legal_unit_id
        FROM etl.data_table
        WHERE batch = p_batch_id AND legal_unit_id IS NOT NULL
        ORDER BY identity_correlation, row_id DESC -- Get latest ID if multiple updates
    ) AS sub
    WHERE dt.batch = p_batch_id
      AND dt.identity_correlation = sub.identity_correlation;

    -- Propagate the most up-to-date legal_unit_id to all future, unprocessed batches for this entity.
    UPDATE etl.data_table dt_future
    SET legal_unit_id = dt_latest.legal_unit_id
    FROM (
        SELECT DISTINCT ON (identity_correlation)
            identity_correlation,
            legal_unit_id
        FROM etl.data_table
        WHERE batch <= p_batch_id AND legal_unit_id IS NOT NULL
        ORDER BY identity_correlation, batch DESC, row_id DESC
    ) AS dt_latest
    WHERE dt_future.batch > p_batch_id
      AND dt_future.identity_correlation = dt_latest.identity_correlation;
END;
$procedure$;

CREATE PROCEDURE etl.process_locations(p_batch_id int)
LANGUAGE plpgsql AS $procedure$
DECLARE
    v_rec RECORD;
BEGIN
    -- Create a view for the physical locations batch.
    EXECUTE format($$
        CREATE OR REPLACE TEMP VIEW source_view_loc_phys AS
        SELECT
            dt.row_id,
            dt.identity_correlation as founding_id,
            dt.physical_location_id AS id,
            dt.legal_unit_id,
            'physical'::text as type,
            dt.physical_address as address,
            dt.comment,
            dt.merge_statuses,
            dt.merge_errors,
            dt.valid_from,
            dt.valid_until
        FROM etl.data_table dt
        WHERE dt.batch = %1$L AND dt.physical_address IS NOT NULL;
    $$, p_batch_id /* %1$L */);

    -- Merge physical locations using a NATURAL KEY for lookup, while preserving
    -- the stable `id`. `natural_identity_columns` specifies the natural key for finding
    -- entities, and `identity_columns` specifies the stable key to preserve.
    CALL sql_saga.temporal_merge(
        target_table => 'etl.location',
        source_table => 'source_view_loc_phys',
        primary_identity_columns => ARRAY['id'],
        natural_identity_columns => ARRAY['legal_unit_id', 'type'],
        ephemeral_columns => ARRAY['comment'],
        mode => 'MERGE_ENTITY_PATCH',
        founding_id_column => 'founding_id',
        update_source_with_identity => true,
        update_source_with_feedback => true,
        feedback_status_column => 'merge_statuses',
        feedback_status_key => 'physical_location',
        feedback_error_column => 'merge_errors',
        feedback_error_key => 'physical_location'
    );

    EXECUTE format('CREATE TEMP TABLE plan_loc_phys_batch_%s AS SELECT * FROM pg_temp.temporal_merge_plan', p_batch_id);
    EXECUTE format('CREATE TEMP TABLE feedback_loc_phys_batch_%s AS SELECT * FROM pg_temp.temporal_merge_feedback', p_batch_id);

    -- Intra-batch back-propagation for physical_location_id.
    UPDATE etl.data_table dt
    SET physical_location_id = sub.physical_location_id
    FROM (
        SELECT DISTINCT ON (identity_correlation)
            identity_correlation,
            physical_location_id
        FROM etl.data_table
        WHERE batch = p_batch_id AND physical_location_id IS NOT NULL
        ORDER BY identity_correlation, row_id DESC
    ) AS sub
    WHERE dt.batch = p_batch_id
      AND dt.identity_correlation = sub.identity_correlation;

    -- Propagate the most up-to-date physical_location_id to future batches.
    UPDATE etl.data_table dt_future
    SET physical_location_id = dt_latest.physical_location_id
    FROM (
        SELECT DISTINCT ON (identity_correlation)
            identity_correlation,
            physical_location_id
        FROM etl.data_table
        WHERE batch <= p_batch_id AND physical_location_id IS NOT NULL
        ORDER BY identity_correlation, batch DESC, row_id DESC
    ) AS dt_latest
    WHERE dt_future.batch > p_batch_id
      AND dt_future.identity_correlation = dt_latest.identity_correlation;

    -- Create a view for the postal locations batch.
    EXECUTE format($$
        CREATE OR REPLACE TEMP VIEW source_view_loc_post AS
        SELECT
            dt.row_id,
            dt.identity_correlation as founding_id,
            dt.postal_location_id AS id,
            dt.legal_unit_id,
            'postal'::text as type,
            dt.postal_address as address,
            dt.comment,
            dt.merge_statuses,
            dt.merge_errors,
            dt.valid_from,
            dt.valid_until
        FROM etl.data_table dt
        WHERE dt.batch = %1$L AND dt.postal_address IS NOT NULL;
    $$, p_batch_id /* %1$L */);

    -- Merge postal locations using a SURROGATE KEY. In this case, each new
    -- postal address is treated as a new conceptual entity, identified by its
    -- surrogate `id`. This demonstrates the traditional identity pattern.
    CALL sql_saga.temporal_merge(
        target_table => 'etl.location',
        source_table => 'source_view_loc_post',
        primary_identity_columns => ARRAY['id'],
        -- Explicitly disable natural key discovery for this call. Postal locations
        -- are only identified by their surrogate 'id'.
        natural_identity_columns => ARRAY[]::text[],
        ephemeral_columns => ARRAY['comment'],
        mode => 'MERGE_ENTITY_PATCH',
        founding_id_column => 'founding_id',
        update_source_with_identity => true,
        update_source_with_feedback => true,
        feedback_status_column => 'merge_statuses',
        feedback_status_key => 'postal_location',
        feedback_error_column => 'merge_errors',
        feedback_error_key => 'postal_location'
    );

    EXECUTE format('CREATE TEMP TABLE plan_loc_post_batch_%s AS SELECT * FROM pg_temp.temporal_merge_plan', p_batch_id);
    EXECUTE format('CREATE TEMP TABLE feedback_loc_post_batch_%s AS SELECT * FROM pg_temp.temporal_merge_feedback', p_batch_id);

    -- Intra-batch back-propagation for postal_location_id.
    UPDATE etl.data_table dt
    SET postal_location_id = sub.postal_location_id
    FROM (
        SELECT DISTINCT ON (identity_correlation)
            identity_correlation,
            postal_location_id
        FROM etl.data_table
        WHERE batch = p_batch_id AND postal_location_id IS NOT NULL
        ORDER BY identity_correlation, row_id DESC
    ) AS sub
    WHERE dt.batch = p_batch_id
      AND dt.identity_correlation = sub.identity_correlation;

    -- Propagate the most up-to-date postal_location_id to future batches.
    UPDATE etl.data_table dt_future
    SET postal_location_id = dt_latest.postal_location_id
    FROM (
        SELECT DISTINCT ON (identity_correlation)
            identity_correlation,
            postal_location_id
        FROM etl.data_table
        WHERE batch <= p_batch_id AND postal_location_id IS NOT NULL
        ORDER BY identity_correlation, batch DESC, row_id DESC
    ) AS dt_latest
    WHERE dt_future.batch > p_batch_id
      AND dt_future.identity_correlation = dt_latest.identity_correlation;
END;
$procedure$;

CREATE PROCEDURE etl.process_stats(p_batch_id int)
LANGUAGE plpgsql AS $procedure$
DECLARE
    v_stat_def RECORD;
    v_view_sql TEXT;
    v_rec RECORD;
BEGIN
    FOR v_stat_def IN SELECT * FROM etl.stat_definition LOOP
        RAISE NOTICE 'Processing statistic: % for batch %', v_stat_def.code, p_batch_id;

        -- Dynamically create a view for the current statistic's batch
        v_view_sql := format(
            $SQL$
            CREATE OR REPLACE TEMP VIEW source_view_stat_%1$s AS
            SELECT
                dt.row_id,
                dt.identity_correlation as founding_id,
                dt.%2$I AS id,
                dt.legal_unit_id,
                %3$L::integer as stat_definition_id,
                dt.%1$I as value,
                dt.comment,
                dt.merge_statuses,
                dt.merge_errors,
                dt.valid_from,
                dt.valid_until
            FROM etl.data_table dt
            WHERE dt.batch = %4$L AND dt.%1$I IS NOT NULL;
            $SQL$,
            v_stat_def.code,                         /* %1$s, %1$I */
            format('%s_stat_id', v_stat_def.code), /* %2$I */
            v_stat_def.id,                           /* %3$L */
            p_batch_id                               /* %4$L */
        );
        EXECUTE v_view_sql;

        -- Call temporal_merge for the current statistic
        CALL sql_saga.temporal_merge(
            target_table => 'etl.stat_for_unit',
            source_table => format('source_view_stat_%s', v_stat_def.code)::regclass,
            primary_identity_columns => ARRAY['id'],
            ephemeral_columns => ARRAY['comment'],
            mode => 'MERGE_ENTITY_PATCH',
            founding_id_column => 'founding_id',
            update_source_with_identity => true,
            update_source_with_feedback => true,
            feedback_status_column => 'merge_statuses',
            feedback_status_key => v_stat_def.code,
            feedback_error_column => 'merge_errors',
            feedback_error_key => v_stat_def.code
        );

        EXECUTE format('CREATE TEMP TABLE plan_stat_%s_batch_%s AS SELECT * FROM pg_temp.temporal_merge_plan', v_stat_def.code, p_batch_id);
        EXECUTE format('CREATE TEMP TABLE feedback_stat_%s_batch_%s AS SELECT * FROM pg_temp.temporal_merge_feedback', v_stat_def.code, p_batch_id);

        -- Intra-batch back-propagation for the current statistic's ID.
        EXECUTE format(
            $SQL$
            UPDATE etl.data_table dt
            SET %1$I = sub.%1$I
            FROM (
                SELECT DISTINCT ON (identity_correlation)
                    identity_correlation,
                    %1$I
                FROM etl.data_table
                WHERE batch = %2$L AND %1$I IS NOT NULL
                ORDER BY identity_correlation, row_id DESC
            ) AS sub
            WHERE dt.batch = %2$L
              AND dt.identity_correlation = sub.identity_correlation;
            $SQL$,
            format('%s_stat_id', v_stat_def.code), /* %1$I */
            p_batch_id                             /* %2$L */
        );

        -- Propagate the most up-to-date stat ID to future batches.
        EXECUTE format(
            $SQL$
            UPDATE etl.data_table dt_future
            SET %1$I = dt_latest.%1$I
            FROM (
                SELECT DISTINCT ON (identity_correlation)
                    identity_correlation,
                    %1$I
                FROM etl.data_table
                WHERE batch <= %2$L AND %1$I IS NOT NULL
                ORDER BY identity_correlation, batch DESC, row_id DESC
            ) AS dt_latest
            WHERE dt_future.batch > %2$L
              AND dt_future.identity_correlation = dt_latest.identity_correlation;
            $SQL$,
            format('%s_stat_id', v_stat_def.code), /* %1$I */
            p_batch_id                             /* %2$L */
        );
    END LOOP;
END;
$procedure$;

CREATE PROCEDURE etl.process_idents(p_batch_id int)
LANGUAGE plpgsql AS $procedure$
DECLARE
    v_ident_type RECORD;
    v_column_name TEXT;
    v_sql TEXT;
BEGIN
    -- This procedure demonstrates a dynamic, metadata-driven approach.
    -- It iterates through the known identifier types and checks if a
    -- corresponding column exists in the source data table before processing.
    FOR v_ident_type IN SELECT * FROM etl.ident_type LOOP
        v_column_name := v_ident_type.code || '_ident';

        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'etl'
              AND table_name = 'data_table'
              AND column_name = v_column_name
        ) THEN
            RAISE NOTICE 'Processing identifier type: % (column: %)', v_ident_type.code, v_column_name;

            -- Dynamically build and execute the MERGE statement for this identifier type.
            v_sql := format(
                $SQL$
                MERGE INTO etl.ident AS t
                USING (
                    -- The source data is temporal, but the target `ident` table is not.
                    -- We must therefore select only the latest value for each conceptual entity.
                    SELECT DISTINCT ON (dt.identity_correlation)
                        dt.legal_unit_id,
                        %1$L::integer AS ident_type_id,
                        dt.%2$I AS ident_value
                    FROM etl.data_table dt
                    WHERE dt.batch = %3$L::integer
                      AND dt.legal_unit_id IS NOT NULL
                      AND dt.%2$I IS NOT NULL
                    ORDER BY dt.identity_correlation, dt.valid_from DESC
                ) AS s
                ON t.legal_unit_id = s.legal_unit_id AND t.ident_type_id = s.ident_type_id
                WHEN MATCHED AND t.ident_value IS DISTINCT FROM s.ident_value THEN
                    UPDATE SET ident_value = s.ident_value
                WHEN NOT MATCHED THEN
                    INSERT (legal_unit_id, ident_type_id, ident_value)
                    VALUES (s.legal_unit_id, s.ident_type_id, s.ident_value);
                $SQL$,
                v_ident_type.id, /* %1$L */
                v_column_name,   /* %2$I */
                p_batch_id       /* %3$L */
            );
            EXECUTE v_sql;
        END IF;
    END LOOP;
END;
$procedure$;

CREATE PROCEDURE etl.process_activities(p_batch_id int)
LANGUAGE plpgsql AS $procedure$
DECLARE
    v_activity_type RECORD;
    v_view_sql TEXT;
    v_rec RECORD;
BEGIN
    FOR v_activity_type IN SELECT * FROM etl.activity_type LOOP
        -- Only process if there's data for this activity type in the current batch
        IF EXISTS (SELECT 1 FROM etl.data_table WHERE batch = p_batch_id AND activity_code = v_activity_type.code) THEN
            RAISE NOTICE 'Processing activity type: % for batch %', v_activity_type.code, p_batch_id;

            -- Create a simple, updatable view by removing the JOIN and adding the
            -- activity_type_id as a literal. This makes the view updatable
            -- for the feedback columns.
            v_view_sql := format(
                $SQL$
                CREATE OR REPLACE TEMP VIEW source_view_activity AS
                SELECT
                    row_id,
                    legal_unit_id,
                    %1$L::integer as activity_type_id,
                    comment,
                    merge_statuses,
                    merge_errors,
                    valid_from,
                    valid_until
                FROM etl.data_table
                WHERE batch = %2$L AND activity_code = %3$L;
                $SQL$,
                v_activity_type.id,   /* %1$L */
                p_batch_id,           /* %2$L */
                v_activity_type.code  /* %3$L */
            );
            EXECUTE v_view_sql;

            -- Call temporal_merge on a table with a natural key.
            -- Note that `update_source_with_identity` is false, as there is no
            -- surrogate key to write back.
            CALL sql_saga.temporal_merge(
                target_table => 'etl.activity'::regclass,
                source_table => 'source_view_activity'::regclass,
                primary_identity_columns => ARRAY['legal_unit_id', 'activity_type_id'],
                ephemeral_columns => ARRAY['comment'],
                mode => 'MERGE_ENTITY_PATCH',
                update_source_with_identity => false,
                update_source_with_feedback => true,
                feedback_status_column => 'merge_statuses',
                feedback_status_key => 'activity',
                feedback_error_column => 'merge_errors',
                feedback_error_key => 'activity'
            );

            EXECUTE format('CREATE TEMP TABLE plan_activity_%s_batch_%s AS SELECT * FROM pg_temp.temporal_merge_plan', v_activity_type.code, p_batch_id);
            EXECUTE format('CREATE TEMP TABLE feedback_activity_%s_batch_%s AS SELECT * FROM pg_temp.temporal_merge_feedback', v_activity_type.code, p_batch_id);
        END IF;
    END LOOP;
END;
$procedure$;

--------------------------------------------------------------------------------
\echo '--- Step 4: Run ETL Driver for Batches ---'
--------------------------------------------------------------------------------
CREATE PROCEDURE etl.run_batch(p_batch_id int)
LANGUAGE plpgsql AS $procedure$
BEGIN
    RAISE NOTICE '--- Processing Batch % ---', p_batch_id;

    -- For complex, multi-step ETL processes, the standard and most robust
    -- solution is to temporarily disable all relevant temporal foreign key triggers
    -- for the duration of the batch transaction. This prevents FK violations
    -- on transient, inconsistent states between procedure calls.
    CALL sql_saga.disable_temporal_triggers(
        'etl.legal_unit',
        'etl.location',
        'etl.stat_for_unit',
        'etl.activity'
    );

    CALL etl.process_legal_units(p_batch_id);
    -- These procedures depend on the legal_unit_id being populated.
    CALL etl.process_locations(p_batch_id);
    CALL etl.process_stats(p_batch_id);
    CALL etl.process_idents(p_batch_id);
    CALL etl.process_activities(p_batch_id);

    -- Re-enable the triggers now that the batch has reached a consistent state.
    CALL sql_saga.enable_temporal_triggers(
        'etl.legal_unit',
        'etl.location',
        'etl.stat_for_unit',
        'etl.activity'
    );
END;
$procedure$;

DO $do$
DECLARE
    v_batch_id int;
BEGIN
    FOR v_batch_id IN SELECT DISTINCT batch FROM etl.data_table ORDER BY batch LOOP
        CALL etl.run_batch(v_batch_id);
    END LOOP;
END;
$do$;

\echo '--- ETL Plans and Feedback ---'

\echo '--- Batch 1 ---'
\echo '--- legal_unit ---'
TABLE plan_lu_batch_1 ORDER BY plan_op_seq;
TABLE feedback_lu_batch_1 ORDER BY source_row_id;
\echo '--- physical_location ---'
TABLE plan_loc_phys_batch_1 ORDER BY plan_op_seq;
TABLE feedback_loc_phys_batch_1 ORDER BY source_row_id;
\echo '--- postal_location ---'
TABLE plan_loc_post_batch_1 ORDER BY plan_op_seq;
TABLE feedback_loc_post_batch_1 ORDER BY source_row_id;
\echo '--- stat_employees ---'
TABLE plan_stat_employees_batch_1 ORDER BY plan_op_seq;
TABLE feedback_stat_employees_batch_1 ORDER BY source_row_id;
\echo '--- stat_turnover ---'
TABLE plan_stat_turnover_batch_1 ORDER BY plan_op_seq;
TABLE feedback_stat_turnover_batch_1 ORDER BY source_row_id;
\echo '--- activity_manufacturing ---'
TABLE plan_activity_manufacturing_batch_1 ORDER BY plan_op_seq;
TABLE feedback_activity_manufacturing_batch_1 ORDER BY source_row_id;
\echo '--- activity_retail ---'
TABLE plan_activity_retail_batch_1 ORDER BY plan_op_seq;
TABLE feedback_activity_retail_batch_1 ORDER BY source_row_id;

\echo '--- Batch 2 ---'
\echo '--- legal_unit ---'
TABLE plan_lu_batch_2 ORDER BY plan_op_seq;
TABLE feedback_lu_batch_2 ORDER BY source_row_id;
\echo '--- physical_location ---'
TABLE plan_loc_phys_batch_2 ORDER BY plan_op_seq;
TABLE feedback_loc_phys_batch_2 ORDER BY source_row_id;
\echo '--- postal_location ---'
TABLE plan_loc_post_batch_2 ORDER BY plan_op_seq;
TABLE feedback_loc_post_batch_2 ORDER BY source_row_id;
\echo '--- stat_employees ---'
TABLE plan_stat_employees_batch_2 ORDER BY plan_op_seq;
TABLE feedback_stat_employees_batch_2 ORDER BY source_row_id;
\echo '--- stat_turnover ---'
TABLE plan_stat_turnover_batch_2 ORDER BY plan_op_seq;
TABLE feedback_stat_turnover_batch_2 ORDER BY source_row_id;
\echo '--- activity_retail ---'
TABLE plan_activity_retail_batch_2 ORDER BY plan_op_seq;
TABLE feedback_activity_retail_batch_2 ORDER BY source_row_id;

\echo '--- Batch 3 ---'
\echo '--- legal_unit ---'
TABLE plan_lu_batch_3 ORDER BY plan_op_seq;
TABLE feedback_lu_batch_3 ORDER BY source_row_id;
\echo '--- physical_location ---'
TABLE plan_loc_phys_batch_3 ORDER BY plan_op_seq;
TABLE feedback_loc_phys_batch_3 ORDER BY source_row_id;
\echo '--- postal_location ---'
TABLE plan_loc_post_batch_3 ORDER BY plan_op_seq;
TABLE feedback_loc_post_batch_3 ORDER BY source_row_id;
\echo '--- stat_employees ---'
TABLE plan_stat_employees_batch_3 ORDER BY plan_op_seq;
TABLE feedback_stat_employees_batch_3 ORDER BY source_row_id;
\echo '--- stat_turnover ---'
TABLE plan_stat_turnover_batch_3 ORDER BY plan_op_seq;
TABLE feedback_stat_turnover_batch_3 ORDER BY source_row_id;
\echo '--- activity_retail ---'
TABLE plan_activity_retail_batch_3 ORDER BY plan_op_seq;
TABLE feedback_activity_retail_batch_3 ORDER BY source_row_id;

\echo '--- Batch 4 ---'
\echo '--- legal_unit ---'
TABLE plan_lu_batch_4 ORDER BY plan_op_seq;
TABLE feedback_lu_batch_4 ORDER BY source_row_id;
\echo '--- physical_location ---'
TABLE plan_loc_phys_batch_4 ORDER BY plan_op_seq;
TABLE feedback_loc_phys_batch_4 ORDER BY source_row_id;
\echo '--- postal_location ---'
TABLE plan_loc_post_batch_4 ORDER BY plan_op_seq;
TABLE feedback_loc_post_batch_4 ORDER BY source_row_id;
\echo '--- stat_employees ---'
TABLE plan_stat_employees_batch_4 ORDER BY plan_op_seq;
TABLE feedback_stat_employees_batch_4 ORDER BY source_row_id;
\echo '--- stat_turnover ---'
TABLE plan_stat_turnover_batch_4 ORDER BY plan_op_seq;
TABLE feedback_stat_turnover_batch_4 ORDER BY source_row_id;
\echo '--- activity_retail ---'
TABLE plan_activity_retail_batch_4 ORDER BY plan_op_seq;
TABLE feedback_activity_retail_batch_4 ORDER BY source_row_id;

\echo '--- ETL Data Table: Final State (all generated IDs are populated) ---'
TABLE etl.data_table ORDER BY identity_correlation, row_id;

\echo '--- Final Target States ---'
\echo '--- legal_unit ---'
TABLE etl.legal_unit ORDER BY id, valid_from;
\echo '--- location ---'
TABLE etl.location ORDER BY id, valid_from;
\echo '--- stat_for_unit ---'
TABLE etl.stat_for_unit ORDER BY id, valid_from;
\echo '--- ident (non-temporal) ---'
TABLE etl.ident ORDER BY id;
\echo '--- activity (temporal, natural key) ---'
TABLE etl.activity ORDER BY legal_unit_id, activity_type_id, valid_from;

ROLLBACK;
\i sql/include/test_teardown.sql
