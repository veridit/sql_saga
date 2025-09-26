/* This extension is non-relocatable */
CREATE SCHEMA sql_saga;
GRANT USAGE ON SCHEMA sql_saga TO PUBLIC;

CREATE TYPE sql_saga.drop_behavior AS ENUM ('CASCADE', 'RESTRICT');
CREATE TYPE sql_saga.fk_actions AS ENUM ('CASCADE', 'SET NULL', 'SET DEFAULT', 'RESTRICT', 'NO ACTION');
CREATE TYPE sql_saga.fk_match_types AS ENUM ('FULL', 'PARTIAL', 'SIMPLE');
CREATE TYPE sql_saga.fg_type AS ENUM ('temporal_to_temporal', 'regular_to_temporal');
COMMENT ON TYPE sql_saga.fg_type IS 'Distinguishes between foreign keys from a temporal table to another temporal table, and from a regular (non-temporal) table to a temporal table.';

CREATE TYPE sql_saga.unique_key_type AS ENUM ('primary', 'natural', 'predicated');
COMMENT ON TYPE sql_saga.unique_key_type IS 'Distinguishes between a temporal primary key, a natural key (unique, for FKs), and a predicated key (a unique index with a WHERE clause).';

-- This enum represents Allen's Interval Algebra, a set of thirteen mutually
-- exclusive relations that can hold between two temporal intervals. These
-- relations are fundamental to the logic of the temporal_merge planner.
-- See: https://en.wikipedia.org/wiki/Allen%27s_interval_algebra
--
-- The relations are defined for two intervals, X and Y, with start and end points
-- X.start, X.end, Y.start, Y.end. For sql_saga, which uses inclusive-start and
-- exclusive-end intervals `[)`, the conditions are adapted accordingly.
CREATE TYPE sql_saga.allen_interval_relation AS ENUM (
        -- X [) entirely before Y [)
        -- Condition: X.end < Y.start
        'precedes',
        -- X [) meets Y [) at the boundary
        -- Condition: X.end = Y.start
        'meets',
        -- X [) starts before Y [) and they overlap
        -- Condition: X.start < Y.start AND X.end > Y.start AND X.end < Y.end
        'overlaps',
        -- X [) and Y [) share the same start, but X ends before Y
        -- Condition: X.start = Y.start AND X.end < Y.end
        'starts',
        -- X [) is entirely contained within Y [) but does not share a boundary
        -- Condition: X.start > Y.start AND X.end < Y.end
        'during',
        -- X [) and Y [) share the same end, but X starts after Y
        -- Condition: X.start > Y.start AND X.end = Y.end
        'finishes',
        -- X [) and Y [) are the exact same interval
        -- Condition: X.start = Y.start AND X.end = Y.end
        'equals',
        -- Inverse relations (Y relative to X)
        'preceded_by',   -- Y precedes X
        'met_by',        -- Y meets X
        'overlapped_by', -- Y overlaps X
        'started_by',    -- Y starts X
        'contains',      -- Y is during X
        'finished_by'    -- Y finishes X
    );


/*
 * All referencing columns must be either name or regsomething in order for
 * pg_dump to work properly.  Plain OIDs are not allowed but attribute numbers
 * are, so that we don't have to track renames.
 *
 * Anything declared as regsomething and created for the period (such as the
 * "__as_of" function), should be UNIQUE.  If Postgres already verifies
 * uniqueness, such as constraint names on a table, then we don't need to do it
 * also.
 */

CREATE TABLE sql_saga.era (
    table_schema name NOT NULL,
    table_name name NOT NULL,
    era_name name NOT NULL DEFAULT 'valid',
    valid_from_column_name name NOT NULL,
    valid_until_column_name name NOT NULL,
    -- active_column_name name NOT NULL,
    range_type regtype NOT NULL,
    range_subtype regtype NOT NULL,
    -- The category of the range's subtype (e.g., 'D' for DateTime, 'N' for Numeric).
    -- This is cached for performance and clarity.
    -- See: https://www.postgresql.org/docs/current/catalog-pg-type.html#CATALOG-TYPCATEGORY-TABLE
    range_subtype_category char(1) NOT NULL,
    bounds_check_constraint name,
    synchronize_valid_to_column name,
    synchronize_range_column name,
    trigger_applies_defaults boolean NOT NULL DEFAULT false,
    -- infinity_check_constraint name NOT NULL,
    -- generated_always_trigger name NOT NULL,
    audit_schema_name name,
    audit_table_name name,
    -- audit_trigger name NOT NULL,
    -- delete_trigger name NOT NULL,
    --excluded_column_names name[] NOT NULL DEFAULT '{}',
    -- UNIQUE(...) for each trigger/function name.

    PRIMARY KEY (table_schema, table_name, era_name),

    CHECK (valid_from_column_name <> valid_until_column_name)
);
COMMENT ON TABLE sql_saga.era IS 'The main catalog for sql_saga.  All "DDL" operations for periods must first take an exclusive lock on this table.';
GRANT SELECT ON TABLE sql_saga.era TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('sql_saga.era', '');

CREATE TABLE sql_saga.system_time_era (
    table_schema name NOT NULL,
    table_name name NOT NULL,
    era_name name NOT NULL,
    infinity_check_constraint name NOT NULL,
    generated_always_trigger name NOT NULL,
    write_history_trigger name NOT NULL,
    truncate_trigger name NOT NULL,
    excluded_column_names name[] NOT NULL DEFAULT '{}',

    PRIMARY KEY (table_schema, table_name, era_name),
    FOREIGN KEY (table_schema, table_name, era_name) REFERENCES sql_saga.era(table_schema, table_name, era_name) ON DELETE CASCADE,

    CHECK (era_name = 'system_time')
);
GRANT SELECT ON TABLE sql_saga.system_time_era TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('sql_saga.system_time_era', '');
COMMENT ON TABLE sql_saga.system_time_era IS 'Stores metadata specific to system-versioned eras.';

CREATE TABLE sql_saga.unique_keys (
    unique_key_name name NOT NULL,
    table_schema name NOT NULL,
    table_name name NOT NULL,
    key_type sql_saga.unique_key_type NOT NULL,
    column_names name[] NOT NULL,
    era_name name NOT NULL,
    unique_constraint name,
    exclude_constraint name,
    check_constraint name,
    predicate text,
    mutually_exclusive_columns name[],
    partial_index_names name[],
    partial_exclude_constraint_names name[],

    PRIMARY KEY (unique_key_name),

    FOREIGN KEY (table_schema, table_name, era_name) REFERENCES sql_saga.era (table_schema, table_name, era_name) ON DELETE CASCADE
);
GRANT SELECT ON TABLE sql_saga.unique_keys TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('sql_saga.unique_keys', '');

COMMENT ON TABLE sql_saga.unique_keys IS 'A registry of UNIQUE/PRIMARY keys using era WITHOUT OVERLAPS';
COMMENT ON COLUMN sql_saga.unique_keys.mutually_exclusive_columns IS 'For complex keys, stores the subset of columns that are mutually exclusive (i.e., exactly one must be NOT NULL).';
COMMENT ON COLUMN sql_saga.unique_keys.partial_index_names IS 'For mutually exclusive keys, stores the names of the partial unique indexes created for performance.';
COMMENT ON COLUMN sql_saga.unique_keys.partial_exclude_constraint_names IS 'For mutually exclusive keys, stores the names of the partial exclusion constraints created for correctness.';
COMMENT ON COLUMN sql_saga.unique_keys.check_constraint IS 'For mutually exclusive keys, stores the name of the CHECK constraint that enforces the XOR logic (exactly one column is NOT NULL).';

CREATE TABLE sql_saga.foreign_keys (
    foreign_key_name name NOT NULL,
    type sql_saga.fg_type NOT NULL,
    table_schema name NOT NULL,
    table_name name NOT NULL,
    column_names name[] NOT NULL,
    fk_era_name name, -- Null for non-temporal tables
    fk_table_columns_snapshot name[] NOT NULL,
    unique_key_name name NOT NULL,
    match_type sql_saga.fk_match_types NOT NULL DEFAULT 'SIMPLE',
    update_action sql_saga.fk_actions NOT NULL DEFAULT 'NO ACTION',
    delete_action sql_saga.fk_actions NOT NULL DEFAULT 'NO ACTION',

    -- For temporal FKs
    fk_insert_trigger name,
    fk_update_trigger name,

    -- For regular FKs
    fk_check_constraint name,
    fk_helper_function text, -- regprocedure signature

    -- These are always on the unique key's table
    uk_update_trigger name NOT NULL,
    uk_delete_trigger name NOT NULL,
    fk_index_name name, -- Stores the name of an index created automatically on the FK table

    PRIMARY KEY (foreign_key_name),

    -- No longer possible to have a direct FK to sql_saga.era
    FOREIGN KEY (unique_key_name) REFERENCES sql_saga.unique_keys ON DELETE CASCADE,

    CHECK (delete_action NOT IN ('CASCADE', 'SET NULL', 'SET DEFAULT')),
    CHECK (update_action NOT IN ('CASCADE', 'SET NULL', 'SET DEFAULT')),

    CHECK (
        CASE type
            WHEN 'temporal_to_temporal' THEN
                fk_era_name IS NOT NULL
                AND fk_insert_trigger IS NOT NULL AND fk_update_trigger IS NOT NULL
                AND fk_check_constraint IS NULL AND fk_helper_function IS NULL
            WHEN 'regular_to_temporal' THEN
                fk_era_name IS NULL
                AND fk_insert_trigger IS NULL AND fk_update_trigger IS NULL
                AND fk_check_constraint IS NOT NULL AND fk_helper_function IS NOT NULL
        END
    )
);
GRANT SELECT ON TABLE sql_saga.foreign_keys TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('sql_saga.foreign_keys', '');

COMMENT ON TABLE sql_saga.foreign_keys IS 'A registry of foreign keys. Supports both temporal-to-temporal and regular-to-temporal relationships.';
COMMENT ON COLUMN sql_saga.foreign_keys.fk_table_columns_snapshot IS 'A snapshot of all columns on the fk table, used by the rename_following event trigger to detect column renames.';

CREATE TABLE sql_saga.system_versioning (
    table_schema name NOT NULL,
    table_name name NOT NULL,
    era_name name NOT NULL,
    history_schema_name name NOT NULL,
    history_table_name name NOT NULL,
    view_schema_name name NOT NULL,
    view_table_name name NOT NULL,

    -- These functions should be of type regprocedure, but that blocks pg_upgrade.
    func_as_of text NOT NULL,
    func_between text NOT NULL,
    func_between_symmetric text NOT NULL,
    func_from_to text NOT NULL,

    PRIMARY KEY (table_schema, table_name),

    FOREIGN KEY (table_schema, table_name, era_name) REFERENCES sql_saga.era(table_schema, table_name, era_name) ON DELETE CASCADE,

    CHECK (era_name = 'system_time'),

    UNIQUE (history_schema_name, history_table_name),
    UNIQUE (view_schema_name, view_table_name),
    UNIQUE (func_as_of),
    UNIQUE (func_between),
    UNIQUE (func_between_symmetric),
    UNIQUE (func_from_to)
);
GRANT SELECT ON TABLE sql_saga.system_versioning TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('sql_saga.system_versioning', '');
COMMENT ON TABLE sql_saga.system_versioning IS 'A registry of tables with SYSTEM VERSIONING';


-- Types for temporal_merge
DROP TYPE IF EXISTS sql_saga.temporal_merge_mode CASCADE;
CREATE TYPE sql_saga.temporal_merge_mode AS ENUM (
     -- The main workhorse.
    'MERGE_ENTITY_UPSERT',
    'UPDATE_FOR_PORTION_OF',
    -- Variety of UPSERT/UPDATE where NULL's are ignored.
    'MERGE_ENTITY_PATCH',
    'PATCH_FOR_PORTION_OF',
    -- Variety of UPSERT/UPDATE where everything is replaced.
    'MERGE_ENTITY_REPLACE',
    'REPLACE_FOR_PORTION_OF',
    -- Special purpose restricted operations.
    'INSERT_NEW_ENTITIES',
    'DELETE_FOR_PORTION_OF'
);
COMMENT ON TYPE sql_saga.temporal_merge_mode IS
'Defines the behavior of the temporal_merge procedure.
-- The main workhorse.
- MERGE_ENTITY_UPSERT: Inserts new entities or performs a partial update on existing timelines. NULL is treated as an explicit value.
- UPDATE_FOR_PORTION_OF: Applies a surgical partial update to a time portion of an existing entity. Skips new entities.
-- Variety of UPSERT/UPDATE where NULL''s are ignored.
- MERGE_ENTITY_PATCH: Merges source with target, patching data by ignoring NULL values in the source.
- PATCH_FOR_PORTION_OF: Applies a surgical patch to a time portion of an existing entity, ignoring NULL values in the source.
-- Variety of UPSERT/UPDATE where everything is replaced.
- MERGE_ENTITY_REPLACE: Merges source with target, replacing overlapping data.
- REPLACE_FOR_PORTION_OF: Applies a surgical replacement of a time portion of an existing entity.
-- Special purpose restricted operations.
- INSERT_NEW_ENTITIES: Inserts only new entities.
- DELETE_FOR_PORTION_OF: Performs a surgical deletion of a time portion from an existing entity.';

CREATE TYPE sql_saga.temporal_merge_delete_mode AS ENUM (
    'NONE',
    'DELETE_MISSING_TIMELINE',
    'DELETE_MISSING_ENTITIES',
    'DELETE_MISSING_TIMELINE_AND_ENTITIES'
);

COMMENT ON TYPE sql_saga.temporal_merge_delete_mode IS
'Controls deletion behavior for `replace` modes.
- NONE (default): No deletions occur.
- DELETE_MISSING_TIMELINE: For entities in the source, any part of their target timeline not covered by the source is deleted.
- DELETE_MISSING_ENTITIES: Any entity in the target that is not present in the source is completely deleted.
- DELETE_MISSING_TIMELINE_AND_ENTITIES: A combination of both timeline and entity deletion.';

CREATE TYPE sql_saga.temporal_merge_feedback_status AS ENUM (
    'APPLIED',
    'SKIPPED_IDENTICAL',
    'SKIPPED_FILTERED',
    'SKIPPED_NO_TARGET',
    'ERROR'
);

COMMENT ON TYPE sql_saga.temporal_merge_feedback_status IS
'Defines the possible return statuses for a row processed by the `temporal_merge` executor.
- APPLIED: The operation was successfully planned and executed, resulting in a change to the target table.
- SKIPPED_IDENTICAL: A benign no-op where the source data was identical to the target data.
- SKIPPED_FILTERED: A benign no-op where the source row was correctly filtered by the mode''s logic (e.g., an `INSERT_NEW_ENTITIES` for an entity that already exists).
- SKIPPED_NO_TARGET: An actionable no-op where the operation failed because the target entity was not found. This signals a potential data quality issue.
- ERROR: A catastrophic planner failure occurred, indicating a bug in the merge logic. The `error_message` column will be populated.';

CREATE TYPE sql_saga.temporal_merge_plan_action AS ENUM (
    'INSERT',
    'UPDATE',
    'DELETE',
    'SKIP_IDENTICAL',
    'SKIP_NO_TARGET',
    'SKIP_FILTERED',
    'ERROR'
);

COMMENT ON TYPE sql_saga.temporal_merge_plan_action IS
'Represents the internal DML action to be taken by the executor for a given atomical time segment, as determined by the planner.
These values use a "future tense" convention (e.g., SKIP_...) as they represent a plan for an action, not a completed result.
The order of these values is critical, as it defines the execution order when sorting the plan:
INSERTs must happen before UPDATEs, which must happen before DELETEs to ensure foreign key consistency,
that is check on the intermediate MVCC snapshots between the changes in the same transaction.
- INSERT: A new historical record will be inserted.
- UPDATE: An existing historical record will be modified (typically by shortening its period).
- DELETE: An existing historical record will be deleted.
- SKIP_IDENTICAL: A historical record segment is identical to the source data and requires no change.
- SKIP_NO_TARGET: A source row should be skipped because its target entity does not exist in a mode that requires it (e.g. PATCH_FOR_PORTION_OF). This is used by the executor to generate a SKIPPED_NO_TARGET feedback status.
- SKIP_FILTERED: A source row should be skipped because it was correctly filtered by the mode''s logic (e.g. INSERT_NEW_ENTITIES for an entity that already exists).
- ERROR: A safeguard action indicating the planner could not generate a valid plan for a row, signaling a bug.';

-- Defines the effect of an UPDATE on a timeline, used for ordering DML.
CREATE TYPE sql_saga.temporal_merge_update_effect AS ENUM (
    'GROW',   -- The new period is a superset of the old one.
    'NONE',   -- The period is unchanged (a data-only update).
    'MOVE',   -- The period shifts without being a pure grow or shrink.
    'SHRINK'  -- The new period is a subset of the old one.
);

COMMENT ON TYPE sql_saga.temporal_merge_update_effect IS
'Defines the effect of an UPDATE on a timeline segment, used for ordering DML operations to ensure temporal integrity.
The planner relies on this specific ENUM order for sorting: timeline-extending operations must execute before timeline-shortening operations.
- GROW: The new period is a superset of the old one. These are executed first.
- NONE: The period is unchanged (a data-only update).
- MOVE: The period shifts without being a pure grow or shrink.
- SHRINK: The new period is a subset of the old one. These are executed last.';

CREATE TYPE sql_saga.temporal_merge_plan AS (
    plan_op_seq BIGINT,
    row_ids BIGINT[],
    operation sql_saga.temporal_merge_plan_action,
    update_effect sql_saga.temporal_merge_update_effect,
    corr_ent TEXT,
    new_ent BOOLEAN,
    entity_ids JSONB,
    s_t_relation sql_saga.allen_interval_relation,
    b_a_relation sql_saga.allen_interval_relation,
    old_valid_from TEXT,
    old_valid_until TEXT,
    new_valid_from TEXT,
    new_valid_until TEXT,
    data JSONB,
    feedback JSONB,
    trace JSONB
);

-- Defines the structure for a temporal executor feedback result.
CREATE TYPE sql_saga.temporal_merge_feedback AS (
    source_row_id BIGINT,
    target_entity_ids JSONB,
    status sql_saga.temporal_merge_feedback_status,
    error_message TEXT
);


CREATE TYPE sql_saga.updatable_view_type AS ENUM ('for_portion_of', 'current');
COMMENT ON TYPE sql_saga.updatable_view_type IS 'Defines the semantic type of an updatable view. "for_portion_of" provides direct access to historical records, while "current" provides a simplified view of only the currently active data.';

CREATE TYPE sql_saga.trigger_action AS ENUM ('enable', 'disable');
COMMENT ON TYPE sql_saga.trigger_action IS 'Defines the action for the manage_temporal_fk_triggers procedure.';

CREATE VIEW sql_saga.information_schema__era AS
    SELECT current_catalog AS table_catalog,
           e.table_schema,
           e.table_name,
           e.era_name,
           e.valid_from_column_name,
           e.valid_until_column_name
    FROM sql_saga.era AS e;


CREATE TABLE sql_saga.updatable_view (
    view_schema name NOT NULL,
    view_name name NOT NULL,
    view_type sql_saga.updatable_view_type NOT NULL,

    -- These three columns form the composite foreign key to the era table.
    -- They are necessary because an era_name is only unique within a single table.
    table_schema name NOT NULL,
    table_name name NOT NULL,
    era_name name NOT NULL,

    trigger_name name NOT NULL,
    current_func text, -- Stores the function call, e.g., 'now()' or 'my_test_now()'

    PRIMARY KEY (view_schema, view_name),
    FOREIGN KEY (table_schema, table_name, era_name) REFERENCES sql_saga.era (table_schema, table_name, era_name) ON DELETE CASCADE,
    CHECK ((current_func IS NULL) = (view_type <> 'current'))
);
GRANT SELECT ON TABLE sql_saga.updatable_view TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('sql_saga.updatable_view', '');
COMMENT ON TABLE sql_saga.updatable_view IS 'A registry of updatable views created by sql_saga, linking a view to its underlying temporal table and era.';


