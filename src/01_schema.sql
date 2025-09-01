/* This extension is non-relocatable */
CREATE SCHEMA sql_saga;
GRANT USAGE ON SCHEMA sql_saga TO PUBLIC;

CREATE TYPE sql_saga.drop_behavior AS ENUM ('CASCADE', 'RESTRICT');
CREATE TYPE sql_saga.fk_actions AS ENUM ('CASCADE', 'SET NULL', 'SET DEFAULT', 'RESTRICT', 'NO ACTION');
CREATE TYPE sql_saga.fk_match_types AS ENUM ('FULL', 'PARTIAL', 'SIMPLE');
CREATE TYPE sql_saga.fg_type AS ENUM ('temporal_to_temporal', 'regular_to_temporal');
COMMENT ON TYPE sql_saga.fg_type IS 'Distinguishes between foreign keys from a temporal table to another temporal table, and from a regular (non-temporal) table to a temporal table.';

-- This enum represents Allen's Interval Algebra, a set of thirteen mutually
-- exclusive relations that can hold between two temporal intervals. These
-- relations are fundamental to the logic of the temporal_merge planner.
-- See: https://en.wikipedia.org/wiki/Allen%27s_interval_algebra
--
-- The relations are defined for two intervals, X and Y, with start and end points
-- X.start, X.end, Y.start, Y.end. For sql_saga, which uses inclusive-start and
-- exclusive-end intervals `[)`, the conditions are adapted accordingly.
DO $$ BEGIN
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
        'preceded by',   -- Y precedes X
        'met by',        -- Y meets X
        'overlapped by', -- Y overlaps X
        'started by',    -- Y starts X
        'contains',      -- Y is during X
        'finished by'    -- Y finishes X
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;


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
    bounds_check_constraint name NOT NULL,
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
    FOREIGN KEY (table_schema, table_name, era_name) REFERENCES sql_saga.era(table_schema, table_name, era_name),

    CHECK (era_name = 'system_time')
);
GRANT SELECT ON TABLE sql_saga.system_time_era TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('sql_saga.system_time_era', '');
COMMENT ON TABLE sql_saga.system_time_era IS 'Stores metadata specific to system-versioned eras.';

CREATE TABLE sql_saga.unique_keys (
    unique_key_name name NOT NULL,
    table_schema name NOT NULL,
    table_name name NOT NULL,
    column_names name[] NOT NULL,
    era_name name NOT NULL,
    unique_constraint name NOT NULL,
    exclude_constraint name NOT NULL,
    predicate text,

    PRIMARY KEY (unique_key_name),

    FOREIGN KEY (table_schema, table_name, era_name) REFERENCES sql_saga.era (table_schema, table_name, era_name)
);
GRANT SELECT ON TABLE sql_saga.unique_keys TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('sql_saga.unique_keys', '');

COMMENT ON TABLE sql_saga.unique_keys IS 'A registry of UNIQUE/PRIMARY keys using era WITHOUT OVERLAPS';

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

    PRIMARY KEY (foreign_key_name),

    -- No longer possible to have a direct FK to sql_saga.era
    FOREIGN KEY (unique_key_name) REFERENCES sql_saga.unique_keys,

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

    FOREIGN KEY (table_schema, table_name, era_name) REFERENCES sql_saga.era(table_schema, table_name, era_name),

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
    'upsert_patch',
    'upsert_replace',
    'patch_only',
    'replace_only',
    'insert_only'
);
COMMENT ON TYPE sql_saga.temporal_merge_mode IS 'Defines the behavior of the temporal_merge procedure, specifying how source data should be applied to the target table (e.g., insert and update, or only insert).';

DO $$ BEGIN
    CREATE TYPE sql_saga.temporal_merge_delete_mode AS ENUM (
        'NONE',
        'DELETE_MISSING_TIMELINE',
        'DELETE_MISSING_ENTITIES',
        'DELETE_MISSING_TIMELINE_AND_ENTITIES'
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

COMMENT ON TYPE sql_saga.temporal_merge_delete_mode IS
'Controls deletion behavior for `replace` modes.
- NONE (default): No deletions occur.
- DELETE_MISSING_TIMELINE: For entities in the source, any part of their target timeline not covered by the source is deleted.
- DELETE_MISSING_ENTITIES: Any entity in the target that is not present in the source is completely deleted.
- DELETE_MISSING_TIMELINE_AND_ENTITIES: A combination of both timeline and entity deletion.';

DO $$ BEGIN
    CREATE TYPE sql_saga.temporal_merge_status AS ENUM ('APPLIED', 'SKIPPED', 'TARGET_NOT_FOUND', 'ERROR');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

COMMENT ON TYPE sql_saga.temporal_merge_status IS
'Defines the possible return statuses for a row processed by `temporal_merge`.
- APPLIED: The operation was successfully planned and executed, resulting in a change to the target table.
- SKIPPED: A successful but non-operative outcome where the row was benignly skipped (e.g., an idempotent update, or an `insert_only` on a pre-existing entity).
- TARGET_NOT_FOUND: A successful but non-operative outcome where a patch/replace operation could not find its target, signaling a potential data issue.
- ERROR: A catastrophic failure occurred. The transaction was rolled back, and the `error_message` column will be populated.';

DO $$ BEGIN
    CREATE TYPE sql_saga.planner_action AS ENUM ('INSERT', 'UPDATE', 'DELETE', 'IDENTICAL');
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

COMMENT ON TYPE sql_saga.planner_action IS
'Represents the type of DML operation to be performed for a given time segment.
- INSERT: A new historical record will be inserted.
- UPDATE: An existing historical record will be modified (typically by shortening its period).
- DELETE: An existing historical record will be deleted.
- IDENTICAL: An existing historical record is identical to the source data and requires no change. This is used by the planner to identify idempotent operations.';

-- Defines the structure for a single operation in a temporal execution plan.
DO $$ BEGIN
    CREATE TYPE sql_saga.temporal_plan_op AS (
        plan_op_seq BIGINT,
        source_row_ids INTEGER[],
        operation sql_saga.planner_action,
        entity_ids JSONB, -- A JSONB object representing the composite key, e.g. {"id": 1} or {"stat_definition_id": 1, "establishment_id": 101}
        old_valid_from TEXT,
        new_valid_from TEXT,
        new_valid_until TEXT,
        data JSONB,
        relation sql_saga.allen_interval_relation
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;

-- Defines the structure for a temporal merge operation result.
DO $$ BEGIN
    CREATE TYPE sql_saga.temporal_merge_result AS (
        source_row_id INTEGER,
        target_entity_ids JSONB,
        status sql_saga.temporal_merge_status,
        error_message TEXT
    );
EXCEPTION
    WHEN duplicate_object THEN null;
END $$;


CREATE TYPE sql_saga.updatable_view_type AS ENUM ('for_portion_of', 'current');
COMMENT ON TYPE sql_saga.updatable_view_type IS 'Defines the semantic type of an updatable view. "for_portion_of" provides direct access to historical records, while "current" provides a simplified view of only the currently active data.';

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

    PRIMARY KEY (view_schema, view_name),
    FOREIGN KEY (table_schema, table_name, era_name) REFERENCES sql_saga.era (table_schema, table_name, era_name) ON DELETE CASCADE
);
GRANT SELECT ON TABLE sql_saga.updatable_view TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('sql_saga.updatable_view', '');
COMMENT ON TABLE sql_saga.updatable_view IS 'A registry of updatable views created by sql_saga, linking a view to its underlying temporal table and era.';


