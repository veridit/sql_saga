/* This extension is non-relocatable */
CREATE SCHEMA sql_saga;
GRANT USAGE ON SCHEMA sql_saga TO PUBLIC;

CREATE TYPE sql_saga.drop_behavior AS ENUM ('CASCADE', 'RESTRICT');
CREATE TYPE sql_saga.fk_actions AS ENUM ('CASCADE', 'SET NULL', 'SET DEFAULT', 'RESTRICT', 'NO ACTION');
CREATE TYPE sql_saga.fk_match_types AS ENUM ('FULL', 'PARTIAL', 'SIMPLE');
CREATE TYPE sql_saga.fg_type AS ENUM ('temporal_to_temporal', 'standard_to_temporal');

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

    -- For standard FKs
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
            WHEN 'standard_to_temporal' THEN
                fk_era_name IS NULL
                AND fk_insert_trigger IS NULL AND fk_update_trigger IS NULL
                AND fk_check_constraint IS NOT NULL AND fk_helper_function IS NOT NULL
        END
    )
);
GRANT SELECT ON TABLE sql_saga.foreign_keys TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('sql_saga.foreign_keys', '');

COMMENT ON TABLE sql_saga.foreign_keys IS 'A registry of foreign keys. Supports both temporal-to-temporal and standard-to-temporal relationships.';
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


CREATE VIEW sql_saga.information_schema__era AS
    SELECT current_catalog AS table_catalog,
           e.table_schema,
           e.table_name,
           e.era_name,
           e.valid_from_column_name,
           e.valid_until_column_name
    FROM sql_saga.era AS e;


CREATE TABLE sql_saga.api_view (
    table_schema name NOT NULL,
    table_name name NOT NULL,
    era_name name NOT NULL,
    view_schema_name name NOT NULL,
    view_table_name name NOT NULL,
    trigger_name name NOT NULL,
    -- truncate_trigger name NOT NULL,

    PRIMARY KEY (table_schema, table_name, era_name),

    FOREIGN KEY (table_schema, table_name, era_name) REFERENCES sql_saga.era (table_schema, table_name, era_name),

    UNIQUE (view_schema_name, view_table_name)
);
GRANT SELECT ON TABLE sql_saga.api_view TO PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('sql_saga.api_view', '');

CREATE TYPE sql_saga.temporal_merge_mode AS ENUM (
    'upsert_patch',
    'upsert_replace',
    'patch_only',
    'replace_only',
    'insert_only'
);

-- Use Allen's Interval Relation for covering all possible cases of overlap. Ref. https://ics.uci.edu/~alspaugh/cls/shr/allen.html
CREATE TYPE public.allen_interval_relation AS ENUM (
    'precedes',      -- X before Y: X.until < Y.from
                     -- X: [ XXXX )
                     -- Y:           [ YYYY )
    'meets',         -- X meets Y: X.until = Y.from
                     -- X: [ XXXX )
                     -- Y:         [ YYYY )
    'overlaps',      -- X overlaps Y
                     -- X: [ XXXX----)
                     -- Y:      [----YYYY )
    'starts',        -- X starts Y
                     -- X: [ XXXX )
                     -- Y: [ YYYYYYYY )
    'during',        -- X during Y (X is contained in Y)
                     -- X:   [ XXXX )
                     -- Y: [ YYYYYYYY )
    'finishes',      -- X finishes Y
                     -- X:      [ XXXX )
                     -- Y: [ YYYYYYYY )
    'equals',        -- X equals Y
                     -- X: [ XXXX )
                     -- Y: [ YYYY )
    'overlapped_by', -- X is overlapped by Y (Y overlaps X)
                     -- X:      [----XXXX )
                     -- Y: [ YYYY----)
    'started_by',    -- X is started by Y (Y starts X)
                     -- X: [ XXXXXXX )
                     -- Y: [ YYYY )
    'contains',      -- X contains Y (Y is during X)
                     -- X: [ XXXXXXX )
                     -- Y:   [ YYYY )
    'finished_by',   -- X is finished by Y (Y finishes X)
                     -- X: [ XXXXXXX )
                     -- Y:      [ YYYY )
    'met_by',        -- X is met by Y (Y meets X)
                     -- X:         [ XXXX )
                     -- Y: [ YYYY )
    'preceded_by'    -- X is preceded by Y (Y precedes X)
                     -- X:           [ XXXX )
                     -- Y: [ YYYY )
);

COMMENT ON TYPE public.allen_interval_relation IS
'Allen''s interval algebra relations for two intervals X=[X.from, X.until) and Y=[Y.from, Y.until), using [inclusive_start, exclusive_end) semantics.
The ASCII art illustrates interval X relative to interval Y.';

CREATE FUNCTION public.allen_get_relation(
    x_from anyelement, x_until anyelement,
    y_from anyelement, y_until anyelement
) RETURNS public.allen_interval_relation
LANGUAGE sql IMMUTABLE PARALLEL SAFE AS
$BODY$
    SELECT CASE
        -- Cases where start points are the same
        WHEN x_from = y_from AND x_until = y_until THEN 'equals'::public.allen_interval_relation
        WHEN x_from = y_from AND x_until < y_until THEN 'starts'::public.allen_interval_relation
        WHEN x_from = y_from AND x_until > y_until THEN 'started_by'::public.allen_interval_relation
        -- Cases where end points are the same
        WHEN x_from > y_from AND x_until = y_until THEN 'finishes'::public.allen_interval_relation
        WHEN x_from < y_from AND x_until = y_until THEN 'finished_by'::public.allen_interval_relation
        -- Case where one interval is during another
        WHEN x_from > y_from AND x_until < y_until THEN 'during'::public.allen_interval_relation
        WHEN x_from < y_from AND x_until > y_until THEN 'contains'::public.allen_interval_relation
        -- Cases where intervals are adjacent
        WHEN x_until = y_from THEN 'meets'::public.allen_interval_relation
        WHEN y_until = x_from THEN 'met_by'::public.allen_interval_relation
        -- Cases where intervals overlap
        WHEN x_from < y_from AND x_until > y_from AND x_until < y_until THEN 'overlaps'::public.allen_interval_relation
        WHEN y_from < x_from AND y_until > x_from AND y_until < x_until THEN 'overlapped_by'::public.allen_interval_relation
        -- Cases where intervals are disjoint
        WHEN x_until < y_from THEN 'precedes'::public.allen_interval_relation
        WHEN y_until < x_from THEN 'preceded_by'::public.allen_interval_relation
    END;
$BODY$;

COMMENT ON FUNCTION public.allen_get_relation IS
'Calculates the Allen Interval Algebra relation between two intervals X and Y,
assuming [inclusive_start, exclusive_end) semantics.';
