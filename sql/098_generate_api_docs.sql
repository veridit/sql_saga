\i sql/include/test_setup.sql

-- Create the docs directory if it doesn't exist
\! mkdir -p docs

-- Turn off all decorative output for clean markdown
\t
\a

-- Redirect output to the API documentation file
\o doc/api.md

-- Generate the header for the documentation file
SELECT '# sql_saga API Reference

This document is automatically generated from the database schema by the `80_generate_api_docs.sql` test. Do not edit it manually.

';

-- Query to introspect and format ENUM types
WITH enums AS (
    SELECT
        t.oid,
        t.typname AS enum_name,
        d.description
    FROM pg_type t
    JOIN pg_namespace n ON n.oid = t.typnamespace
    LEFT JOIN pg_description d ON d.objoid = t.oid
    WHERE n.nspname = 'sql_saga' AND t.typtype = 'e'
),
enum_vals AS (
    SELECT
        e.enumtypid,
        string_agg(format('- %s', e.enumlabel), E'\n' ORDER BY e.enumsortorder) as values_md
    FROM pg_enum e
    GROUP BY e.enumtypid
)
SELECT
    '## Enumerated Types' || E'\n\n' ||
    string_agg(
        format(
            '### %s' || E'\n\n' ||
            COALESCE('> ' || replace(description, E'\n', E'\n> ') || E'\n\n', '') ||
            '```sql' || E'\n' ||
            '%s' || E'\n' ||
            '```',
            e.enum_name,
            ev.values_md
        ),
        E'\n\n' ORDER BY e.enum_name
    )
FROM enums e JOIN enum_vals ev ON e.oid = ev.enumtypid;


-- Query to introspect and format the API documentation
CREATE OR REPLACE TEMP VIEW funcs AS
    SELECT
        p.proname AS func_name,
        CASE
            WHEN p.prokind = 'a' THEN
                -- Format aggregate signature
                format('AGGREGATE %s(%s)', p.proname, pg_get_function_arguments(p.oid))
            WHEN p.prokind = 'p' THEN
                -- Format procedure signature (name and arguments) with multi-line formatting
                format('PROCEDURE %s(%s)%s', p.proname,
                    CASE WHEN pg_get_function_arguments(p.oid) = '' THEN ''
                    ELSE E'\n    ' || replace(pg_get_function_arguments(p.oid), ', ', E',\n    ') || E'\n'
                    END,
                    CASE WHEN p.prosecdef THEN E'\nSECURITY DEFINER' ELSE E'\nSECURITY INVOKER' END)
            ELSE -- 'f' for function
                -- Format function signature (name, arguments, and return type) with multi-line formatting
                format('FUNCTION %s(%s) RETURNS %s%s', p.proname,
                    CASE WHEN pg_get_function_arguments(p.oid) = '' THEN ''
                    ELSE E'\n    ' || replace(pg_get_function_arguments(p.oid), ', ', E',\n    ') || E'\n'
                    END,
                    pg_get_function_result(p.oid),
                    CASE WHEN p.prosecdef THEN E'\nSECURITY DEFINER' ELSE E'\nSECURITY INVOKER' END)
        END AS func_def,
        d.description,
        CASE
            WHEN p.proname IN ('add_era', 'drop_era') THEN 1
            WHEN p.proname IN ('add_unique_key', 'drop_unique_key', 'drop_unique_key_by_name') THEN 2
            WHEN p.proname IN ('add_foreign_key', 'add_temporal_foreign_key', 'add_regular_foreign_key', 'drop_foreign_key', 'drop_foreign_key_by_name') THEN 3
            WHEN p.proname IN ('add_current_view', 'drop_current_view', 'add_for_portion_of_view', 'drop_for_portion_of_view') THEN 4
            WHEN p.proname IN ('temporal_merge', 'disable_temporal_triggers', 'enable_temporal_triggers', 'manage_temporal_fk_triggers') THEN 5
            WHEN p.proname IN ('add_system_versioning', 'drop_system_versioning', 'set_system_time_era_excluded_columns', 'drop_system_time_era') THEN 6
            WHEN p.prokind = 'a' THEN 7
            WHEN p.proname LIKE 'health_check%' THEN 8
            WHEN p.proname IN ('get_allen_relation', 'drop_protection', 'rename_following') THEN 9
        END AS category_order,
        CASE
            WHEN p.proname IN ('add_era', 'drop_era') THEN 'Era Management'
            WHEN p.proname IN ('add_unique_key', 'drop_unique_key', 'drop_unique_key_by_name') THEN 'Unique Keys'
            WHEN p.proname IN ('add_foreign_key', 'add_temporal_foreign_key', 'add_regular_foreign_key', 'drop_foreign_key', 'drop_foreign_key_by_name') THEN 'Foreign Keys'
            WHEN p.proname IN ('add_current_view', 'drop_current_view', 'add_for_portion_of_view', 'drop_for_portion_of_view') THEN 'Updatable Views'
            WHEN p.proname IN ('temporal_merge', 'disable_temporal_triggers', 'enable_temporal_triggers', 'manage_temporal_fk_triggers') THEN 'Bulk Data Loading'
            WHEN p.proname IN ('add_system_versioning', 'drop_system_versioning', 'set_system_time_era_excluded_columns', 'drop_system_time_era') THEN 'System Versioning'
            WHEN p.prokind = 'a' THEN 'Aggregates'
            WHEN p.proname LIKE 'health_check%' THEN 'Health Checks'
            WHEN p.proname IN ('get_allen_relation', 'drop_protection', 'rename_following') THEN 'Internal and Helper Functions'
        END AS category_name
    FROM
        pg_proc p
    JOIN
        pg_namespace n ON p.pronamespace = n.oid
    LEFT JOIN
        pg_description d ON d.objoid = p.oid
    LEFT JOIN
        pg_aggregate agg ON agg.aggfnoid = p.oid
    WHERE
        n.nspname = 'sql_saga'
        AND p.prokind IN ('f', 'p', 'a')
        AND p.prorettype <> 'trigger'::regtype -- Exclude all DML trigger functions
        -- Exclude other internal helper functions that are not triggers
        AND p.proname NOT LIKE E'\\_\\_%'
        AND p.proname NOT IN (
            'temporal_merge_plan', 'temporal_merge_execute',
            'temporal_merge_delete_temp_tables',
            'covers_without_gaps_transfn', 'covers_without_gaps_finalfn',
            'first_sfunc'
        );

SELECT
    E'\n\n' || string_agg(
        '## ' || category_name || E'\n\n' || api_docs,
        E'\n\n' ORDER BY category_order
    )
FROM (
    SELECT
        category_name,
        category_order,
        string_agg(
            format(
                '### %s' || E'\n\n' ||
                COALESCE('> ' || replace(description, E'\n', E'\n> ') || E'\n\n', '') ||
                '```sql' || E'\n' ||
                '%s' || E'\n' ||
                '```',
                func_name,
                func_def
            ),
            E'\n\n' ORDER BY func_name, func_def
        ) AS api_docs
    FROM funcs
    WHERE category_name IS NOT NULL
    GROUP BY category_name, category_order
) sub;




-- Stop redirecting output
\o

-- Turn decorative output back on for the test result
\t
\a

-- A simple select to confirm the script ran
SELECT 'API documentation generated in doc/api.md' AS result;

-- Check for any public functions that have not been assigned to a category
SELECT 'Uncategorized API functions found. Please add them to a category in `sql/80_generate_api_docs.sql`:' AS " ",
       string_agg(format(E'\n- %s', func_def), '' ORDER BY func_name) AS " "
FROM funcs
WHERE category_name IS NULL
HAVING count(*) > 0;

DROP VIEW funcs;

\i sql/include/test_teardown.sql
