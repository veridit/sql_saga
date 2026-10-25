/*
 * Public management functions: SECURITY INVOKER entry points over private SECURITY DEFINER workers (1.4, review
 * round 2, R1 and R2).
 *
 * PostgreSQL authorizes ALTER TABLE and the other DDL these functions stand for against the current user at the
 * moment of the statement. The workers sql_saga.__api_<name> change user tables with the extension owner's rights,
 * and inside them the current user is the extension owner. So each public function sql_saga.<name> is a C function
 * (api_trampoline, src/sql_saga.c), SECURITY INVOKER, with the worker's arguments, defaults and result, which
 * records the current user -- the role that called it, exactly what native DDL checks, also inside another SECURITY
 * DEFINER function -- and calls the worker with the same arguments. The worker's checks (__internal_require_owner,
 * __internal_require_references) authorize that recorded role. The workers are not executable by PUBLIC; the
 * trampoline calls its worker directly, which needs no EXECUTE privilege.
 *
 * Each signature here must equal its worker's: test 144 compares them, and api_trampoline finds the worker by its
 * exact argument types. The worker's comment is the public function's comment (copied below).
 */

CREATE FUNCTION sql_saga.add_current_view(
    table_oid regclass,
    era_name name DEFAULT 'valid'::name,
    delete_mode name DEFAULT 'delete_as_cutoff'::name,
    current_func_name text DEFAULT NULL::text)
RETURNS boolean
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.add_era(
    table_oid regclass,
    range_column_name name,
    era_name name DEFAULT 'valid'::name,
    synchronize_columns boolean DEFAULT true,
    valid_from_column_name name DEFAULT NULL::name,
    valid_until_column_name name DEFAULT NULL::name,
    valid_to_column_name name DEFAULT NULL::name,
    create_columns boolean DEFAULT false,
    add_defaults boolean DEFAULT true,
    add_bounds_check boolean DEFAULT true,
    range_type regtype DEFAULT NULL::regtype,
    bounds_check_constraint name DEFAULT NULL::name,
    ephemeral_columns name[] DEFAULT NULL::name[])
RETURNS boolean
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.add_for_portion_of_view(
    table_oid regclass,
    era_name name DEFAULT 'valid'::name,
    ephemeral_columns name[] DEFAULT NULL::name[])
RETURNS boolean
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.add_foreign_key(
    fk_table_oid regclass,
    fk_column_names name[],
    pk_table_oid regclass,
    pk_column_names name[],
    fk_era_name name DEFAULT NULL::name,
    match_type sql_saga.fk_match_types DEFAULT 'SIMPLE'::sql_saga.fk_match_types,
    update_action sql_saga.fk_actions DEFAULT 'NO ACTION'::sql_saga.fk_actions,
    delete_action sql_saga.fk_actions DEFAULT 'NO ACTION'::sql_saga.fk_actions,
    foreign_key_name name DEFAULT NULL::name,
    create_index boolean DEFAULT true)
RETURNS name
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.add_regular_foreign_key(
    fk_table_oid regclass,
    fk_column_names name[],
    unique_key_name name,
    match_type sql_saga.fk_match_types DEFAULT 'SIMPLE'::sql_saga.fk_match_types,
    update_action sql_saga.fk_actions DEFAULT 'NO ACTION'::sql_saga.fk_actions,
    delete_action sql_saga.fk_actions DEFAULT 'NO ACTION'::sql_saga.fk_actions,
    foreign_key_name name DEFAULT NULL::name,
    fk_check_constraint name DEFAULT NULL::name,
    fk_helper_function text DEFAULT NULL::text,
    uk_update_trigger name DEFAULT NULL::name,
    uk_delete_trigger name DEFAULT NULL::name,
    create_index boolean DEFAULT true)
RETURNS name
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE PROCEDURE sql_saga.add_synchronize_temporal_columns_trigger(
    IN table_oid regclass,
    IN era_name name)
LANGUAGE c SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.add_system_versioning(
    table_oid regclass,
    history_table_name name DEFAULT NULL::name,
    view_name name DEFAULT NULL::name,
    function_as_of_name name DEFAULT NULL::name,
    function_between_name name DEFAULT NULL::name,
    function_between_symmetric_name name DEFAULT NULL::name,
    function_from_to_name name DEFAULT NULL::name,
    history_table_must_exist boolean DEFAULT false)
RETURNS void
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.add_temporal_foreign_key(
    fk_table_oid regclass,
    fk_column_names name[],
    fk_era_name name,
    unique_key_name name,
    match_type sql_saga.fk_match_types DEFAULT 'SIMPLE'::sql_saga.fk_match_types,
    update_action sql_saga.fk_actions DEFAULT 'NO ACTION'::sql_saga.fk_actions,
    delete_action sql_saga.fk_actions DEFAULT 'NO ACTION'::sql_saga.fk_actions,
    foreign_key_name name DEFAULT NULL::name,
    uk_update_trigger name DEFAULT NULL::name,
    uk_delete_trigger name DEFAULT NULL::name,
    create_index boolean DEFAULT true)
RETURNS name
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.add_unique_key(
    table_oid regclass,
    column_names name[],
    era_name name DEFAULT 'valid'::name,
    key_type sql_saga.unique_key_type DEFAULT 'natural'::sql_saga.unique_key_type,
    enforce_consistency_with_primary_key boolean DEFAULT NULL::boolean,
    unique_key_name name DEFAULT NULL::name,
    unique_constraint name DEFAULT NULL::name,
    exclude_constraint name DEFAULT NULL::name,
    predicate text DEFAULT NULL::text,
    mutually_exclusive_columns name[] DEFAULT NULL::name[])
RETURNS name
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.drop_current_view(
    table_oid regclass,
    era_name name DEFAULT 'valid'::name,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior)
RETURNS boolean
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.drop_era(
    table_oid regclass,
    era_name name DEFAULT 'valid'::name,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior,
    cleanup boolean DEFAULT false)
RETURNS boolean
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.drop_for_portion_of_view(
    table_oid regclass,
    era_name name DEFAULT 'valid'::name,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior,
    cleanup boolean DEFAULT false)
RETURNS boolean
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.drop_foreign_key(
    table_oid regclass,
    column_names name[],
    era_name name DEFAULT NULL::name,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior,
    drop_index boolean DEFAULT true)
RETURNS void
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.drop_foreign_key_by_name(
    table_oid regclass,
    key_name name,
    drop_index boolean DEFAULT true)
RETURNS boolean
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE PROCEDURE sql_saga.drop_synchronize_temporal_columns_trigger(
    IN table_oid regclass,
    IN era_name name)
LANGUAGE c SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.drop_system_time_era(
    table_oid regclass,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior,
    cleanup boolean DEFAULT true)
RETURNS boolean
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.drop_system_versioning(
    table_oid regclass,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior,
    cleanup boolean DEFAULT true)
RETURNS boolean
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.drop_unique_key(
    table_oid regclass,
    column_names name[],
    era_name name DEFAULT 'valid'::name,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior,
    cleanup boolean DEFAULT true)
RETURNS void
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.drop_unique_key_by_name(
    table_oid regclass,
    key_name name,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior,
    cleanup boolean DEFAULT true)
RETURNS void
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.rebuild_system_versioning_views(
    table_oid regclass,
    without_columns name[] DEFAULT '{}'::name[])
RETURNS void
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.set_era_ephemeral_columns(
    table_class regclass,
    era_name name DEFAULT 'valid'::name,
    ephemeral_columns name[] DEFAULT NULL::name[])
RETURNS void
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.set_system_time_era_excluded_columns(
    table_oid regclass,
    excluded_column_names name[])
RETURNS void
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.sync_all_history_policies()
RETURNS void
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

CREATE FUNCTION sql_saga.sync_history_policies(
    table_oid regclass)
RETURNS void
LANGUAGE c VOLATILE SECURITY INVOKER
AS 'sql_saga', 'api_trampoline';

/* The documentation of each public function is its worker's. */
DO $copy_api_comments$
DECLARE
    w record;
BEGIN
    FOR w IN
        SELECT pub.oid::pg_catalog.regprocedure AS public_routine, pub.prokind, d.description
        FROM pg_catalog.pg_proc AS wk
        JOIN pg_catalog.pg_description AS d
          ON d.objoid = wk.oid AND d.classoid = 'pg_catalog.pg_proc'::pg_catalog.regclass AND d.objsubid = 0
        JOIN pg_catalog.pg_proc AS pub
          ON pub.pronamespace = wk.pronamespace AND pub.proname = pg_catalog.substr(wk.proname, 7)
         AND pub.proargtypes = wk.proargtypes
        WHERE wk.pronamespace = 'sql_saga'::pg_catalog.regnamespace AND wk.proname LIKE E'\\_\\_api\\_%'
    LOOP
        EXECUTE pg_catalog.format('COMMENT ON %s %s IS %L',
            CASE WHEN w.prokind = 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END, w.public_routine, w.description);
    END LOOP;
END;
$copy_api_comments$;
