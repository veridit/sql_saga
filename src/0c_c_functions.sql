/*
 * C Helper functions
 */
CREATE OR REPLACE FUNCTION sql_saga.covers_without_gaps_transfn(internal, anyrange, anyrange)
RETURNS internal
AS 'sql_saga', 'covers_without_gaps_transfn'
LANGUAGE c;

CREATE OR REPLACE FUNCTION sql_saga.__internal_alter_table_changes_column_shape(command pg_ddl_command)
RETURNS boolean
AS 'sql_saga', 'alter_table_changes_column_shape'
LANGUAGE c STRICT;

REVOKE ALL ON FUNCTION sql_saga.__internal_alter_table_changes_column_shape(pg_ddl_command) FROM PUBLIC;

CREATE OR REPLACE FUNCTION sql_saga.__internal_refuse_managed_relation_identity_change()
RETURNS event_trigger
AS 'sql_saga', 'refuse_managed_relation_identity_change'
LANGUAGE c;

REVOKE ALL ON FUNCTION sql_saga.__internal_refuse_managed_relation_identity_change() FROM PUBLIC;

CREATE OR REPLACE FUNCTION sql_saga.__internal_record_alter_function_resolved()
RETURNS event_trigger
AS 'sql_saga', 'record_alter_function_resolved'
LANGUAGE c;

REVOKE ALL ON FUNCTION sql_saga.__internal_record_alter_function_resolved() FROM PUBLIC;

CREATE OR REPLACE FUNCTION sql_saga.fk_insert_check_c()
RETURNS trigger
AS 'sql_saga', 'fk_insert_check_c'
LANGUAGE c;

CREATE OR REPLACE FUNCTION sql_saga.fk_update_check_c()
RETURNS trigger
AS 'sql_saga', 'fk_update_check_c'
LANGUAGE c;

CREATE OR REPLACE FUNCTION sql_saga.uk_delete_check_c()
RETURNS trigger
AS 'sql_saga', 'uk_delete_check_c'
LANGUAGE c;

CREATE OR REPLACE FUNCTION sql_saga.uk_update_check_c()
RETURNS trigger
AS 'sql_saga', 'uk_update_check_c'
LANGUAGE c;

/*
 * The referencing side of a regular (non-temporal) to temporal foreign key: a constraint trigger on the
 * child table (see add_regular_foreign_key). The lookups switch to this function's owner in C, so the
 * function itself is not SECURITY DEFINER.
 */
CREATE OR REPLACE FUNCTION sql_saga.regular_fk_check_c()
RETURNS trigger
AS 'sql_saga', 'regular_fk_check_c'
LANGUAGE c;

-- Referenced side of a regular foreign key on TRUNCATE (1.4): refuses TRUNCATE of the parent while a child row
-- still references it, as PostgreSQL does for native foreign keys.
CREATE OR REPLACE FUNCTION sql_saga.uk_truncate_check_c()
RETURNS trigger
AS 'sql_saga', 'uk_truncate_check_c'
LANGUAGE c;

-- EXECUTE is granted to PUBLIC explicitly, so it holds also when the extension is created by a role whose default
-- privileges revoke EXECUTE from PUBLIC. Firing a trigger needs no privilege, but CREATE TRIGGER does: a restore
-- (pg_restore of the post-data trigger, also --no-owner) creates it as the table owner. The function only checks
-- the foreign key its trigger arguments name, with lookups run as its owner.
GRANT EXECUTE ON FUNCTION sql_saga.regular_fk_check_c() TO PUBLIC;
-- The same holds for the TRUNCATE trigger on the parent (1.4), restored as the parent's owner.
GRANT EXECUTE ON FUNCTION sql_saga.uk_truncate_check_c() TO PUBLIC;

CREATE OR REPLACE FUNCTION sql_saga.generated_always_as_row_start_end()
RETURNS trigger
AS 'sql_saga', 'generated_always_as_row_start_end'
LANGUAGE c STRICT SECURITY DEFINER SET search_path = sql_saga, pg_catalog, public;

CREATE OR REPLACE FUNCTION sql_saga.write_history()
RETURNS trigger
AS 'sql_saga', 'write_history'
LANGUAGE c STRICT SECURITY DEFINER SET search_path = sql_saga, pg_catalog, public;

CREATE OR REPLACE FUNCTION sql_saga.covers_without_gaps_finalfn(internal, anyrange, anyrange)
RETURNS boolean
AS 'sql_saga', 'covers_without_gaps_finalfn'
LANGUAGE c;

COMMENT ON FUNCTION sql_saga.generated_always_as_row_start_end() IS
'Trigger function to set the system time range on system-versioned tables.';

COMMENT ON FUNCTION sql_saga.write_history() IS
'Trigger function to write the old row version to the history table for system-versioned tables.';

/*
 * The clock a current view is bound to (the right-hand side of its "<range> @> <clock>" filter), as SQL text
 * deparsed by PostgreSQL from the view's rule. The fixed search_path makes every non-pg_catalog function,
 * operator and cast print schema-qualified, so the text re-binds exactly the objects the view is bound to when
 * parsed under the same path. SECURITY INVOKER: it reads only the rule of a view, which the caller can already
 * see through pg_get_viewdef. EXECUTE is granted to PUBLIC explicitly (also against default privileges that
 * revoke it) because current_view_trigger runs as the invoker of the DML on the view, whoever that is.
 */
CREATE OR REPLACE FUNCTION sql_saga.__internal_current_view_clock(view_oid oid)
RETURNS text
AS 'sql_saga', 'current_view_clock'
LANGUAGE c STRICT STABLE SECURITY INVOKER SET search_path = pg_catalog, pg_temp;

GRANT EXECUTE ON FUNCTION sql_saga.__internal_current_view_clock(oid) TO PUBLIC;

-- 1.4 (review finding F3): the API functions are SECURITY DEFINER and change user tables as the extension owner, so
-- each first requires the role that called it (the outer user) to own the table, and a foreign key REFERENCES on the
-- referenced table, as the native commands do. Private: only sql_saga's own functions call them.
CREATE OR REPLACE FUNCTION sql_saga.__internal_require_owner(table_oid regclass, purpose text DEFAULT NULL)
RETURNS void
AS 'sql_saga', 'require_owner'
LANGUAGE c STABLE;
REVOKE ALL ON FUNCTION sql_saga.__internal_require_owner(regclass, text) FROM PUBLIC;

CREATE OR REPLACE FUNCTION sql_saga.__internal_require_references(table_oid regclass)
RETURNS void
AS 'sql_saga', 'require_references'
LANGUAGE c STABLE;
REVOKE ALL ON FUNCTION sql_saga.__internal_require_references(regclass) FROM PUBLIC;
