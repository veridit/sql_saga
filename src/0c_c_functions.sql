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

-- EXECUTE is granted to PUBLIC explicitly, so it holds also when the extension is created by a role whose default
-- privileges revoke EXECUTE from PUBLIC. Firing a trigger needs no privilege, but CREATE TRIGGER does: a restore
-- (pg_restore of the post-data trigger, also --no-owner) creates it as the table owner. The function only checks
-- the foreign key its trigger arguments name, with lookups run as its owner.
GRANT EXECUTE ON FUNCTION sql_saga.regular_fk_check_c() TO PUBLIC;

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
