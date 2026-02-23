/*
 * C Helper functions
 */
CREATE OR REPLACE FUNCTION sql_saga.covers_without_gaps_transfn(internal, anyrange, anyrange)
RETURNS internal
AS 'sql_saga', 'covers_without_gaps_transfn'
LANGUAGE c;

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
