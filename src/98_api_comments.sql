-- Era Management
COMMENT ON FUNCTION sql_saga.add_era(regclass, name, name, name, regtype, name, name, name, boolean, boolean, boolean) IS
'Registers a table as a temporal table using convention-over-configuration. It can create and manage temporal columns, constraints, and synchronization triggers.';

COMMENT ON FUNCTION sql_saga.drop_era(regclass, name, sql_saga.drop_behavior, boolean) IS
'Deregisters a temporal table, removing all associated constraints, triggers, and metadata.';

-- Unique Keys
COMMENT ON FUNCTION sql_saga.add_unique_key(regclass, name[], name, sql_saga.unique_key_type, name, name, name, text) IS
'Adds a temporal unique key to a table, ensuring uniqueness across time for a given set of columns within an era. Supports primary, natural, and predicated keys.';

COMMENT ON FUNCTION sql_saga.drop_unique_key(regclass, name[], name, sql_saga.drop_behavior, boolean) IS
'Drops a temporal unique key identified by its table, columns, and era.';

COMMENT ON FUNCTION sql_saga.drop_unique_key_by_name(regclass, name, sql_saga.drop_behavior, boolean) IS
'Drops a temporal unique key identified by its unique name.';

-- Foreign Keys
COMMENT ON FUNCTION sql_saga.add_foreign_key(regclass, name[], name, sql_saga.fk_match_types, sql_saga.fk_actions, sql_saga.fk_actions, name, name, text, name, name) IS
'Adds a foreign key from a regular (non-temporal) table to a temporal table. It ensures that any referenced key exists at some point in the target''s history.';

COMMENT ON FUNCTION sql_saga.add_foreign_key(regclass, name[], name, name, sql_saga.fk_match_types, sql_saga.fk_actions, sql_saga.fk_actions, name, name, name, name, name) IS
'Adds a temporal foreign key from one temporal table to another. It ensures that for any given time slice in the referencing table, a corresponding valid time slice exists in the referenced table.';

COMMENT ON FUNCTION sql_saga.drop_foreign_key(regclass, name[], name, sql_saga.drop_behavior) IS
'Drops a foreign key. The era name must be provided for temporal-to-temporal keys and omitted for regular-to-temporal keys.';

COMMENT ON FUNCTION sql_saga.drop_foreign_key_by_name(regclass, name) IS
'Drops any type of foreign key by its unique name.';

-- Updatable Views
COMMENT ON FUNCTION sql_saga.add_current_view(regclass, name, name, text) IS
'Creates a view that shows only the current state of data, making it ideal for ORMs and REST APIs. It provides a trigger for safe, explicit SCD Type 2 updates and soft-deletes.';

COMMENT ON FUNCTION sql_saga.drop_current_view(regclass, name, sql_saga.drop_behavior) IS
'Drops the "current" view associated with a table and era.';

COMMENT ON FUNCTION sql_saga.add_for_portion_of_view(regclass, name) IS
'Creates a specialized view that emulates the SQL:2011 `FOR PORTION OF` syntax, allowing a data change to be applied to a specific time slice of a record''s history.';

COMMENT ON FUNCTION sql_saga.drop_for_portion_of_view(regclass, name, sql_saga.drop_behavior, boolean) IS
'Drops the "for_portion_of" view associated with a table and era.';

-- System Versioning
COMMENT ON FUNCTION sql_saga.add_system_versioning(regclass, name, name, name, name, name, name) IS
'Adds system versioning to a table, creating a history table and triggers to automatically track all data changes.';

COMMENT ON FUNCTION sql_saga.drop_system_versioning(regclass, sql_saga.drop_behavior, boolean) IS
'Removes system versioning from a table, dropping the history table and all associated objects.';

COMMENT ON FUNCTION sql_saga.set_system_time_era_excluded_columns(regclass, name[]) IS
'Sets the list of columns to be excluded from system versioning. Changes to these columns will not create a new history record.';

COMMENT ON FUNCTION sql_saga.drop_system_time_era(regclass, sql_saga.drop_behavior, boolean) IS
'Drops the internal system time era metadata. This is a lower-level function typically called by `drop_system_versioning`.';

-- Aggregates
COMMENT ON AGGREGATE sql_saga.covers_without_gaps(anyrange, anyrange) IS
'A temporal aggregate that checks if a set of ranges (`covered`) completely covers a target range (`target`) without any gaps.';

COMMENT ON AGGREGATE sql_saga.first(anyelement) IS
'A simple aggregate to get the first element from a group. Useful for cases where order is guaranteed.';

-- Health Checks
COMMENT ON FUNCTION sql_saga.health_checks() IS
'An event trigger function that runs after DDL commands to check for inconsistencies in sql_saga''s metadata catalogs.';

-- Internal and Helper Functions
COMMENT ON FUNCTION sql_saga.allen_get_relation(anycompatible, anycompatible, anycompatible, anycompatible) IS
'Calculates the Allen''s Interval Algebra relation between two intervals.';

COMMENT ON FUNCTION sql_saga.drop_protection() IS
'An event trigger function that prevents accidental dropping of sql_saga-managed objects.';

COMMENT ON FUNCTION sql_saga.rename_following() IS
'An event trigger function that follows object renames and updates sql_saga''s metadata accordingly.';

COMMENT ON FUNCTION sql_saga.generated_always_as_row_start_end() IS
'Trigger function to set `system_valid_from` and `system_valid_until` on system-versioned tables.';

COMMENT ON FUNCTION sql_saga.write_history() IS
'Trigger function to write the old row version to the history table for system-versioned tables.';
