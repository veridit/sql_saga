# sql_saga API Reference

This document is automatically generated from the database schema by the `80_generate_api_docs.sql` test. Do not edit it manually.


## Enumerated Types

### allen_interval_relation

```sql
- precedes
- meets
- overlaps
- starts
- during
- finishes
- equals
- preceded_by
- met_by
- overlapped_by
- started_by
- contains
- finished_by
```

### drop_behavior

```sql
- CASCADE
- RESTRICT
```

### fg_type

> Distinguishes between foreign keys from a temporal table to another temporal table, and from a regular (non-temporal) table to a temporal table.

```sql
- temporal_to_temporal
- regular_to_temporal
```

### fk_actions

```sql
- CASCADE
- SET NULL
- SET DEFAULT
- RESTRICT
- NO ACTION
```

### fk_match_types

```sql
- FULL
- PARTIAL
- SIMPLE
```

### temporal_merge_delete_mode

> Controls deletion behavior for `replace` modes.
> - NONE (default): No deletions occur.
> - DELETE_MISSING_TIMELINE: For entities in the source, any part of their target timeline not covered by the source is deleted.
> - DELETE_MISSING_ENTITIES: Any entity in the target that is not present in the source is completely deleted.
> - DELETE_MISSING_TIMELINE_AND_ENTITIES: A combination of both timeline and entity deletion.

```sql
- NONE
- DELETE_MISSING_TIMELINE
- DELETE_MISSING_ENTITIES
- DELETE_MISSING_TIMELINE_AND_ENTITIES
```

### temporal_merge_feedback_status

> Defines the possible return statuses for a row processed by the `temporal_merge` executor.
> - APPLIED: The operation was successfully planned and executed, resulting in a change to the target table.
> - SKIPPED_IDENTICAL: A benign no-op where the source data was identical to the target data.
> - SKIPPED_FILTERED: A benign no-op where the source row was correctly filtered by the mode's logic (e.g., an `INSERT_NEW_ENTITIES` for an entity that already exists).
> - SKIPPED_NO_TARGET: An actionable no-op where the operation failed because the target entity was not found. This signals a potential data quality issue.
> - SKIPPED_ECLIPSED: A benign no-op where the source row was completely covered (eclipsed) by one or more other rows within the same source batch, making it redundant.
> - ERROR: A catastrophic planner failure occurred, indicating a bug in the merge logic. The `error_message` column will be populated.

```sql
- APPLIED
- SKIPPED_IDENTICAL
- SKIPPED_FILTERED
- SKIPPED_NO_TARGET
- SKIPPED_ECLIPSED
- ERROR
```

### temporal_merge_mode

> Defines the behavior of the temporal_merge procedure.
> -- The main workhorse.
> - MERGE_ENTITY_UPSERT: Inserts new entities or performs a partial update on existing timelines. NULL is treated as an explicit value.
> - UPDATE_FOR_PORTION_OF: Applies a surgical partial update to a time portion of an existing entity. Skips new entities.
> -- Variety of UPSERT/UPDATE where NULL's are ignored.
> - MERGE_ENTITY_PATCH: Merges source with target, patching data by ignoring NULL values in the source.
> - PATCH_FOR_PORTION_OF: Applies a surgical patch to a time portion of an existing entity, ignoring NULL values in the source.
> -- Variety of UPSERT/UPDATE where everything is replaced.
> - MERGE_ENTITY_REPLACE: Merges source with target, replacing overlapping data.
> - REPLACE_FOR_PORTION_OF: Applies a surgical replacement of a time portion of an existing entity.
> -- Special purpose restricted operations.
> - INSERT_NEW_ENTITIES: Inserts only new entities.
> - DELETE_FOR_PORTION_OF: Performs a surgical deletion of a time portion from an existing entity.

```sql
- MERGE_ENTITY_UPSERT
- UPDATE_FOR_PORTION_OF
- MERGE_ENTITY_PATCH
- PATCH_FOR_PORTION_OF
- MERGE_ENTITY_REPLACE
- REPLACE_FOR_PORTION_OF
- INSERT_NEW_ENTITIES
- DELETE_FOR_PORTION_OF
```

### temporal_merge_plan_action

> Represents the internal DML action to be taken by the executor for a given atomical time segment, as determined by the planner.
> These values use a "future tense" convention (e.g., SKIP_...) as they represent a plan for an action, not a completed result.
> The order of these values is critical, as it defines the execution order when sorting the plan:
> INSERTs must happen before UPDATEs, which must happen before DELETEs to ensure foreign key consistency,
> that is check on the intermediate MVCC snapshots between the changes in the same transaction.
> - INSERT: A new historical record will be inserted.
> - UPDATE: An existing historical record will be modified (typically by shortening its period).
> - DELETE: An existing historical record will be deleted.
> - SKIP_IDENTICAL: A historical record segment is identical to the source data and requires no change.
> - SKIP_NO_TARGET: A source row should be skipped because its target entity does not exist in a mode that requires it (e.g. PATCH_FOR_PORTION_OF). This is used by the executor to generate a SKIPPED_NO_TARGET feedback status.
> - SKIP_FILTERED: A source row should be skipped because it was correctly filtered by the mode's logic (e.g. INSERT_NEW_ENTITIES for an entity that already exists).
> - SKIP_ECLIPSED: A source row is completely covered by other rows in the same batch and is therefore redundant. This is used by the executor to generate a SKIPPED_ECLIPSED feedback status.
> - ERROR: A safeguard action indicating the planner could not generate a valid plan for a row, signaling a bug.

```sql
- INSERT
- UPDATE
- DELETE
- SKIP_IDENTICAL
- SKIP_NO_TARGET
- SKIP_FILTERED
- SKIP_ECLIPSED
- ERROR
```

### temporal_merge_update_effect

> Defines the effect of an UPDATE on a timeline segment, used for ordering DML operations to ensure temporal integrity.
> The planner relies on this specific ENUM order for sorting: timeline-extending operations must execute before timeline-shortening operations.
> - GROW: The new period is a superset of the old one. These are executed first.
> - NONE: The period is unchanged (a data-only update).
> - MOVE: The period shifts without being a pure grow or shrink.
> - SHRINK: The new period is a subset of the old one. These are executed last.

```sql
- GROW
- NONE
- MOVE
- SHRINK
```

### trigger_action

> Defines the action for the manage_temporal_fk_triggers procedure.

```sql
- enable
- disable
```

### unique_key_type

> Distinguishes between a temporal primary key, a natural key (unique, for FKs), and a predicated key (a unique index with a WHERE clause).

```sql
- primary
- natural
- predicated
```

### updatable_view_type

> Defines the semantic type of an updatable view. "for_portion_of" provides direct access to historical records, while "current" provides a simplified view of only the currently active data.

```sql
- for_portion_of
- current
```


## Era Management

### add_era

> Registers a table as a temporal table using convention-over-configuration. It can create and manage temporal columns, constraints, and synchronization triggers.

```sql
FUNCTION add_era(
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
    bounds_check_constraint name DEFAULT NULL::name
) RETURNS boolean
SECURITY DEFINER
```

### drop_era

> Deregisters a temporal table, removing all associated constraints, triggers, and metadata.

```sql
FUNCTION drop_era(
    table_oid regclass,
    era_name name DEFAULT 'valid'::name,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior,
    cleanup boolean DEFAULT false
) RETURNS boolean
SECURITY DEFINER
```

## Unique Keys

### add_unique_key

> Adds a temporal unique key to a table, ensuring uniqueness across time for a given set of columns within an era. Supports primary, natural, and predicated keys. Can also enforce consistency between a natural key and the primary key.

```sql
FUNCTION add_unique_key(
    table_oid regclass,
    column_names name[],
    era_name name DEFAULT 'valid'::name,
    key_type sql_saga.unique_key_type DEFAULT 'natural'::sql_saga.unique_key_type,
    enforce_consistency_with_primary_key boolean DEFAULT NULL::boolean,
    unique_key_name name DEFAULT NULL::name,
    unique_constraint name DEFAULT NULL::name,
    exclude_constraint name DEFAULT NULL::name,
    predicate text DEFAULT NULL::text,
    mutually_exclusive_columns name[] DEFAULT NULL::name[]
) RETURNS name
SECURITY DEFINER
```

### drop_unique_key

> Drops a temporal unique key identified by its table, columns, and era.

```sql
FUNCTION drop_unique_key(
    table_oid regclass,
    column_names name[],
    era_name name DEFAULT 'valid'::name,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior,
    cleanup boolean DEFAULT true
) RETURNS void
SECURITY DEFINER
```

### drop_unique_key_by_name

> Drops a temporal unique key identified by its unique name.

```sql
FUNCTION drop_unique_key_by_name(
    table_oid regclass,
    key_name name,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior,
    cleanup boolean DEFAULT true
) RETURNS void
SECURITY DEFINER
```

## Foreign Keys

### add_foreign_key

> Adds a foreign key constraint. This is a declarative wrapper that automatically determines whether to create a temporal or regular foreign key by introspecting the schema, and looks up the internal unique key name.

```sql
FUNCTION add_foreign_key(
    fk_table_oid regclass,
    fk_column_names name[],
    pk_table_oid regclass,
    pk_column_names name[],
    fk_era_name name DEFAULT NULL::name,
    match_type sql_saga.fk_match_types DEFAULT 'SIMPLE'::sql_saga.fk_match_types,
    update_action sql_saga.fk_actions DEFAULT 'NO ACTION'::sql_saga.fk_actions,
    delete_action sql_saga.fk_actions DEFAULT 'NO ACTION'::sql_saga.fk_actions,
    foreign_key_name name DEFAULT NULL::name,
    create_index boolean DEFAULT true
) RETURNS name
SECURITY DEFINER
```

### add_regular_foreign_key

> Adds a foreign key from a regular (non-temporal) table to a temporal table. It ensures that any referenced key exists at some point in the target's history.

```sql
FUNCTION add_regular_foreign_key(
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
    create_index boolean DEFAULT true
) RETURNS name
SECURITY DEFINER
```

### add_temporal_foreign_key

> Adds a temporal foreign key from one temporal table to another. It ensures that for any given time slice in the referencing table, a corresponding valid time slice exists in the referenced table.

```sql
FUNCTION add_temporal_foreign_key(
    fk_table_oid regclass,
    fk_column_names name[],
    fk_era_name name,
    unique_key_name name,
    match_type sql_saga.fk_match_types DEFAULT 'SIMPLE'::sql_saga.fk_match_types,
    update_action sql_saga.fk_actions DEFAULT 'NO ACTION'::sql_saga.fk_actions,
    delete_action sql_saga.fk_actions DEFAULT 'NO ACTION'::sql_saga.fk_actions,
    foreign_key_name name DEFAULT NULL::name,
    fk_insert_trigger name DEFAULT NULL::name,
    fk_update_trigger name DEFAULT NULL::name,
    uk_update_trigger name DEFAULT NULL::name,
    uk_delete_trigger name DEFAULT NULL::name,
    create_index boolean DEFAULT true
) RETURNS name
SECURITY DEFINER
```

### drop_foreign_key

```sql
FUNCTION drop_foreign_key(
    table_oid regclass,
    column_names name[],
    era_name name DEFAULT NULL::name,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior,
    drop_index boolean DEFAULT true
) RETURNS void
SECURITY DEFINER
```

### drop_foreign_key_by_name

```sql
FUNCTION drop_foreign_key_by_name(
    table_oid regclass,
    key_name name,
    drop_index boolean DEFAULT true
) RETURNS boolean
SECURITY DEFINER
```

## Updatable Views

### add_current_view

> Creates a view that shows only the current state of data, making it ideal for ORMs and REST APIs. It provides a trigger for safe, explicit SCD Type 2 updates and soft-deletes.

```sql
FUNCTION add_current_view(
    table_oid regclass,
    era_name name DEFAULT 'valid'::name,
    delete_mode name DEFAULT 'delete_as_cutoff'::name,
    current_func_name text DEFAULT NULL::text
) RETURNS boolean
SECURITY DEFINER
```

### add_for_portion_of_view

> Creates a specialized view that emulates the SQL:2011 `FOR PORTION OF` syntax, allowing a data change to be applied to a specific time slice of a record's history.

```sql
FUNCTION add_for_portion_of_view(
    table_oid regclass,
    era_name name DEFAULT 'valid'::name
) RETURNS boolean
SECURITY DEFINER
```

### drop_current_view

> Drops the "current" view associated with a table and era.

```sql
FUNCTION drop_current_view(
    table_oid regclass,
    era_name name DEFAULT 'valid'::name,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior
) RETURNS boolean
SECURITY DEFINER
```

### drop_for_portion_of_view

> Drops the "for_portion_of" view associated with a table and era.

```sql
FUNCTION drop_for_portion_of_view(
    table_oid regclass,
    era_name name DEFAULT 'valid'::name,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior,
    cleanup boolean DEFAULT false
) RETURNS boolean
SECURITY DEFINER
```

## Bulk Data Loading

### disable_temporal_triggers

> Disables all sql_saga-managed temporal foreign key triggers for the specified tables. This is a targeted alternative to `ALTER TABLE ... DISABLE TRIGGER USER`, intended for complex ETL batches where transient, inconsistent states are expected. It does not affect non-FK triggers, such as those for synchronized columns.

```sql
PROCEDURE disable_temporal_triggers(
    VARIADIC p_table_oids regclass[]
)
SECURITY INVOKER
```

### enable_temporal_triggers

> Re-enables all sql_saga-managed temporal foreign key triggers for the specified tables. This should be called at the end of an ETL batch transaction to restore integrity checks.

```sql
PROCEDURE enable_temporal_triggers(
    VARIADIC p_table_oids regclass[]
)
SECURITY INVOKER
```

### manage_temporal_fk_triggers

```sql
PROCEDURE manage_temporal_fk_triggers(
    IN p_action sql_saga.trigger_action,
    VARIADIC p_table_oids regclass[]
)
SECURITY INVOKER
```

### temporal_merge

> Executes a set-based temporal merge operation. It generates a plan using temporal_merge_plan and then executes it.

```sql
PROCEDURE temporal_merge(
    IN target_table regclass,
    IN source_table regclass,
    IN primary_identity_columns text[] DEFAULT NULL::text[],
    IN mode sql_saga.temporal_merge_mode DEFAULT 'MERGE_ENTITY_PATCH'::sql_saga.temporal_merge_mode,
    IN era_name name DEFAULT 'valid'::name,
    IN row_id_column name DEFAULT 'row_id'::name,
    IN founding_id_column name DEFAULT NULL::name,
    IN update_source_with_identity boolean DEFAULT false,
    IN natural_identity_columns text[] DEFAULT NULL::text[],
    IN delete_mode sql_saga.temporal_merge_delete_mode DEFAULT 'NONE'::sql_saga.temporal_merge_delete_mode,
    IN update_source_with_feedback boolean DEFAULT false,
    IN feedback_status_column name DEFAULT NULL::name,
    IN feedback_status_key name DEFAULT NULL::name,
    IN feedback_error_column name DEFAULT NULL::name,
    IN feedback_error_key name DEFAULT NULL::name,
    IN ephemeral_columns text[] DEFAULT NULL::text[],
    IN delay_constraints boolean DEFAULT true
)
SECURITY INVOKER
```

## System Versioning

### add_system_versioning

> Adds system versioning to a table, creating a history table and triggers to automatically track all data changes.

```sql
FUNCTION add_system_versioning(
    table_oid regclass,
    history_table_name name DEFAULT NULL::name,
    view_name name DEFAULT NULL::name,
    function_as_of_name name DEFAULT NULL::name,
    function_between_name name DEFAULT NULL::name,
    function_between_symmetric_name name DEFAULT NULL::name,
    function_from_to_name name DEFAULT NULL::name
) RETURNS void
SECURITY DEFINER
```

### drop_system_time_era

> Drops the internal system time era metadata. This is a lower-level function typically called by `drop_system_versioning`.

```sql
FUNCTION drop_system_time_era(
    table_oid regclass,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior,
    cleanup boolean DEFAULT true
) RETURNS boolean
SECURITY DEFINER
```

### drop_system_versioning

> Removes system versioning from a table, dropping the history table and all associated objects.

```sql
FUNCTION drop_system_versioning(
    table_oid regclass,
    drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT'::sql_saga.drop_behavior,
    cleanup boolean DEFAULT true
) RETURNS boolean
SECURITY DEFINER
```

### set_system_time_era_excluded_columns

> Sets the list of columns to be excluded from system versioning. Changes to these columns will not create a new history record.

```sql
FUNCTION set_system_time_era_excluded_columns(
    table_oid regclass,
    excluded_column_names name[]
) RETURNS void
SECURITY DEFINER
```

## Aggregates

### covers_without_gaps

> A temporal aggregate that checks if a set of ranges (`covered`) completely covers a target range (`target`) without any gaps.

```sql
AGGREGATE covers_without_gaps(anyrange, anyrange)
```

### first

> A simple aggregate to get the first element from a group. Useful for cases where order is guaranteed.

```sql
AGGREGATE first(anyelement)
```

## Health Checks

### health_checks

> An event trigger function that runs after DDL commands to check for inconsistencies in sql_saga's metadata catalogs.

```sql
FUNCTION health_checks() RETURNS event_trigger
SECURITY DEFINER
```

## Internal and Helper Functions

### drop_protection

> An event trigger function that prevents accidental dropping of sql_saga-managed objects.

```sql
FUNCTION drop_protection() RETURNS event_trigger
SECURITY DEFINER
```

### get_allen_relation

> Calculates the Allen's Interval Algebra relation between two intervals. This is a high-performance, inlinable SQL function.

```sql
FUNCTION get_allen_relation(
    x_from anycompatible,
    x_until anycompatible,
    y_from anycompatible,
    y_until anycompatible
) RETURNS sql_saga.allen_interval_relation
SECURITY INVOKER
```

### rename_following

> An event trigger function that follows object renames and updates sql_saga's metadata accordingly.

```sql
FUNCTION rename_following() RETURNS event_trigger
SECURITY DEFINER
```
