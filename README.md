# sql_saga

`sql_saga` is a PostgreSQL plugin tailored for National Statistical Offices (NSOs) worldwide,
enabling efficient and intuitive handling of temporal tables.

Drawing inspiration from Nordic sagas, the project aims at the seamless blending of
ancient narrative with the contemporary purpose of global statistics.

### What is a "Saga"?

In the context of this extension, a **Saga** represents the complete history of a table's data over time. A Saga can be composed of one or more **Eras**, where each Era is a distinct temporal period defined by a pair of columns (e.g., `valid_from`/`valid_until` or `transaction_from`/`transaction_until`). This allows a single table to have its data managed across multiple, independent timelines if needed.

## Features

- Support for foreign keys between temporal tables, and from regular (non-temporal) tables to temporal tables.
- High-performance, set-based API for bulk temporal data loading (`temporal_merge`).
- Intuitive API for seamless integration with existing NSO systems.
- Intuitive fetching of current data.
- Compatible with PostgREST - that creates REST endpoints for the API's.
- Built upon the robust and reliable PostgreSQL database system.
- Supports change tracking and delete in accordance with NSO requirements.

## Installation

TODO: Build a docker image with postgres and the sql_saga extension.

TODO: Build an Ubuntu packate with sql_saga.

`CREATE EXTENSION sql_saga;`

## Core Concepts

A temporal table has `valid_from` and `valid_until` columns, which define a `[)` period (inclusive start, exclusive end), aligning with PostgreSQL's native range types. While `DATE` is used in these examples for simplicity, any data type that can form a range is supported, including `TIMESTAMPTZ`, `TIMESTAMP`, `INTEGER`, `BIGINT`, and `NUMERIC`.

### Entity Identifiers

A key concept in temporal data modeling is the **entity identifier**. Since a temporal table tracks the history of an entity over time, a single conceptual "thing" (like a company or a person) will have multiple rows in the table, each representing a different slice of its history.

The entity identifier is the column (or set of columns) that holds the same value for all rows that belong to the same conceptual entity. A common naming convention for this column is `entity_id` or simply `id`. In the examples below, the `id` column in `establishment` serves this purpose.

The primary key of the temporal table is typically a composite key that includes the entity identifier and a temporal column (e.g., `(id, valid_from)`) to uniquely identify each historical version of the entity.

A row is considered **"current"** if its validity period `[valid_from, valid_until)` contains the present moment (e.g., `now()` or `CURRENT_DATE`). This is a powerful concept as it correctly includes records with a known future end date, such as a contract that is active today but expires next month. This `is_current` check can be efficiently served by a standard B-tree index on the temporal columns.

### Managing Historical Changes: Slowly Changing Dimensions (SCD)
When an attribute of an entity changes over time, we need a strategy to record that change. The most robust method for this is the **Type 2 Slowly Changing Dimension (SCD Type 2)**. Instead of overwriting old data, this pattern preserves history by:
1.  "Closing out" a historical record by updating its `valid_until` to the timestamp of the change.
2.  Inserting a new record with the updated data, which becomes the new "current" version.

`sql_saga` automates this pattern through its updatable views, making it easy to maintain a complete and accurate history of every entity.

### Level 2: Common Patterns

#### Updatable Views for Simplified Data Management
To simplify common interactions with temporal data, `sql_saga` provides two types of updatable views. These views act as a stable, user-friendly API layer on top of your temporal tables, and are especially useful for integration with tools like PostgREST.

##### The `for_portion_of` View: Applying Changes to a Time Slice
This view is a specialized tool that provides a powerful feature that emulates the SQL:2011 `FOR PORTION OF` clause. It exists for one purpose: to apply a data change to a specific slice of an entity's timeline. The trigger will automatically split, update, and insert historical records to correctly reflect the change.

- **Surgical `UPDATE` is the only supported operation.** To use it, you must provide `valid_from` and `valid_until` in the `SET` clause, which act as parameters defining the time period to be changed.
- **`INSERT`, `DELETE`, and simple `UPDATE`s (e.g., historical corrections) are intentionally not supported on the view.** These operations should be performed directly on the base table. `DELETE` is unsupported because standard SQL provides no way to pass the required `[from, until)` parameters to a `DELETE` trigger, unlike `UPDATE` which can use the `SET` clause for this purpose. This focused design ensures the view's purpose is clear and prevents accidental misuse.

**Known Limitation:** The trigger for this view performs `DELETE` and `INSERT` operations that can create a transient, inconsistent state. If the base table is referenced by a temporal foreign key from another table, an `UPDATE` that creates a temporary gap in history may cause the foreign key check to fail.

For these more complex scenarios, the `temporal_merge` procedure is the recommended solution as it is designed to handle such dependencies correctly. However, if you must use the `for_portion_of` view, you can manually disable and re-enable the outgoing foreign key triggers on the referenced table within a transaction.

**Manual Workaround Example:**
You can find the trigger names to disable in the `uk_update_trigger` and `uk_delete_trigger` columns of the `sql_saga.foreign_keys` metadata table.
```sql
BEGIN;

-- Temporarily disable the outgoing FK triggers on the 'legal_unit' table.
ALTER TABLE readme.legal_unit DISABLE TRIGGER establishment_legal_unit_id_valid_uk_update;
ALTER TABLE readme.legal_unit DISABLE TRIGGER establishment_legal_unit_id_valid_uk_delete;

-- Perform the historical update via the view
UPDATE readme.legal_unit__for_portion_of_valid
SET status = 'inactive', valid_from = '2023-09-01', valid_until = '2023-11-01'
WHERE id = 1;

-- Re-enable the triggers
ALTER TABLE readme.legal_unit ENABLE TRIGGER establishment_legal_unit_id_valid_uk_update;
ALTER TABLE readme.legal_unit ENABLE TRIGGER establishment_legal_unit_id_valid_uk_delete;

COMMIT;
```

**Example: Marking a legal unit as inactive for a specific period**
```sql
-- This query marks legal_unit 1 as inactive only for the period from 2023-09-01 to 2023-11-01.
UPDATE legal_unit__for_portion_of_valid
SET
    status = 'inactive', -- The new data value
    -- These act as parameters for the trigger:
    valid_from = '2023-09-01',
    valid_until = '2023-11-01'
WHERE
    id = 1; -- The entity identifier
```

##### The `current` View: A Simple View of the Present
This view is designed to be the primary interface for most applications (e.g., ORMs, REST APIs). It simplifies interaction by showing only the records that are currently active. Because it is based on the concept of "now", the `current` view can only be created on eras that use a `date` or `timestamp`-based data type. It provides a safe, explicit protocol for data modification.

- **`INSERT`**: Creates a new entity. The `valid_from` is automatically set to the current time, and `valid_until` is set to 'infinity'.
- **`UPDATE` (SCD Type 2)**: A standard `UPDATE ... SET column = 'new value'` automatically performs a **Type 2 Slowly Changing Dimension** operation. The current record is closed out (its `valid_until` is set to `now()`), and a new record is inserted with the updated data, becoming the new current version.

###### Advanced Usage: Exposing All Temporal Columns in Views
The updatable views (`for_portion_of` and `current`) are designed for transparency. They will always expose *all* columns from the underlying base table. This allows for advanced use cases where multiple temporal representations are managed on the same table.

- **Regular Usage:** If your table only has `valid_from` and `valid_until`, the views will expose just those two columns.
- **Advanced Usage:** If you add synchronized columns like a human-readable `valid_to` or a native `range` column to your base table, they will also be visible in the views. This allows you to interact with whichever temporal representation is most convenient.

###### Ending an Entity's Timeline (Soft-Delete)
`sql_saga` provides two configurable modes for handling the end of an entity's timeline, controlled by the `delete_mode` parameter in `add_current_view`. This allows you to choose between maximum ORM compatibility and maximum auditability.

**1. Simple Cutoff Mode (Default)**
This is the default mode, provided for compatibility with ORMs and other tools that expect to be able to use standard `DELETE` statements.
- **`DELETE` is a Soft-Delete**: A standard `DELETE FROM my_table__current_valid WHERE ...` statement is allowed. The trigger intercepts this operation and performs a soft-delete by setting the `valid_until` of the current record to `now()`. While convenient, this provides no way to record *why* the entity's timeline was ended.

**2. Documented Ending Mode**
This mode (`delete_mode := 'delete_as_documented_ending'`) is recommended for systems where auditability is critical. It enforces a clear and unambiguous protocol for ending an entity's timeline.
- **`DELETE` is Disallowed**: A direct `DELETE` statement on the view is forbidden. This prevents accidental, undocumented data loss and forces developers to be explicit about their intent.
- **Documented Soft-Delete via `UPDATE`**: To end an entity's timeline, you must use a special `UPDATE` statement: `UPDATE my_table__current_valid SET valid_from = 'infinity' WHERE ...`. This signals the trigger to close out the current record. You can also include other columns in the `SET` clause (e.g., `SET valid_from = 'infinity', status = 'archived'`) to record the reason for the change on the now-historical record.

**Example: Changing an employee's department (SCD Type 2)**
```sql
-- Bob moves from Sales to Management.
-- sql_saga automatically handles the history.
UPDATE employees__current_valid SET department = 'Management' WHERE id = 2;
```

**Example: Soft-deleting an employee record (Documented Ending Mode)**
```sql
-- Alice leaves the company, and we record the reason.
UPDATE employees__current_valid SET valid_from = 'infinity', status = 'resigned' WHERE id = 1;
```

##### Security Model: `SECURITY INVOKER`
All triggers on these views are `SECURITY INVOKER` (the default). This is a key security feature. It means that any DML operation on a view is executed with the permissions of the *calling user*. The system checks the user's permissions on the underlying base table before allowing the operation, so a user can only do what they are already allowed to do. This ensures seamless compatibility with PostgreSQL's Row-Level Security (RLS) and standard table `GRANT`s.

#### Foreign Keys from Regular (Non-Temporal) Tables

`sql_saga` also supports foreign keys from a regular (non-temporal) table to a temporal table. This is useful for ensuring that a reference in a regular table points to an entity that exists (or existed at some point) in a temporal table.

For example, a regular `projects` table might reference a lead employee from a temporal `employees` table:

```
TABLE projects (
    id int,
    name text,
    lead_employee_id int
)

TABLE employees (
    id int,
    valid_from date,
    valid_until date,
    name text
)
```

A foreign key from `projects.lead_employee_id` to `employees.id` ensures that any `lead_employee_id` in the `projects` table corresponds to a valid employee in the `employees` table's history. Unlike temporal-to-temporal foreign keys which check for coverage over a period, this type of foreign key simply checks for the *existence* of the key in the referenced temporal table at any point in its history.

This validation is implemented using a `CHECK` constraint on the regular table, which calls a high-performance helper function created by `sql_saga`.

#### High-Performance Bulk Data Loading (`temporal_merge`)
- `temporal_merge(p_target_table regclass, p_source_table regclass, p_id_columns TEXT[], p_ephemeral_columns TEXT[], p_mode sql_saga.temporal_merge_mode DEFAULT 'MERGE_ENTITY_PATCH', p_era_name name DEFAULT 'valid', p_source_row_id_column name DEFAULT 'row_id', p_founding_id_column name DEFAULT NULL, p_update_source_with_assigned_entity_ids BOOLEAN DEFAULT false, p_delete_mode sql_saga.temporal_merge_delete_mode DEFAULT 'NONE')`: A powerful, set-based procedure for performing `INSERT`, `UPDATE`, and `DELETE` operations on temporal tables from a source table. It is designed to solve complex data loading scenarios (e.g., idempotent imports, data corrections) in a single, efficient, and transactionally-safe statement. The API is designed to be orthogonal: `p_mode` controls the non-destructive merge behavior, and `p_delete_mode` provides optional, destructive overrides.
  - `p_target_table`: The temporal table to merge data into.
  - `p_source_table`: A table (usually temporary) containing the source data.
  - `p_id_columns`: An array of column names that form the conceptual entity identifier.
  - `p_ephemeral_columns`: An array of column names that should not be considered when comparing for data changes, but whose values should still be updated (e.g., `edit_comment`).
  - `p_mode`: Controls the scope and payload semantics of the merge. By default, all modes are non-destructive to the timeline.
    - `'MERGE_ENTITY_PATCH'`: (Default) Merges the source timeline with the target entity's full timeline, patching data for overlapping periods. Preserves non-overlapping parts of the target timeline.
    - `'MERGE_ENTITY_REPLACE'`: Merges the source timeline with the target entity's full timeline, replacing data for overlapping periods. Preserves non-overlapping parts of the target timeline.
    - `'INSERT_NEW_ENTITIES'`: Inserts entities that are entirely new to the target table.
    - `'PATCH_FOR_PORTION_OF'`: Applies a surgical patch to a specific time portion of an existing entity.
    - `'REPLACE_FOR_PORTION_OF'`: Applies a surgical replacement of a specific time portion of an existing entity.
    - `'DELETE_FOR_PORTION_OF'`: Performs a surgical deletion of a specific time portion from an existing entity.
  - `p_source_row_id_column`: The name of the column in the source table that uniquely identifies each row (default: `row_id`). This column is required for providing feedback on a per-row basis.
  - `p_founding_id_column`: The name of a column in the source table used to group multiple rows that belong to the same *new* conceptual entity. This allows `temporal_merge` to resolve intra-batch dependencies (e.g., an `INSERT` and a `REPLACE` for the same new entity in one call). If this is `NULL`, the `p_source_row_id_column` is used as the default founding identifier.
    - **Important:** The scope of a `founding_id` is limited to a single `temporal_merge` call. All rows belonging to a single founding event *must* be processed within the same source table in a single call. Splitting a `founding_id` set across multiple `temporal_merge` calls will result in the creation of multiple, distinct entities, as the procedure has no memory of `founding_id` values used in previous calls.
  - `p_update_source_with_assigned_entity_ids`: If `true`, the procedure will update the source table with any generated surrogate key values for newly inserted entities. This simplifies multi-step import processes by removing the need for manual ID propagation between steps.
  - `p_delete_mode`: Provides optional, destructive overrides.
    - `'NONE'` (Default): No destructive operations occur.
    - `'DELETE_MISSING_TIMELINE'`: Enables "source-as-truth" timeline replacement. For any entity present in the source, any part of its timeline in the target that is **not** covered by the source's timeline will be **deleted**.
    - `'DELETE_MISSING_ENTITIES'`: Deletes entire entities. When used with `MERGE_ENTITY_*` modes, any entity in the target that is not present in the source is completely deleted.
    - `'DELETE_MISSING_TIMELINE_AND_ENTITIES'`: A combination of both destructive behaviors.

  ##### Executor State and Feedback
  The procedure uses two session-scoped temporary tables to manage its state: `temporal_merge_plan` (which stores the execution plan) and `temporal_merge_feedback` (which stores the final row-by-row feedback). These tables are created in the `pg_temp` schema and are automatically cleaned up at the end of the transaction (`ON COMMIT DROP`).

  - **Caveat for Multi-Role Sessions:** Because temporary tables are owned by the role that creates them, calling `temporal_merge` as different roles within the same session (e.g., via `SET ROLE`) can lead to permission errors. If the procedure is called by a superuser and then later by an unprivileged user, the second call may fail as the unprivileged user might not have permission to `TRUNCATE` the tables created by the superuser.
  - **Solution:** In the rare case that you need to call `temporal_merge` as multiple different roles within a single session, it is safest to manually drop both temporary tables before changing roles: `DROP TABLE IF EXISTS pg_temp.temporal_merge_plan, pg_temp.temporal_merge_feedback;`

#### System Versioning (History Tables)
`sql_saga` provides full support for system-versioned tables, creating a complete, queryable history of every row. This tracks the state of data over time ("What did this record look like last year?"). When this feature is enabled, the columns `system_valid_from` and `system_valid_until` are added to the table.

### Level 3: The Deep Dive - Complete API Reference

## Usage

This section provides a guide to using `sql_saga`, organized into three levels of complexity.

### Level 1: The Quick Tour

This example provides a complete, runnable demonstration of the core DDL functions to set up temporal tables and relationships.

#### Activate

```
CREATE TABLE legal_unit (
  id SERIAL NOT NULL,
  legal_ident VARCHAR NOT NULL,
  name VARCHAR NOT NULL,
  status TEXT, -- e.g., 'active', 'inactive'
  valid_from DATE,
  valid_to DATE, -- Optional: for human-readable inclusive end dates
  valid_until DATE
  -- Note: A primary key on temporal tables is often not on the temporal columns
);

-- Register the table as a temporal table (an "era") using default column names.
-- Explicitly enable synchronization for the 'valid_to' column.
SELECT sql_saga.add_era('legal_unit'::regclass, p_synchronize_valid_to_column := 'valid_to');
-- Add temporal unique keys. A name is generated if the last argument is omitted.
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit'::regclass, column_names => ARRAY['id'], unique_key_name => 'legal_unit_id_valid');
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit'::regclass, column_names => ARRAY['legal_ident'], unique_key_name => 'legal_unit_legal_ident_valid');
-- Add a predicated unique key (e.g., only active units must have a unique name).
SELECT sql_saga.add_unique_key(
    table_oid => 'legal_unit'::regclass,
    column_names => ARRAY['name'],
    predicate => 'status = ''active''',
    unique_key_name => 'legal_unit_active_name_valid'
);


CREATE TABLE establishment (
  id SERIAL NOT NULL,
  name VARCHAR NOT NULL,
  address TEXT NOT NULL,
  legal_unit_id INTEGER NOT NULL,
  valid_from DATE,
  valid_until DATE
);

SELECT sql_saga.add_era(table_oid => 'establishment'::regclass);
SELECT sql_saga.add_unique_key(table_oid => 'establishment'::regclass, column_names => ARRAY['id'], unique_key_name => 'establishment_id_valid');
SELECT sql_saga.add_unique_key(table_oid => 'establishment'::regclass, column_names => ARRAY['name'], unique_key_name => 'establishment_name_valid');
-- Add a temporal foreign key. It references a temporal unique key.
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'establishment'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    fk_era_name => 'valid',
    unique_key_name => 'legal_unit_id_valid'
);

-- Add a foreign key from a regular table to a temporal table.
-- Note that fk_era_name is omitted for the standard table.
CREATE TABLE projects (id serial primary key, name text, legal_unit_id int);
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'projects'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    unique_key_name => 'legal_unit_id_valid'
);
```

#### Deactivate

```
-- Foreign keys must be dropped before the unique keys they reference.
SELECT sql_saga.drop_foreign_key(
    table_oid => 'establishment'::regclass,
    column_names => ARRAY['legal_unit_id'],
    era_name => 'valid'
);
-- For regular-to-temporal FKs, era_name is omitted.
SELECT sql_saga.drop_foreign_key(
    table_oid => 'projects'::regclass,
    column_names => ARRAY['legal_unit_id']
);

SELECT sql_saga.drop_unique_key(
    table_oid => 'establishment'::regclass,
    column_names => ARRAY['id'],
    era_name => 'valid'
);
SELECT sql_saga.drop_unique_key(
    table_oid => 'establishment'::regclass,
    column_names => ARRAY['name'],
    era_name => 'valid'
);
SELECT sql_saga.drop_era('establishment'::regclass);


SELECT sql_saga.drop_unique_key(
    table_oid => 'legal_unit'::regclass,
    column_names => ARRAY['id'],
    era_name => 'valid'
);
SELECT sql_saga.drop_unique_key(
    table_oid => 'legal_unit'::regclass,
    column_names => ARRAY['legal_ident'],
    era_name => 'valid'
);
-- For predicated unique keys, the predicate is not needed for dropping.
SELECT sql_saga.drop_unique_key(
    table_oid => 'legal_unit'::regclass,
    column_names => ARRAY['name'],
    era_name => 'valid'
);
SELECT sql_saga.drop_era('legal_unit'::regclass);
```

## Development
The test suite uses `pg_regress` and is designed to be fully idempotent, creating a temporary database for each run to ensure a clean state.

- To run all tests:
  ```bash
  make install && make test; make diff-fail-all
  ```
- To run fast tests (excluding benchmarks):
  ```bash
  make install && make test fast; make diff-fail-all
  ```
- To run a specific test:
  ```bash
  make install && make test TESTS="02_periods"; make diff-fail-all
  ```
- To run a subset of tests:
  ```bash
  make install && make test TESTS="02_periods 03_api_symmetry_test"; make diff-fail-all
  ```
- To quickly review and fix any diffs:
  ```bash
  make diff-fail-all vim
  ```

#### Era Management
- `add_era(table_oid regclass, valid_from_column_name name DEFAULT 'valid_from', ..., p_synchronize_valid_to_column name DEFAULT NULL, p_synchronize_range_column name DEFAULT NULL, p_add_defaults boolean DEFAULT true, p_add_bounds_check boolean DEFAULT true) RETURNS boolean`: Registers a table as a temporal table using convention-over-configuration.
  - The `range_type` is automatically inferred from the column data types.
  - To enable synchronization with a `valid_to`-style column or a native `range` column, provide the column names via `p_synchronize_valid_to_column` or `p_synchronize_range_column`. This also adds a `NOT NULL` constraint to the synchronized columns and creates a unified trigger to keep all temporal representations consistent.
  - `valid_to` synchronization is only supported for **discrete types** (e.g., `date`, `integer`).
  - `p_add_defaults`: If `true` (the default), `sql_saga` will set `DEFAULT 'infinity'` on the `valid_until` column for data types that support it. This simplifies `INSERT` statements for open-ended periods. Set to `false` if you wish to manage default values manually.
  - `p_add_bounds_check`: If `true` (the default), `sql_saga` will add a `CHECK` constraint to ensure that `valid_from < valid_until`. For data types that support infinity, it also checks that `valid_from > '-infinity'`. Set to `false` to disable this check for advanced use cases where you need to manage temporal integrity at the application level.
- `drop_era(table_oid regclass, era_name name DEFAULT 'valid', drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT false) RETURNS boolean`

#### Unique Keys
- `add_unique_key(table_oid regclass, column_names name[], era_name name DEFAULT 'valid', unique_key_name name DEFAULT NULL, unique_constraint name DEFAULT NULL, exclude_constraint name DEFAULT NULL, predicate text DEFAULT NULL) RETURNS name`
- `drop_unique_key(table_oid regclass, column_names name[], era_name name, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT true) RETURNS void`
- `drop_unique_key_by_name(table_oid regclass, key_name name, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT true) RETURNS void`

#### Foreign Keys
- **For temporal-to-temporal foreign keys:**
  - `add_foreign_key(fk_table_oid regclass, fk_column_names name[], fk_era_name name, unique_key_name name, match_type sql_saga.fk_match_types DEFAULT 'SIMPLE', update_action sql_saga.fk_actions DEFAULT 'NO ACTION', delete_action sql_saga.fk_actions DEFAULT 'NO ACTION', foreign_key_name name DEFAULT NULL, fk_insert_trigger name DEFAULT NULL, fk_update_trigger name DEFAULT NULL, uk_update_trigger name DEFAULT NULL, uk_delete_trigger name DEFAULT NULL) RETURNS name`
- **For regular-to-temporal foreign keys:**
  - `add_foreign_key(fk_table_oid regclass, fk_column_names name[], unique_key_name name, match_type sql_saga.fk_match_types DEFAULT 'SIMPLE', update_action sql_saga.fk_actions DEFAULT 'NO ACTION', delete_action sql_saga.fk_actions DEFAULT 'NO ACTION', foreign_key_name name DEFAULT NULL, fk_check_constraint name DEFAULT NULL, fk_helper_function text DEFAULT NULL, uk_update_trigger name DEFAULT NULL, uk_delete_trigger name DEFAULT NULL) RETURNS name`
- **Dropping foreign keys:**
  - `drop_foreign_key(table_oid regclass, column_names name[], era_name name DEFAULT NULL, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT')`: Drops a foreign key. For temporal-to-temporal keys, `era_name` must be provided. For regular-to-temporal keys, `era_name` should be omitted.
  - `drop_foreign_key_by_name(table_oid regclass, key_name name) RETURNS boolean`: Drops any type of foreign key by its unique generated or user-provided name.

#### Updatable Views (for PostgREST and `FOR PORTION OF` emulation)
- `add_for_portion_of_view(table_oid regclass, era_name name DEFAULT 'valid', ...)`: Creates a specialized view that emulates the SQL:2011 `FOR PORTION OF` syntax, allowing a change to be applied to a specific time slice of a record's history.
- `drop_for_portion_of_view(table_oid regclass, era_name name DEFAULT 'valid', drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT')`: Drops the `for_portion_of` view associated with the specified table and era.
- `add_current_view(table_oid regclass, era_name name DEFAULT 'valid', delete_mode name DEFAULT 'delete_as_cutoff', p_current_func_name text DEFAULT NULL)`: Creates a view that shows only the *current* state of data, making it ideal for ORMs and REST APIs.
- `drop_current_view(table_oid regclass, era_name name DEFAULT 'valid', drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT')`: Drops the `current` view associated with the specified table and era.

#### High-Performance Bulk Data Loading (`temporal_merge`)
- `temporal_merge(p_target_table regclass, p_source_table regclass, p_id_columns TEXT[], p_ephemeral_columns TEXT[], p_mode sql_saga.temporal_merge_mode DEFAULT 'MERGE_ENTITY_PATCH', p_era_name name DEFAULT 'valid', p_source_row_id_column name DEFAULT 'row_id', p_founding_id_column name DEFAULT NULL, p_update_source_with_assigned_entity_ids BOOLEAN DEFAULT false, p_delete_mode sql_saga.temporal_merge_delete_mode DEFAULT 'NONE')`

#### System Versioning (History Tables)
- `add_system_versioning(table_oid regclass, history_table_name name DEFAULT NULL, view_name name DEFAULT NULL)`
- `drop_system_versioning(table_oid regclass, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT true)`
- `set_system_time_era_excluded_columns(table_oid regclass, excluded_column_names name[]) RETURNS void`
- `generated_always_as_row_start_end() RETURNS trigger` (C function)
- `write_history() RETURNS trigger` (C function)

## Dependencies

- [PostgreSQL](https://www.postgresql.org/)
- [PostgREST](https://postgrest.org/)

## Honorable Mentions

`sql_saga` draws upon code and concepts from the following GitHub projects:

- [`periods`](https://github.com/xocolatl/periods/): Support for SQL:2016 in postgres with valid time (period) and known time (system time/transaction time).
- [`time_for_keys`](https://github.com/pjungwir/time_for_keys): Triggers for foreign keys with time.

We express our gratitude to the authors and contributors of these projects for their invaluable work.

## License

`sql_saga` is licensed under the MIT License. See [LICENSE](LICENSE) for more details.

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute and the process for submitting pull requests.

## Acknowledgements

- The PostgreSQL community for their continued support and development of an exceptional database system.
- All contributors and users of `sql_saga` who have provided feedback, suggestions, and code.

---

<p align="center">
  <img src="./assets/sql_saga_logo.png" alt="sql_saga logo" width="200"/>
</p>

---

For any issues or improvements, please use github.
