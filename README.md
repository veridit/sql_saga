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
- Supports using pg_stat_monitor for stats.
  On macOs use `brew install pgxnclient` and `USE_PGXS=1 pgxn install --verbose --pg_config /Applications/Postgres.app/Contents/Versions/18/bin/pg_config pg_stat_monitor` where you specify the version you wish to install for, here it was 18. Then activate with `echo "ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_monitor';" | psql` and restart the instance using the PostgresApp. Also run `echo 'CREATE EXTENSION pg_stat_monitor;' | psql sql_saga_regress` to install it for the test database.
- Supports using auto_explain for capturing query plans of internal statements (useful for performance debugging).
  Add to shared_preload_libraries: `psql -c "ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_monitor, auto_explain';"` and restart PostgreSQL. Then enable in your session:
  ```sql
  SET auto_explain.log_min_duration = 0;  -- Log all statements (use higher value in production)
  SET auto_explain.log_analyze = true;    -- Include actual execution times
  SET auto_explain.log_nested_statements = true;  -- Capture EXECUTE inside functions
  SET client_min_messages = LOG;          -- Show LOG output in psql
  ```

## Installation

### With Docker

You can build `sql_saga` from source within your own Docker image. Here is an example snippet to add to your `Dockerfile`, based on the official PostgreSQL image:

```dockerfile
# Start from your desired PostgreSQL version
FROM postgres:16

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    postgresql-server-dev-$(pg_config --version | awk '{print $2}' | cut -d. -f1) \
    && rm -rf /var/lib/apt/lists/*

# Clone, build, and install sql_saga
ARG sql_saga_release=main # Or a specific commit/tag/branch
WORKDIR /tmp
RUN git clone https://github.com/veridit/sql_saga.git && \
  cd sql_saga && \
  git checkout ${sql_saga_release} && \
  make install && \
  cd / && \
  rm -rf /tmp/sql_saga
```

You can then build your image, for example:
```bash
docker build -t my-postgres-with-saga .
```

To use a specific version of `sql_saga`, you can use a build argument:
```bash
docker build --build-arg sql_saga_release=1ed0d06a90bc -t my-postgres-with-saga .
```

Once your database container is running, connect to your database and run:

`CREATE EXTENSION sql_saga;`

This will make all `sql_saga` functions and features available.

TODO: Build an Ubuntu packate with sql_saga.

## Core Concepts

A temporal table tracks the validity of data over time using a `[)` period (inclusive start, exclusive end). The recommended approach is to use a **range column** (e.g., `valid_range daterange NOT NULL`) as the single source of truth for temporal data. This aligns with PostgreSQL 18's native temporal features and provides optimal performance.

**Alternative: Synchronized Columns** - For REST API compatibility (e.g., PostgREST) or human-readable queries, you can optionally synchronize `valid_from` and `valid_until` columns alongside the range. This comes at a small performance cost due to trigger-based synchronization. A `valid_to` column (inclusive end date) can also be synchronized for human-friendly display.

While `DATE` ranges are used in these examples, any data type that can form a range is supported, including `TIMESTAMPTZ`, `TIMESTAMP`, `INTEGER`, `BIGINT`, and `NUMERIC`.

### Entity Identifiers

A key concept in temporal data modeling is the **entity identifier**. Since a temporal table tracks the history of an entity over time, a single conceptual "thing" (like a company or a person) will have multiple rows in the table, each representing a different slice of its history.

The entity identifier is the column (or set of columns) that holds the same value for all rows that belong to the same conceptual entity. A common naming convention for this column is `entity_id` or simply `id`. In the examples below, the `id` column in `establishment` serves this purpose.

The primary key of the temporal table is typically a composite key that includes the entity identifier and the temporal range using PostgreSQL 18's `WITHOUT OVERLAPS` constraint (e.g., `PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)`). This ensures each historical version is unique and non-overlapping. **This is a requirement for tables that need to store history using SCD Type 2 operations.**

**Note on Incompatible Schemas:** To preserve history, `sql_saga` needs to be able to insert new versions of an entity with the same stable identifier. Certain schema designs are fundamentally incompatible with this pattern:
1.  The table cannot have a simple `PRIMARY KEY` that does not include the temporal columns. Use `PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)` or a composite key with temporal columns.
2.  The table cannot use a `GENERATED ALWAYS AS IDENTITY` column. If you use an identity column, it must be `GENERATED BY DEFAULT AS IDENTITY` to allow `sql_saga` to insert historical records with a specific, existing ID.

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

- **Surgical `UPDATE` is the primary supported operation.** To apply a change to a specific time slice, you must provide `valid_from` and `valid_until` in the `SET` clause. Simple `UPDATE`s that do not change the validity period are also permitted for historical corrections on existing records.
- **`INSERT` and `DELETE` are intentionally not supported on the view.** These operations should be performed directly on the base table. `DELETE` is unsupported because standard SQL provides no way to pass the required `[from, until)` parameters to a `DELETE` trigger, unlike `UPDATE` which can use the `SET` clause for this purpose. This focused design ensures the view's purpose is clear and prevents accidental misuse.

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

```sql
TABLE projects (
    id int,
    name text,
    lead_employee_id int
)

TABLE employees (
    id int,
    valid_range daterange NOT NULL,
    name text
)
```

A foreign key from `projects.lead_employee_id` to `employees.id` ensures that any `lead_employee_id` in the `projects` table corresponds to a valid employee in the `employees` table's history. Unlike temporal-to-temporal foreign keys which check for coverage over a period, this type of foreign key simply checks for the *existence* of the key in the referenced temporal table at any point in its history.

This validation is implemented using a `CHECK` constraint on the regular table, which calls a high-performance helper function created by `sql_saga`.

#### High-Performance Bulk Data Loading (`temporal_merge`)
- `temporal_merge(target_table regclass, source_table regclass, identity_columns TEXT[], natural_identity_columns TEXT[], ...)`: A powerful, set-based procedure for performing `INSERT`, `UPDATE`, and `DELETE` operations on temporal tables from a source table. It is designed to solve complex data loading scenarios (e.g., idempotent imports, data corrections) in a single, efficient, and transactionally-safe statement. The API is designed to be orthogonal: `mode` controls the non-destructive merge behavior, and `delete_mode` provides optional, destructive overrides.
  - `target_table`: The temporal table to merge data into.
  - `source_table`: A table (usually temporary) containing the source data.
  - `identity_columns`: An array of column names that form the stable, conceptual entity identifier (e.g., a surrogate primary key like `id`). For SCD Type 2 operations, these columns must be part of a composite key that includes the temporal range (e.g., `PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)`) to allow multiple historical versions of the same entity.
  - `natural_identity_columns`: An array of column names that form a "natural" or "business" key. This key is used to look up existing entities in the target table when the stable identifier in `identity_columns` is not known by the source (e.g., is `NULL` for new entities). This is the primary mechanism for preventing duplicate entities when loading data from external systems.
  - `ephemeral_columns`: (Optional, Default: `NULL`) An array of column names that should not be considered when comparing for data changes, but whose values should still be updated. This is ideal for metadata like `edit_comment` or `batch_id` that should be attached to a historical record without creating a new version of that record if only the metadata changes. Any synchronized temporal columns (e.g., a `valid_to` column) are automatically treated as ephemeral and do not need to be specified here.
  - `mode`: Controls the scope and payload semantics of the merge. By default, all modes are non-destructive to the timeline.
    - `'MERGE_ENTITY_PATCH'`: (Default) Merges the source with the target timeline. For overlapping periods, it patches data by applying non-`NULL` values from the source; target data is preserved for any attribute that is `NULL` or absent in the source. This is a stateless operation. It preserves non-overlapping parts of the target timeline.
    - `'MERGE_ENTITY_REPLACE'`: Merges the source timeline with the target entity's full timeline, completely replacing data for overlapping periods with the source data. Preserves non-overlapping parts of the target timeline.
    - `'MERGE_ENTITY_UPSERT'`: A partial update mode similar to `PATCH`, but it treats `NULL` as an explicit value. `NULL` values in the source will overwrite existing data in the target.
    - `'INSERT_NEW_ENTITIES'`: Inserts entities that are entirely new to the target table.
    - `'UPDATE_FOR_PORTION_OF'`: Applies a surgical partial update to a specific time portion of an existing entity, treating `NULL` as an explicit value. It ignores source rows for new entities.
    - `'PATCH_FOR_PORTION_OF'`: Applies a surgical patch to a specific time portion of an existing entity, ignoring `NULL` values in the source.
    - `'REPLACE_FOR_PORTION_OF'`: Applies a surgical replacement of a specific time portion of an existing entity.
    - `'DELETE_FOR_PORTION_OF'`: Performs a surgical deletion of a specific time portion from an existing entity. This is a powerful feature for correcting historical errors. If the deleted portion is in the middle of an existing time slice, the procedure will automatically split the original record into two, leaving a gap where the deleted portion was. This is achieved by the planner assigning a special `NULL` data payload to the deleted segment, which prevents it from being coalesced with the surrounding, data-bearing segments.
  - `row_id_column`: The name of the column in the source table that uniquely identifies and orders each row (default: `row_id`). This is required for feedback and for resolving temporal overlaps in the source.
  - `founding_id_column`: The name of a column used to group multiple source rows that belong to the same *new* conceptual entity. This allows `temporal_merge` to resolve intra-batch dependencies (e.g., an `INSERT` and a `REPLACE` for the same new entity in one call). If `NULL`, the `row_id_column` is used as the default.
    - **Important:** The scope of a correlation identifier is limited to a single `temporal_merge` call. All rows belonging to a single "founding event" *must* be processed within the same source table in a single call.
  - `update_source_with_identity`: If `true`, the procedure will update the source table with any generated identity key values for newly inserted entities. This simplifies multi-step import processes by removing the need for manual ID propagation between steps.
  - `delete_mode`: Provides optional, destructive overrides.
    - `'NONE'` (Default): No destructive operations occur.
    - `'DELETE_MISSING_TIMELINE'`: Enables "source-as-truth" timeline replacement. For any entity present in the source, any part of its timeline in the target that is **not** covered by the source's timeline will be **deleted**.
    - `'DELETE_MISSING_ENTITIES'`: Deletes entire entities. When used with `MERGE_ENTITY_*` modes, any entity in the target that is not present in the source is completely deleted.
    - `'DELETE_MISSING_TIMELINE_AND_ENTITIES'`: A combination of both destructive behaviors.
  - `update_source_with_feedback`: If `true`, the procedure will update the source table with status and error feedback for each source row. Requires either status or error feedback columns to be set.
  - `feedback_status_column`: The name of the `jsonb` column in the source table to write status messages to. If provided, `feedback_status_key` must also be set.
  - `feedback_status_key`: The key within the `jsonb` status column where the status for this merge will be written.
  - `feedback_error_column`: The name of the `jsonb` column in the source table to write error messages to. If provided, `feedback_error_key` must also be set.
  - `feedback_error_key`: The key within the `jsonb` error column where the error message for this specific merge operation will be written.

  ##### Planner Optimizations for Natural Keys
  The `temporal_merge` procedure is designed for high performance and automatically optimizes its query plan based on the nullability of your natural key columns (`natural_identity_columns`). It dynamically generates one of three strategies to ensure efficient entity lookups:
  -   **All keys `NOT NULL`:** Uses a simple and fast `WHERE ... IN (...)` clause, which is ideal for indexes on non-nullable columns.
  -   **Multiple nullable keys:** For complex keys where multiple columns can be `NULL` (e.g., XOR foreign keys), it generates a `UNION ALL` of simple `=` joins. This pattern is highly effective at enabling the PostgreSQL planner to use partial indexes.
  -   **Single nullable key:** Falls back to a null-safe `IS NOT DISTINCT FROM` join to ensure correctness.

  ##### The `INSERT -> UPDATE -> DELETE` Execution Strategy
  To ensure correctness and compatibility with both temporal foreign keys and uniqueness constraints, the `temporal_merge` executor guarantees that all DML operations are performed in a specific order: all `INSERT`s are executed first, followed by all `UPDATE`s, and finally all `DELETE`s.

  This strategy is critical for handling SCD Type 2 changes on a referenced table. By inserting the new version of a record *before* updating (shortening) the old one, it ensures that there is always at least one covering record for any dependent foreign keys. This prevents `AFTER` triggers from failing due to a transient gap in the timeline. To accommodate the temporary timeline overlap this creates, the procedure internally uses deferred constraints, which are checked only at the end of the operation when the timeline is once again consistent.

  **Known Limitation:** While this strategy is robust, it cannot prevent all foreign key violations. An `AFTER` trigger on a parent table fires immediately after a `DELETE` or `UPDATE`, and may fail if it sees child records whose timeline is not yet covered by a new parent record (which is handled in a separate statement). This is most common in modes like `MERGE_ENTITY_REPLACE` or when using a `delete_mode`.

  This limitation applies not only to inconsistencies created *between multiple, separate procedure calls* (a common ETL issue), but can also occur *within a single call* for complex replacement or deletion scenarios.

  The core issue is that the correct processing order for parent/child tables depends on the type of operation:
  - **For `INSERT`s:** You must process the parent table first to generate the ID that the child table will reference.
  - **For timeline-shrinking `UPDATE`s or `DELETE`s:** You must process the child table first. If you shorten or delete the parent's timeline first, its `AFTER` trigger will fire and see that the child timeline is no longer covered, causing a foreign key violation.

  Since a single ETL batch can contain both `INSERT`s and `DELETE`s/`UPDATE`s, there is no single fixed processing order that is always correct. The standard and most robust solution for this pattern is to **temporarily disable all relevant temporal foreign key triggers** for the duration of the batch transaction using `sql_saga`'s helper procedures. This allows the transaction to reach a temporarily inconsistent state, with the guarantee that the database's deferred uniqueness constraints on each table will still ensure the final state of each timeline is internally consistent. `sql_saga` provides `disable_temporal_triggers` and `enable_temporal_triggers` for this purpose, which are safer than a broad `ALTER TABLE ... DISABLE TRIGGER USER` because they only affect `sql_saga`-managed foreign key triggers.

  **Example: Disabling Triggers for a Batch Operation**
  ```sql
  BEGIN;
  -- Disable all sql_saga-managed temporal foreign key triggers for the tables in this batch.
  -- This is a targeted alternative to ALTER TABLE ... DISABLE TRIGGER USER and does not affect
  -- non-FK triggers, such as those for synchronized columns.
  CALL sql_saga.disable_temporal_triggers('etl.legal_unit', 'etl.location');

  -- Process all changes in their logical order.
  CALL etl.process_legal_units(p_batch_id);
  CALL etl.process_locations(p_batch_id);
  -- ...process other dependent tables...

  -- Re-enable the triggers. The final state is now consistent.
  CALL sql_saga.enable_temporal_triggers('etl.legal_unit', 'etl.location');
  COMMIT;
  ```

  ##### Executor State and Feedback
  The procedure uses two session-scoped temporary tables to manage its state: `temporal_merge_plan` (which stores the execution plan) and `temporal_merge_feedback` (which stores the final row-by-row feedback). These tables are created in the `pg_temp` schema and are automatically cleaned up at the end of the transaction (`ON COMMIT DROP`).

  ##### Performance Considerations: Batch Sizing
  For optimal performance, process data in batches of approximately **1,000 rows per `temporal_merge` call**. Benchmarks show:
  
  | Batch Size | Throughput (MERGE_ENTITY_*) | Throughput (UPDATE_FOR_PORTION_OF) |
  |------------|----------------------------|-----------------------------------|
  | 100 rows   | ~1,300-1,500 rows/s        | ~1,500-1,700 rows/s              |
  | 1,000 rows | ~2,800-3,000 rows/s        | ~7,000-8,000 rows/s              |
  | 10,000+ rows | ~1,100-1,200 rows/s      | ~5,500-5,800 rows/s              |

  The 1,000-row sweet spot balances the per-call planning overhead (~300-400ms) against efficient set-based execution. Larger batches hit diminishing returns as the planner's internal data structures grow, while smaller batches pay the planning overhead too frequently.

  **Example: Batched ETL Processing**
  ```sql
  DO $$
  DECLARE
      v_batch_size CONSTANT int := 1000;
      v_offset int := 0;
      v_processed int;
  BEGIN
      LOOP
          -- Create a batch from the staging table
          CREATE TEMP TABLE batch_source ON COMMIT DROP AS
          SELECT * FROM staging.imports
          ORDER BY row_id
          LIMIT v_batch_size OFFSET v_offset;
          
          GET DIAGNOSTICS v_processed = ROW_COUNT;
          EXIT WHEN v_processed = 0;
          
          -- Process the batch
          CALL sql_saga.temporal_merge(
              target_table => 'production.entities'::regclass,
              source_table => 'batch_source'::regclass,
              primary_identity_columns => '{id}'
          );
          
          DROP TABLE batch_source;
          v_offset := v_offset + v_batch_size;
      END LOOP;
  END $$;
  ```

  - **Caveat for Multi-Role Sessions:** Because temporary tables are owned by the role that creates them, calling `temporal_merge` as different roles within the same session (e.g., via `SET ROLE`) can lead to permission errors. If the procedure is called by a superuser and then later by an unprivileged user, the second call may fail as the unprivileged user might not have permission to `TRUNCATE` the tables created by the superuser.
  - **Solution:** In the rare case that you need to call `temporal_merge` as multiple different roles within a single session, it is safest to manually drop both temporary tables before changing roles: `DROP TABLE IF EXISTS pg_temp.temporal_merge_plan, pg_temp.temporal_merge_feedback;`
  - **Debugging GUCs:** To aid in debugging, `temporal_merge` respects three session-level configuration variables (GUCs). They are disabled by default.
    - `SET sql_saga.temporal_merge.log_plan = true;`: Logs the generated execution plan to the server log.
    - `SET sql_saga.temporal_merge.log_feedback = true;`: Logs the final row-by-row feedback to the server log.
    - `SET sql_saga.temporal_merge.log_sql = true;`: Logs the full, dynamically generated SQL of the planner query to the server log. This is useful for performance tuning and debugging complex merge scenarios.

#### System Versioning (History Tables)
`sql_saga` provides full support for system-versioned tables, creating a complete, queryable history of every row. This tracks the state of data over time ("What did this record look like last year?"). When this feature is enabled, the columns `system_valid_from` and `system_valid_until` are added to the table.

### Level 3: The Deep Dive - Complete API Reference

For a complete, auto-generated reference of all functions and procedures, please see the [API Documentation](./doc/api.md).

## Usage

This section provides a guide to using `sql_saga`, organized into three levels of complexity.

### Level 1: The Quick Tour

This example provides a complete, runnable demonstration of the core DDL functions to set up temporal tables and relationships.

#### Activate (Recommended: Range-Only Tables)

The simplest and fastest approach uses a `valid_range` column with PostgreSQL 18's native `WITHOUT OVERLAPS` constraint:

```sql
CREATE TABLE legal_unit (
  id INTEGER NOT NULL,
  legal_ident VARCHAR NOT NULL,
  name VARCHAR NOT NULL,
  status TEXT, -- e.g., 'active', 'inactive'
  valid_range daterange NOT NULL,
  PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)
);

-- Register the table as a temporal table (an "era").
SELECT sql_saga.add_era('legal_unit'::regclass, 'valid_range', 'valid');

-- Add additional temporal unique keys. The PK already provides one.
SELECT sql_saga.add_unique_key(
    table_oid => 'legal_unit'::regclass,
    column_names => ARRAY['legal_ident'],
    key_type => 'natural',
    unique_key_name => 'legal_unit_legal_ident_valid'
);

-- Add a predicated unique key (e.g., only active units must have a unique name).
SELECT sql_saga.add_unique_key(
    table_oid => 'legal_unit'::regclass,
    column_names => ARRAY['name'],
    key_type => 'predicated',
    predicate => 'status = ''active''',
    unique_key_name => 'legal_unit_active_name_valid'
);


CREATE TABLE establishment (
  id INTEGER NOT NULL,
  name VARCHAR NOT NULL,
  address TEXT NOT NULL,
  legal_unit_id INTEGER NOT NULL,
  valid_range daterange NOT NULL,
  PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)
);

SELECT sql_saga.add_era('establishment'::regclass, 'valid_range', 'valid');

-- Add a temporal foreign key.
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'establishment'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'legal_unit'::regclass,
    pk_column_names => ARRAY['id']
);

-- Add a foreign key from a regular table to a temporal table.
CREATE TABLE projects (id serial primary key, name text, legal_unit_id int);
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'projects'::regclass,
    fk_column_names => ARRAY['legal_unit_id'],
    pk_table_oid => 'legal_unit'::regclass,
    pk_column_names => ARRAY['id']
);
```

#### Alternative: REST/Human-Friendly Tables with Synchronized Columns

For compatibility with REST APIs like PostgREST, or for human-readable queries, you can add synchronized `valid_from`, `valid_until`, and optional `valid_to` columns. These are automatically kept in sync with the authoritative `valid_range` column via triggers:

```sql
CREATE TABLE legal_unit (
  id INTEGER NOT NULL,
  name VARCHAR NOT NULL,
  valid_range daterange NOT NULL,
  valid_from DATE,        -- Synchronized: lower(valid_range)
  valid_until DATE,       -- Synchronized: upper(valid_range)
  valid_to DATE,          -- Synchronized: upper(valid_range) - 1 (human-friendly inclusive end)
  PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)
);

-- Enable era with synchronized columns
SELECT sql_saga.add_era(
    'legal_unit'::regclass,
    'valid_range',
    'valid',
    synchronize_columns := true,  -- Auto-detect valid_from/valid_until
    valid_to_column_name := 'valid_to'  -- Also sync the inclusive end date
);
```

**Note:** Synchronized columns add a small performance overhead due to trigger execution. For bulk data loading, the range-only approach is recommended.

To ensure performant foreign key checks, `add_foreign_key` automatically creates an optimal index (GIST for temporal tables, B-tree for regular tables) on the foreign key columns. This can be disabled via the `create_index` parameter. The index is automatically removed when the foreign key is dropped.

#### Deactivate

```
-- Foreign keys must be dropped before the unique keys they reference.
-- For temporal tables, era_name is not needed if the table has only one era.
SELECT sql_saga.drop_foreign_key(
    table_oid => 'establishment'::regclass,
    column_names => ARRAY['legal_unit_id']
);
-- For regular tables, era_name is always omitted.
SELECT sql_saga.drop_foreign_key(
    table_oid => 'projects'::regclass,
    column_names => ARRAY['legal_unit_id']
);

SELECT sql_saga.drop_unique_key(
    table_oid => 'establishment'::regclass,
    column_names => ARRAY['id'],
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

### Git Hooks and Scratch Directories

This project uses a scratch directory (`tmp/`) for local experiments and AI tool interaction. Files in this directory can be locally staged to view changes with `git diff`, but a pre-commit hook will prevent them from ever being committed.

**One-Time Setup:** To enable this and other project conventions, all developers must configure Git to use our shared hooks path after cloning:

```bash
git config core.hooksPath devops/githooks
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
