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

## Temporal Tables at a Glance

A simplified example to illustrate the concept.
A temporal table has `valid_from` and `valid_until` columns, which define a `[)` period (inclusive start, exclusive end).
The currently valid row has `infinity` in the `valid_until` column.

### Example: Establishment History

```
TABLE establishment (
    id,
    valid_from date,
    valid_until date,
    name
)
```
```
------+------------+-------------+-------------------------------------
id    | valid_from | valid_until |  name
------+------------+-------------+-------------------------------------
01    | 2023-01-01 |  2023-07-01 |  AutoParts LLC
01    | 2023-07-01 |  2024-01-01 |  AutoSpareParts INC
01    | 2024-01-01 |  infinity   |  SpareParts Corporation
02    | 2022-01-01 |  2022-07-01 |  Gasoline Refinement LLC
02    | 2022-07-01 |  2023-01-01 |  Gasoline and Diesel Refinement LLC
02    | 2023-01-01 |  infinity   |  General Refinement LLC
------+------------+-------------+-------------------------------------
```

### Example: Non-Temporal Reference Table

A regular table of statistical definitions (no temporal tracking needed):
```
TABLE stat_definition (
    code,
    stat_type,
    frequency,
    name
)
```
```
-----------+-----------+-----------+---------------------------
code       | stat_type | frequency |  name
-----------+-----------+-----------+---------------------------
employees  |   int     |   yearly  |  Number of people employed
turnover   |   int     |   yearly  |  Turnover (Local Currency)
-----------+-----------+-----------+---------------------------
```

### Example: Temporal Foreign Keys

A table tracking measured values over time, with a temporal foreign key to `establishment.id`:
```
TABLE stat_for_unit (
    id,
    stat_definition_id,
    valid_from,
    valid_until,
    establishment_id,
    value
)
```
```
-----------+------------+-------------+--------+------------
 stat_def  | valid_from | valid_until | est_id | value
-----------+------------+-------------+--------+------------
 employees | 2020-01-01 |  2024-01-01 |  01    |         90
 employees | 2024-01-01 |  infinity   |  01    |        130
 turnover  | 2023-01-01 |  2024-01-01 |  01    | 10 000 000
 turnover  | 2024-01-01 |  infinity   |  01    | 30 000 000
 employees | 2022-01-01 |  2023-01-01 |  02    |         20
 employees | 2023-01-01 |  infinity   |  02    |         80
 turnover  | 2022-01-01 |  2023-01-01 |  02    | 40 000 000
 turnover  | 2023-01-01 |  infinity   |  02    | 70 000 000
-----------+------------+-------------+--------+------------
```

**The key insight:** For temporal foreign keys, the referenced table must have the foreign key value available for the *entire* period `[valid_from, valid_until)`. Multiple rows can satisfy this - the periods do not need to align between tables.

For example, this `stat_for_unit` row covering `[2022-01-01, 2023-01-01)`:
```
turnover  | 2022-01-01 | 2023-01-01 |  02    | 40 000 000
```
is covered by these two *contiguous* `establishment` rows:
```
02    | 2022-01-01 |  2022-07-01 |  Gasoline Refinement LLC
02    | 2022-07-01 |  2023-01-01 |  Gasoline and Diesel Refinement LLC
```

## Installation

### With Docker

You can build `sql_saga` from source within your own Docker image. `sql_saga` requires PostgreSQL 18.
`make install` installs the `sql_saga` extension and its two internal shared libraries (`sql_saga` in C
and `sql_saga_native` in Rust). Building needs a Rust toolchain and libclang; `cargo-pgrx` is not
required. Only `CREATE EXTENSION sql_saga;` is used: `sql_saga_native` is not an extension. A multi-stage
build keeps the toolchains out of the runtime image:

```dockerfile
FROM postgres:18 AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential git ca-certificates curl pkg-config libclang-dev \
    postgresql-server-dev-18 \
    && rm -rf /var/lib/apt/lists/*
RUN curl -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"
ARG sql_saga_release=main # Or a specific commit/tag/branch
RUN git clone https://github.com/veridit/sql_saga.git /src/sql_saga && \
    cd /src/sql_saga && git checkout ${sql_saga_release} && \
    make && make install DESTDIR=/out

FROM postgres:18
COPY --from=build /out/ /
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

### Backup and Restore

Databases using `sql_saga` can be dumped with `pg_dump` and restored with `pg_restore`, serially
(the default) or with `--single-transaction`, into a new database or, with `--clean`, into a database
that already holds them.

**Parallel restore (`pg_restore -j N`) is not supported** until a PostgreSQL bug is fixed:
`sql_saga` keeps its bookkeeping in extension configuration tables that reference each other by
foreign key (for example `sql_saga.system_versioning` references `sql_saga.era`). `pg_dump`
computes the order in which their data must be loaded, but does not write that order into the
archive, so parallel workers can load a referencing table before the table it references and the
restore fails with a foreign key violation. The bug affects every extension whose configuration
tables use foreign keys; details and a minimal reproducer are in `doc/decision-log.md`.
User tables, including tables with `sql_saga` temporal foreign keys, are not affected.

Workaround for large databases: restore the schema and `sql_saga`'s own catalog data serially, then
everything else in parallel (0 failures in 20 runs, where plain `-j 4` failed 2 of 20):

```bash
pg_restore -l backup.dump > all.list
grep    'TABLE DATA sql_saga ' all.list > saga.list
grep -v 'TABLE DATA sql_saga ' all.list > rest.list
pg_restore --exit-on-error --section=pre-data -d mydb backup.dump
pg_restore --exit-on-error -L saga.list -d mydb backup.dump
pg_restore --exit-on-error -j 4 --section=data --section=post-data -L rest.list -d mydb backup.dump
```

#### Restoring over an existing database (`--clean`)

`pg_restore --clean` (with or without `--if-exists`, serially or with `--single-transaction`) restores
into a database that already holds the dumped `sql_saga` tables. `--clean` drops every object of the
dump before recreating it, one object at a time: a table's triggers, views, query functions and
history policies go first, the table itself last. `sql_saga` normally refuses to drop such objects on
their own (for example `cannot drop trigger "b_truncate" on table "b" because it is used in SYSTEM_TIME
period`). It recognizes the teardown phase of `pg_restore --clean` and lets those drops through. It also
removes the history policies whose names the dump cannot know, because they carry this installation's
token.

The teardown is recognized by the session `pg_restore` sets up. If you instead run a restore script
through `psql` (`pg_restore --clean -f script.sql`, then `psql -f script.sql`), or connect with your own
`application_name` (`PGAPPNAME`), opt in explicitly:

```bash
pg_restore --clean --if-exists -f - backup.dump | PGOPTIONS='-c sql_saga.clean_restore=on' psql -v ON_ERROR_STOP=1 -d mydb
```

The opt-in alone does not unlock anything. A drop is only treated as part of a restore teardown in a
session with the settings every `pg_dump` script starts with (`check_function_bodies = off` and
`row_security = off`). Accidental drops keep their protection.

`pg_restore --clean --create` (which drops and recreates the whole database) needs none of this.

In a database with `ALTER DEFAULT PRIVILEGES ... REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC`, `--clean`
recreates functions under those default privileges. This is standard PostgreSQL behaviour. The
extension's own functions then keep the restricted default you configured. The system-versioning
query functions (`<table>__as_of` and friends) are granted to `PUBLIC` again when their catalog row is
restored, as they are when created.

#### Privileges and history policies

Privileges and history policies need no extra step before or after `pg_dump` and `pg_restore`.

- Derived views are created with `PUBLIC` privileges, and system-versioning query functions are explicitly
  executable by `PUBLIC`. Authorization still comes from the caller's privileges on the base table. Restoring
  objects in a different order does not copy or synchronize the base table's ACL onto derived objects.
- History policy copies and the two fixed history policies carry a per-installation token in their names.
  The token is stored in `sql_saga.installation`, which is deliberately not dumped. A restore target gets a
  new token, so policies replayed from the dump do not collide with policies created in the target. The next
  policy synchronization removes managed policies carrying the old token. After a normal restore, each
  history table has one reader policy, one base-select policy, and one copy per base `SELECT` or `ALL` policy.
- Policy expressions and internal history views refer to base tables by name rather than by a stored OID, so
  those references remain valid in the restored database.

Replaying a dump into a database that already holds it without `--clean` fails with ordinary
"already exists" errors. Use `--clean` (see above).

If policy DDL is restored while event triggers are disabled with
`session_replication_role = replica`, managed old-token policies can remain until the next synchronization.
Repair every system-versioned table with:

```sql
SELECT sql_saga.sync_history_policies(
    format('%I.%I', sv.table_schema, sv.table_name)::regclass
)
FROM sql_saga.system_versioning AS sv;
```

A non-managed policy created on a history table while event triggers are disabled is different. History
reads by callers subject to the base table's RLS fail closed, and `sync_history_policies` refuses to continue.
Drop that policy, define it on the base table if it is needed, then run synchronization again. `sql_saga`
never silently drops a user's policy.

The parallel restore limitation above is unrelated to privileges and history policies.

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

#### Privileges and Security

##### Privileges are checked on the base table at access time

`sql_saga` does not keep a copy of who may access a base table. Its derived views and query functions are
available to `PUBLIC`, but they run with the caller's rights. PostgreSQL checks the caller's privileges on the
base table, including row-level security policies, when the data is used.

| Derived object | Granted to | How access is decided |
|---|---|---|
| `<table>__for_portion_of_<era>` | `PUBLIC`: SELECT, INSERT, UPDATE, DELETE | `security_invoker = on`; the caller needs the corresponding base-table privilege |
| `<table>__current_<era>` | `PUBLIC`: SELECT, INSERT, UPDATE, DELETE | `security_invoker = on`, `security_barrier = true`; the caller needs the corresponding base-table privilege |
| `<table>_with_history` | `PUBLIC`: SELECT | `security_invoker = on`; the caller needs SELECT on the base table |
| `<table>__as_of`, `__between`, `__between_symmetric`, `__from_to` | `PUBLIC`: EXECUTE | Security-invoker functions over `<table>_with_history` |
| `<table>_history` | `PUBLIC`: SELECT, filtered by RLS | Rows are available only to callers with SELECT on the base table, as described below |

Manage access by granting and revoking privileges on the base table:

```sql
GRANT SELECT ON employees TO reporting;
GRANT SELECT, INSERT, UPDATE, DELETE ON employees TO hr;

-- Removes access through the current view, history view, and query functions too.
REVOKE SELECT ON employees FROM reporting;
```

A caller without the required base-table privilege gets an error naming the base table, whichever derived
view or function was used:

```text
ERROR:  permission denied for table employees
```

Reading `<table>_history` directly is the exception to the error behavior. A caller without SELECT on the
base table gets zero rows because the history table's restrictive policy filters everything.

PostgreSQL role membership works normally. A base-table privilege granted to a group role is available to
members that inherit that role.

##### Column privileges

The generated views select every column of the base table. A role with SELECT on only some columns therefore
gets `permission denied for table <table>` when reading a generated current, portion, or history view, even if
the query projects only granted columns. This is the same fail-closed behavior as `SELECT *` on the base table.
Give such a role table-level SELECT or create a separate application view over the permitted columns.

##### Ownership and hardened databases

Derived objects are owned by the owner of the base table. Ownership changes on the base table are propagated
to them.

Some databases revoke the default EXECUTE privilege from `PUBLIC`. `sql_saga` explicitly grants the required
EXECUTE privilege on its portion and current view trigger functions, `temporal_merge` and its helper routines,
and the four query functions created by `add_system_versioning`. These routines still run with the caller's
rights, so EXECUTE alone does not grant access to base-table data.

Direct grants on a derived object cannot widen access beyond the base table. Direct revokes can make that
derived object unavailable, so applications should manage privileges on the base table instead.

##### Protected security properties

Event triggers refuse changes that would bypass access-time checks. For example:

```text
ERROR:  view "employees__current_valid" must keep security_invoker because it is used in a current view for table "employees"
ERROR:  view "employees_with_history" must keep security_invoker because it is used in SYSTEM VERSIONING for table "employees"
```

#### Row-Level Security and History

When a base table uses row-level security (RLS), its history obeys the same `SELECT` and `ALL` policies. Define
policies on the base table only. `sql_saga` keeps the history policies synchronized.

```sql
CREATE TABLE doc (
    id int NOT NULL,
    tenant text NOT NULL,
    body text
);
SELECT sql_saga.add_system_versioning('doc');

ALTER TABLE doc ENABLE ROW LEVEL SECURITY;
CREATE POLICY tenant_a ON doc FOR SELECT TO role_a USING (tenant = 'a');
-- NOTICE:  sql_saga: Created policy sql_saga_bf833db18461d646 on doc_history as the history copy of policy tenant_a on doc

CREATE POLICY tenant_b ON doc FOR ALL TO role_b USING (tenant = 'b');
-- NOTICE:  sql_saga: Created policy sql_saga_0c78df7d1efef0b3 on doc_history as the history copy of policy tenant_b on doc

GRANT SELECT ON doc TO role_a, role_b;
```

Each base `SELECT` or `ALL` policy gets a `FOR SELECT` copy with the same roles, permissive or restrictive
mode, and `USING` expression. `WITH CHECK` is not copied, and policies only for INSERT, UPDATE, or DELETE are
not copied. Copy names use `sql_saga_` plus a 16-digit hexadecimal hash of the base policy name and the
installation token, so the copy names in the examples above differ in your database.

Policy DDL on the base table synchronizes the copy immediately. A changed policy is reported as a drop and a
create because its copy is replaced:

```sql
ALTER POLICY tenant_a ON doc USING (tenant = 'a' AND id = 1);
-- NOTICE:  sql_saga: Dropped policy sql_saga_bf833db18461d646 on doc_history, the history copy of policy tenant_a on doc
-- NOTICE:  sql_saga: Created policy sql_saga_bf833db18461d646 on doc_history as the history copy of policy tenant_a on doc

DROP POLICY tenant_b ON doc;
-- NOTICE:  sql_saga: Dropped policy sql_saga_0c78df7d1efef0b3 on doc_history, the history copy of policy tenant_b on doc
```

Policy expressions are resolved by column name on the history table. If a referenced column is absent or has
a different type, synchronization is refused rather than installing a wrong filter:

```text
ERROR:  sql_saga: policy by_added on doc references column added_later, which doc_history lacks
```

`FORCE ROW LEVEL SECURITY` is mirrored from the base table to history:

```text
NOTICE:  sql_saga: Set row level security on doc_history to FORCE
NOTICE:  sql_saga: Set row level security on doc_history to NO FORCE
```

Superusers, roles with `BYPASSRLS`, and an owner when FORCE is not enabled see all history rows, just as they
see all base rows. When RLS is disabled on the base table, callers with SELECT on it read history without an
RLS filter.

##### Fixed state of the history table

Every history table has RLS enabled, SELECT granted to `PUBLIC`, FORCE matching the base table, and two fixed
policies:

- `sql_saga_history_reader_<token>`, permissive, admits callers that have SELECT on the base table and are not
  subject to its RLS.
- `sql_saga_history_base_select_<token>`, restrictive, rejects callers without SELECT on the base table.

Changing this state directly is refused:

```text
ERROR:  history table "doc_history" must keep row level security enabled, and forced exactly when its base table is, because it is used in SYSTEM VERSIONING for table "doc"
ERROR:  cannot drop or alter policy "sql_saga_bf833db18461d646" on "doc_history" because it is managed by sql_saga for table "doc"
```

User policies belong on the base table. Creating a non-managed policy on history, for any policy command, or
renaming a managed history policy to a non-managed name is refused:

```text
ERROR:  sql_saga: policies on history table "doc_history" are managed by sql_saga
HINT:  Create the policy on "doc"; sql_saga copies its SELECT and ALL policies to the history.
```

##### Fail closed when policies are out of sync

Policy copies are maintained by event triggers. If event triggers are disabled, a base policy can change
without its copy. Before an RLS-subject caller reads history, `sql_saga` verifies the copies and fails closed:

```text
ERROR:  sql_saga: history of doc is not synchronized (1 policy copies missing, 0 stale)
HINT:  Run: SELECT sql_saga.sync_history_policies('doc');
```

The base-table owner, or a member of its owner role, can run:

```sql
SELECT sql_saga.sync_history_policies('doc');
```

The function is idempotent. It creates missing copies, replaces stale copies, drops orphaned managed copies,
mirrors FORCE, and does nothing when the managed state is already correct.

A non-managed policy can exist on history only if event triggers were disabled. Reads still fail closed, now
including the unmanaged count:

```text
ERROR:  sql_saga: history of doc is not synchronized (0 policy copies missing, 0 stale, 1 not managed by sql_saga)
HINT:  Drop the policies not managed by sql_saga on doc_history (define them on doc instead), then run: SELECT sql_saga.sync_history_policies('doc');
```

Synchronization never silently drops a user's policy. It refuses until the policy is removed:

```text
ERROR:  sql_saga: policies on history table "doc_history" are managed by sql_saga
HINT:  Drop policy "sneaky" on "doc_history" and create it on "doc"; sql_saga copies its SELECT and ALL policies to the history.
```

Drop the policy from history, create it on the base table if needed, and run synchronization again.

##### Performance note

For callers subject to RLS, PostgreSQL may be unable to use a GiST range index for an as-of or range-only
history query because range operators are not leakproof and cannot be evaluated ahead of the security policy.
Btree key lookups such as `WHERE id = 42` can still use their index. If range-only history queries are slow,
include an indexed entity key where possible.

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
  - `ephemeral_columns`: (Optional, Default: `NULL`) An array of column names that should not be considered when comparing for data changes, but whose values should still be updated. This is ideal for metadata like `edit_comment` or `batch_id` that should be attached to a historical record without creating a new version of that record if only the metadata changes. Any synchronized temporal columns (e.g., a `valid_to` column) are automatically treated as ephemeral and do not need to be specified here. **Note:** If not provided, `temporal_merge` will auto-discover ephemeral columns from the era's metadata (see `add_era()`).
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

##### Manual schema changes

For a system-versioned table, `sql_saga` does not propagate column changes between the base table and its history table. Change both tables explicitly, then rebuild the derived views and query functions:

```sql
sql_saga.rebuild_system_versioning_views(
    table_oid regclass,
    without_columns name[] DEFAULT '{}'
) RETURNS void
```

Columns correspond by name, not by physical position. Every base-table column must exist in the history table with the same type, typmod, domain, and collation. The history table may have additional columns. `<table>_with_history` exposes base columns in base-table order, followed by history-only columns in history-table order.

`without_columns` omits named columns from the rebuilt views and functions for that call only. It does not alter either table, change the history writer, or store configuration. Use it to remove view dependencies before dropping, renaming, or changing a column. A later call without that name includes the column again if it still exists.

The caller must be a member of the base table's owner role. A successful rebuild reports exactly:

```text
NOTICE:  sql_saga: Rebuilt SYSTEM VERSIONING views and query functions for table ms
```

#### Add a column

Add the column to the base and history tables with compatible definitions, then rebuild:

```sql
ALTER TABLE employees ADD COLUMN department text;
ALTER TABLE employees_history ADD COLUMN department text;
SELECT sql_saga.rebuild_system_versioning_views('employees');
```

Until the history column exists, inserts can succeed because they do not write an old row, but an update or delete that needs to write history fails before changing the base row. Rebuild also refuses the incompatible pair. The regression-tested text is:

```text
ERROR:  sql_saga: column missing_in_history of base table ms is missing from history table ms_history
HINT:  Run: ALTER TABLE public.ms_history ADD COLUMN missing_in_history integer; SELECT sql_saga.rebuild_system_versioning_views('public.ms');
```

#### Drop a column

First rebuild without the column so the derived objects no longer depend on it. Then drop it from both tables and rebuild normally:

```sql
SELECT sql_saga.rebuild_system_versioning_views(
    'employees',
    ARRAY['salary']::name[]
);
ALTER TABLE employees DROP COLUMN salary;
ALTER TABLE employees_history DROP COLUMN salary;
SELECT sql_saga.rebuild_system_versioning_views('employees');
```

If you want to retain old values, omit the history-table `DROP COLUMN`. The column then becomes history-only. It remains visible in `employees_with_history`, with past values for history rows and `NULL` for current rows.

Dropping a history-only column uses the same pattern: rebuild with that column in `without_columns`, drop it from the history table, then rebuild normally.

#### Change a column type

Remove the column from the derived objects, change both table definitions, then rebuild it back in:

```sql
SELECT sql_saga.rebuild_system_versioning_views(
    'employees',
    ARRAY['code']::name[]
);
ALTER TABLE employees ALTER COLUMN code TYPE varchar(40);
ALTER TABLE employees_history ALTER COLUMN code TYPE varchar(40);
SELECT sql_saga.rebuild_system_versioning_views('employees');
```

You may rebuild again with `without_columns` while the two definitions differ, but writes that need history still fail. `without_columns` only shapes the views and functions. It never permits the history writer to omit a base value. The exact tested mismatch is:

```text
ERROR:  sql_saga: column code has type character varying(40) in base table ms but type character varying(20) in history table ms_history
HINT:  Run: ALTER TABLE public.ms_history ALTER COLUMN code TYPE character varying(40); SELECT sql_saga.rebuild_system_versioning_views('public.ms');
```

Typmod and collation differences are also mismatches. For example:

```text
ERROR:  sql_saga: column label has type text COLLATE "C" in base table ms but type text COLLATE "POSIX" in history table ms_history
HINT:  Run: ALTER TABLE public.ms_history ALTER COLUMN label TYPE text COLLATE "C"; SELECT sql_saga.rebuild_system_versioning_views('public.ms');
```

#### Rename a column

First remove the old name from the derived objects. Rename the history column, then the base column, then rebuild normally:

```sql
SELECT sql_saga.rebuild_system_versioning_views(
    'employees',
    ARRAY['tenant']::name[]
);
ALTER TABLE employees_history RENAME COLUMN tenant TO account;
ALTER TABLE employees RENAME COLUMN tenant TO account;
SELECT sql_saga.rebuild_system_versioning_views('employees');
```

The order matters when a base-table row-level security policy uses the column. Rename the history column first. If the base column is renamed first, policy synchronization fails closed because the copied policy cannot yet reference the new name in the history table. The exact tested error is:

```text
ERROR:  sql_saga: policy tenant_a on rt references column account, which rt_history lacks
```

After both renames, rebuilding recreates the views and as-of functions and synchronizes the copied history policy with the new column name.

#### History-only columns

You can add a column only to the history table and expose it through the history view:

```sql
ALTER TABLE employees_history ADD COLUMN legacy_note text;
UPDATE employees_history
SET legacy_note = 'imported record'
WHERE id = 42;
SELECT sql_saga.rebuild_system_versioning_views('employees');
```

Current rows and new history rows projected from the base table receive `NULL` for history-only columns. A history-only column's `DEFAULT` is not applied by the history writer. Existing rows may show a default because PostgreSQL applied it when the column was added, while subsequently written history rows still receive `NULL`.

Constraints remain your responsibility. For example, a history-only `NOT NULL` column that cannot accept `NULL` makes a history write fail with PostgreSQL's own error:

```text
ERROR:  null value in column "must_have" of relation "ms_history" violates not-null constraint
```

#### Dependent user objects

Rebuilding drops and recreates `<table>_with_history`, the internal history gate view, and the four as-of functions without `CASCADE`. If one of your views, functions, or other objects depends on them, PostgreSQL refuses the rebuild with its normal dependency error. `sql_saga` never uses `CASCADE`. Drop and recreate your dependent objects explicitly around the rebuild. The regression test confirms SQLSTATE `2BP01`:

```text
NOTICE:  rebuild refused: SQLSTATE 2BP01
```

#### Argument and ownership errors

The implemented checks include these exact regression-tested messages:

```text
ERROR:  no table name specified
ERROR:  table ms_plain does not have SYSTEM VERSIONING
ERROR:  must be owner of table ms
ERROR:  sql_saga: without_columns must not contain NULL
ERROR:  sql_saga: column payload is listed more than once in without_columns
ERROR:  sql_saga: column no_such_column does not exist in either ms or ms_history
ERROR:  sql_saga: cannot exclude system time column system_valid_range from SYSTEM VERSIONING views for table ms
```

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

#### Tables with Audit Columns (Ephemeral Columns)

For tables with audit columns like `edit_at` or `edit_by_user_id`, you can configure these as **ephemeral columns** at the era level. Ephemeral columns are excluded from coalescing comparisons, allowing adjacent temporal rows with identical business data but different audit timestamps to be merged into a single row:

```sql
CREATE TABLE audited_entity (
  id INTEGER NOT NULL,
  name VARCHAR NOT NULL,
  edit_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  edit_by_user_id INTEGER NOT NULL,
  valid_range daterange NOT NULL,
  PRIMARY KEY (id, valid_range WITHOUT OVERLAPS)
);

-- Register with ephemeral columns - these are excluded from coalescing comparisons
SELECT sql_saga.add_era(
    'audited_entity'::regclass,
    'valid_range',
    'valid',
    ephemeral_columns => ARRAY['edit_at', 'edit_by_user_id']
);

-- All views and temporal_merge calls on this era will automatically
-- inherit the ephemeral_columns setting, enabling proper coalescing.
SELECT sql_saga.add_for_portion_of_view('audited_entity'::regclass);
```

You can also configure ephemeral columns after era creation using `set_era_ephemeral_columns()`:

```sql
SELECT sql_saga.set_era_ephemeral_columns(
    'existing_table'::regclass,
    ephemeral_columns => ARRAY['edit_at', 'edit_by_user_id']
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

### C Development (LSP Setup)

For IDE support when editing C files (`src/*.c`), generate the compilation database:

```bash
make compile_commands.json
```

This creates a `compile_commands.json` with PostgreSQL include paths from `pg_config`, enabling clangd and other LSPs to resolve headers correctly.

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
