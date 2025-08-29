# sql_saga

`sql_saga` is a PostgreSQL plugin tailored for National Statistical Offices (NSOs) worldwide,
enabling efficient and intuitive handling of temporal tables.

Drawing inspiration from Nordic sagas, the project aims at the seamless blending of
ancient narrative with the contemporary purpose of global statistics.

### What is a "Saga"?

In the context of this extension, a **Saga** represents the complete history of a table's data over time. A Saga can be composed of one or more **Eras**, where each Era is a distinct temporal period defined by a pair of columns (e.g., `valid_from`/`valid_until` or `transaction_from`/`transaction_until`). This allows a single table to have its data managed across multiple, independent timelines if needed.

## Features

- Temporal Table Design Suggestions
- Support for foreign keys between temporal tables, and from standard (non-temporal) tables to temporal tables.
- High-performance, set-based API for bulk temporal data loading (`temporal_merge`).
- Intuitive API for seamless integration with existing NSO systems.
- Intuitive fetching of current data.
- Compatible with PostgREST - that creates REST endpoints for the API's.
- Built upon the robust and reliable PostgreSQL database system.
- Supports change tracking and delete in accordance with NSO requirements.

## Temporal Tables with Foreign Keys example

A simplified example to illustrate the concept.
A temporal table has `valid_from` and `valid_until` columns, which define a `[)` period (inclusive start, exclusive end), aligning with PostgreSQL's native range types. While `DATE` is used in these examples for simplicity, any data type that can form a range is supported, including `TIMESTAMPTZ`, `TIMESTAMP`, `INTEGER`, `BIGINT`, and `NUMERIC`.

### Entity Identifiers

A key concept in temporal data modeling is the **entity identifier**. Since a temporal table tracks the history of an entity over time, a single conceptual "thing" (like a company or a person) will have multiple rows in the table, each representing a different slice of its history.

The entity identifier is the column (or set of columns) that holds the same value for all rows that belong to the same conceptual entity. A common naming convention for this column is `entity_id` or simply `id`. In the examples below, the `id` column in `establishment` serves this purpose.

The primary key of the temporal table is typically a composite key that includes the entity identifier and a temporal column (e.g., `(id, valid_from)`) to uniquely identify each historical version of the entity.

The currently valid row has `infinity` in the `valid_until` column.

### Temporal Table with Valid Time

For users who prefer to work with inclusive end dates (e.g., a `valid_to` column), `sql_saga` provides a convenience trigger `sql_saga.synchronize_valid_to_until()`. This trigger can be used to automatically maintain the relationship `valid_until = valid_to + '1 day'`.

Example table:
```
TABLE establishment (
    id,
    valid_from date,
    valid_until date,
    name
)
```
Example data
```
------+------------+-------------+------------------------------------
id    | valid_from | valid_until |  name
------+------------+-------------+------------------------------------
01    | 2023-01-01 |  2023-07-01 |  AutoParts LLC
01    | 2023-07-01 |  2024-01-01 |  AutoSpareParts INC
01    | 2024-01-01 |  infinity   |  SpareParts Corporation
02    | 2022-01-01 |  2022-07-01 |  Gasoline Refinement LLC
02    | 2022-07-01 |  2023-01-01 |  Gasoline and Diesel Refinement LLC
02    | 2023-01-01 |  infinity   |  General Refinement LLC
------+------------+-------------+------------------------------------
```

A regular table of statistical values
```
TABLE stat_definition(
  code,
  stat_type,
  frequency,
  name,
)
```
Example values measured for an establishment.
```
----------+-----------+--------------+---------------------------
code      | stat_type |   frequency  |  name
----------+-----------+--------------+---------------------------
employees |   int     |   yearly     |  Number of people employed
turnover  |   int     |   yearly     |  Turnover (Local Currency)
----------+-----------+--------------+---------------------------
```
There is no temporal information for the `stat_definition` table,
as we don't report on their historic development.

A table for tracking the measured values over time,
using `valid_from` and `valid_until`, in addition to having
a regular foreign key to `stat_definition_id`, and a temporal
foreign key to `establishment.id`.

```
TABLE stat_for_unit (
    id
    stat_definition_id,
    valid_from,
    valid_until,
    establishment_id,
    value,
)
```

Some example data to show how measurements are kept in `stat_for_unit`.
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

The purpose of this extension is to make sure that for foreign keys
between temporal tables, the linked table, in this case `establishment`,
must have the linked foreign key available for the entire period `[valid_from, valid_until)`
of the `stat_for_unit` table.

Notice that there can be multiple matching rows, and the periods do not
need to align between the tables.

So this line from `stat_for_unit` which represents the period `[2022-01-01, 2023-01-01)`
```
turnover  | ... | 2022-01-01 | 2023-01-01 |  02    | 40 000 000
```
is covered by these two contiguous lines in `establishment` for periods `[2022-01-01, 2022-07-01)` and `[2022-07-01, 2023-01-01)`
```
02    | ... | 2022-01-01 | 2022-07-01 |  Gasoline Refinement LLC
02    | ... | 2022-07-01 | 2023-01-01 |  Gasoline and Diesel Refinement LLC
```

### Foreign Keys from Standard (Non-Temporal) Tables

`sql_saga` also supports foreign keys from a standard (non-temporal) table to a temporal table. This is useful for ensuring that a reference in a standard table points to an entity that exists (or existed at some point) in a temporal table.

For example, a standard `projects` table might reference a lead employee from a temporal `employees` table:

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

This validation is implemented using a `CHECK` constraint on the standard table, which calls a high-performance helper function created by `sql_saga`.

## Installation

TODO: Build a docker image with postgres and the sql_saga extension.

TODO: Build an Ubuntu packate with sql_saga.

`CREATE EXTENSION sql_saga;`

## Usage

Detailed examples and explanations on how to use the `sql_saga` system.

### Activate

```
CREATE TABLE legal_unit (
  id SERIAL NOT NULL,
  legal_ident VARCHAR NOT NULL,
  name VARCHAR NOT NULL,
  status TEXT, -- e.g., 'active', 'inactive'
  valid_from DATE,
  valid_until DATE,
  valid_to DATE -- Optional: for human-readable inclusive end dates
  -- Note: A primary key on temporal tables is often not on the temporal columns
);

-- Optional: a trigger to keep valid_to and valid_until in sync.
CREATE TRIGGER legal_unit_synchronize_validity
    BEFORE INSERT OR UPDATE ON legal_unit
    FOR EACH ROW EXECUTE FUNCTION sql_saga.synchronize_valid_to_until();

-- Register the table as a temporal table (an "era")
SELECT sql_saga.add_era(table_oid => 'legal_unit', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
-- Add temporal unique keys. A name is generated if the last argument is omitted.
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit', column_names => ARRAY['id'], unique_key_name => 'legal_unit_id_valid');
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit', column_names => ARRAY['legal_ident'], unique_key_name => 'legal_unit_legal_ident_valid');
-- Add a predicated unique key (e.g., only active units must have a unique name).
SELECT sql_saga.add_unique_key(
    table_oid => 'legal_unit',
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

SELECT sql_saga.add_era(table_oid => 'establishment', valid_from_column_name => 'valid_from', valid_until_column_name => 'valid_until');
SELECT sql_saga.add_unique_key(table_oid => 'establishment', column_names => ARRAY['id'], unique_key_name => 'establishment_id_valid');
SELECT sql_saga.add_unique_key(table_oid => 'establishment', column_names => ARRAY['name'], unique_key_name => 'establishment_name_valid');
-- Add a temporal foreign key. It references a temporal unique key.
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'establishment',
    fk_column_names => ARRAY['legal_unit_id'],
    fk_era_name => 'valid',
    unique_key_name => 'legal_unit_id_valid'
);

-- Add a foreign key from a standard table to a temporal table.
-- Note that fk_era_name is omitted for the standard table.
CREATE TABLE projects (id serial primary key, name text, legal_unit_id int);
SELECT sql_saga.add_foreign_key(
    fk_table_oid => 'projects',
    fk_column_names => ARRAY['legal_unit_id'],
    unique_key_name => 'legal_unit_id_valid'
);
```

### Deactivate

```
-- Foreign keys must be dropped before the unique keys they reference.
SELECT sql_saga.drop_foreign_key(
    table_oid => 'establishment',
    column_names => ARRAY['legal_unit_id'],
    era_name => 'valid'
);
-- For standard-to-temporal FKs, era_name is omitted.
SELECT sql_saga.drop_foreign_key(
    table_oid => 'projects',
    column_names => ARRAY['legal_unit_id']
);

SELECT sql_saga.drop_unique_key(
    table_oid => 'establishment',
    column_names => ARRAY['id'],
    era_name => 'valid'
);
SELECT sql_saga.drop_unique_key(
    table_oid => 'establishment',
    column_names => ARRAY['name'],
    era_name => 'valid'
);
SELECT sql_saga.drop_era('establishment');


SELECT sql_saga.drop_unique_key(
    table_oid => 'legal_unit',
    column_names => ARRAY['id'],
    era_name => 'valid'
);
SELECT sql_saga.drop_unique_key(
    table_oid => 'legal_unit',
    column_names => ARRAY['legal_ident'],
    era_name => 'valid'
);
-- For predicated unique keys, the predicate is not needed for dropping.
SELECT sql_saga.drop_unique_key(
    table_oid => 'legal_unit',
    column_names => ARRAY['name'],
    era_name => 'valid'
);
SELECT sql_saga.drop_era('legal_unit');
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

## API Reference

### Era Management
- `add_era(table_oid regclass, valid_from_column_name name, valid_until_column_name name, era_name name DEFAULT 'valid', range_type regtype DEFAULT NULL, bounds_check_constraint name DEFAULT NULL, create_columns boolean DEFAULT false) RETURNS boolean`
- `drop_era(table_oid regclass, era_name name DEFAULT 'valid', drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT false) RETURNS boolean`

### Unique Keys
- `add_unique_key(table_oid regclass, column_names name[], era_name name DEFAULT 'valid', unique_key_name name DEFAULT NULL, unique_constraint name DEFAULT NULL, exclude_constraint name DEFAULT NULL, predicate text DEFAULT NULL) RETURNS name`
- `drop_unique_key(table_oid regclass, column_names name[], era_name name, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT true) RETURNS void`
- `drop_unique_key_by_name(table_oid regclass, key_name name, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT true) RETURNS void`

### Foreign Keys
- **For temporal-to-temporal foreign keys:**
  - `add_foreign_key(fk_table_oid regclass, fk_column_names name[], fk_era_name name, unique_key_name name, match_type sql_saga.fk_match_types DEFAULT 'SIMPLE', update_action sql_saga.fk_actions DEFAULT 'NO ACTION', delete_action sql_saga.fk_actions DEFAULT 'NO ACTION', foreign_key_name name DEFAULT NULL, fk_insert_trigger name DEFAULT NULL, fk_update_trigger name DEFAULT NULL, uk_update_trigger name DEFAULT NULL, uk_delete_trigger name DEFAULT NULL) RETURNS name`
- **For standard-to-temporal foreign keys:**
  - `add_foreign_key(fk_table_oid regclass, fk_column_names name[], unique_key_name name, match_type sql_saga.fk_match_types DEFAULT 'SIMPLE', update_action sql_saga.fk_actions DEFAULT 'NO ACTION', delete_action sql_saga.fk_actions DEFAULT 'NO ACTION', foreign_key_name name DEFAULT NULL, fk_check_constraint name DEFAULT NULL, fk_helper_function text DEFAULT NULL, uk_update_trigger name DEFAULT NULL, uk_delete_trigger name DEFAULT NULL) RETURNS name`
- **Dropping foreign keys:**
  - `drop_foreign_key(table_oid regclass, column_names name[], era_name name DEFAULT NULL, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT') RETURNS void`: Drops a foreign key. For temporal-to-temporal keys, `era_name` must be provided. For standard-to-temporal keys, `era_name` should be omitted.
  - `drop_foreign_key_by_name(table_oid regclass, key_name name) RETURNS boolean`: Drops any type of foreign key by its unique generated or user-provided name.

### Updatable Views (for PostgREST and `FOR PORTION OF` emulation)
The `add_api` function creates views to simplify interaction with temporal tables. This includes a view that only shows the *current* state of data (ideal for PostgREST) and a view that emulates the `FOR PORTION OF` syntax for updating historical records.

- `add_api(table_oid regclass DEFAULT NULL, era_name name DEFAULT 'valid') RETURNS boolean`
- `drop_api(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT false) RETURNS boolean`

### High-Performance Bulk Data Loading (`temporal_merge`)
- `temporal_merge(p_target_table regclass, p_source_table regclass, p_id_columns TEXT[], p_ephemeral_columns TEXT[], p_mode sql_saga.temporal_merge_mode DEFAULT 'upsert_patch', p_era_name name DEFAULT 'valid', p_founding_id_column name DEFAULT NULL, p_update_source_with_assigned_entity_ids BOOLEAN DEFAULT false)`: A powerful, set-based procedure for performing `INSERT`, `UPDATE`, and `DELETE` operations on temporal tables from a source table. It is designed to solve complex data loading scenarios (e.g., idempotent imports, data corrections) in a single, efficient, and transactionally-safe statement.
  - `p_target_table`: The temporal table to merge data into.
  - `p_source_table`: A table (usually temporary) containing the source data. Must have a `row_id` integer column for feedback.
  - `p_id_columns`: An array of column names that form the conceptual entity identifier.
  - `p_ephemeral_columns`: An array of column names that should not be considered when comparing for data changes, but whose values should still be updated (e.g., `edit_comment`).
  - `p_mode`: Controls the merge behavior.
    - `'upsert_patch'`: Inserts new entities and updates existing ones. `NULL` values in the source are ignored, preserving existing data.
    - `'upsert_replace'`: Inserts new entities and updates existing ones. `NULL` values in the source will overwrite existing data.
    - `'patch_only'` / `'replace_only'`: Only affects entities that already exist in the target table.
    - `'insert_only'`: Only inserts new entities.
  - `p_founding_id_column`: The name of a column in the source table used to group multiple rows that belong to the same *new* conceptual entity. This allows `temporal_merge` to resolve intra-batch dependencies (e.g., an `INSERT` and a `REPLACE` for the same new entity in one call).
  - `p_update_source_with_assigned_entity_ids`: If `true`, the procedure will update the source table with any generated surrogate key values for newly inserted entities. This simplifies multi-step import processes by removing the need for manual ID propagation between steps.

### System Versioning (History Tables)
`sql_saga` provides full support for system-versioned tables, creating a complete, queryable history of every row. This tracks the state of data over time ("What did this record look like last year?"). When this feature is enabled, the columns `system_valid_from` and `system_valid_until` are added to the table.

- `add_system_versioning(table_oid regclass, history_table_name name DEFAULT NULL, view_name name DEFAULT NULL)`
- `drop_system_versioning(table_oid regclass, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT true)`
- `set_system_time_era_excluded_columns(table_oid regclass, excluded_column_names name[]) RETURNS void`
- `generated_always_as_row_start_end() RETURNS trigger` (C function)
- `write_history() RETURNS trigger` (C function)

### Convenience Triggers
- `synchronize_valid_to_until() RETURNS trigger`

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

For more any issues or improvements, please use github.
