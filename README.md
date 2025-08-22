# sql_saga

`sql_saga` is a PostgreSQL plugin tailored for National Statistical Offices (NSOs) worldwide,
enabling efficient and intuitive handling of temporal tables.

Drawing inspiration from Nordic sagas, the project aims at the seamless blending of
ancient narrative with the contemporary purpose of global statistics.

An Sql Saga is the history of a table (an era) over multiple periods of time.

## Features

- Temporal Table Design Suggestions
- Support for foreign keys between temporal tables.
- Intuitive API for seamless integration with existing NSO systems.
- Intuitive fetching of current data.
- Compatible with PostgREST - that creates REST endpoints for the API's.
- Built upon the robust and reliable PostgreSQL database system.
- Supports change tracking and delete in accordance with NSO requirements.

## Temporal Tables with Foreign Keys example

A simplified example to illustrate the concept.
A temporal table has `valid_from` and `valid_until` columns, which define a `[)` period (inclusive start, exclusive end), aligning with PostgreSQL's native range types.

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
  valid_from TIMESTAMPTZ,
  valid_until TIMESTAMPTZ,
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
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit', column_names => ARRAY['name'], unique_key_name => 'legal_unit_name_valid');
SELECT sql_saga.add_unique_key(table_oid => 'legal_unit', column_names => ARRAY['legal_ident'], unique_key_name => 'legal_unit_legal_ident_valid');


CREATE TABLE establishment (
  id SERIAL NOT NULL,
  name VARCHAR NOT NULL,
  address TEXT NOT NULL,
  legal_unit_id INTEGER NOT NULL,
  valid_from TIMESTAMPTZ,
  valid_until TIMESTAMPTZ
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

```

### Deactivate

```
-- Foreign keys must be dropped before the unique keys they reference.
SELECT sql_saga.drop_foreign_key(table_oid => 'establishment', key_name => 'establishment_legal_unit_id_valid');

SELECT sql_saga.drop_unique_key(table_oid => 'establishment', key_name => 'establishment_id_valid');
SELECT sql_saga.drop_unique_key(table_oid => 'establishment', key_name => 'establishment_name_valid');
SELECT sql_saga.drop_era('establishment');


SELECT sql_saga.drop_unique_key(table_oid => 'legal_unit', key_name => 'legal_unit_id_valid');
SELECT sql_saga.drop_unique_key(table_oid => 'legal_unit', key_name => 'legal_unit_name_valid');
SELECT sql_saga.drop_unique_key(table_oid => 'legal_unit', key_name => 'legal_unit_legal_ident_valid');
SELECT sql_saga.drop_era('legal_unit');
```

## Development
Run all regression tests with
```
make install && make test
```

To run a single test file:
```
make test TESTS=22_covers_without_gaps_test
```

To run a subset of tests:
```
make test TESTS="22_covers_without_gaps_test 23_create_temporal_foreign_key_test"
```

To quickly review and fix any diffs you can use
```
make vimdiff-fail-all
```

## API Reference

### Era Management
- `add_era(table_oid regclass, valid_from_column_name name, valid_until_column_name name, era_name name DEFAULT 'valid', range_type regtype DEFAULT NULL, bounds_check_constraint name DEFAULT NULL) RETURNS boolean`
- `drop_era(table_oid regclass, era_name name DEFAULT 'valid', drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT true) RETURNS boolean`

### Unique Keys
- `add_unique_key(table_oid regclass, column_names name[], era_name name DEFAULT 'valid', unique_key_name name DEFAULT NULL, unique_constraint name DEFAULT NULL, exclude_constraint name DEFAULT NULL) RETURNS name`
- `drop_unique_key(table_oid regclass, key_name name, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT true) RETURNS void`
- `drop_unique_key(table_oid regclass, column_names name[], era_name name, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT true) RETURNS void`

### Foreign Keys
- `add_foreign_key(fk_table_oid regclass, fk_column_names name[], fk_era_name name, unique_key_name name, match_type sql_saga.fk_match_types DEFAULT 'SIMPLE', update_action sql_saga.fk_actions DEFAULT 'NO ACTION', delete_action sql_saga.fk_actions DEFAULT 'NO ACTION', foreign_key_name name DEFAULT NULL, fk_insert_trigger name DEFAULT NULL, fk_update_trigger name DEFAULT NULL, uk_update_trigger name DEFAULT NULL, uk_delete_trigger name DEFAULT NULL) RETURNS name`
- `drop_foreign_key(table_oid regclass, key_name name) RETURNS boolean`
- `drop_foreign_key(table_oid regclass, column_names name[], era_name name, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT') RETURNS void`

### Updatable Views (for PostgREST)
- `add_api(table_oid regclass DEFAULT NULL, era_name name DEFAULT 'valid') RETURNS boolean`
- `drop_api(table_oid regclass, era_name name, drop_behavior sql_saga.drop_behavior DEFAULT 'RESTRICT', cleanup boolean DEFAULT false) RETURNS boolean`

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
