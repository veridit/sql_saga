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
A table has `valid_from` and `valid_to` date columns, which define an inclusive period `[valid_from, valid_to]`.

For `sql_saga` to work, an additional `valid_after` column is required. It represents the exclusive start of the period, `(valid_after, valid_to]`, which simplifies contiguity checks. When two periods are contiguous, the `valid_to` of the first period equals the `valid_after` of the second.

It is recommended to use a trigger to keep `valid_from` and `valid_after` synchronized. The generic trigger function `sql_saga.synchronize_valid_from_after()` is included with the extension to help with this synchronization.

The currently valid row has `infinity` in the `valid_to` column.

### Temporal Table with Valid Time

For human readability `valid_from` is used, while for the extension `valid_after` is required. They should be kept synchronized such that `valid_from = valid_after + interval '1 day'`. The conceptual examples below show `valid_after` in table definitions but omit it from data listings for simplicity.

Example table:
```
TABLE establishment (
    id
    valid_after date,
    valid_from date,
    valid_to date,
    name,
)
```
Example data
```
------+------------+--------------+------------------------------------
id    | valid_from |   valid_to   |  name
------+------------+--------------+------------------------------------
01    | 2023-01-01 |   2023-06-30 |  AutoParts LLC
01    | 2023-07-01 |   2023-12-31 |  AutoSpareParts INC
01    | 2024-01-01 |   infinity   |  SpareParts Corporation
02    | 2022-01-01 |   2022-06-30 |  Gasoline Refinement LLC
02    | 2022-07-01 |   2022-12-31 |  Gasoline and Diesel Refinement LLC
02    | 2023-01-01 |   infinity   |  General Refinement LLC
------+------------+--------------+------------------------------------
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
using `valid_after`, `valid_from` and `valid_to`, in addition to having
a regular foreign key to `stat_definition_id`, and a temporal
foreign key to `establishment.id`.

```
TABLE stat_for_unit (
    id
    stat_definition_id,
    valid_after,
    valid_from,
    valid_to,
    establishment_id,
    value,
)
```

Some example data to show how measurements are kept in `stat_for_unit`.
```
-----------+------------+------------+--------+-----------
 stat_def  | valid_from | valid_to   | est_id | value
-----------+------------+------------+--------+-----------
 employees | 2020-01-01 | 2023-12-31 |  01    |         90
 employees | 2024-01-01 | infinity   |  01    |        130
 turnover  | 2023-01-01 | 2023-12-31 |  01    | 10 000 000
 turnover  | 2024-01-01 | infinity   |  01    | 30 000 000
 employees | 2022-01-01 | 2022-12-31 |  02    |         20
 employees | 2023-01-01 | infinity   |  02    |         80
 turnover  | 2022-01-01 | 2022-12-31 |  02    | 40 000 000
 turnover  | 2023-01-01 | infinity   |  02    | 70 000 000
-----------+------------+------------+--------+-----------
```

The purpose of this extension is to make sure that for foreign keys
between temporal tables, the linked table, in this case `establishment`,
must have the linked foreign key available for the entire period `[valid_from, valid_to]`
of the `stat_for_unit` table.

Notice that there can be multiple matching rows, and the periods do not
need to align between the tables.

So this line from `stat_for_unit` which represents the period `[2022-01-01, 2022-12-31]`
```
turnover  | ... | 2022-01-01 | 2022-12-31 |  02    | 40 000 000
```
is covered by these two contiguous lines in `establishment` for periods `[2022-01-01, 2022-06-30]` and `[2022-07-01, 2022-12-31]`
```
02    | ... | 2022-01-01 | 2022-06-30 |  Gasoline Refinement LLC
02    | ... | 2022-07-01 | 2022-12-31 |  Gasoline and Diesel Refinement LLC
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
  valid_after TIMESTAMPTZ,
  valid_from TIMESTAMPTZ,
  valid_to TIMESTAMPTZ
  -- Note: A primary key on temporal tables is often not on the temporal columns
);

-- It is recommended to create a trigger to keep valid_from and valid_after in sync.
CREATE TRIGGER legal_unit_synchronize_validity
    BEFORE INSERT OR UPDATE ON legal_unit
    FOR EACH ROW EXECUTE FUNCTION sql_saga.synchronize_valid_from_after();

-- Register the table as a temporal table (an "era")
SELECT sql_saga.add_era('legal_unit', 'valid_after', 'valid_to');
-- Add temporal unique keys. A name is generated if the last argument is omitted.
SELECT sql_saga.add_unique_key('legal_unit', ARRAY['id'], 'legal_unit_id_valid');
SELECT sql_saga.add_unique_key('legal_unit', ARRAY['name'], 'legal_unit_name_valid');
SELECT sql_saga.add_unique_key('legal_unit', ARRAY['legal_ident'], 'legal_unit_legal_ident_valid');


CREATE TABLE establishment (
  id SERIAL NOT NULL,
  name VARCHAR NOT NULL,
  address TEXT NOT NULL,
  legal_unit_id INTEGER NOT NULL,
  valid_after TIMESTAMPTZ,
  valid_from TIMESTAMPTZ,
  valid_to TIMESTAMPTZ
);

-- It is recommended to create a trigger to keep valid_from and valid_after in sync.
CREATE TRIGGER establishment_synchronize_validity
    BEFORE INSERT OR UPDATE ON establishment
    FOR EACH ROW EXECUTE FUNCTION sql_saga.synchronize_valid_from_after();

SELECT sql_saga.add_era('establishment','valid_after','valid_to');
SELECT sql_saga.add_unique_key('establishment', ARRAY['id'], 'establishment_id_valid');
SELECT sql_saga.add_unique_key('establishment', ARRAY['name'], 'establishment_name_valid');
-- Add a temporal foreign key. It references a temporal unique key.
SELECT sql_saga.add_foreign_key('establishment', ARRAY['legal_unit_id'], 'valid', 'legal_unit_id_valid');

```

### Deactivate

```
-- Foreign keys must be dropped before the unique keys they reference.
SELECT sql_saga.drop_foreign_key('establishment', 'establishment_legal_unit_id_valid');

SELECT sql_saga.drop_unique_key('establishment', 'establishment_id_valid');
SELECT sql_saga.drop_unique_key('establishment', 'establishment_name_valid');
SELECT sql_saga.drop_era('establishment');


SELECT sql_saga.drop_unique_key('legal_unit', 'legal_unit_id_valid');
SELECT sql_saga.drop_unique_key('legal_unit', 'legal_unit_name_valid');
SELECT sql_saga.drop_unique_key('legal_unit', 'legal_unit_legal_ident_valid');
SELECT sql_saga.drop_era('legal_unit');
```

## Development
Run regression tests with
```
make install && make installcheck
```

To quickly review and fix any diffs you can use
```
make vimdiff-fail-all
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

For more any issues or improvements, please use github.
