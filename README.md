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
A table has a `valid_from` and `valid_to` date, such that information
as it evolves over time can be stored.
The currently valid row has `infinite` in the `valid_to` column.

### Temporal Table with Valid Time

Example table:
```
TABLE establishment (
    id
    valid_from date,
    valid_to date,
    name,
)
```
Example data
```
------+-------------+--------------+------------------------------------
id    | valid_from  |   valid_to   |  name
------+-------------+--------------+------------------------------------
01    | 2023-01-01  |   2023-06-30 |  AutoParts LLC
01    | 2023-07-01  |   2023-12-31 |  AutoSpareParts INC
01    | 2024-01-01  |   infinity   |  SpareParts Corporation
02    | 2022-01-01  |   2022-06-30 |  Gasoline Refinement LLC
02    | 2022-07-01  |   2022-12-31 |  Gasoline and Diesel Refinement LLC
02    | 2023-01-01  |   infinity   |  General Refinement LLC
------+-------------+--------------+------------------------------------
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
using `valid_from` and `valid_to`, in addition to having
a regular foreign key to `stat_definition_id`, and a temporal
foreign key to `establishment.id`.

```
TABLE stat_for_unit (
    id
    stat_definition_id,
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
 employees | 2024-01-01 | infinty    |  01    |        130
 turnover  | 2023-01-01 | 2023-12-31 |  01    | 10 000 000
 turnover  | 2024-01-01 | infinty    |  01    | 30 000 000
 employees | 2022-01-01 | 2022-12-31 |  02    |         20
 employees | 2023-01-01 | infinty    |  02    |         80
 turnover  | 2022-01-01 | 2022-12-31 |  02    | 40 000 000
 turnover  | 2023-01-01 | infinty    |  02    | 70 000 000
-----------+------------+------------+--------+-----------
```

The purpose of this extension is to make sure that for foreign keys
between temporal tables, the linked table, in this case `establishment`,
must have the linked foreign key available for the entire period between
`valid_from` and `valid_to` of the `stat_for_unit` table.

Notice that there can be multiple matching rows, the `valid_from` and `valid_to`
do not need to align between the tables.

So this line from `stat_for_unit`
```
turnover  | 2022-01-01 | 2022-12-31 |  02    | 40 000 000
```
matches these lines in `establishment`
```
02    | 2022-01-01  |   2022-06-30 |  Gasoline Refinement LLC
02    | 2022-07-01  |   2022-12-31 |  Gasoline and Diesel Refinement LLC
```

## Installation

TODO: Build a docker image with postgres and the sql_saga extension.

TODO: Build an Ubuntu packate with sql_saga.

`CREATE EXTENSION sql_saga;`

## Usage

Detailed examples and explanations on how to use the `sql_saga` system.

### Activate

```
CREATE TABLE legal_unit_era (
  id SERIAL NOT NULL,
  legal_ident VARCHAR NOT NULL,
  name VARCHAR NOT NULL,
  valid_from TIMESTAMPTZ,
  valid_to TIMESTAMPTZ,
  PRIMARY KEY (id, valid_from, valid_to)
);

sql_saga.add_era('legal_unit_era','valid_from','valid_to');
sql_saga.add_unique_key('legal_unit_era', ARRAY['id']);
sql_saga.add_unique_key('legal_unit_era', ARRAY['name']);
sql_saga.add_unique_key('legal_unit_era', ARRAY['legal_ident']);
sql_saga.add_api('legal_unit_era');


CREATE TABLE establishment_era (
  id SERIAL NOT NULL,
  name VARCHAR NOT NULL,
  address TEXT NOT NULL,
  legal_unit_id INTEGER NOT NULL,
  valid_from TIMESTAMPTZ,
  valid_to TIMESTAMPTZ,
  PRIMARY KEY (id, valid_from, valid_to)
);

sql_saga.add_era('establishment_era','valid_from','valid_to');
sql_saga.add_unique_key('establishment_era', ARRAY['id']);
sql_saga.add_unique_key('establishment_era', ARRAY['name']);
sql_saga.add_foreign_key('establishment_era', ARRAY['legal_unit_id'], 'legal_unit_era', ARRAY['id'])
sql_saga.add_api('establishment_era');

```

### Deactivate

```
sql_saga.drop_api('establishment_era');
sql_saga.drop_unique_key('establishment_era', ARRAY['id']);
sql_saga.drop_unique_key('establishment_era', ARRAY['name']);
sql_saga.drop_foreign_key('establishment_era', ARRAY['legal_unit_id'], 'legal_unit_era', ARRAY['id'])
sql_saga.drop_era('establishment_era','valid_from','valid_to');


sql_saga.drop_api('person_era');
sql_saga.drop_unique_key('legal_unit_era', ARRAY['id']);
sql_saga.drop_unique_key('legal_unit_era', ARRAY['name']);
sql_saga.drop_unique_key('legal_unit_era', ARRAY['legal_ident']);
sql_saga.drop_era('person_era','valid_from','valid_to');
```

## Development
Run regression tests with
```
make && make install && make installcheck
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
