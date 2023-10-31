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
  <img src="assets/sql_saga_logo.png" alt="sql_saga logo" width="200"/>
</p>

---

For more any issues or improvements, please use github.
