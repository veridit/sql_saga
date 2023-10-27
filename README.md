# pg_saga

`pg_saga` is a PostgreSQL plugin tailored for National Statistical Offices (NSOs) worldwide,
enabling efficient and intuitive handling of temporal tables.

Drawing inspiration from Nordic sagas, the project aims at the seamless blending of
ancient narrative with the contemporary purpose of global statistics.

## Features

- Temporal Table Design Suggestions
- Support for foreign keys between temporal tables.
- Intuitive API for seamless integration with existing NSO systems.
- Intuitive fetching of current data.
- Compatible with PostgREST - that creates REST endpoints for the API's.
- Built upon the robust and reliable PostgreSQL database system.
- Supports change tracking and delete in accordance with NSO requirements.

## Installation

TODO: Build a docker image with postgres and the pg_saga extension.

TODO: Build an Ubuntu packate with pg_saga.

`CREATE EXTENSION pg_saga`

## Usage

Detailed examples and explanations on how to use the `pg_saga` system.

## Dependencies

- [PostgreSQL](https://www.postgresql.org/)
- [PostgREST](https://postgrest.org/)

## Honorable Mentions

`pg_saga` draws upon code and concepts from the following GitHub projects:

- [`periods`](https://github.com/xocolatl/periods/): Support for SQL:2016 in postgres with valid time (period) and known time (system time/transaction time).
- [`time_for_keys`](https://github.com/pjungwir/time_for_keys): Triggers for foreign keys with time.

We express our gratitude to the authors and contributors of these projects for their invaluable work.

## License

`pg_saga` is licensed under the MIT License. See [LICENSE](LICENSE) for more details.

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details on how to contribute and the process for submitting pull requests.

## Acknowledgements

- The PostgreSQL community for their continued support and development of an exceptional database system.
- All contributors and users of `pg_saga` who have provided feedback, suggestions, and code.

---

<p align="center">
  <img src="saga_logo.png" alt="pg_saga logo" width="200"/>
</p>

---

For more any issues or improvements, please use github.
