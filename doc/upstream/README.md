# Upstream: temporal SQL in PostgreSQL and the SQL standard

Studies for hug epic 0t4v31. Research and drafts only: nothing here has been sent to the PostgreSQL community.

| Document | Content |
|---|---|
| [for-portion-of-revert.md](for-portion-of-revert.md) | Why `UPDATE/DELETE FOR PORTION OF` was reverted from PostgreSQL 19, and the same races run against sql_saga |
| [temporal-model-map.md](temporal-model-map.md) | What PostgreSQL has and is working on, against sql_saga's three areas, and which proposal to make first |
| [temporal-dml.md](temporal-dml.md) | Principles, design space and syntax for temporal INSERT, UPDATE, DELETE and MERGE, with every example run against sql_saga |
| [syntax-candidates.md](syntax-candidates.md) | Three candidate SQL syntaxes for every temporal_merge case and for regular-to-temporal foreign keys, compared |
| [concurrency/](concurrency/) | The isolation tester specs behind the revert study: `gen.py` writes them, `run.sh DBNAME` runs them |
| [repro/](repro/) | Reproducers of sql_saga bugs found while mapping the modes (hug tasks rtwrp6 and 2hxkwx, fixed for 1.15; test 163; y7hbe9, open), and the examples of temporal-dml.md (`temporal_dml_examples.sql`, `.out`) |
| [pg-dump-config-fk-order/](pg-dump-config-fk-order/) | The approved, not yet sent pgsql-bugs report on pg_dump losing the load order of extension configuration tables, with its reproducer (hug hsa1hp) |
