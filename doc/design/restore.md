# Restoring dumps: `pg_restore --clean` and dumps of older releases

Status: implemented in 1.2 (`--clean`) and 1.3 (old dumps), current in 1.12. The decision record is in
`doc/decision-log.md`, "Recognize the Teardown of `pg_restore --clean` in the Drop Protection", including the 1.4
amendment. User procedures: README.md (restore sections) and `doc/upgrade.md`. Tests: 029,
`devops/restore_roundtrip.sh` (variants `clean`, `clean_single_txn`, `clean_same`, `clean_psql`,
`clean_psql_without_opt_in`, old-dump cases `old_dump_10`, `old_dump_11`, `old_dump_12`,
`old_dump_17`, `old_dump_18`, `old_dump_19`, `old_dump_110`, `old_dump_111`).

## `pg_restore --clean` into a database that already holds sql_saga families

### What `--clean` emits

`--clean` drops every archived object in the reverse of the archive's creation order before recreating it: every
dependent (triggers, policies, functions, views, constraints) in its own statement, each base table last. The
catalog rows that describe a family exist until the base table goes. Drop protection checks after each statement
that every object a catalog row names still exists, so the first DROP of every family was refused. Native temporal
foreign keys (`FOREIGN KEY ... PERIOD`) are plain constraints and were never affected.

### What does not work

- "Is the base table in this DROP?": each DROP is its own statement; the base table goes in a later one.
- Deferring the checks to commit: a serial `pg_restore` commits every statement.
- `session_replication_role = replica`: needs superuser and switches off every event trigger, history policy
  synchronization included. An operator procedure, not a fix.
- "A missing object means the family is being torn down": allows exactly the accidental drop the protection is for.
- Testing `search_path = ''`: every sql_saga event trigger function sets its own `search_path`.

### The signature

`sql_saga.__internal_is_clean_teardown()` is true only when all of these hold:

1. the recorded immediate caller is a superuser (1.4; ordinary roles cannot forge the exemption);
2. `check_function_bodies = off` and `row_security = off`, which pg_dump and pg_restore set in every session they
   open, and in a `-f` script;
3. a pg_restore session: `application_name = 'pg_restore'` (libpq's default), or the explicit opt-in
   `sql_saga.clean_restore = on` for a restore script run through `psql`;
4. the top-level statement is a DROP, or `ALTER TABLE ... DROP CONSTRAINT`, read from `current_query()`: pg_restore
   sends each archive entry as its own query.

During the teardown, drop protection skips its "object still needed" refusals, skips cleanup of objects pg_restore
drops itself, and drops the managed history policies of each touched family itself, because their names carry
this installation's token, which the dump cannot name. The health check does not resynchronize history policies
during the teardown.

### Why this does not weaken the protection

The protection is against accidental drops in psql, applications and migration tools. None of them has all four
conditions; each condition alone keeps every refusal (test 029). It is not a security boundary and does not need to
be one: whoever can run these DROPs owns the objects and could disable the triggers anyway. Only refusals are
relaxed, and only for DROP statements: every CREATE, ALTER and COPY of the same restore keeps every check.

### Hardened databases

With default privileges that revoke EXECUTE from PUBLIC, recreated as-of functions would lose their PUBLIC grant.
An AFTER INSERT trigger on `sql_saga.system_versioning` grants it when a catalog row is restored, keeping the
history access rule that these functions are executable by PUBLIC (`history-access.md`).

## Dumps of older releases

`pg_dump` does not record the extension version: the archive says `CREATE EXTENSION IF NOT EXISTS sql_saga`.
Restored into a cluster whose default is newer, an old dump's catalog rows land in a newer catalog without running
any update: a 1.0 or 1.1 dump with system versioning failed outright (a `NOT NULL` snapshot column without a
default), and one without kept 1.0 helper functions that then failed `check_integrity()`.

Owner decision (1.3): no `after_restore()` function. A dump is restored **at its own version** from the current
package, then updated and checked:

1. create the extension at the dump's recorded version in an empty database;
2. restore; the archive's `CREATE EXTENSION IF NOT EXISTS` is then a no-op;
3. `ALTER EXTENSION sql_saga UPDATE`;
4. `check_integrity()`.

With psql and `--single-transaction`, all four steps are one transaction. With pg_restore invocations, each phase is
atomic, not the whole. The version cannot be read reliably from an archive (1.0 and 1.1 catalog dumps are
byte-identical when only eras, keys and temporal foreign keys exist), so the procedure takes it as input: record
`extversion` with every backup. A wrong guess is usually refused atomically, with one known exception: a 1.0 dump
with `for_portion_of` or `current` views restored as 1.1 succeeds, and the views miss the grants the 1.0 to 1.1
step gives them. Hence: always use the source's recorded version.

Since 1.4, `sql_saga.era.catalog_format` has no default and a named check,
`"restore old sql_saga dumps per doc/upgrade.md"`. It names the release that introduced the current era layout: 1.3,
then 1.4 (1.4 to 1.7), 1.8, which dropped the unused audit columns, and 1.9, which added `provenance_columns`. A plain restore of a dump older than the
catalog layout fails at the first catalog COPY with that name instead of loading old rows into a newer catalog. (A
pre-1.8 dump restored into 1.8 fails one step earlier, at the same COPY: it names the dropped column
`audit_schema_name`.) A current dump names the
column and restores plainly. The exact commands are in `doc/upgrade.md`, "Restore at the dump's own extension
version". `pg_restore --clean` is not usable for old dumps: it drops the pre-created extension and recreates the
default version.
