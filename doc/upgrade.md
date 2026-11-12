# Upgrading sql_saga

This guide describes the normal PostgreSQL extension upgrade workflow. The current
package is **1.5**, supporting a fresh install and direct upgrades from released
**1.0, 1.1, 1.2, 1.3 and 1.4** on PostgreSQL 18.

## Before the maintenance window

- Run the upgrade and integrity check as the extension owner or a superuser.
- Record `SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'` with
  every backup. PostgreSQL dumps do not reliably pin the extension version.
- Verify a backup by restoring it on a scratch server and rehearse the upgrade there.
- Keep the previous package and libraries for recovery before the upgrade commits.
- Identify the exact cluster using `PGHOST`, `PGPORT`, `PGUSER`, `$DB` and the
  server's PostgreSQL 18 `pg_config`. Do not deploy to another local cluster.
- For sources before 1.4, read the historical migration requirements in
  [the 1.4 migration guide](upgrade-1.4.md). Its historical commands name 1.4,
  but when using the current package update **directly to 1.5** instead.
  Orphan rows, changed routine ACLs, mispaired XOR keys and noncanonical generated
  objects can intentionally refuse an upgrade. Never bypass an integrity gate.

## Deploy the whole package

Stop application writers **before** replacing libraries or restarting PostgreSQL.
Keep them stopped until every database is upgraded and verified. New libraries
can otherwise execute against an old catalog between restart and upgrade.

Install the matching control file, SQL scripts, C library and internal native
library together. `make install` uses the directories reported by `pg_config`.
Packagers can stage with `make install DESTDIR=/private/staging` before deployment.
The native library is not a separate extension and must not be installed using
`cargo pgrx install`.

The package contains:

- `sql_saga.control`, with the current `default_version`.
- `sql_saga--1.5.sql`, the fresh installation.
- `sql_saga--<source>--1.5.sql`, one direct script for each supported source.
- Every frozen released installation and upgrade artifact in `versions/`.
- `sql_saga_upgrade_preflight.sql`.
- `$libdir/sql_saga` and `$libdir/sql_saga_native` for the same release.

Restart PostgreSQL after replacing either library. The native library version is
embedded from `sql_saga.control` at build time. The direct upgrades require
`sql_saga.native_library_version()` to return `1.5`. Do not run an older upgrade
script with the current native library loaded, because its guard requires its
own release's library.

## Rehearse without committing

Use a restored copy first. With writers stopped and the matching libraries loaded,
this runs the actual upgrade checks and rolls all database changes back:

```bash
psql -X -v ON_ERROR_STOP=1 -d "$DB" -c "BEGIN; ALTER EXTENSION sql_saga UPDATE; ROLLBACK;"
```

A successful rehearsal prints `ROLLBACK`. It takes the same locks as the real
upgrade and only predicts success if nothing relevant changes afterwards.
The preflight file is an additional diagnostic for older releases, not a
substitute for the real rollback-only upgrade.

## Upgrade and verify

```sql
SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'sql_saga';
SELECT source, target, path
FROM pg_catalog.pg_extension_update_paths('sql_saga')
WHERE target = '1.5' AND source IN ('1.0', '1.1', '1.2', '1.3', '1.4')
ORDER BY source;
ALTER EXTENSION sql_saga UPDATE;
SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'sql_saga';
SELECT sql_saga.native_library_version();
SELECT sql_saga.check_integrity();
```

Expected paths are `<source>--1.5`, never an intermediate hop. Both version
queries must report `1.5` and integrity must complete without error. Exercise
application reads and writes before reopening writers. Repeat for every database
that contains the extension.

## What changes in 1.5

The `for_portion_of` trigger supports the new deletion protocol. The update replaces
only `sql_saga.for_portion_of_trigger()` using `CREATE OR REPLACE`, retaining its
OID, dependent view triggers, owner and site-specific privileges. It does not
rebuild user tables or rewrite their data. Existing managed views use the updated
trigger immediately.

The catalog format remains **1.4**. A catalog-format marker names the compatible
layout, not the current extension or native-library version. Do not change it to
1.5 or add a default to bypass old-dump safeguards.

Sources before 1.4 also undergo the historical migrations documented in
[the 1.4 guide](upgrade-1.4.md), including caller-rights management, regular-FK
conversion and generated-object repairs. Their refusal and recovery requirements
remain applicable.

## Restore at the dump's own extension version

Restore a dump at the extension version recorded with the backup, then upgrade.
Do not let a default-version `CREATE EXTENSION` load old catalog rows directly
into a newer installation. Do not use `pg_restore --clean` after preinstalling the
old extension, because it would drop that extension and recreate the default.

For a plain SQL dump, with `OLD_VERSION` taken from the backup's metadata:

<!-- BEGIN old-dump-restore -->
```bash
OLD_VERSION=1.0
pg_restore -f restore.sql backup.dump
psql -X -v ON_ERROR_STOP=1 --single-transaction -d mydb -c "CREATE EXTENSION IF NOT EXISTS btree_gist" -c "CREATE EXTENSION sql_saga VERSION '$OLD_VERSION'" -f restore.sql -c "ALTER EXTENSION sql_saga UPDATE" -c "DO \$\$ BEGIN PERFORM sql_saga.check_integrity(); END \$\$"
```
<!-- END old-dump-restore -->

Use a new empty database in place of `mydb`, and set `OLD_VERSION` to the
recorded source version, not the illustrative 1.0. The entire restore and upgrade
run in one transaction.

For an archive, preinstall that same old version, restore without dropping it,
then upgrade and check integrity. Keep writers stopped throughout. See the
[historical restore details](upgrade-1.4.md#restore-at-the-dumps-own-extension-version)
for access gates and generated function grants.

## Failure and recovery

A refused upgrade rolls back atomically, leaving the old extension version.
Correct the reported condition and retry. Keep writers stopped while new
libraries are paired with the old schema. To reopen without upgrading, redeploy
the previous package and restart first.

After a successful committed upgrade there is no downgrade script. Replacing
package files does not undo catalog changes. Recovery requires restoring the
verified pre-upgrade backup with its matching package, then rehearsing again.

## Release engineering and validation

Released files are immutable. The 1.4 assets were reproduced from immutable tag
`v1.4`, commit `8759b7b`, using only that tag's ordered SQL sources and its own
update generator, not development source or shelved prototypes.

`devops/generate_update_scripts_1_5.py` authenticates those assets by SHA-256,
retargets native guards, actionable hints and locks in embedded older migrations,
and appends the same 1.5 routine delta to every path. Catalog markers and the
historical migration source setting remain unchanged. Each script has exactly
one final integrity gate. Unmanifested SQL changes fail generation rather than
silently disappearing from upgrades.

`make test-generator` runs generator unit and parallel make wiring checks without
a database. `make test-upgrade` builds immutable released tags privately and uses
uniquely owned databases to compare direct upgrades with a fresh current install,
including ACLs and generated objects. It requires a disposable or explicitly
chosen server and a matching installed or privately staged candidate package.
For a private stage, set `UPGRADE_CANDIDATE_DIR` to its `extension/` directory
and `UPGRADE_CANDIDATE_LIB_DIR` to its PostgreSQL library directory. The harness
routes each unique database to those files without changing global packages.
`UPGRADE_ONLY=artifacts` checks tag provenance and package completeness without
creating databases. `UPGRADE_ONLY=extension_scripts` checks a fresh candidate in
one uniquely owned database.
