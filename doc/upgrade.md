# Upgrading sql_saga

This guide describes the normal PostgreSQL extension upgrade workflow. The current
package is **1.11**, supporting a fresh install and direct upgrades from released
**1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9 and 1.10** on PostgreSQL 18.

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
  but when using the current package update **directly to 1.11** instead.
  Orphan rows, changed routine ACLs, mispaired XOR keys and noncanonical generated
  objects can intentionally refuse an upgrade. Never bypass an integrity gate.
- A `for_portion_of` view that already has a user trigger named like its 1.6 apply
  trigger (`<row trigger>_apply`, for example `for_portion_of_valid_apply`) refuses
  the upgrade. So does a `current` view with a user trigger named like its 1.7 apply
  trigger (for example `current_valid_apply`). `sql_saga_upgrade_preflight.sql`
  lists them; rename them first.
- A row of `sql_saga.era` with `audit_schema_name` or `audit_table_name` set refuses
  the upgrade. No release writes or reads these columns, so a value was written by
  hand; set both to NULL. A history table of a system-versioned table that is not
  persistent (`UNLOGGED`, only possible with event triggers bypassed) also refuses
  it; run `ALTER TABLE <history table> SET LOGGED`. The preflight lists both.
- The 1.9 update replaces `sql_saga.add_era` (and its private worker) with a new
  signature. If their `EXECUTE` privileges differ from what a new function gets in
  that database (for example a `REVOKE ... FROM PUBLIC` on `add_era`, or a grant on
  the worker), the update refuses and prints the statements that reset them before
  and reapply them to the new signature after. The preflight lists them.

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
- `sql_saga--1.11.sql`, the fresh installation.
- `sql_saga--<source>--1.11.sql`, one direct script for each supported source.
- Every frozen released installation and upgrade artifact in `versions/`.
- `sql_saga_upgrade_preflight.sql`.
- `$libdir/sql_saga` and `$libdir/sql_saga_native` for the same release.

Restart PostgreSQL after replacing either library. The native library version is
embedded from `sql_saga.control` at build time. The direct upgrades require
`sql_saga.native_library_version()` to return `1.11`. Do not run an older upgrade
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
WHERE target = '1.11' AND source IN ('1.0', '1.1', '1.2', '1.3', '1.4', '1.5', '1.6', '1.7', '1.8', '1.9', '1.10')
ORDER BY source;
ALTER EXTENSION sql_saga UPDATE;
SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'sql_saga';
SELECT sql_saga.native_library_version();
SELECT sql_saga.check_integrity();
```

Expected paths are `<source>--1.11`, never an intermediate hop. Both version
queries must report `1.11` and integrity must complete without error. Exercise
application reads and writes before reopening writers. Repeat for every database
that contains the extension.

## What changes in 1.11

`temporal_merge` writes each data value with its column's type, identically under the
native planner (the default) and the PL/pgSQL planner
(`sql_saga.temporal_merge.use_plpgsql_planner`). Every release before 1.11:

- With the native planner, rounded `numeric` values through a 64-bit float, so
  `12345678901234567890.123456789012345678900` was stored as `12345678901234567000`,
  and dropped trailing zeros (`1.10` became `1.1`). 1.11 keeps the exact value and scale.
- With the native planner, stored a `json`/`jsonb` value of a new entity's second and
  later pieces as a JSON string (`"{\"k\": 1}"` instead of `{"k": 1}`).
- With the PL/pgSQL planner, failed on any array or composite column
  (`malformed array literal`) and on a `jsonb` column holding a JSON string.

Rows written before are not rewritten: a rounded number cannot be recovered, and a
`jsonb` string cannot be told apart from one written on purpose. Check numeric columns
loaded with `temporal_merge` against their source if exact values matter.

The update replaces `sql_saga.temporal_merge_execute` with `CREATE OR REPLACE`, retaining
its OID, owner and site-specific privileges, and needs the 1.11 native library (the
numeric and jsonb fixes are in it). The catalog is unchanged (catalog format **1.9**),
so a 1.9 or 1.10 dump restores plainly. See [the 1.10 guide](upgrade-1.10.md).

Sources before 1.10 also receive the 1.10 provenance corrections (below).

## What changes in 1.10

1.10 corrects the provenance trigger of 1.9 (doc/design/provenance.md):

- A provenance value keeps its column's type. 1.9 evaluated each `DEFAULT` as text, so a
  `jsonb` provenance column stored a JSON string (`"{\"by\": ...}"`) instead of an object.
  Rows written by 1.9 are not rewritten; repair them, if any, with the trigger disabled
  in one transaction, as for a backfill.
- A provenance column whose `DEFAULT` was dropped after registration refuses writes,
  naming the repair, where 1.9 stored NULL (or kept a supplied value).
  `check_integrity()` reports such a column.
- A generated column is refused as a provenance column; 1.9 accepted it and then failed
  every write.

The update replaces `sql_saga.provenance_trigger()`, the shared provenance validation and
`sql_saga.check_integrity()` with `CREATE OR REPLACE`, retaining their OIDs, owners and
site-specific privileges. The catalog is unchanged (catalog format **1.9**), so a 1.9 dump
restores plainly into 1.10. See [the 1.9 guide](upgrade-1.9.md).

Sources before 1.9 also receive provenance columns and the 1.9 replacement of `add_era`
(below).

## What changes in 1.9

Eras can have **provenance columns** (doc/design/provenance.md): ephemeral columns,
such as `edit_by_user_id` and `edit_at`, that sql_saga fills from their DEFAULT,
evaluated as the writing role, on every changed or new piece of a timeline, and keeps
on untouched pieces. Pieces with different provenance are not merged. Name them with
the new trailing argument `provenance_columns` of `add_era`, or later with the new
`sql_saga.set_era_provenance_columns(table, era, columns)`; each creates the managed
row trigger `<era>_provenance` on the table, running the new
`sql_saga.provenance_trigger()`, executable by `PUBLIC`.

The catalog table `sql_saga.era` gains the column `provenance_columns` (`name[]`,
NULL on every existing era, so nothing changes until you set it). The catalog format
becomes **1.9**: the update sets `catalog_format` to `1.9` on every era row and
replaces the named check `"restore old sql_saga dumps per doc/upgrade.md"`
accordingly. A plain restore of a 1.8 dump into 1.9 therefore fails at the first era
row; restore it at its own version as described below.

`sql_saga.add_era` and its private worker `sql_saga.__api_add_era` are dropped and
created with the new signature, so their OIDs change. A call that names its arguments
or passes the first thirteen positionally works unchanged. Their `EXECUTE`
privileges are compared with those a new function gets; any difference refuses the
update (see "Before the maintenance window"). The other changed routines are
replaced with `CREATE OR REPLACE`, retaining their OIDs, owners and site-specific
privileges. No user table is rebuilt and no user data is rewritten. See
[the 1.8 guide](upgrade-1.8.md).

Sources before 1.8 also receive the 1.8 catalog change (below).

## What changes in 1.8

The catalog table `sql_saga.era` loses the columns `audit_schema_name` and
`audit_table_name`. They came from the periods extension, whose audit tables sql_saga
replaced with its own system versioning (`sql_saga.system_versioning` and the history
tables), and no release ever wrote them. The update drops them only when every row is
NULL, and otherwise refuses without changing anything.

The catalog format becomes **1.8**: the update sets `catalog_format` to `1.8` on every
era row and replaces the named check `"restore old sql_saga dumps per doc/upgrade.md"`
accordingly. A plain restore of a 1.4 to 1.7 dump into 1.8 therefore fails at the first
era row; restore it at its own version as described below.

A history table of a system-versioned table must stay persistent: `ALTER TABLE ... SET
UNLOGGED` on it is refused with the hint `Run: ALTER TABLE <history table> SET LOGGED;`.
Earlier releases carried a check for this that read the never-populated audit columns
and so never fired. If event triggers were bypassed, `check_integrity()` reports an
unlogged history table with the same repair.

`temporal_merge` builds the SQL for multi-column keys with the columns in their
declared order. Before, the order could follow the plan PostgreSQL chose for a catalog
lookup, so the generated SQL (and its cached plan) could differ between servers.
Results were the same either way.

The changed routines are replaced with `CREATE OR REPLACE`, retaining their OIDs,
owners and site-specific privileges. No routine is added or removed, no user table is
rebuilt and no user data is rewritten. See [the 1.7 guide](upgrade-1.7.md).

Sources before 1.7 also receive the 1.7 batching of `current` views (below).

## What changes in 1.7

An `INSERT` through a `current` view is applied as one batch: the row trigger only
queues each new row, and a new statement-level trigger on the view applies the queue
with one `temporal_merge`. The update replaces the changed routines with
`CREATE OR REPLACE`, retaining their OIDs, owners and site-specific privileges, and
creates the new `sql_saga.current_view_apply_trigger()`, executable by `PUBLIC` like
the row trigger function. It then adds the apply trigger, `AFTER INSERT FOR EACH
STATEMENT`, to every existing `current` view, named after the view's row trigger with
the suffix `_apply`. Views keep their OID, grants, comments and other triggers. No
user table is rebuilt and no data is rewritten. See [the 1.6 guide](upgrade-1.6.md).

Sources before 1.6 also receive the 1.6 batching of `for_portion_of` views (below).

## What changes in 1.6

A statement through a `for_portion_of` view is applied as one batch: the row
trigger only queues each matched row, and a new statement-level trigger on the view
applies the queue with one `temporal_merge`. The update replaces the changed
routines with `CREATE OR REPLACE`, retaining their OIDs, owners and site-specific
privileges, and creates the new `sql_saga.for_portion_of_apply_trigger()`, executable
by `PUBLIC` like the row trigger function. It then adds the apply trigger,
`AFTER UPDATE FOR EACH STATEMENT`, to every existing `for_portion_of` view, named after
the view's row trigger with the suffix `_apply`. Views keep their OID, grants,
comments and other triggers. No user table is rebuilt and no data is rewritten.

Sources before 1.5 also receive the 1.5 deletion protocol (below).

## What changes in 1.5

The `for_portion_of` trigger supports the new deletion protocol. The update replaces
only `sql_saga.for_portion_of_trigger()` using `CREATE OR REPLACE`, retaining its
OID, dependent view triggers, owner and site-specific privileges. It does not
rebuild user tables or rewrite their data. Existing managed views use the updated
trigger immediately. See [the 1.5 guide](upgrade-1.5.md).

From 1.4 to 1.7 the catalog format was **1.4**; 1.8 changed it to **1.8** and 1.9 to
**1.9**, which 1.10 and 1.11 keep (above). A
catalog-format marker names the compatible layout, not the current extension or
native-library version. Do not change it by hand or add a default to bypass old-dump
safeguards.

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

Released files are immutable. The 1.10 assets were reproduced from immutable tag
`v1.10`, commit `39da88a`, using only that tag's ordered SQL sources and its own
update generator, not development source or shelved prototypes. Two independent
builds gave byte-identical files.

`devops/generate_update_scripts_1_11.py` authenticates those assets by SHA-256,
retargets native guards, actionable hints and locks in embedded older migrations,
and appends the same 1.11 tail to every path: the 1.11 routine delta and
`devops/upgrade_1_11_repairs.sql`. Its specification tests check that every guard and
lock names exactly `1.11` (not `1.1` or `1.10`).
Each script has exactly one final integrity gate. Unmanifested SQL changes, including
a catalog table change outside the declared tables, fail generation rather than
silently disappearing from upgrades. The specification tests of the released 1.4 to
1.10 generators run against their own tagged source trees.

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

### Cutting the next release (maintainers)

The release data lives in `devops/generate_update_scripts_<X_Y>.py`; `devops/direct_update.py` builds the
scripts. For a release that changes routines (as 1.6 to 1.11 did), with N the new and P the previous version:

1. Freeze P: build `sql_saga--P.sql` and every `sql_saga--<source>--P.sql` twice from `git archive vP`, check that
   both builds are byte-identical, copy them into `versions/` and record their SHA-256.
2. Add `devops/generate_update_scripts_N.py` (a `Release(...)` with those hashes, the routines changed since P,
   new routines and their privilege statements) and `devops/upgrade_N_repairs.sql` for data repairs that need the
   new definitions. The generator refuses any source change outside the manifest. A release that also changes a
   catalog table (as 1.8 did) declares it in `changed_tables` and supplies `devops/upgrade_N_schema.sql`, placed
   after the routines (its DDL fires the event triggers, which must already run the new definitions); if the layout
   changes, bump `catalog_format` there and in the source. A routine whose signature changes (as `add_era` in 1.9)
   goes in `replaced_routines`, and a new or changed comment in `commented_routines`. Add its specification
   test, and point the P generator's test at `released_tree.released_src('vP')`.
3. Set `default_version` in `sql_saga.control` and `expected/001_install.out`, and replace the version rule in the
   Makefile (only the current version's rule is kept; older releases build from their own tags).
4. Extend `sql_saga_upgrade_preflight.sql` for the P release shape, and `devops/upgrade_test.sh` with the vP
   baseline, the frozen P artifacts, `catalog_format_of` for N, and a gate for what N fixes. A gate must fail on
   the vP binaries (run it once there), and must not name N or its catalog format literally: sections written for
   one release run again for every later one. Do not edit `devops/upgrade_test.sh` while it runs (bash reads the
   file as it goes); rerun instead. A native library change takes effect only after `make install` and a
   PostgreSQL restart: check `sql_saga.native_library_version()` before trusting a local result.
5. Freeze this guide as `doc/upgrade-P.md` and describe N here.
6. Branch CI, fast-forward master, master CI on the exact head (`gh workflow run` if the head is `[skip ci]`), then
   the annotated tag `vN` ("sql_saga N") on that commit.
