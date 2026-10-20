# Upgrading sql_saga to 1.2

sql_saga 1.2 supports in-place upgrades from the released 1.0 and 1.1 builds. Plan a maintenance window because the update takes locks on managed objects.

## Prerequisites

- **PostgreSQL 18.** sql_saga 1.2 supports PostgreSQL 18 only.
- **Privileges.** Run the preflight, the update and the verification as the extension owner or a superuser. `ALTER EXTENSION ... UPDATE` requires ownership of the extension, and `sql_saga.check_integrity()` is not executable by `PUBLIC`.
- **The right cluster.** Every command below addresses the production installation: `$DB`, `PGHOST`, `PGPORT` and `PGUSER` must point at it, and `pg_config` must be the one belonging to the PostgreSQL 18 server that runs it, not another local cluster.
- **A verified backup and a rehearsed restore.** Take a backup of every database that has sql_saga installed, and restore it once on a scratch server before the window (see [Dump and restore](#dump-and-restore)). Rehearse the update itself on that restored copy.
- **The previous package.** Keep the 1.0 or 1.1 package files and libraries you are replacing, so you can redeploy them (see [Failure and recovery](#failure-and-recovery)).

## Supported paths

- **1.0 to 1.2:** PostgreSQL selects the generated direct `sql_saga--1.0--1.2.sql` script. This performs the frozen 1.0-to-1.1 migration, followed by the 1.1-to-1.2 body. The build authenticates the frozen script by SHA-256 and changes exactly two lines of it: the native guard is retargeted to 1.2, and the regular foreign key helper validation reads one row per helper and key (see "Shared regular foreign key helpers" below). The build fails if either change does not apply to exactly one line or touches any other line.
- **1.1 to 1.2:** PostgreSQL uses `sql_saga--1.1--1.2.sql` directly.

Do not force an intermediate 1.0-to-1.1 hop while the 1.2 native library is loaded. The immutable 1.0-to-1.1 script correctly requires the 1.1 native library.

### Shared regular foreign key helpers

Regular foreign keys that reference the same unique key share one generated helper function. The released 1.0-to-1.1 script checks each helper, but builds the expected helper from the key columns once per foreign key. With two or more regular foreign keys on one key it therefore refuses a healthy, canonical helper (`is not the exact released 1.0 or canonical 1.1 regular foreign key helper`). Both 1.2 paths, 1.0 to 1.2 and 1.1 to 1.2, check one row per helper and key, and accept such databases.

The released standalone `sql_saga--1.0--1.1.sql` is frozen and keeps the defect. Update 1.0 directly to 1.2. If you must stop at 1.1, first drop all but one regular foreign key per referenced key with `sql_saga.drop_foreign_key`, run `ALTER EXTENSION sql_saga UPDATE TO '1.1'`, and then re-add them with `sql_saga.add_regular_foreign_key`.

1.2 also lets regular foreign keys share a helper whose schema or name needs quoting (for example a referenced table in a schema with a space). Before 1.2, adding the second such foreign key failed with `function ... already exists with same argument types`.

## Before the maintenance window

1. Deploy the complete 1.2 package. `make install` places the SQL files, flattened to their file names, in `$(pg_config --sharedir)/extension/`, which is where PostgreSQL looks for them (the source tree's `versions/` directory is not a runtime location):
   - `sql_saga.control`
   - `sql_saga--1.2.sql`
   - `sql_saga--1.0--1.2.sql`
   - `sql_saga--1.0.sql`
   - `sql_saga--1.0--1.1.sql`
   - `sql_saga--1.1.sql`
   - `sql_saga--1.1--1.2.sql`
   - `sql_saga_upgrade_preflight.sql`

   and the libraries in `$(pg_config --pkglibdir)`: `$libdir/sql_saga` and `$libdir/sql_saga_native`.
2. Restart PostgreSQL after replacing either library. The loaded native library must be the 1.2 build.
3. Run the read-only preflight in every database:

```bash
psql -X -v ON_ERROR_STOP=1 -d "$DB" -f "$(pg_config --sharedir)/extension/sql_saga_upgrade_preflight.sql"
```

The preflight is a quick first look, not a guarantee. To find out exactly whether the update will pass, run the update itself and roll it back. It runs every check the real update runs, including the final integrity gate, and leaves the database unchanged:

```bash
psql -X -v ON_ERROR_STOP=1 -d "$DB" -c "BEGIN; ALTER EXTENSION sql_saga UPDATE TO '1.2'; ROLLBACK;"
```

If it prints `ROLLBACK`, the real update will pass as long as nothing changes in between. If it fails, the error names the object and the repair. The dry run takes the same locks as the update, so run it outside busy hours.

Preflight accepts only the released 1.0 or 1.1 installation shapes. It reports derived grants that the 1.0 migration normalizes and refuses policies created directly on managed history tables. Move such policies to the base table, where sql_saga can synchronize them safely.

Before upgrading from 1.1, every cataloged regular-to-temporal foreign-key helper must still be the exact canonical helper generated by released 1.1. If validation refuses a helper, restore the released 1.1 definition or recreate the foreign key with sql_saga.

### Changed privileges on `add_system_versioning`

1.2 adds a trailing `history_table_must_exist boolean DEFAULT false` parameter to `sql_saga.add_system_versioning`, so the update replaces the 1.1 function. Every 1.1 call form still works. The replacement gets the privileges that a fresh 1.2 installation gets under the database's current default privileges. Privileges changed on the 1.1 function (a `REVOKE ... FROM PUBLIC`, a `GRANT` to a role, or default privileges changed since 1.1 was installed) cannot be carried over: granted inside the update script they would become extension initial privileges, which `pg_dump` omits, so a later restore would silently undo them. The update therefore refuses, and the refusal rolls back the whole update, leaving the database exactly as before. Its hint gives the exact statements:

```text
ERROR:  sql_saga: the privileges on sql_saga.add_system_versioning(regclass, name, name, name, name, name, name) differ from those its 1.2 replacement gets; the update replaces this function, and cannot carry changed privileges over
HINT:  Reset them, update, and apply them to the new signature after the update. Run: GRANT EXECUTE ON FUNCTION sql_saga.add_system_versioning(regclass, name, name, name, name, name, name) TO PUBLIC; ALTER EXTENSION sql_saga UPDATE TO '1.2'; REVOKE ALL ON FUNCTION sql_saga.add_system_versioning(regclass, name, name, name, name, name, name, boolean) FROM PUBLIC CASCADE;
```

The statements reset the whole privilege set, not only what differs: they revoke every privilege on the 1.1 function from every grantee other than the owner with `CASCADE` (which also removes grants made through a grant option), grant exactly what the replacement will get, update, and then do the same on the new signature with the original set, grant options included. Run them in one explicit transaction, so that a failure anywhere rolls back the resetting grant too and the function is never more accessible than intended. With the hint above that is:

```sql
BEGIN;
GRANT EXECUTE ON FUNCTION sql_saga.add_system_versioning(regclass, name, name, name, name, name, name) TO PUBLIC;
ALTER EXTENSION sql_saga UPDATE TO '1.2';
REVOKE ALL ON FUNCTION sql_saga.add_system_versioning(regclass, name, name, name, name, name, name, boolean) FROM PUBLIC CASCADE;
COMMIT;
```

Use the statements from your own hint, which may differ. Do not run them in autocommit mode: if the update then fails, the resetting `GRANT` stays committed and the function is left more accessible than before. Privileges applied after the update are ordinary privileges, and `pg_dump` repeats them.

To read the privileges the replacement will get under the current default privileges (global and schema-specific alike), the update briefly creates a function named `sql_saga.__sql_saga_1_2_upgrade_acl_probe()` and drops it again before anything else. It exists only inside the update's transaction, as a member of the extension like everything the update creates, and never survives it: a refusal rolls it back with everything else. If a function of that name already exists, the update refuses and asks you to rename it.

## Upgrade

Stop application writers, confirm the starting version is 1.0 or 1.1, then update directly to 1.2:

```sql
SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'sql_saga';
SET lock_timeout = '30s';
ALTER EXTENSION sql_saga UPDATE TO '1.2';
```

PostgreSQL chooses the shortest available path. You can inspect the selected paths before the window:

```sql
SELECT source, target, path
FROM pg_catalog.pg_extension_update_paths('sql_saga')
WHERE target = '1.2'
  AND source IN ('1.0', '1.1')
ORDER BY source;
```

Expected paths are `1.0--1.2` and `1.1--1.2`.

The update is atomic: it runs in one transaction, and any refusal or error rolls back everything, leaving the previous version installed exactly as it was. It validates generated helpers and the loaded 1.2 native library before mutation and refuses unmanaged policies on managed history tables. It also takes a transaction-scoped advisory lock, in the current database only, keyed to the update path. That lock only keeps two concurrent updates of the same database from interleaving. It does not serialize updates across databases, and it does not coordinate application writers, which is why they must be stopped. Never bypass any of this with `CASCADE`, disabled event triggers, or a changed `session_replication_role`.

### What the update keeps

From 1.1, the update keeps every managed family as it is: the four generated query functions of each system-versioned table, their views, policies and privileges are not regenerated. It adds the column snapshot that 1.2 tracks for each system-versioned table and fills it from the table's current columns.

From 1.0, the embedded 1.0-to-1.1 migration deliberately reconciles older generated objects: it rebuilds history views and query functions, synchronizes history policies and normalizes direct grants on derived objects, exactly as an update to 1.1 did. Expect those objects and their privileges to change. The result matches a fresh 1.2 installation.

### Final integrity gate and damaged families

The last statement of the update runs the complete `sql_saga.check_integrity()`, after every definition and privilege change. If any managed family has been damaged since it was created (for example a dropped synchronization trigger, a hand-altered generated query function, or a missing policy copy on a history table), the update fails with the checker's exact message, which names the object and the repair, and the whole update rolls back. For example:

```text
ERROR:  sql_saga: integrity violation for system-versioned table "public.sv": generated query function role "as_of" definition is noncanonical; repair with rebuild_system_versioning_views
```

The database stays at its previous version, unchanged. Repair the named object with the function the message names (such as `rebuild_system_versioning_views`, `sync_history_policies`, or dropping and re-adding the era, key or foreign key), then run `ALTER EXTENSION sql_saga UPDATE TO '1.2'` again. Repeat until it succeeds. To find damage before the window, run the rehearsal update on a restored copy. Never work around the gate.

On the 1.0 path, damage that the embedded 1.0-to-1.1 migration rebuilds (generated query functions, views, history policies) is reconciled rather than refused. Damage it does not rebuild, such as a dropped era synchronization trigger, is refused.

## Verify

Run as the extension owner or a superuser:

```sql
SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'sql_saga';
SELECT sql_saga.native_library_version();
SELECT count(*) FROM sql_saga.installation;
SELECT to_regclass('sql_saga.temporal_merge_cache');
SELECT sql_saga.check_integrity();
```

Expected results are `1.2`, `1.2`, `1`, `NULL`, and a successful `check_integrity()` call, which returns nothing (`void`). If `check_integrity()` raises an error, do not restart the application: repair the object the message names with the function it names, and run the check again.

The update must leave no extension initial privileges on detached managed objects (sql_saga-generated objects that are not extension members). This query counts them for sql_saga's managed objects only, so the initial privileges of other extensions in the same database do not affect it:

```sql
WITH managed (classoid, objoid) AS (
    SELECT 'pg_catalog.pg_class'::regclass::oid, x.oid::oid
    FROM sql_saga.system_versioning AS sv
    CROSS JOIN LATERAL unnest(ARRAY[
        to_regclass(format('%I.%I', sv.history_schema_name, sv.history_table_name)),
        to_regclass(format('%I.%I', sv.history_schema_name, sv.history_table_name || '_gated')),
        to_regclass(format('%I.%I', sv.view_schema_name, sv.view_table_name))]) AS x(oid)
    UNION
    SELECT 'pg_catalog.pg_class'::regclass::oid, to_regclass(format('%I.%I', uv.view_schema, uv.view_name))::oid
    FROM sql_saga.updatable_view AS uv
    UNION
    SELECT 'pg_catalog.pg_proc'::regclass::oid, x.oid::oid
    FROM sql_saga.system_versioning AS sv
    CROSS JOIN LATERAL unnest(ARRAY[
        to_regprocedure(sv.func_as_of), to_regprocedure(sv.func_between),
        to_regprocedure(sv.func_between_symmetric), to_regprocedure(sv.func_from_to)]) AS x(oid)
    UNION
    SELECT 'pg_catalog.pg_proc'::regclass::oid, to_regprocedure(fk.fk_helper_function)::oid
    FROM sql_saga.foreign_keys AS fk
    WHERE fk.type = 'regular_to_temporal'
)
SELECT count(*)
FROM pg_catalog.pg_init_privs AS i
JOIN managed AS m ON (m.classoid, m.objoid) = (i.classoid, i.objoid)
WHERE i.privtype = 'e'
  AND NOT EXISTS (
      SELECT
      FROM pg_catalog.pg_depend AS d
      JOIN pg_catalog.pg_extension AS e ON e.oid = d.refobjid
      WHERE e.extname = 'sql_saga'
        AND (d.classid, d.objid, d.deptype) = (i.classoid, i.objoid, 'e'::"char"));
```

The expected result is `0`. The query can only see objects that sql_saga still catalogs. An initial-privilege row whose object belongs to no extension at all carries no record of which extension it came from, so no query can attribute it to sql_saga. The initial privileges of the extension's own members are legitimate and are not counted.

Then run application smoke tests as every relevant role, especially RLS tenant reads through `*_with_history` and the temporal query functions.

## Failure and recovery

- **The update fails.** Nothing changed: the transaction rolled back and the database is still at 1.0 or 1.1. Fix the reported cause and retry.
- **The update succeeded but verification or the application fails.** The new version is committed. There is no downgrade script from 1.2. Redeploying the previous package files and libraries does not turn the database back into 1.0 or 1.1, and an extension at version 1.2 needs the 1.2 libraries. To go back, restore the backup taken before the window, with the previous package deployed.

## Dump and restore

`pg_dump` does **not** record the extension version: the archive says `CREATE EXTENSION IF NOT EXISTS sql_saga`,
which installs the destination's *default* version, and then loads the old catalog rows into it without
running any update script. With the current package installed, a plain `pg_restore` of an older dump either
fails (a database with system versioning: `null value in column "table_columns_snapshot"`) or produces a
database whose objects are still in the old shape (`check_integrity()` refuses it).

A dump of the current version restores as usual. To restore a dump made by an older sql_saga (1.0 or 1.1), install
that version first: the current package ships every released install script, and they load with the
current libraries. `IF NOT EXISTS` then keeps it, the dump restores into its own version, and
`ALTER EXTENSION sql_saga UPDATE` runs the normal upgrade. Set `OLD_VERSION` to the version of the database that
was dumped (`SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'` on the source; keep it with your
backups, since it cannot be read reliably from the archive):

<!-- BEGIN old-dump-restore -->
```bash
OLD_VERSION=1.0
pg_restore -f restore.sql backup.dump
psql -X -v ON_ERROR_STOP=1 --single-transaction -d mydb -c "CREATE EXTENSION IF NOT EXISTS btree_gist" -c "CREATE EXTENSION sql_saga VERSION '$OLD_VERSION'" -f restore.sql -c "ALTER EXTENSION sql_saga UPDATE" -c "DO \$\$ BEGIN PERFORM sql_saga.check_integrity(); END \$\$"
```
<!-- END old-dump-restore -->

The whole restore, the update and the integrity check are one transaction: if any step fails, `mydb` is left
exactly as it was (empty). `mydb` must be a new, empty database. Equivalent with `pg_restore` itself, for
example in a split restore: run `CREATE EXTENSION sql_saga VERSION '<old>' CASCADE` in the new database
before the first `pg_restore` invocation, and `ALTER EXTENSION sql_saga UPDATE` plus
`SELECT sql_saga.check_integrity()` after the last one (that is not atomic across invocations). Do not use
`pg_restore --clean` for an old dump: it drops the extension you installed and recreates the default version.

If you install the wrong old version, the procedure usually fails and rolls back (a 1.1 dump with system
versioning or managed views restored as 1.0; a 1.0 dump with regular foreign keys or system versioning restored
as 1.1). It does not fail for a 1.0 dump with `for_portion_of`/`current` views restored as 1.1: the views
then miss the grants the 1.0 to 1.1 step gives them. Use the version of the source.

Restores mint a new installation token on the destination.

## Historical 1.0 to 1.1 notes

The 1.0-to-1.1 migration removed the persistent `temporal_merge_cache_*` API, normalized direct grants copied onto derived objects, introduced the installation token and gated history views, and reconciled managed families. Those operations remain embedded unchanged in the generated direct 1.0-to-1.2 path. New deployments should install 1.2, and existing 1.0 installations should update directly to 1.2 rather than stopping at 1.1.
