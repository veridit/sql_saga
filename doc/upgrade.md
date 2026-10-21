# Upgrading sql_saga to 1.3

sql_saga 1.3 supports in-place upgrades from the released 1.0, 1.1 and 1.2 builds, each with one direct update script. Plan a maintenance window because the update takes locks on managed objects.

## Prerequisites

- **PostgreSQL 18.** sql_saga 1.3 supports PostgreSQL 18 only.
- **Privileges.** Run the preflight, the update and the verification as the extension owner or a superuser. `ALTER EXTENSION ... UPDATE` requires ownership of the extension, and `sql_saga.check_integrity()` is not executable by `PUBLIC`.
- **The right cluster.** Every command below addresses the production installation: `$DB`, `PGHOST`, `PGPORT` and `PGUSER` must point at it, and `pg_config` must be the one belonging to the PostgreSQL 18 server that runs it, not another local cluster.
- **A verified backup and a rehearsed restore.** Take a backup of every database that has sql_saga installed, and restore it once on a scratch server before the window (see [Dump and restore](#dump-and-restore)). Rehearse the update itself on that restored copy.
- **The previous package.** Keep the 1.0, 1.1 or 1.2 package files and libraries you are replacing, so you can redeploy them (see [Failure and recovery](#failure-and-recovery)).

## Supported paths

Each released version has one generated, direct update script to 1.3. PostgreSQL selects it; there is no intermediate hop:

- **1.0 to 1.3:** `sql_saga--1.0--1.3.sql`
- **1.1 to 1.3:** `sql_saga--1.1--1.3.sql`
- **1.2 to 1.3:** `sql_saga--1.2--1.3.sql`

The build generates the three scripts (`devops/generate_update_scripts.py`, Python 3, standard library only). Their inputs are the released artifacts `sql_saga--1.0--1.2.sql`, `sql_saga--1.1--1.2.sql` and `sql_saga--1.2.sql`, each authenticated by SHA-256, plus the routines changed since 1.2 and the 1.3 schema and repair steps. For 1.0 and 1.1, the frozen update to 1.2 is embedded with exactly these changes, and the build refuses any other difference: its native-library guard, its update hint and its final integrity gate are retargeted to 1.3, and, in the embedded 1.0 copy only, the statements that grant `EXECUTE` to `PUBLIC` on routines that already exist on 1.0 are left out (the two `GRANT`s on `for_portion_of_trigger()` and `current_view_trigger()` and the grant block for the six `temporal_merge` routines); the grants on helpers new in 1.1 remain. The released files in `versions/` are not changed. Every script records its source version first and ends with exactly one final integrity gate. The build fails on anything it does not recognize.

Do not run an older update script (to 1.1 or 1.2) while the 1.3 native library is loaded: their native-library guards require the library of their own version. Update directly to 1.3.

The package also ships the earlier update scripts to 1.1 and 1.2. They are released artifacts and are never changed; with the 1.3 libraries loaded, only the direct updates to 1.3 apply.

## What changes in 1.3

### Regular foreign keys: the referencing side becomes a constraint trigger

Up to 1.2, the referencing (child) side of a regular foreign key (`add_regular_foreign_key`, or `add_foreign_key` from a non-temporal table) is a `CHECK` constraint that calls a generated helper function (`<parent>_<columns>_exists`) reading the parent table. 1.3 replaces each such `CHECK` with a constraint trigger on the child table, under the same name, so `sql_saga.foreign_keys.fk_check_constraint` keeps naming the referencing-side object:

```sql
CREATE CONSTRAINT TRIGGER <fk_check_constraint> AFTER INSERT OR UPDATE ON <child> FROM <parent>
    DEFERRABLE INITIALLY IMMEDIATE FOR EACH ROW EXECUTE FUNCTION sql_saga.regular_fk_check_c(...)
```

`pg_dump` emits the trigger after the data of every table, so a restore no longer depends on the order in which tables are loaded. `fk_helper_function` becomes `NULL` for every converted foreign key. A helper function that no foreign key uses any more is dropped without `CASCADE`, but only when nothing else depends on it and it is not a member of an extension. Otherwise it is kept, and a `NOTICE` names it and the reason (`sql_saga: Kept function ..., no longer used by any foreign key, because ...`).

A constraint trigger only checks rows written after it exists. So, before it converts anything, the update scans every regular foreign key once, with that foreign key's `MATCH` rule. It refuses, atomically, while any child row references a key that no parent row has. Such rows can exist on 1.0 to 1.2. For example, a parent row deleted with `session_replication_role = replica` skips the parent-side check, and the CHECK of a mispaired foreign key validated the wrong columns. The refusal names the foreign key, the row count and a query that lists the rows:

```text
ERROR:  sql_saga: cannot convert the referencing side of foreign key activity_note_legal_unit_id_fkey to a constraint trigger: 2 rows of public.activity_note reference a missing key of public.legal_unit; list them with SELECT fk.* FROM public.activity_note AS fk WHERE fk.legal_unit_id IS NOT NULL AND NOT EXISTS (SELECT FROM public.legal_unit AS uk WHERE uk.id = fk.legal_unit_id)
HINT:  Delete or correct those rows on the current version, then run ALTER EXTENSION sql_saga UPDATE again.
```

Names that need quoting are quoted in both the message and the query. Run the query, decide per row what is correct (correct the reference, restore the missing parent row, or delete the child row), and update again. The update never guesses: it does not delete, move or re-point any row.

Other 1.3 behavior of regular foreign keys, which the converted triggers have too:

- The referenced side refuses a `DELETE` or key `UPDATE` only when a child row still references the key and no other parent row has it, so `temporal_merge` and direct DML may reshape a referenced parent's timeline.
- `temporal_merge` also defers these constraint triggers while it reorganizes a timeline.
- `MATCH SIMPLE` and `MATCH FULL` are enforced. `MATCH PARTIAL` is refused when the foreign key is created.

### Foreign keys created against a sorted XOR key

sql_saga 1.0, 1.1 and 1.2 could sort the catalog column list of a unique key with mutually exclusive (XOR) columns, so a foreign key created afterwards could pair its child columns with other referenced columns than intended. The objects of such a foreign key agree with each other, so the older versions saw nothing wrong. The pairing the user intended cannot be recovered from the database, so the update does not re-pair silently. It refuses, atomically, and names each mispaired foreign key with:

- a `drop_foreign_key_by_name` statement and an `add_foreign_key` statement that pairs the columns by position against the unique key's constraint order;
- for a regular foreign key, a query that lists the rows it may have let in.

The statements are a proposal. Confirm that the pairing is the one you intended before you use them, and fix the affected rows. Then:

1. On the current version, run the `drop_foreign_key_by_name` statement for every mispaired foreign key the update reports.
2. Run `ALTER EXTENSION sql_saga UPDATE TO '1.3'`.
3. On 1.3, run the `add_foreign_key` statements. They validate the existing rows and refuse while a row references a missing key.

### Managed views

1.1 and 1.2 do not follow an `ADD COLUMN` on the base table of a managed `for_portion_of` or `current` view, so such a view can lag its base table: it shows a prefix of the base table's current columns. From 1.1 and 1.2, the update repairs a view that lags in this way, and only in this way, in place. It keeps its OID, its grants, its comment and the user views that depend on it, and it stays a non-member of the extension. This in-place repair is a required acceptance criterion of the 1.3 update; this document does not record its result. A view that differs from its base table in any other way is not covered by this promise. From 1.0, the embedded 1.0-to-1.1 migration rebuilds every managed view, as an update to 1.1 did. When a user view depends on such a view, the update refuses, atomically, and names the dependent view. Drop the dependent view, update, then recreate it.

### Privileges

- Routines whose signature is unchanged keep their privileges, including site-specific `GRANT` and `REVOKE`: the update redefines them with `CREATE OR REPLACE`, which keeps the existing privileges, and does not replay grants. On the 1.0 path this holds because the generated script leaves out the frozen 1.0-to-1.1 migration's grants on routines that already exist (see [Supported paths](#supported-paths)); a site that revoked `EXECUTE` from `PUBLIC` on, for example, `for_portion_of_trigger()` keeps that revocation.
- Routines new in 1.3 get explicit privileges, the same as in a fresh 1.3 installation. The new private helpers are revoked from `PUBLIC`, with three deliberate exceptions that are granted `EXECUTE` to `PUBLIC` explicitly, also where default privileges revoke it:
  - `sql_saga.regular_fk_check_c()`, the trigger function of regular foreign keys. Creating the trigger, as `pg_restore` does when it restores as the table owner, needs `EXECUTE`. The function only enforces the foreign key that its trigger arguments name.
  - `sql_saga.__internal_current_view_clock(oid)` and `sql_saga.__internal_current_view_now(oid, regtype)`, which give a `current` view's trigger the clock that view is bound to. The trigger runs as whoever writes through the view, so they are called as that writer. They are `SECURITY INVOKER` with a fixed `search_path`, and they only read and evaluate the clock expression PostgreSQL already parsed into the view's rule.
- A routine whose signature changes is replaced, and the replacement cannot carry changed privileges over (see the next section).

### Changed privileges on `add_system_versioning`

This applies to updates from 1.0 and 1.1 only. 1.2 added a trailing `history_table_must_exist boolean DEFAULT false` parameter to `sql_saga.add_system_versioning`, so the embedded update to 1.2 replaces the 1.1 function. Every 1.1 call form still works. The replacement gets the privileges that a fresh installation gets under the database's current default privileges.

Privileges changed on the 1.1 function cannot be carried over. That covers a `REVOKE ... FROM PUBLIC`, a `GRANT` to a role, and default privileges changed since 1.1 was installed. Granted inside the update script, they would become extension initial privileges, which `pg_dump` omits, so a later restore would silently undo them. The update therefore refuses, and the refusal rolls back the whole update, leaving the database exactly as before. The message text comes from the embedded 1.2 step and says 1.2, but its hint names the update to 1.3:

```text
ERROR:  sql_saga: the privileges on sql_saga.add_system_versioning(regclass, name, name, name, name, name, name) differ from those its 1.2 replacement gets; the update replaces this function, and cannot carry changed privileges over
HINT:  Reset them, update, and apply them to the new signature after the update. Run: GRANT EXECUTE ON FUNCTION sql_saga.add_system_versioning(regclass, name, name, name, name, name, name) TO PUBLIC; ALTER EXTENSION sql_saga UPDATE TO '1.3'; REVOKE ALL ON FUNCTION sql_saga.add_system_versioning(regclass, name, name, name, name, name, name, boolean) FROM PUBLIC CASCADE;
```

The statements reset the whole privilege set, not only what differs:

1. They revoke every privilege on the 1.1 function from every grantee other than the owner, with `CASCADE`, which also removes grants made through a grant option.
2. They grant exactly what the replacement will get.
3. They run the update.
4. They do the same on the new signature with the original set, grant options included.

Run them in one explicit transaction, so that a failure anywhere also rolls back the resetting grant and the function is never more accessible than intended. With the hint above that is:

```sql
BEGIN;
GRANT EXECUTE ON FUNCTION sql_saga.add_system_versioning(regclass, name, name, name, name, name, name) TO PUBLIC;
ALTER EXTENSION sql_saga UPDATE TO '1.3';
REVOKE ALL ON FUNCTION sql_saga.add_system_versioning(regclass, name, name, name, name, name, name, boolean) FROM PUBLIC CASCADE;
COMMIT;
```

Use the statements from your own hint, which may differ. Do not run them in autocommit mode: if the update then fails, the resetting `GRANT` stays committed and the function is left more accessible than before. Privileges applied after the update are ordinary privileges, and `pg_dump` repeats them.

To read the privileges the replacement will get under the current default privileges (global and schema-specific alike), the update briefly creates a function named `sql_saga.__sql_saga_1_2_upgrade_acl_probe()` and drops it again before it replaces the function. It exists only inside the update's transaction, as a member of the extension like everything the update creates, and never survives it. A refusal rolls it back with everything else. If a function of that name already exists, the update refuses and asks you to rename it.

From 1.2, `add_system_versioning` already has the 1.2 signature and is not replaced.

### Shared regular foreign key helpers (1.0 and 1.1)

Regular foreign keys that reference the same unique key share one generated helper function. The released standalone `sql_saga--1.0--1.1.sql` builds the expected helper once per foreign key. With two or more regular foreign keys on one key, it therefore refuses a healthy, canonical helper (`is not the exact released 1.0 or canonical 1.1 regular foreign key helper`). The direct update to 1.3 embeds the generated 1.0-to-1.2 script, which checks one row per helper and key and accepts such databases. The helpers are then dropped by the conversion described above. The standalone `sql_saga--1.0--1.1.sql` is frozen and keeps the defect, so update 1.0 directly to 1.3.

## Before the maintenance window

1. Deploy the complete 1.3 package. `make install` places the SQL files, flattened to their file names, in `$(pg_config --sharedir)/extension/`, which is where PostgreSQL looks for them (the source tree's `versions/` directory is not a runtime location):
   - `sql_saga.control` (`default_version = '1.3'`)
   - `sql_saga--1.3.sql`
   - `sql_saga--1.0--1.3.sql`
   - `sql_saga--1.1--1.3.sql`
   - `sql_saga--1.2--1.3.sql`
   - `sql_saga--1.0.sql`, `sql_saga--1.1.sql`, `sql_saga--1.2.sql` (the released install scripts, needed to restore an older dump)
   - `sql_saga--1.0--1.1.sql`, `sql_saga--1.1--1.2.sql`, `sql_saga--1.0--1.2.sql` (released update scripts, unchanged)
   - `sql_saga_upgrade_preflight.sql`

   and the libraries in `$(pg_config --pkglibdir)`: `$libdir/sql_saga` and `$libdir/sql_saga_native`.
2. Restart PostgreSQL after replacing either library. The loaded native library must be the 1.3 build (`sql_saga.native_library_version()` returns `1.3`): every update script refuses if it is not, and nothing it did in its transaction is committed. **Stop application writers before this restart, not only before the update**, and keep them stopped until the update and `check_integrity()` have passed (see [Upgrade](#upgrade)). Between the restart and `ALTER EXTENSION sql_saga UPDATE`, the 1.3 libraries run against the old schema, and a write in that interval can fail. The restart therefore belongs inside the maintenance window, together with the update.
3. Run the read-only preflight in every database:

```bash
psql -X -v ON_ERROR_STOP=1 -d "$DB" -f "$(pg_config --sharedir)/extension/sql_saga_upgrade_preflight.sql"
```

The preflight accepts only the released installation shapes. It reports derived grants that the 1.0 migration normalizes. It also refuses policies created directly on managed history tables; move such policies to the base table, where sql_saga can synchronize them safely. It is a quick first look, not a guarantee.

For every source version, the reliable check is a rehearsal: run the update itself and roll it back. It runs every check the real update runs, including the orphan scan and the final integrity gate, and leaves the database unchanged:

```bash
psql -X -v ON_ERROR_STOP=1 -d "$DB" -c "BEGIN; ALTER EXTENSION sql_saga UPDATE TO '1.3'; ROLLBACK;"
```

If it prints `ROLLBACK`, the real update will pass as long as nothing changes in between. If it fails, the error names the object and the repair. The dry run takes the same locks as the update, so run it outside busy hours, and with the 1.3 libraries loaded, so on the restored rehearsal copy or inside the window.

Before upgrading from 1.1, every cataloged regular-to-temporal foreign-key helper must still be the exact canonical helper generated by released 1.1. If validation refuses a helper, restore the released 1.1 definition or recreate the foreign key with sql_saga.

## Upgrade

Application writers must already be stopped: they stop before the restart into the 1.3 libraries, not just before this step. Confirm the starting version is 1.0, 1.1 or 1.2, then update directly to 1.3:

```sql
SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'sql_saga';
SET lock_timeout = '30s';
ALTER EXTENSION sql_saga UPDATE TO '1.3';
```

You can inspect the selected paths before the window:

```sql
SELECT source, target, path
FROM pg_catalog.pg_extension_update_paths('sql_saga')
WHERE target = '1.3'
  AND source IN ('1.0', '1.1', '1.2')
ORDER BY source;
```

Expected paths are `1.0--1.3`, `1.1--1.3` and `1.2--1.3`.

The update is atomic: it runs in one transaction, and any refusal or error rolls back everything, leaving the previous version installed exactly as it was. Within that transaction it validates, among other things:

- the loaded 1.3 native library;
- the generated helpers;
- that there are no unmanaged policies on managed history tables.

It takes a transaction-scoped advisory lock, in the current database only, which only keeps two concurrent updates of the same database from interleaving. It does not serialize updates across databases, and it does not coordinate application writers, which is why they must be stopped.

The update disables sql_saga's own rename-following event trigger for its duration and puts back the state it found. If you have set that trigger to `ENABLE ALWAYS` or `ENABLE REPLICA`, the state is kept. If you have disabled it, the final integrity gate refuses with the repair, and nothing changes. The update never re-enables it behind your back.

Never bypass any of this with `CASCADE`, disabled event triggers, or a changed `session_replication_role`.

### What the update keeps

- From 1.1 and 1.2, the update keeps every managed family as it is. The four generated query functions of each system-versioned table are not regenerated, and neither are their views, policies or privileges. The update adds what 1.3 tracks: the catalog format marker, and, from 1.1, the column snapshot of each system-versioned table, filled from the table's current columns. It converts regular foreign keys as described above.
- From 1.0, the embedded 1.0-to-1.1 migration deliberately reconciles older generated objects, exactly as an update to 1.1 did. It rebuilds history views and query functions, synchronizes history policies and normalizes direct grants on derived objects. Expect those objects and their privileges to change. The result matches a fresh 1.3 installation.

### Final integrity gate and damaged families

The last statement of the update runs the complete `sql_saga.check_integrity()`, after every definition and privilege change. Some managed families may have been damaged since they were created: a dropped synchronization trigger, a hand-altered generated query function, or a missing policy copy on a history table. The update then fails with the checker's exact message, which names the object and the repair, and the whole update rolls back. For example:

```text
ERROR:  sql_saga: integrity violation for system-versioned table "public.sv": generated query function role "as_of" definition is noncanonical; repair with rebuild_system_versioning_views
```

The database stays at its previous version, unchanged. Repair the named object with the function the message names, such as `rebuild_system_versioning_views`, `sync_history_policies`, or dropping and re-adding the era, key or foreign key. Then run `ALTER EXTENSION sql_saga UPDATE TO '1.3'` again, and repeat until it succeeds. To find damage before the window, run the rehearsal update on a restored copy. Never work around the gate.

On the 1.0 path, the embedded 1.0-to-1.1 migration reconciles the damage it rebuilds (generated query functions, views, history policies) rather than refusing it. Damage it does not rebuild, such as a dropped era synchronization trigger, is refused.

## Verify

Run as the extension owner or a superuser:

```sql
SELECT extversion FROM pg_catalog.pg_extension WHERE extname = 'sql_saga';
SELECT sql_saga.native_library_version();
SELECT count(*) FROM sql_saga.installation;
SELECT to_regclass('sql_saga.temporal_merge_cache');
SELECT count(*) FROM sql_saga.foreign_keys WHERE type = 'regular_to_temporal' AND fk_helper_function IS NOT NULL;
SELECT sql_saga.check_integrity();
```

Expected results:

| Query | Result |
|---|---|
| `extversion` | `1.3` |
| `native_library_version()` | `1.3` |
| rows in `sql_saga.installation` | `1` |
| `to_regclass('sql_saga.temporal_merge_cache')` | `NULL` |
| regular foreign keys still naming a helper | `0` |
| `check_integrity()` | succeeds and returns nothing (`void`) |

If `check_integrity()` raises an error, do not restart the application: repair the object the message names with the function it names, and run the check again.

The update must leave no extension initial privileges on detached managed objects, that is, sql_saga-generated objects that are not extension members. This query counts them for sql_saga's managed objects only, so the initial privileges of other extensions in the same database do not affect it:

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

The expected result is `0`. The query can only see objects that sql_saga still catalogs. A row in `pg_init_privs` whose object belongs to no extension at all carries no record of which extension it came from, so no query can attribute it to sql_saga. The initial privileges of the extension's own members are legitimate and are not counted.

Then run application smoke tests as every relevant role. Cover especially RLS tenant reads through `*_with_history` and the temporal query functions, and writes to tables with regular foreign keys.

## Failure and recovery

- **The update fails.** Nothing changed: the transaction rolled back and the database is still at its previous version. Fix the reported cause, for example the orphan rows the message lists, a mispaired foreign key, or a damaged family, then retry. For example, after the orphan refusal above:

  ```sql
  -- on the current version: inspect, then correct or delete each listed row
  SELECT fk.* FROM public.activity_note AS fk WHERE fk.legal_unit_id IS NOT NULL
      AND NOT EXISTS (SELECT FROM public.legal_unit AS uk WHERE uk.id = fk.legal_unit_id);
  ALTER EXTENSION sql_saga UPDATE TO '1.3';
  ```

  Between attempts the database runs the previous version's schema with the 1.3 libraries loaded. Keep application writers stopped, or redeploy the previous package and restart before reopening, until the update has passed.
- **The update succeeded but verification or the application fails.** The new version is committed. There is no downgrade script from 1.3. Redeploying the previous package files and libraries does not turn the database back into its previous version, and an extension at version 1.3 needs the 1.3 libraries. To go back, restore the backup taken before the window, with the previous package deployed.

## Dump and restore

### Catalog-format safeguard introduced in 1.3

The `sql_saga.era` catalog carries `catalog_format = '1.3'`. The value names the release that introduced this catalog format. It is not a count of schema changes and not a second extension version. Using a release name makes the marker traceable to a released layout, and a later release can keep the same value if it remains compatible with this format.

The column deliberately has **no default**. The column list of a dump made by 1.0, 1.1 or 1.2 omits it, so restoring those rows directly into a fresh 1.3 installation would supply NULL. This named constraint rejects that mistake:

```sql
CONSTRAINT "restore old sql_saga dumps per doc/upgrade.md"
CHECK (catalog_format IS NOT NULL AND catalog_format = '1.3')
```

The explicit NULL test matters: PostgreSQL CHECK constraints accept an unknown result, so equality alone would not reject the missing value. Public registration functions write the marker explicitly, and the extension update populates it on existing rows as part of the catalog migration.

This is a restore guard, not automatic conversion and not a replacement for `check_integrity()`. It detects an incompatible era row when that row is loaded. A database with no era rows gives it nothing to check, and a compatible marker does not prove that every generated object is intact. Do not add a default, disable the constraint, or stamp old rows manually to get a restore past it. Restore the old dump at its own extension version, then run the update as described below.

The guard cannot make `pg_dump` include `VERSION` in its `CREATE EXTENSION` statement, because sql_saga does not control that statement. Keep the source extension version with the backup, and preinstall that version explicitly when restoring an old archive.

### Restore at the dump's own extension version

`pg_dump` does **not** record the extension version. The archive says `CREATE EXTENSION IF NOT EXISTS sql_saga`, which installs the destination's *default* version, 1.3, and then loads the old catalog rows into it without running any update script. With the 1.3 package installed, a plain `pg_restore` of a 1.0, 1.1 or 1.2 dump therefore fails. For a dump with era rows, the safeguard above rejects them. Other dumps fail on a missing catalog column (for example `null value in column "table_columns_snapshot"` with system versioning). Or the restore produces a database whose objects are still in the old shape, which `check_integrity()` refuses.

A dump made by 1.3 restores as usual. To restore a dump made by an older sql_saga (1.0, 1.1 or 1.2), install that version first. The 1.3 package ships every released install script, and they load with the current libraries. `IF NOT EXISTS` then keeps that version, the dump restores into it, and `ALTER EXTENSION sql_saga UPDATE` runs the normal update to 1.3, including the regular foreign key conversion and its orphan scan. Set `OLD_VERSION` to the version of the database that was dumped: run `SELECT extversion FROM pg_extension WHERE extname = 'sql_saga'` on the source, and keep the result with your backups, since it cannot be read reliably from the archive.

<!-- BEGIN old-dump-restore -->
```bash
OLD_VERSION=1.0
pg_restore -f restore.sql backup.dump
psql -X -v ON_ERROR_STOP=1 --single-transaction -d mydb -c "CREATE EXTENSION IF NOT EXISTS btree_gist" -c "CREATE EXTENSION sql_saga VERSION '$OLD_VERSION'" -f restore.sql -c "ALTER EXTENSION sql_saga UPDATE" -c "DO \$\$ BEGIN PERFORM sql_saga.check_integrity(); END \$\$"
```
<!-- END old-dump-restore -->

The whole restore, the update and the integrity check are one transaction: if any step fails, `mydb` is left
exactly as it was (empty). `mydb` must be a new, empty database. The same works with `pg_restore` itself, for
example in a split restore. Run the steps in this order (this is not atomic across invocations):

1. `CREATE EXTENSION sql_saga VERSION '<old>' CASCADE` in the new database.
2. Every `pg_restore` invocation for schema and data.
3. If those invocations used `--no-acl` (or `-x`), the archive's ACL entries, for example
   `pg_restore -l backup.dump | grep ' ACL ' > acl.list` and then `pg_restore -L acl.list -d mydb backup.dump`.
4. `ALTER EXTENSION sql_saga UPDATE`.
5. `SELECT sql_saga.check_integrity()`.

The ACL entries must be restored before the update, not only before the explicit check: the update itself ends
with `check_integrity()`. The access gate on a system-versioned table's history view is part of its canonical
shape, so without its grants the update and the check can refuse with `history family definition is noncanonical`
(the 1.1 and 1.2 paths preserve existing family privileges rather than recreating them). Restore the ACL entries
before the check for a dump of any version.

Do not use `pg_restore --clean` for an old dump: it drops the extension you installed and recreates the default version.

If you install the wrong old version, the procedure usually fails and rolls back, for example:
- a 1.1 dump with system versioning or managed views, restored as 1.0;
- a 1.0 dump with regular foreign keys or system versioning, restored as 1.1.

It does not fail for a 1.0 dump with `for_portion_of`/`current` views restored as 1.1: the views then miss the
grants the 1.0-to-1.1 step gives them. Do not rely on a failure to detect the mismatch; use the version of the
source. A 1.3 dump needs no preinstalled version: restore it as usual.

Restores mint a new installation token on the destination.

## Historical notes

The 1.0-to-1.1 migration removed the persistent `temporal_merge_cache_*` API, normalized direct grants copied onto derived objects, introduced the installation token and gated history views, and reconciled managed families. The 1.1-to-1.2 migration added the column snapshot of system-versioned tables and the new `add_system_versioning` signature. Both remain embedded, unchanged apart from the retargeted guard, hint and final gate, in the generated direct 1.0-to-1.3 and 1.1-to-1.3 paths. New deployments should install 1.3, and existing 1.0, 1.1 and 1.2 installations should update directly to 1.3.
