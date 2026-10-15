# Upgrading sql_saga 1.0 to 1.1

sql_saga 1.1 supports an in-place upgrade only from the released `v1.0` build (`9e39fcc`). Plan a maintenance window because the update takes locks on every managed base and history table.

## Before the window

1. Deploy `sql_saga.control`, `sql_saga--1.0.sql`, `sql_saga--1.1.sql`, `sql_saga--1.0--1.1.sql`, `sql_saga_upgrade_preflight.sql`, `$libdir/sql_saga`, and `$libdir/sql_saga_native`.
2. Restart PostgreSQL after replacing either library.
3. Run the read-only preflight in every database:

```bash
psql -X -v ON_ERROR_STOP=1 -d "$DB" -f "$(pg_config --sharedir)/extension/sql_saga_upgrade_preflight.sql"
```

Preflight refuses installations other than the supported 1.0 release. It lists direct grants on derived objects that the update will remove. It also refuses policies created directly on managed history tables. Move those policies to the base table, where sql_saga can copy them safely.

The persistent `temporal_merge_cache_*` API is removed in 1.1. Remove external dependencies before upgrading.

## Upgrade

Stop application writers, then run:

```sql
SELECT extversion FROM pg_extension WHERE extname = 'sql_saga';
SET lock_timeout = '30s';
ALTER EXTENSION sql_saga UPDATE TO '1.1';
```

The update is atomic. Before any privileged mutation, it validates every cataloged regular-FK helper against exactly the released 1.0 definition or the canonical 1.1 definition derived for that helper. It then takes an extension-wide advisory lock, verifies that the loaded native library reports version 1.1, removes the obsolete cache API without `CASCADE`, mints the installation token, and reconciles all managed families through the same generators used by a fresh installation. If a user dependency prevents removal of an obsolete object, or a generated helper has any other definition, the update fails with an actionable error and leaves 1.0 intact.

Never force the update with `CASCADE`, disabled event triggers, or a changed `session_replication_role`.

## Verify

```sql
SELECT extversion FROM pg_extension WHERE extname = 'sql_saga';
SELECT sql_saga.native_library_version();
SELECT count(*) FROM sql_saga.installation;
SELECT to_regclass('sql_saga.temporal_merge_cache');
SELECT count(*)
FROM pg_catalog.pg_init_privs AS i
WHERE i.privtype = 'e'
  AND NOT EXISTS (
      SELECT
      FROM pg_catalog.pg_depend AS d
      JOIN pg_catalog.pg_extension AS e ON e.oid = d.refobjid
      WHERE e.extname = 'sql_saga'
        AND (d.classid, d.objid, d.deptype) = (i.classoid, i.objoid, 'e'::"char"));
```

Expected results are `1.1`, `1.1`, `1`, `NULL`, and `0`. The last query verifies that the update did not leave extension initial-privilege rows on detached objects. Run application smoke tests as every relevant role, especially RLS tenant reads through `*_with_history` and the as-of functions.

## Behavior changes

- Direct grants copied onto generated history, system-versioning, FOR PORTION OF, current, and query-function objects are normalized away. Grant access on the base table.
- Policies on managed history tables are rejected. Define policies on the base table and run `sql_saga.sync_history_policies`.
- Generated history tables gain RLS and FORCE parity with the base table, tokenized fixed policies, and policy copies.
- The persistent cache API no longer exists.
- `sql_saga_native` remains an internal library, not a separately installable extension.

## Dump and restore

A dump records the installed extension version. A 1.0 dump restores as 1.0 when the frozen 1.0 artifact is available, after which this procedure upgrades it. A 1.1 dump restores as 1.1 and mints a new installation token on the destination.
