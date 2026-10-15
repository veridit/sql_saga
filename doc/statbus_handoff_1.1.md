# Handoff to the statbus agent: sql_saga 1.1

Status: all work is on branch `sql-saga-1.1-ci` (CI green on Linux), not yet merged to master; tag `v1.0` is pushed.
statbus deploys tag `v1.0` (`9e39fcc`) today.

## What changed for statbus

1. **Privileges are decided at access time (STATBUS-481/482).** Derived objects no longer carry copies of the
   base table's grants. FOR PORTION OF views, current views and `<t>_with_history` are `security_invoker` and
   granted to PUBLIC; the as-of functions are executable by PUBLIC. Access is decided by the caller's privileges
   and RLS on the base table. Manage grants on base tables only.
   - The REVOKE incident (`REVOKE ALL ON FUNCTION public.upgrade_schedule(...)` aborting pg_restore with
     "cannot revoke INSERT directly from establishment__for_portion_of_valid") cannot happen any more:
     GRANT/REVOKE no longer trigger sql_saga's checks.
   - **481B workaround is no longer needed**: whatever statbus does to keep derived-object ACLs in step with
     base tables can be removed. Please verify which statbus code that is and remove it after upgrading.
2. **RLS on history.** History tables have RLS enabled. Every SELECT/ALL policy on a base table gets a copy on its
   history table (`sql_saga_<hash>` names, NOTICE on creation). Define policies on base tables only; CREATE POLICY
   on a history table is refused. If a base table's policies change while event triggers are off, reads of its
   history by RLS-subject roles fail closed until `SELECT sql_saga.sync_history_policies('<table>');` is run.
   - **Performance:** for roles subject to an RLS policy that actually filters, as-of and range-only history
     queries cannot use the GiST range index (range operators are not leakproof); btree key lookups still can.
     Include the entity key in history queries where possible. Full scans of `_with_history` cost +40-50 %.
3. **Test 352 needs a rewrite.** It asserted the old ACL-mirroring behaviour (derived objects carrying copies of
   base grants). Under 1.1, assert instead: a role without SELECT on the base gets
   `permission denied for table <base>` through every view/function; a role with SELECT reads; RLS filters
   history like the base. Please re-read 352 against this list.
4. **Restore.** Plain `pg_restore`, `--single-transaction`, `--no-owner` and now `--clean` all work. A restore
   script piped through psql with `--clean` needs `PGOPTIONS='-c sql_saga.clean_restore=on'`. Parallel
   `pg_restore -j N` remains unsupported (PostgreSQL pg_dump bug); README "Backup and Restore" has a two-step
   workaround (catalog data serially via `-L`, the rest with `-j`).
5. **Manual schema changes on system-versioned tables.** ADD/DROP/RENAME/retype columns on base and history
   yourself, then `SELECT sql_saga.rebuild_system_versioning_views('<table>');`. Writes refuse a history table
   that lacks a base column or has a different type, with a HINT containing the exact DDL. Rename the history
   column first when a base policy uses the column. README "Manual schema changes" has the workflows.
6. **Error messages** name relations once-quoted (`on table "my schema.T"`); statbus tests that match sql_saga
   error text for names needing quotes may need updating (simple names are unchanged).
7. **Regular-to-temporal foreign keys** (`add_regular_foreign_key`): writers of the child table need neither
   SELECT on the parent nor EXECUTE on the helper, also in hardened databases, as with native foreign keys.
   The helper (`<parent>_<cols>_exists`) is now SECURITY DEFINER owned by the parent's owner; the update script
   reconciles existing 1.0 helpers. If statbus granted parent SELECT or helper EXECUTE only to make child
   inserts work, those grants can be removed.
8. **Packaging.** `make install` installs `$libdir/sql_saga_native` as an internal library. There is no
   `sql_saga_native` extension and no `cargo pgrx install` step; the Docker build needs a Rust toolchain and
   libclang, not cargo-pgrx (README "With Docker", PostgreSQL 18). Old images may still contain a stale
   `sql_saga_native.control`; it is harmless and removed by the next `make install`.

## Upgrade 1.0 -> 1.1 (runbook: `doc/upgrade.md` in sql_saga)

statbus runs `v1.0` (9e39fcc), the only version the update supports.

1. Build the new image from sql_saga master (`make && make install`; Rust toolchain + libclang, no cargo-pgrx).
   It installs `sql_saga--1.0.sql` (frozen v1.0), `sql_saga--1.1.sql`, `sql_saga--1.0--1.1.sql`,
   `sql_saga_upgrade_preflight.sql` and both libraries.
2. Restart PostgreSQL on the new image.
3. Per database, read-only preflight:
   `psql -X -v ON_ERROR_STOP=1 -d "$DB" -f "$(pg_config --sharedir)/extension/sql_saga_upgrade_preflight.sql"`.
   It refuses anything but v1.0, refuses policies created directly on history tables (move them to the base),
   and lists direct grants on derived objects that the update removes (grant on base tables instead).
   Remove any statbus use of the dropped `temporal_merge_cache_*` API first.
4. Maintenance window, writers stopped: `SET lock_timeout = '30s'; ALTER EXTENSION sql_saga UPDATE TO '1.1';`
   Atomic; on any error the database stays at 1.0 with an actionable message. Never use CASCADE, disabled event
   triggers or `session_replication_role` to force it. Re-running after success is a no-op.
5. Verify (expect `1.1`, `1.1`, `1`, NULL, `0`): the queries in `doc/upgrade.md` "Verify", then smoke-test
   RLS tenant reads through `*_with_history` and the as-of functions as each application role.

statbus follow-ups after upgrading: remove the 481B workaround (item 1), rewrite test 352 (item 3), and drop
grants that existed only to make regular-FK child inserts or derived objects work (items 1, 7).
