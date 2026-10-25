# Architectural Decision Log

This document records significant architectural decisions made during the development of `sql_saga`.

---

## Decision: ON DELETE CASCADE for Regular-to-Temporal Foreign Keys, in the Existing C Trigger (1.4, 2026-10-21)

### Problem

StatBus has non-temporal addendum tables (notes, tags, external identifiers) that reference temporal legal
units and establishments and have no meaning without them. Their references to plain tables use native
`ON DELETE CASCADE`. For the temporal parents only `NO ACTION` and `RESTRICT` existed.

### When a child goes

A regular reference only requires that its key exists in the parent at some time. The child rows go when
the **last** parent row with their key is deleted. Deleting one timeline row while another row of the
same key remains keeps them. This is the rule `NO ACTION` already uses to decide whether a delete orphans
a child, so the two actions disagree only on what happens at that point: refuse or delete.
`temporal_merge` defers the parent-side trigger, so reshaping a timeline never cascades. Deleting an
entity with `DELETE_MISSING_ENTITIES` does. It defers it also with `delay_constraints => false`, which keeps
only the native constraints immediate: a regular reference is judged by the timeline the merge leaves.

### Implementation

The parent-side `uk_delete` trigger (`uk_delete_check_c`, C) already finds the referencing children and
checks for another parent row with the key, with cached SPI plans and the child foreign-key index. With
`delete_action = CASCADE` it runs one more cached plan, `DELETE FROM ONLY child WHERE key = $1`, instead
of raising the violation. Like native referential actions, it runs as the owner of the child table with child
triggers firing, so system versioning, the child's own foreign keys and native cascades below it all
apply. It uses the same snapshot handling as the existing lookups: under `REPEATABLE READ`, a child
row committed after the snapshot makes the delete fail with 40001 rather than leave an orphan.

Rust was considered and rejected. The cost is in the two index lookups and the delete, which are the same
from either language, and every foreign-key trigger is C and shares its helpers. The Rust library holds
the `temporal_merge` planner and executor.

### Scope

- `CASCADE` only for `regular_to_temporal`. A `temporal_to_temporal` key is a native `FOREIGN KEY ...
  PERIOD`, and PostgreSQL 18 refuses referential actions on those ("unsupported ON DELETE action for
  foreign key constraint using PERIOD").
- `SET NULL`, `SET DEFAULT` and `ON UPDATE` actions other than `NO ACTION`/`RESTRICT` stay refused, now
  with an explicit 0A000 error from `add_*_foreign_key` instead of a catalog CHECK violation.
- `TRUNCATE` fires no row triggers. Up to 1.3 it therefore silently left the children of a regular foreign key
  without a parent. 1.4 adds a statement-level `AFTER TRUNCATE` trigger per regular foreign key on the parent
  (`foreign_keys.uk_truncate_trigger`) that refuses, as native foreign keys do, while a child row references it.

### Measured cost (macOS, PostgreSQL 18.3, best of 3; `tmp/bench_cascade_run.sh`)

Measured on the final 1.4 code (after the security fixes and the per-transaction trigger verification cache) and on
v1.3 in the same session. The native baseline varied between 55 and 69 ms across runs on this machine, so
differences under about 20% are noise.

| 10,000 keys, 2 timeline rows each | 1.4 ms | 1.3 ms |
|---|---|---|
| CASCADE, delete every row (20,000 children cascaded) | 200.4 | n/a |
| CASCADE, delete one of two rows (children kept) | 101.0 | n/a |
| NO ACTION, delete one of two rows (children kept) | 124.0 | 102.6 |
| NO ACTION, delete every row of unreferenced keys | 57.9 | 51.4 |
| regular FK, insert 20,000 child rows | 138.7 | 118.2 |
| native FK, insert 20,000 child rows | 129.0 | 142.2 |
| native PostgreSQL CASCADE between plain tables (10,000 parents, 20,000 children) | 69.3 | 54.8 |

When the key survives, CASCADE costs the same as `NO ACTION`. The cost grows about linearly.

The verification that a trigger is sql_saga's own (review finding F1) first ran a catalog query on every firing,
which made bulk deletes and child inserts about 20 times slower (1,210 ms instead of 51 ms for the unreferenced
delete). It is now done once per trigger per transaction, and again after any DDL on the table.

---

## Decision: Use Constraint Triggers for Regular-to-Temporal Foreign Keys (1.3, 2026-10-20)

### Problem

A regular child row refers to a parent key that must exist at some time, without specifying a child
period. Versions 1.0 through 1.2 enforce the child side with a CHECK that calls a parent-reading SQL
helper. A dump can create that CHECK before loading either table, then load the child first. The
child load fails even in a serial or single-transaction restore. Wrapping the restore in one
transaction does not make an immediate CHECK wait for the parent data.

### Decision

Use a row-level constraint trigger, `DEFERRABLE INITIALLY IMMEDIATE`, for the child side. It checks
INSERT and every UPDATE, not only updates naming the FK columns. A deferred event for an obsolete
row version may be skipped only because the surviving version has its own check. The trigger is a
post-data dump object, so restored data does not depend on which table's data is loaded first.

The parent-side check asks whether a child still refers to the old key **and** no parent timeline
segment retains that key. Removing one segment is not the same as removing the key. Deferred
checks allow a merge to remove and replace segments, but reject a transaction that leaves a real
orphan. Row locking and transaction-snapshot checks must retain this rule under concurrent writes,
including REPEATABLE READ, rather than merely passing single-session tests.

`temporal_merge` may defer its intended constraints. Since `SET CONSTRAINTS schema.name` can match
more than one constraint, it must refuse a name collision before target DML rather than silently
deferring an unrelated constraint. It does not rename or change a user-owned constraint.

The existing catalog field `foreign_keys.fk_check_constraint` names the new child trigger. A fresh
1.3 FK needs no helper function. The `fk_helper_function` API argument remains in the signature for
call compatibility, but an explicit non-NULL value is refused rather than silently ignored.

### Upgrade and validation contract

The direct update scripts validate existing regular references before replacing CHECKs. Orphans
cause an atomic refusal with an executable scan, not a partially converted database. Historical
column-pairing damage is diagnosed separately and requires confirmation of the intended pairing.
Old dumps are restored at their own extension version, then upgraded, as described in `upgrade.md`.

Regression tests 125, 128 and 129 cover representation, pairing, lifecycle, deferral and row-version
behavior. `devops/test_regular_fk_concurrency.sh` covers controlled two-session interleavings on an
explicitly identified disposable cluster. Upgrade and restore harnesses exercise released-version
conversion and post-data trigger restoration. These are release gates, not a substitute for running
the final assembled 1.3 package.

---

## Decision: Recognize the Teardown of `pg_restore --clean` in the Drop Protection (2026-10-15)

### Problem
`pg_restore --clean` into a database that already holds `sql_saga` families failed on the first
`sql_saga`-managed object it dropped. Examples: `cannot drop trigger "b_truncate" on table "b" because it
is used in SYSTEM_TIME period`, the FOR PORTION OF and current view triggers, regular foreign key
triggers, unique key constraints, and the history policies. `--clean` issues one DROP per archived
object, dependents first and each base table last. The catalog rows only disappear with the final
`DROP EXTENSION`. The drop protection, which checks after every statement that each object named in
the catalog still exists, therefore refused the first DROP of every family. After that, every later
DROP failed too, because the checks cover all families on every `sql_drop` event.

### Options considered
- "The base table is dropped in the same command": not visible. Each DROP is its own statement, and
  the base table goes in a later one.
- Defer the checks to commit: serial `pg_restore` commits every statement.
- `session_replication_role = replica`: needs superuser, disables every event trigger (history policy
  synchronization included). It is an operator procedure, not a fix.
- Treat a missing object as a family teardown: would allow exactly the accidental drops the protection
  exists for.

### Decision
`sql_saga.__internal_is_clean_teardown()` recognizes the teardown phase. All of these must hold:
`check_function_bodies = off` and `row_security = off` (set by every `pg_dump` script and `pg_restore`
session); a `pg_restore` session (`application_name = 'pg_restore'`, libpq's default for `pg_restore`,
or the explicit opt-in `sql_saga.clean_restore = on` for a restore script run through `psql`); and a
top-level DROP statement (`current_query()`). During the teardown, `drop_protection`:
- skips its "object still needed" refusals;
- skips cleanup of objects that `pg_restore` drops itself, without `IF EXISTS` (template sync functions,
  regular-FK CHECK and helper functions, history policy copies of a dropped base policy);
- drops the managed history policies of each touched family itself (DEBUG). Their names carry this
  installation's token, which the dump cannot name, and they depend on the base table.

`health_checks` does not resynchronize history policies during the teardown. Separately, a trigger on
`sql_saga.system_versioning` grants `EXECUTE` on the as-of functions to `PUBLIC` when a row is inserted,
so `--clean` into a hardened database (default privileges revoking `EXECUTE`) keeps the design rule
that these functions are executable by `PUBLIC`.

### Why protection is not weakened
The protection guards against accidental drops in interactive sessions, applications and migrations.
None of these has all three conditions. Restore-like settings alone, the opt-in alone, or
`application_name = 'pg_restore'` alone all keep every refusal (test 029). It is not a security
boundary, and it does not need to be one. Whoever can run these DROPs owns the objects and can already
disable the triggers or the event triggers. Non-DROP statements of the same restore keep every check.

### Verification
`devops/restore_roundtrip.sh` variants `clean`, `clean_single_txn`, `clean_same` (a dump of the
destination itself, without `--if-exists`), `clean_psql` (opt-in), and `clean_psql_without_opt_in`
(must be refused). Regression test `029_drop_protection_restore_clean`.

---

## Decision: Keep Foreign Keys Between Catalog Tables; Parallel Restore Unsupported (2026-10-13)

### Problem
`pg_restore -j N` of a database using `sql_saga` fails intermittently (about 40 % of runs on
PostgreSQL 18.3) with foreign key violations while loading `sql_saga`'s own catalog tables, for
example `COPY failed for table "system_versioning": ... violates foreign key constraint
"system_versioning_table_schema_table_name_era_name_fkey"`. Serial restore always succeeds.

### Cause (PostgreSQL, not sql_saga)
Extension configuration tables (`pg_extension_config_dump`) get their foreign keys at
`CREATE EXTENSION` time, before their data is loaded. `pg_dump`'s `processExtensionTables()`
computes "child data depends on parent data" (added 2015, commit `ebd092bc`), but `dumpTableData()`
writes each TABLE DATA archive entry with only its own TABLE as prerequisite and
`BuildArchiveDependencies()` skips such entries (policy from 2012, commit `8a504a36`). The order is
therefore used only to sort the archive, which serial restore follows, and never reaches the
dependency graph that parallel restore schedules by. Present in `master` as of 2026-10-13.
A minimal two-table extension reproduces it: serial restore succeeds, `-j 2` failed 5 of 5 runs
(PostgreSQL 18.3):

```sql
-- config_fk_order--1.0.sql (with a matching config_fk_order.control)
CREATE SCHEMA config_fk_order;
CREATE TABLE config_fk_order.parent (id integer PRIMARY KEY);
CREATE TABLE config_fk_order.child (id integer PRIMARY KEY,
    parent_id integer NOT NULL REFERENCES config_fk_order.parent (id));
SELECT pg_catalog.pg_extension_config_dump('config_fk_order.parent', '');
SELECT pg_catalog.pg_extension_config_dump('config_fk_order.child', '');
```
```sh
createdb src && psql -d src -c "CREATE EXTENSION config_fk_order" \
  -c "INSERT INTO config_fk_order.parent SELECT generate_series(1, 500000); INSERT INTO config_fk_order.child VALUES (1, 500000);"
pg_dump -Fc -f x.dump src
createdb a && pg_restore --exit-on-error -d a x.dump        # succeeds
createdb b && pg_restore --exit-on-error -j 2 -d b x.dump   # COPY failed for table "child": ... violates foreign key constraint
pg_restore -l -v x.dump | grep -A1 'TABLE DATA config_fk_order child'   # depends only on its own TABLE
```

### Alternatives considered
| option | parallel restore | integrity |
|---|---|---|
| keep FKs (chosen) | unsupported | full |
| FKs `DEFERRABLE INITIALLY DEFERRED` | still fails (each COPY commits alone) | full |
| FKs `NOT ENFORCED` (PG 18), re-enabled when data is observed | works, but nothing re-enables them at the end of a restore | lost until the next DDL |
| drop FKs, enforce in functions and health check | works | weakened |

### Decision
The foreign keys between `sql_saga`'s catalog tables stay. Parallel restore is documented as
unsupported until PostgreSQL fixes `pg_dump` (fix: keep the own TABLE as first archive dependency
of TABLE DATA and append the referenced tables' TABLE DATA entries). A bug report is drafted.
The restore test harness (`make test-restore`) exercises serial and single-transaction restores;
its parallel case is kept as a known failure so a future PostgreSQL fix is noticed.

---

## Decision: De-prioritizing Statement-Level Triggers (2025-09-01)

This section logs the decision to not pursue the implementation of statement-level triggers at this time.

### Rationale
This feature is not worth the implementation complexity for the following reasons:
1.  **Performance:** The current performance of the row-level triggers is acceptable ("not great, but not horrible").
2.  **Limited Utility:** The primary benefit of statement-level triggers is for validating multi-row DML within a *single statement*. However, the user's typical use cases involve changes across multiple tables within a single transaction. Since statement-level triggers are **not deferrable** to the end of a transaction, they would not solve this more common and complex validation problem.

### Conclusion
Based on this feedback, the "Investigate Statement-Level Triggers" and the dependent "Implement full `ON UPDATE` action support" tasks have been removed from the active `todo.md`. The current C-based, row-level triggers will remain the primary mechanism for foreign key validation. This decision can be revisited in the future if performance requirements change or if PostgreSQL adds support for deferrable statement-level triggers.

---

## Architectural Decision: Supporting Native Range Types (2025-09-01)

This section documents the exploration of different architectural strategies for allowing users to define an era using a single, native PostgreSQL range type column (e.g., `daterange`) instead of the current two-column (`valid_from`, `valid_until`) approach.

### Option A: The "Big Refactor"
-   **Description:** Fundamentally alter the entire extension. All metadata tables, C functions, and PL/pgSQL procedures would be rewritten to operate directly on a single `anyrange` column.
-   **Pros:**
    -   Architecturally "pure"; the internal model would directly reflect the user's data model.
-   **Cons:**
    -   **Massive Effort:** Requires a near-total rewrite of the codebase.
    -   **High Risk:** A rewrite of this magnitude is extremely high-risk and would likely introduce subtle regressions in well-tested components like the C triggers and `temporal_merge` planner.

### Option B: The "View Shim" (User-Managed)
-   **Description:** Require the user to manually create a view over their table that exposes `lower(range_col)` as `valid_from` and `upper(range_col)` as `valid_until`. The user would then register the *view* with `sql_saga`.
-   **Pros:**
    -   Requires zero code changes to the extension.
-   **Cons:**
    -   **Poor Ergonomics:** Pushes all the complexity onto the user, including the non-trivial task of creating `INSTEAD OF` triggers to make their view updatable.
    -   **Performance/Complexity Issues:** Can inhibit index usage and leads to complex "view-on-view" scenarios.

### Option C: The "Generated Columns" (Recommended)
-   **Description:** Overload the `add_era` function. When called with a `range_column_name`, it automatically alters the user's table to add `valid_from` and `valid_until` columns. These columns are defined as `GENERATED ALWAYS AS (lower(...)) STORED` and `GENERATED ALWAYS AS (upper(...)) STORED`. The rest of the extension continues to operate on these generated columns.
-   **Pros:**
    -   **Excellent Ergonomics:** The user experience is seamless.
    -   **Minimal Core Changes:** The complexity is almost entirely isolated to the `add_era` and `drop_era` functions. The rest of the extension, including all C code, remains untouched.
    -   **Performance:** `STORED` generated columns can be indexed, preserving all performance characteristics.
-   **Cons:**
    -   **Modifies User Table:** The function alters the user's schema, which must be clearly documented to avoid surprises.
    -   **Storage Overhead:** The generated `STORED` columns consume additional disk space.

### Option D: The "Metadata Shim"
-   **Description:** Modify the `sql_saga.era` table to store the `range_column_name`. Then, every part of the codebase (C functions, PL/pgSQL) would be modified with conditional logic to dynamically generate `lower(range_col)` and `upper(range_col)` expressions if a range column is being used.
-   **Pros:**
    -   Does not alter the user's table schema. The user's data model remains "clean".
-   **Cons:**
    -   **Pervasive & Complex Code Changes:** This approach pushes complexity deep into the extension's code. Conditional logic (`IF range_col IS NOT NULL THEN ... ELSE ...`) would need to be added to every performance-critical component, including:
        -   The C-based foreign key triggers.
        -   The `temporal_merge` planner's massive dynamic query.
        -   The triggers for the updatable views.
        This makes the extension's internal logic significantly harder to maintain and debug.
    -   **Performance Penalty & Indexing Complexity:** Using functions on a column (e.g., `lower(range_col)`) prevents the query planner from using a standard B-tree index on that column. To maintain performance, users would be **required** to create and maintain functional indexes (e.g., `CREATE INDEX ON my_table (lower(my_range_col))`). This externalizes a critical performance dependency, making the system less robust and harder to use correctly compared to the simple, standard indexes used by the "Generated Columns" approach. The query plans themselves also become more complex for the database to optimize.

### Option E: The "Synchronization Trigger"
-   **Description:** This approach is directly inspired by the existing `synchronize_valid_to_until` trigger. The user would define all three columns in their table (`my_range range`, `valid_from date`, `valid_until date`). A new, generic trigger function would be created. When applied to the table, this trigger would ensure that all three columns are kept in perfect sync on any `INSERT` or `UPDATE`. If any one of the columns is changed, the other two are automatically updated to match.
-   **Pros:**
    -   **Maximum Flexibility & API Ergonomics:** This is the most significant advantage. It provides an ideal experience for both database and API consumers:
        -   **For Database Users:** The ability to use a native `range` type is clean and idiomatic.
        -   **For PostgREST Users:** The synchronized `valid_from` and `valid_until` columns completely bypass the friction of using range types over HTTP. As documented in the [PostgREST documentation](https://docs.postgrest.org/en/v10/how-tos/working-with-postgresql-data-types.html#ranges), using ranges directly requires cumbersome filter syntax (e.g., `?duration=cs.[2023-01-01,2023-01-01]`), precise string representations for `INSERT`/`UPDATE`, and custom casting functions to receive a useful JSON object instead of a string literal. The synchronized columns provide a clean, simple, and direct RESTful interface.
    -   **Built-in Data Integrity:** The trigger is the ideal place to enforce `sql_saga`'s required `[)` semantics (inclusive lower, exclusive upper bound), guaranteeing data quality without extra user setup.
    -   **Leverages Existing Pattern:** Follows a pattern already established and proven within `sql_saga`, making it a consistent and predictable addition.
    -   **No Core Changes:** Like Option C, this requires no changes to the core of the extension (C triggers, `temporal_merge`, etc.).
    -   **Explicit Storage:** The user explicitly defines and stores all three columns, making the storage cost a clear and deliberate part of their data model.
-   **Cons:**
    -   **Requires User to Add Trigger:** This is a minor step, but the user must remember to create the trigger on their table. This can be completely mitigated by making `add_era` smart enough to do it automatically.

### Recommended Architecture: Option E
The **"Synchronization Trigger"** is the superior recommendation. It provides all the benefits of the "Generated Columns" approach (PostgREST ergonomics, performance, isolation from the core extension) while providing far greater flexibility by allowing writes to the `range` column itself. It is the most user-friendly and robust solution.

---

## Decision: Companion Rust Extension for Performance-Critical Paths (2026-02-22)

### Context
The temporal_merge planner was implemented in PL/pgSQL using set-based SQL with CTEs.
At scale (>10K rows), this became a performance bottleneck. The extension also needed
per-connection caching for executor introspection metadata.

### Decision
Implement native planner + executor cache in Rust (pgrx 0.16.1) as a separate shared
library (`sql_saga_native`). Keep C triggers/aggregates in C. Keep executor orchestration
in PL/pgSQL. Runtime detection via `to_regproc()` provides graceful PL/pgSQL fallback.

### Rationale
- **C** (triggers, aggregates): Per-row hot path — minimal overhead, direct SPI access.
  pgrx marshaling would add per-invocation cost with no correctness benefit.
- **Rust** (planner, cache): Batch operations — sweep-line algorithm replaces O(N^2) SQL
  with O(N log N) Rust. thread_local caching gives O(1) cache hits. pgrx overhead
  amortized across thousands of rows per call. 10-13x faster than PL/pgSQL.
- **PL/pgSQL** (executor): Complex dynamic SQL generation, easy to iterate. Calls into
  Rust cache for pre-computed SQL fragments.
