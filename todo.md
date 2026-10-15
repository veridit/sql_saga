# SQL Saga - TODO

A living document of upcoming tasks.

## Current Priority - Critical Issues & Bugs
- [ ] **ACL health check: over-broad gate + exact/inheritance disagreement (DESIGN REPLACED 2026-10-13 - implement the FINAL DESIGN section directly below; the copy-based plan and its AMENDED DESIGN further down are SUPERSEDED and kept only as history)**

  ### FINAL DESIGN (owner-approved 2026-10-13): access-time authority + RLS propagation to history
  Principle: derived objects hold NO copy of who may access them; PostgreSQL checks the base table's
  ACL (and RLS) when data is accessed. Every rule the health check enforces looks at ONE object, so it
  holds in any restore order (serial, single-transaction, `-j N`). Reviews: `tmp/design_access_time_authority.md`,
  `tmp/review_access_time_design.md` (Fable 5.1, with measurements), probes in `tmp/decl/` and `tmp/rv_at/`.
  Full spec: `tmp/design_final.md`.
  | derived object | fixed ACL | enforced by |
  |---|---|---|
  | FOR PORTION OF view, current view | PUBLIC `SELECT, INSERT, UPDATE, DELETE`; `security_invoker = on` | caller's rights + RLS on base |
  | `<t>_with_history` | PUBLIC `SELECT`; `security_invoker = on`; `SELECT .. FROM <t> UNION ALL SELECT .. FROM <t>_history_gated` | current rows: base rights + RLS |
  | `<t>_history_gated` (new, owner-rights) | PUBLIC `SELECT` | STABLE gate raises `42501` unless caller has SELECT on base |
  | as-of functions | explicit PUBLIC `EXECUTE` | read through `_with_history` |
  | `<t>_history` | owner only; RLS ENABLED (not FORCE) | base without RLS: one policy "caller may SELECT base"; base with RLS: sql_saga-owned copies of the base's SELECT/ALL policies (`<policy>__sql_saga`), kept in sync by an event trigger on CREATE/ALTER/DROP POLICY and ALTER TABLE .. ENABLE/DISABLE/FORCE RLS, idempotent so a pg_restore that replays both the base policy and the dumped copy succeeds |
  Also: GRANT/REVOKE are excluded from `__internal_ddl_command_affects_managed_object()` (no GRANT/REVOKE
  needs any action; ~0.2 ms each instead of 600-1000 ms). No C accessor. PUBLIC `EXECUTE` on `temporal_merge`
  and the procedures the view triggers call (hardened databases). Health check invariants: views stay
  `security_invoker`; `_history_gated` keeps its gate; history keeps RLS enabled and not forced; derived
  objects follow the base owner; history policies equal the projection of the base's policies.
  Accepted cost: on RLS tables whose policies really filter, as-of queries on history do full scans
  (policy quals block index pushdown; `USING (true)` policies keep the index). Full scans of `_with_history` +40-50 %.
  Work order (each a RED test commit, independently reviewed, then a GREEN fix commit that does not touch
  the test, independently reviewed, full fast suite + `make test-restore`):
    1. this design; 2. P1 `sql_saga.temporal_merge_cache` is writable by PUBLIC and its `plan_sqls` are executed by
    the next caller (confused deputy, `tmp/rv_at/27_cache_poison.sql`); 3. P2 INSERT through a current view fails
    on range-only eras (`column "valid_range" specified more than once`, `src/3c_current_view_trigger.sql:91-98`);
    4. F15 `pg_restore -j N` fails ~40 % of runs because sql_saga's catalog tables (system_versioning,
    updatable_view, system_time_era, ...) have FKs to `era` and parallel workers load them first.
    DECIDED 2026-10-13: PostgreSQL pg_dump bug (TABLE DATA of extension config tables lacks the FK load
    order in the archive; minimal reproducer fails 5/5). FKs stay; `-j` documented unsupported (README
    "Backup and Restore", `doc/decision-log.md`); `make test-restore` reports `-j` as KNOWN LIMITATION.
    Upstream report draft: `tmp/upstream_pg_dump_fk_order.md` (filing is the owner's call).
  DECISION 1 (owner-approved 2026-10-14): history policy copies are named `sql_saga_<md5(policy||token)[1:16]>`
  with a per-installation token in the non-dumped `sql_saga.installation` table; the event trigger creates copies
  automatically and drops dumped (old-token) copies; no user step after CREATE POLICY or restore. Spec:
  `tmp/design_final.md` section 8.
  DECISION 2 (owner-approved 2026-10-14): RESTRICTIVE `sql_saga_history_base_select` policy on every history table
  (closes direct-read leak; cost only for RLS-on + all-`USING (true)` + RLS-subject + range lookups). Spec section 9.
  DECISION 3 (owner-approved 2026-10-14): policy copies match columns BY NAME (deparse against the base, created on
  history); clear ERROR when history lacks a referenced column. Spec section 10. DECISION 4 (owner-confirmed 2026-10-14): no
  "already synced" marker anywhere (copies are created by the trigger; read-time check is the only, fail-closed guard).
- [ ] **`pg_restore --clean` into a database that has system-versioned tables fails (found 2026-10-15, pre-existing)**
  `ERROR: cannot drop trigger "b_truncate" on table "b" because it is used in SYSTEM_TIME period`: drop protection refuses
  the DROP TRIGGER that `--clean` issues before dropping the table. Same on unmodified master. Needs its own RED/GREEN pair
  (and a `clean` variant in `make test-restore`).
- [x] **CRASH (fixed 2026-10-15, test 026): write_history segfaulted when base and history column positions differ (found 2026-10-14)**
  A column dropped before `add_system_versioning` (history built with `LIKE` loses the slot) makes the first
  UPDATE/DELETE crash the backend (signal 11, server restart). Cause: `src/sql_saga.c:1772` reads
  `values[range_num-1]` (base attnum) from the tuple converted to the HISTORY layout; must use `history_range_num`.
  RED test drafted: `tmp/pair_crash/026_system_versioning_dropped_column.{sql,out}` (crashes today). Own RED/GREEN pair.
- [ ] **Schema changes after system versioning: manual schema changes (owner-approved 2026-10-14; pair after ACL)**
  Today: ADD COLUMN silently not recorded in history; RENAME on base breaks every UPDATE ("could not convert row type");
  DROP COLUMN refused; views frozen. Fix: `rebuild_system_versioning_views(regclass, without_columns)`, write_history
  tolerates history-only columns (NULL), `_with_history` shows the union, clear ERROR + HINT when history lacks a base
  column. User adds/renames columns on history by hand; sql_saga respects it. Spec: `tmp/design_final.md` section 11.
  Design: `tmp/design_manual_schema_changes.md`; owner answers 2026-10-15 (actionable fail fast), section 12.
  The text below this point is the superseded copy-based analysis (defect list F3-F14 remains valid as test input).
  Downstream incident: `REVOKE ALL ON FUNCTION public.upgrade_schedule(text,boolean) FROM PUBLIC` raised
  "cannot revoke INSERT directly from establishment__for_portion_of_valid" and aborted a pg_restore.
  Reproduced on PG 18.3, including a real `pg_dump -Fc | pg_restore --exit-on-error` that aborts on an
  unrelated `REVOKE ... ON public.zzz` (scripts: `tmp/acl_health/`).
  - **Defect 1, the gate** (`src/0e_internal_functions.sql` `__internal_ddl_command_affects_managed_object`):
    every GRANT/REVOKE runs the full scan of every managed family. Verified: for GRANT/REVOKE,
    `pg_event_trigger_ddl_commands()` gives NULL objid/identity. The exact target list lives in the
    `command` (`pg_ddl_command`) column: `CollectedCommand.d.grant.istmt->{objtype,objects}`.
    For ALTER TABLE the objid *is* reported. The comment is wrong there, but rename_following depends on it, so it is out of scope.
    - Fix: a C accessor `sql_saga.__internal_grant_command_objects(pg_ddl_command) RETURNS SETOF (classid, objid)`
      in `src/sql_saga.c` (about 40 lines. A prototype was compiled and checked against PG18:
      `ON t1, t2`, `ALL TABLES IN SCHEMA`, `ALL FUNCTIONS IN SCHEMA`, column grants and sequences all give exact oids).
      TABLE/SEQUENCE (views are TABLE) map to pg_class and FUNCTION/PROCEDURE/ROUTINE map to pg_proc.
      Every other objtype (schema, database, type, language, ...) yields no rows because it cannot change a relation's or function's ACL.
      The gate then uses PK/UNIQUE index probes on `era`, `updatable_view`, `system_versioning` (history, view)
      and the UNIQUE `func_*` text columns. Functions are rendered with `search_path = pg_catalog`. Verified pitfall:
      under the gate's search_path, `oid::regprocedure::text` drops `public.` and misses managed functions.
    - **If the information is unavailable** (not `SCT_Grant`, NULL istmt, future layout): the accessor
      returns one NULL row and the gate returns TRUE. The full check then runs, as it does today, so the gate fails closed.
    - Rejected alternatives: parsing `current_query()` (fragile). Snapshotting managed ACLs at `ddl_command_start`
      and diffing at end (pure SQL, but costs O(managed objects) per statement and keeps state across triggers).
    - The ACL part of `health_checks()` is also **scoped to the touched families**. Persistence, function-existence
      and ownership checks are skipped for GRANT/REVOKE-only command lists, because a GRANT cannot change any of them.
  - **Defect 2, the comparison** (`src/2h_health_checks.sql` REVOKE check, about lines 292-373): chosen fix is
    **exact propagation**, not an inheritance-aware comparison. There will be ONE mirror definition,
    `__internal_acl_mirror(family)`, giving the expected (object, grantee, privilege) set. Propagation is
    `derived := mirror(base)` and the check is `derived = mirror(base)`, so the two cannot disagree by construction.
    Owner entries are excluded (ownership is synced separately). PUBLIC is grantee 0. Grantor and grant option are ignored.
    - Why exact: role membership changes fire no event trigger (verified: GRANT/REVOKE ROLE are invisible). So an
      inheritance-aware invariant can be broken silently later and then fire on an unrelated statement, which is this incident's class.
      It also leaks data. Verified: REVOKE propagation keeps a `_with_history`/`_history` entry for a role that inherits,
      then `REVOKE ru FROM au` leaves `au` reading `a_with_history` (owner-rights view) without base SELECT.
      An exact mirror depends only on the two ACLs. It is deterministic and dump-stable.
    - Order: for base targets, reconcile (base changes are always legitimate, and this heals pre-existing drift). For
      derived targets, refuse if the post-statement ACL differs from the mirror (today's messages, naming the grantee and privilege).
  - **Protection kept** (the check stays a check): a direct REVOKE/GRANT on a FOR PORTION OF view, current view,
    history table, `_with_history` view or as-of function that leaves it different from the mirror is still refused.
    An inheriting role does NOT excuse a direct REVOKE on the view.
  - **Further defects found (same code, fixed by the same mirror rewrite, each gets a failing test):**
    - F3 GRANT-direction mirror image: inheritance-aware REVOKE propagation keeps derived entries that the exact GRANT
      check then rejects. After `GRANT .. TO au; GRANT .. TO ru; REVOKE .. FROM au` on the base, ANY later GRANT anywhere fails.
    - F4 **privilege escalation**: propagation does `GROUP BY object_type`, issuing one cross-product GRANT for all
      pending (object, role) pairs. Verified: a role granted only on table `a` gained SELECT on `b_history`/`b_with_history`
      and read b's data. Another gained UPDATE on a history table (audit trail becomes writable).
    - F5 after ONE `GRANT SELECT` on a system-versioned table, the history ACL materialises the owner's `arwdDxtm`.
      The GRANT check then raises "cannot grant DELETE to history; history objects are read-only" on EVERY later GRANT
      in the database, and pg_restore of that one-table dump aborts (verified).
    - F6 the function REVOKE branch needs an explicit EXECUTE entry per SELECT grantee, but GRANT propagation adds none
      (PUBLIC EXECUTE satisfies `has_function_privilege`). Any REVOKE anywhere fails once a versioned table has a grantee.
    - F10 `add_for_portion_of_view`/`add_current_view`/`add_system_versioning` on an already-granted table create
      derived objects with default ACLs, so the family is born divergent. They will call the same reconcile.
  - **Decisions for owner:**
    - D1 (**REJECTED by the adversarial review, 2026-10-09 - see AMENDED DESIGN below; do not implement this as written**): as-of functions are SECURITY INVOKER SQL over the `_with_history` view, so EXECUTE confers nothing. An exact
      EXECUTE mirror forces pg_dump to emit `REVOKE ALL ON FUNCTION .. FROM PUBLIC; GRANT ..`, whose transient state a
      refusing check rejects, so every versioned table with a grant becomes unrestorable. Recommend mirror_fn = default
      ACL (PUBLIC EXECUTE, never materialised). 093's "function privileges revoked" rows then flip to true.
    - D2: derived objects with custom names that sort before the base (e.g. `aa_hist` for `zbase`) restore their
      ACL before the base's (verified), and a refusing check aborts. Recommend: accept as a documented limitation now and follow up.
    - D3: AGENTS.md wants side effects reported. Should propagated GRANT/REVOKEs emit `RAISE NOTICE 'sql_saga: ...'` (more expected-output churn)?
  - **Tests that must fail first** (hand-edit expected/*.out to the CORRECT output first, then implement):
    - `093_event_trigger_health_checks`: create a FOR PORTION OF family that is divergent through the public API
      (grant the base, then `add_for_portion_of_view`) as a tripwire. Then an unrelated `REVOKE ALL ON FUNCTION
      .. FROM PUBLIC` and an unrelated `GRANT` must succeed (today: ERROR). A direct REVOKE on a `public.` as-of function must
      still be refused (guards the regprocedure pitfall). A replay of pg_dump's ACL statement order must succeed.
    - `012_acl`: add `periods_acl_3 IN ROLE periods_acl_2`, with grants to both on `fpacl`. Expected view ACL contains
      both (today: periods_acl_3 missing). `REVOKE UPDATE ON fpacl FROM periods_acl_3` must succeed (today: ERROR).
      `REVOKE SELECT ON fpacl__for_portion_of_p FROM periods_acl_3` must still fail. Add a system-versioning section
      for F3/F4/F5/F6 with `has_table_privilege` matrices (F4 shows y_a SELECT on b_history = t today).
  - **Performance** (PG 18.3, 300 FOR PORTION OF families x 20 role grants = 26,400 base ACL rows, plus 50 versioned
    tables. pgbench avg latency):
    | statement | today | gate prototype | triggers off |
    |---|---|---|---|
    | unrelated REVOKE ON FUNCTION | 363 ms | 0.14 ms | 0.05 ms |
    | unrelated GRANT ON TABLE | 169 ms | 0.15 ms | 0.07 ms |
    | unrelated REVOKE ON TABLE | 354 ms | 0.12 ms | 0.22 ms |
    | GRANT on one managed family | 84 ms (full scan) | ~1-1.5 ms when scoped | |
    Comparison query alone, all families: today's nested exact `aclexplode` takes 314 ms, `has_table_privilege` 6 ms,
    set-based exact (`EXCEPT` both ways) 29 ms. Scoped to one family: 1.0 ms vs 1.3 ms. Exact adds about 0.3 ms per
    touched family against has_*, and nothing per unrelated statement. The gate does index probes only (a per-row
    `to_regprocedure` variant measured 1.2 ms and was rejected).
    Plan: add `sql/1xx_benchmark_acl_health_checks.sql` in the 100+ harness, logging unrelated vs related GRANT/REVOKE timings.


### AMENDED DESIGN (SUPERSEDED 2026-10-13 by FINAL DESIGN above; was authoritative 2026-10-09)

An independent agent reproduced every testable claim on PostgreSQL 18.3 and reviewed the plan
above. Full artifact, with commands and output: `tmp/design_a_review.md`. Its probes are preserved
in this repository at `tmp/review-rev18/` (`rv/*.sql`, `cbuild/gproto.c` accessor prototype,
`event_trigger.c`, `log/`); nothing depends on the deleted scratch cluster. Verdict: **approve the
direction, reject D1 as written**. Where this section and the rest of this item disagree, this
section wins.

Confirmed by the review:
- The C accessor is NECESSARY. No pure-SQL route to the GRANT target list at `ddl_command_end`
  exists: `classid`, `objid`, `objsubid`, `object_identity`, `schema_name` are all NULL, and
  `command::text` raises "cannot display a value of type pg_ddl_command".
- Gate effect measured independently: unrelated GRANT/REVOKE ~520-620 ms -> ~0.4-0.5 ms.
- F4 is WORSE than this plan states: a role with only SELECT on one table received `rwd` on
  another table's history table and successfully rewrote the audit trail (`UPDATE h_history ...`).

Amendment 1 - **REJECT D1**. Do NOT define `mirror_fn = default ACL`: that is wrong for hardened
  databases that run `ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC`. The
  function mirror is instead `(g, EXECUTE)` for each base SELECT grantee, with `(PUBLIC, EXECUTE)`
  as pass-through (never added, never removed); the check compares the symmetric difference while
  EXCLUDING grantee 0 for functions. This removes F6 and F11, is dump-stable in both hardened and
  non-hardened databases, and keeps 093's expected output meaningful.

Amendment 2 - **ONE MIRROR PER DERIVED-OBJECT KIND**, written down as a table (the plan's
  "ONE mirror definition" is not enough, and reusing the FOR-PORTION-OF projection for history
  tables would recreate F4):

  | derived object kind | mirror projection |
  |---|---|
  | FOR PORTION OF view, current view | all base `(g, p)`, `g != owner` |
  | history table | `(g, SELECT)` only |
  | `_with_history` view | `(g, SELECT)` only |
  | as-of functions | `(g, EXECUTE)` for base SELECT grantees; PUBLIC pass-through |

Amendment 3 - The derived-object check **ACCEPTS when the base ACL is NULL**. This makes serial
  and parallel restore succeed without a GUC and without weakening live protection. Add a
  `pg_restore -j 4` round trip to the tests: it fails today (F12). This supersedes D2 - the
  derived-before-base ordering no longer needs to be a documented limitation.

Amendment 4 - **Reconcile on `ALTER TABLE ... OWNER TO` as well**, not only GRANT/REVOKE:
  ownership change resets the derived objects' ACLs, and one such ALTER fires this health check
  about sixteen times as the trigger's own statements re-fire it.

Amendment 5 - Gate fallback: use the **`xmin` scan** rather than running the full check when the
  accessor returns the NULL row (~3 ms versus ~30 ms). Optional; the fallback is rare either way.

Amendment 6 - NOTICE policy (this answers D3: **yes**): one NOTICE per reconcile that actually
  changed something, formatted as the exact GRANT/REVOKE executed, prefixed `sql_saga: `. Today's
  propagation is silent, which is how F11 went unnoticed. Expected-output churn stays confined to
  012 and 093.

Also required: state the pre-existing RLS limitation of owner-rights `_with_history` views and
  as-of functions (F14) in the docs and here; and state explicitly that column-level grants are NOT
  mirrored (they live in `pg_attribute.attacl` and are invisible to both today's code and the
  mirror).

New defects found in TODAY'S code, each needing a failing test first:
- **F11** an unrelated REVOKE on any object silently strips `PUBLIC EXECUTE` from the as-of
  functions of every system-versioned table, even tables that were never granted.
- **F12** parallel `pg_restore -j N` of an otherwise-healthy dump fails today.
- **F13** default-privilege-hardened databases materialise EXECUTE per grantee today.
- **F14** owner-rights `_with_history` views and as-of functions bypass RLS (pre-existing).

NOT verified - do not assume: the "~1-1.5 ms when scoped" managed-family timing (no prototype of
  the scoped check body exists, so it is a lower bound only); any PostgreSQL major other than 18.3
  (CI installs PG 18 from PGDG, so the "PostgreSQL 11-16" note in AGENTS.md is stale); whether F11
  also contributed to the original incident sequence.

Order of work: write the failing tests first (093, 012, the F11-F14 cases and the `pg_restore -j 4`
  round trip), record them failing against the current extension, then implement the accessor and
  the per-kind mirror.


## Medium Priority - Refactoring & API Improvements
- [ ] Automate README.md example testing (literate programming approach)
- [ ] Improve test documentation for complex/expected-failure cases

## Future Work & New Features
- [ ] Package with pgxman for easier distribution

# Done

## 2026-02-22: Executor Optimization & Pre-Merge Hardening
- Native Rust executor introspection cache (zero SPI on cache hit)
- Partition-by-NULL optimization for XOR nullable identity columns
- Mutually exclusive partition filters for non-XOR correctness
- PATCH_FOR_PORTION_OF extending-range bug fix
- REPLACE/DELETE_FOR_PORTION_OF extending-range regression tests
- Benchmark EXPLAIN log support for native planner

## 2026-01-22: System Time Feature Complete
- Completed system_time (system versioning) with full test coverage
- Drop/rename protection for history tables, views, query functions, triggers
- Health checks: GRANT/REVOKE/ownership propagation to history objects
- Era-level `ephemeral_columns` (fixes statbus coalescing regression)
- New test: 023_system_versioning_query_functions.sql

## 2026-01-21: temporal_merge ANALYZE Optimization
- Added ANALYZE to temp table before executor DML (5x speedup)
- Fixes PostgreSQL underestimating row counts on temp tables

## 2026-01-20: temporal_merge Performance Optimizations
- Hash join optimizations: `=` instead of `IS NOT DISTINCT FROM` (470-500x speedup for specific queries)
- Pre-extract entity_keys in subquery (30-38% speedup)
- Move DEFAULT column exclusion to planner

## 2026-01-20: Statbus FOR_PORTION_OF Bug Fixes
- Fix name merging bug: compute intersection with OLD row's timeline
- Fix ephemeral column preservation: preserve target values when no source data
- Support explicit NULL assignment via UPDATE_FOR_PORTION_OF mode

## 2026-01-19: Performance Optimization Phase Complete
- Regular DML: ~24,000-45,000 rows/s
- temporal_merge batched: ~2,650-8,310 rows/s
- Removed unused FK trigger columns (fk_insert_trigger, fk_update_trigger)
- Template-based sync triggers: 2.3x faster INSERTs, 2.2x faster UPDATEs
- Eclipse detection optimizations with composite indexes

## 2026-01-18
- Eliminate LATERAL jsonb_populate_record (~48% UPDATE speedup)
- Pre-computed valid_range columns in temporal_merge_plan
- Add compound index (id, valid_range) to unique keys (~20x speedup)

## 2026-01-17
- Split-path approach for resolved_atomic_segments (~10x speedup)
- CASE→OR optimization (36-56% speedup)
- Range-only view tests and same-day DELETE fix
- Makefile `renumber-tests-from` target

## Earlier
- PostgreSQL 18 native temporal features (WITHOUT OVERLAPS, temporal FKs)
- Smart MOVE batching with multirange types
- System versioning implementation
- C-based FK triggers
- Event trigger support
- `[valid_from, valid_until)` period semantics
