# SQL Saga - TODO

The record of finished work, newest first. Open work is tracked with hug tasks (branch `hug/tasks`, at
`.hug/tasks/`): `hug task ls`, `hug task next`, `hug tui`.

# Done

## 2026-11-21: Version 1.9, provenance columns (tag v1.9 at fb9b351, CI 44953800430 green; hug s4bvm1)
- Eras can name provenance columns (`add_era(..., provenance_columns => ...)`, `set_era_provenance_columns`): ephemeral
  columns with a DEFAULT that the managed trigger `<era>_provenance` fills, evaluated as the writing role, on every
  changed or new piece; untouched pieces keep theirs, and pieces with different provenance are not merged. Both
  planners strip provenance on changed pieces; the executor marks untouched INSERTs. check_integrity, drop
  protection and rename following cover the trigger and columns. Design doc/design/provenance.md; test 161 runs every
  scenario under both planners
- Catalog: `era.provenance_columns name[]` after `catalog_format`; catalog_format 1.8 -> 1.9. PUBLIC set extended by
  `set_era_provenance_columns` and the new `add_era` signature (test 117)
- Update scripts: `direct_update.py` gains `replaced_routines` (a signature change drops the old routine after
  comparing its EXECUTE privileges with those a new function gets there, read from a probe, as 1.2 did; any difference
  refuses with the reset and reapply statements) and `commented_routines` (comments from src, then the public
  functions' copy). The 1.1 privilege cases now meet two refusals when default privileges changed (add_system_versioning,
  then add_era) and follow both hints. Preflight accepts the 1.8 shape and lists the add_era privileges it would refuse
- Harness fixes found on the way: v1.0's Makefile does not build its native library, so the 1.0 fixtures loaded the
  candidate's (which reads `provenance_columns`); `upgrade_test.sh` now builds the tag's own. The old-dump
  comparison sorted rows within each COPY block (heap order differs after the 1.9 catalog UPDATE)
- Acceptance (local): 146 fast regressions; generator suite 1.4-1.9 and make wiring; upgrade paths 1.0..1.8 -> 1.9
  GREEN, 0 FAIL; restore round trips 12 cases plus old dumps 1.0, 1.1, 1.2, 1.7, 1.8. Benchmarks 100 and 101, v1.8
  then 1.9, two runs each (load average 23-28): temporal_merge PATCH 263-291 ms against 266-266 ms (parent) and
  274-277 against 277-277 ms (child); seeds 102-107 against 102-103 and 119-120 against 116-120 ms; DML steps within
  1-3 ms except one 1.9 outlier (Child INSERT with index 73 ms in run 1, 45 ms in run 2, 45 ms both v1.8 runs)
- doc/upgrade.md describes 1.9, the 1.8 guide is frozen as doc/upgrade-1.8.md

## 2026-11-18: Version 1.8 (tag v1.8 at ea6542e, CI 44390659266 green)
- `sql_saga.era` loses `audit_schema_name` and `audit_table_name` (left from the periods extension, never written).
  The update drops them only when every row is NULL; catalog_format 1.4 -> 1.8, so a plain restore of an older dump
  is refused at the era COPY (it names the dropped column) and the documented old-dump procedure restores it
- `ALTER TABLE <history> SET UNLOGGED` on a system-versioned table's history is refused at once
  (`Run: ALTER TABLE X SET LOGGED;`). The old check read the never-populated columns and never fired.
  `check_integrity()` reports an unlogged history table (event triggers bypassed) with that repair. Test 160
- temporal_merge builds its key-column SQL in declared order. `string_agg` over `unnest(columns) JOIN pg_attribute`
  had no ORDER BY, so the generated join condition followed the join strategy (test 063 failed in CI runs
  42399588469 and 44353986606); a probe toggling hash, merge and nested-loop joins gave two orders before, one after
- Update scripts: `direct_update.py` gains a schema fragment and `changed_tables` (a catalog table change outside them
  fails generation). The fragment runs after the routine delta: the DROP COLUMN fires the event triggers, which in
  the first matrix run still ran the 1.7 health check and failed every path. Preflight accepts 1.7 and lists audit
  values and unlogged history tables. Frozen v1.7 artifacts built twice from bc0fc53, byte-identical
- Native library built unstripped: before Rust 1.98, cargo's strip leaves the Mach-O string pool 4-byte aligned and
  macOS 27's dyld refuses it (rust-lang/rust#157750); upgrade and restore harnesses build released tags the same way
- Acceptance (local, private stage): 145 fast regressions; generator suite and make wiring; upgrade matrix 1.0..1.7
  -> 1.8 GREEN, 0 FAIL; restore round trips 12 cases plus old dumps 1.0, 1.1, 1.2, 1.7. Benchmarks: 111 scaling
  flags one doubling step at random under load average 350-415 (45 users); A/B back to back, v1.7 then 1.8, three
  rounds: 0/6 flagged each, 39.9/47.0/47.6 s against 39.0/49.3/48.1 s. Other 12 benchmarks pass
- doc/upgrade.md describes 1.8, the 1.7 guide is frozen as doc/upgrade-1.7.md; every tmp/ design citation in installed code
  now points at doc/design/ (five comments in four routines, hug bqtzce)

## 2026-11-16: hug tasks and design documents (no release; nothing installed changes)
- Open work moved from todo.md to hug tasks (branch `hug/tasks`); AGENTS.md, CLAUDE.md and CONVENTIONS.md say how.
  Memory was already wired (771 project memories)
- `doc/design/`: five reviewed designs (history access, manual schema changes, drop protection and check_integrity,
  restore, catalog column order) with an index, checked against the 1.7 code. Citations in src, sql, expected, devops
  and doc point there; 5 inside function bodies released in 1.7 wait for the next change to them (hug bqtzce).
  Cited benchmark kept as `devops/bench_regular_fk_cascade.sh`. CI 43872663796 green on 74b0212
- The local PostgreSQL 18 server had stopped on 2026-11-15 (system out of file descriptors); restarted, benchmark
  settings intact

## 2026-11-15: Clean-code pass after 1.7 (no release; nothing installed changes)
- One direct-update builder, `devops/direct_update.py`; `generate_update_scripts_1_6.py` and `_1_7.py` now hold only
  their release data (about 130 duplicated lines gone). Proof: the 1.6 file on the v1.6 tree reproduces all six frozen
  `versions/sql_saga--*--1.6.sql` byte for byte, and the eight 1.7 scripts keep their SHA-256. 1.4 and 1.5 generators
  stay as released. Makefile keeps only the 1.7 rule (90 unreachable lines for 1.2-1.6 removed; old releases build
  from their tags). Maintainer release recipe added to `doc/upgrade.md`
- Removed 12 tracked expected files no test produces (renamed tests, alternates for the removed plan cache) and 3
  untracked empty ones; `__pycache__/` ignored; merged branches deleted locally and on GitHub (current-view-batch,
  for-portion-delete, view-batch-merge, rust-sweep-line-planner, speedup). Kept: `prototype-valid-ranges` (2 commits
  not in master), `prototype/defer-temporal-checks` (shelved, reference) and the 10 old stashes
- `tmp/` 1.4 GB / 28,065 entries archived to `/Users/jhf/ssb/sql_saga_archive/sql_saga_tmp_2026-11-15.tar.zst` (148 MB,
  listing verified), then pruned to the entries tracked files cite plus current evidence (2.4 MB). On 2026-11-16 the
  design notes moved to `doc/design/` and their tmp/ copies were removed (still in the archive). Two stale git
  worktrees in tmp/ (Oct 17/21) unregistered; their uncommitted diffs kept as `tmp/worktree_*_uncommitted.patch`
- Not done, by decision: one shared helper for the two view apply triggers. It would have to be executable by PUBLIC
  (both triggers run as the caller), widening the PUBLIC function set test 117 pins; it changes released routine
  bodies, so it needs a 1.8 release; and the shared part is about 25 lines of two differently shaped triggers
  (two ordered queues vs one, two modes vs one, ephemeral columns vs founding id)
- AGENTS.md test slots through 159; real test names in AGENTS.md and CONVENTIONS.md examples

## 2026-11-13: Version 1.7 (tag v1.7 at bc0fc53, CI run 43446262515 green)
- Batched INSERT through the current view, consistent with the 1.6 for_portion_of view: the row trigger only queues
  each new row (starting now), and one `AFTER INSERT FOR EACH STATEMENT` apply trigger (`<row trigger>_apply`)
  applies the statement with one MERGE_ENTITY_UPSERT `temporal_merge`, each row its own founding id. Results equal
  1.6 row by row (new entity, existing entity, same entity twice: last row wins, RETURNING). 300 rows 60-74 ms
  (1.6: 6.2-7.3 s); single-row cost unchanged. UPDATE and DELETE through the view unchanged. Test 159
- Nested insert through the same view refused (`object_in_use`); missing apply trigger refused; check_integrity,
  drop protection, trigger-rename following and view reshape know the apply trigger
- Direct update scripts from 1.0-1.6 (frozen v1.6 artifacts, two builds byte-identical,
  `devops/generate_update_scripts_1_7.py`); the update adds the apply trigger to existing current views in place
- Acceptance: 144/144 regressions; local private-stage upgrade matrix 2,742 checks, 0 failures; branch CI
  43440617841 green (generator, regressions, restore, seven released-binary upgrades, concurrency); benchmarks 13/13
  after re-running 111 twice (one scaling step flagged under load average ~50, passed twice at ~25); 1.6 performance
  baselines kept. Evidence: `tmp/journal_current_view_batch.md`, `tmp/cv_insert_perf_1[67].out`

## 2026-11-13: Version 1.6 (tag v1.6 at 252c0f7, CI run 43425840814 green)
- Batched writes through the for_portion_of view: the row trigger only queues each matched row's portion, and one
  `AFTER UPDATE FOR EACH STATEMENT` apply trigger (`<row trigger>_apply`) applies the statement with one
  `temporal_merge`, removals then changes. 300 rows about 0.1 s (1.5: 2.9 s), 3,000 rows about 1.2 s, linear at about
  0.4 ms/row; single-row cost unchanged. Test 158
- A base-table trigger writing through the same view is refused (`object_in_use`); the row trigger refuses when the
  apply trigger is missing; check_integrity, drop protection and rename following know the apply trigger. Owner: code
  on the base table uses temporal_merge directly, the view is the simple interface (PostgREST)
- Direct update scripts from 1.0-1.5 (frozen v1.5 artifacts, `devops/generate_update_scripts_1_6.py`); the update adds
  the apply trigger to existing views in place (view OID, grants, other triggers and data kept). Catalog format 1.4
- Acceptance: CI 43420195486 (branch) and 43425840814 (master head): 143/143 regressions, all six released-binary
  upgrades GREEN, restore round trips, concurrency; generator 53 tests plus wiring; benchmarks 14/14 linear, within
  noise of the 1.5 baselines. Evidence: `tmp/journal_view_batch.md`, `tmp/v16_*`

## 2026-11-13: Version 1.5 (tag v1.5 at 545839a, CI run 43397369063 green)
- for_portion_of view delete protocol (STATBUS-611): `UPDATE view SET id = NULL, valid_from = S, valid_until = E
  WHERE id = 10` removes the record over [S, E) with `DELETE_FOR_PORTION_OF`; no dates removes the whole matched
  history. Composite identities use OLD's values. Changing another column at the same time is refused as ambiguous.
  The caller needs DELETE on the table plus the UPDATE/INSERT a split performs; RLS applies. Fixes the 1.4 silent
  no-op (`UPDATE 1`, nothing changed). Test 157
- Direct update scripts from 1.0, 1.1, 1.2, 1.3 and 1.4; frozen 1.4 artifacts; catalog format stays 1.4; portable
  fail-closed scan refusing cascading DDL in migrations
- Acceptance: CI 43320411030 and 43397369063: 142 regressions, 5 native, 42 generator/safety plus wiring, all five
  released-binary upgrades, existing 1.4 views upgraded in place (OIDs, grants, data kept), six split restores 20/20,
  concurrency suites. Ordinary view UPDATE +1.16% (219.0 vs 221.5 ms for 30 rows). Evidence:
  `doc/for-portion-delete-validation.md`

## 2026-10-27: Version 1.4 (tag v1.4 at 8759b7b)
- Regular-to-temporal foreign keys: `ON DELETE CASCADE`; TRUNCATE of a referenced parent refused, as native
- Security from three review rounds: sql_saga runs actions on user objects as the calling user (D0/D1), so
  PostgreSQL decides permissions; public entry points are caller-recording C trampolines over private workers; FK
  triggers refuse foreign use; history written as the history table's owner; no SECURITY DEFINER with a writable
  search_path; predicates canonicalized; FORCE RLS parity at FK creation (Q7). Release gate test 149: allows and
  refuses exactly what PostgreSQL does, for every kind of user
- Correctness: partitioned/predicated referencing refused, per-transaction FK verification, kept plans freed, XOR
  MATCH SIMPLE documented, messages name tables schema-qualified
- Direct update scripts from 1.0-1.3. Acceptance: 141 regressions, native 5, generator 28, all four upgrade paths,
  six split restores 20/20, concurrency suites; drop gate 13.5 ms (<15 ms). Evidence:
  `tmp/release_1_4_accounting.md`, `tmp/bench_d1_final.md`

## 2026-10-21: Version 1.3 (tag v1.3 at 102e0df, CI run 39587241942 green)
- XOR unique-key constraint order preserved and FK columns paired in that order; historical mispairing diagnosed with
  executable repairs
- Regular FKs enforced by deferred child-side constraint triggers instead of CHECK helpers (restore-safe, REPEATABLE
  READ orphan race closed, bounded two-session concurrency harness in CI)
- Managed views follow ADD/DROP/RENAME/TYPE column changes, keep the parsed current-clock binding, refuse user
  dependants precisely; both view types removed on cascading era removal
- Native planner/executor caches invalidated by structured fingerprints on schema changes, rollback and other-session
  DDL (one-row merge +0.42 ms measured)
- Catalog-format safeguard refuses wrong-version restores; integrity checks cover event-trigger shape and enablement;
  unmanaged history policies named with a quoted manual repair
- Direct generated update scripts from 1.0, 1.1 and 1.2 (authenticated frozen inputs, read-only preflight, final
  integrity gate); 1.0 canonical-ACL guard refuses custom privileges with an owner-controlled recovery
  (`doc/upgrade.md`)
- Acceptance: three-version upgrade matrix 468/478/426, restore 15/15 with every split case 20/20, full regression
  and benchmarks 134/134, generator 56, native 5. Evidence: `tmp/fable_release_evidence_final.md`,
  `tmp/v13_upgrade_harness/logs/final2_claims.txt`. Linux CI caught a GNU make 4 sub-second mtime bug, fixed in
  102e0df before tagging
- No external release notice sent (needs separate owner approval)

## 2026-10-20: Version 1.2
- Drop protection judged per statement (no statement-scoped race); unforgeable policy-sync guard
- `check_integrity()`, attribution-based tolerant drops; final integrity gate in the 1.1/1.0 to 1.2 update
- Column snapshot for system-time families (restore column order compared as a set)
- Managed relation rename/move refused with the safe move sequence; `history_table_must_exist`
- Every system versioning create/drop announced with a NOTICE
- Renames followed: FK child columns, referenced key columns (helper regenerated), era range/boundary columns
- Shared regular-FK helpers upgrade cleanly (1.0 and 1.1); quoted helper names can be shared
- Security: history policy/RLS protection no longer bypassable via `sql_saga.managing_history_policies` (1.1 bug)
- Parallel restore without deadlocks (`sql_saga.parallel_restore`, `sync_all_history_policies()`); the documented
  serial/single-transaction workaround for regular-FK CHECK load order was later found insufficient, prompting 1.3
- Upgrade refuses customised `add_system_versioning` privileges with a whole-set hint; exact pre-upgrade check is a
  rolled-back `ALTER EXTENSION UPDATE` (`doc/upgrade.md`)
- Drop API timing harness (15 ms ceiling), upgrade/restore harnesses extended

## 2026-10-15: Privileges, RLS on history, restore, manual schema changes
- ACL health check replaced by access-time privileges (STATBUS-481/482 REVOKE incident fixed; GRANT/REVOKE need
  no action): derived views security_invoker and granted to PUBLIC, gated history view, history RLS
  with token-named policy copies, FORCE mirrored, fail-closed read check, `sync_history_policies` (af8998c)
- P1 shared plan cache removed (7e107de); P2 current-view INSERT on range-only primary key (c48af0a)
- write_history crash with differing column positions (1a18968)
- User policies on history refused, also when created with event triggers off (de87212)
- Plain restore of a regular FK plus system versioning (0e994cc)
- `pg_restore --clean` supported, opt-in `sql_saga.clean_restore` for psql-fed scripts, hardened regrant (b1ed797)
- Manual schema changes: by-name history writer, `rebuild_system_versioning_views`, actionable errors (2b442f4)
- Relation names in messages quoted once (79cdb0d)
- `sql_saga_native` is an internal library, no control file, no cargo-pgrx (75a7bff)
- Parallel restore documented as a PostgreSQL pg_dump limitation with a workaround (347027b)
- Regular FK helper runs as the referenced table owner, PUBLIC-executable; no parent SELECT needed (eac0ca8)
- Version 1.1 with `sql_saga--1.0--1.1.sql`, frozen v1.0 script in `versions/`, preflight, `doc/upgrade.md`,
  `make test-upgrade` (ae56001); benchmark baselines refreshed (ce66fee)
- CI on Linux runs native unit tests, regression, restore round trips and the upgrade harness
- `devops/benchmark_settings.sh` enables or disables the benchmark server settings (539b89a)

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
