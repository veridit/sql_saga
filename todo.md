# SQL Saga - TODO

A living document of upcoming tasks.

## Current Priority - Version 1.3 release in progress

Completed and regression-tested on the development branch:
- [x] Preserve XOR unique-key constraint order and pair foreign-key columns in that order
- [x] Diagnose historical foreign-key mispairing with executable, option-preserving repairs
- [x] Managed views follow ADD, DROP and RENAME COLUMN while preserving view state and refusing user dependants
- [x] Integrity checks cover event-trigger shape, membership and enablement, with tested repair hints
- [x] Reject old catalog rows restored into the wrong version with the named catalog-format safeguard
- [x] Fix quoted relation lookup and ephemeral/key-consistency metadata renames
- [x] Remove both managed view types on cascading era removal, preserve user synchronization-function dependants and give ordered teardown guidance (134, independent G4)

Release gates still open:
- [ ] Replace regular-FK child CHECKs with deferred constraint triggers and validate lifecycle/merge/restore behavior
  - [ ] Queue checks for surviving rows after a deferred key check followed by a payload-only UPDATE
  - [ ] Prevent concurrent parent deletion from leaving an orphan under REPEATABLE READ
  - [ ] Avoid deferring unrelated constraint triggers with the same schema-qualified name
  - [ ] Refuse noncanonical child-trigger replacements and preserve unrelated same-name constraints
  - [ ] Adapt historical helper/CHECK fixtures without weakening pairing or enforcement assertions
- [ ] Follow supported column type changes in managed views, with clear refusals for era columns and user dependants
- [ ] Invalidate native planner/executor metadata after target type changes, including rollback and other-session DDL
  - [ ] Use structured fingerprints so quoted names cannot collide with encoded column lists
- [x] Freeze the original tagged 1.2 install and direct-update artifacts without changing released files
- [ ] Build direct 1.0, 1.1 and 1.2 to 1.3 update scripts
  - [ ] Authenticate frozen inputs and test deterministic generation, SQL splitting and signature checks
  - [ ] Preserve existing routine ACLs and apply explicit privileges only to new routines
  - [ ] Preserve managed-view identity, grants and extension membership when repairing stale views during UPDATE
- [ ] Order XOR/view/FK repairs and the catalog-format migration before the final integrity gate
- [ ] Test old dumps at their own version, wrong-version restore rejection, orphan refusal and atomic upgrade rollback
- [ ] Validate tag-built StatBus-shaped databases through upgrade, single-transaction restore and split parallel restore
- [ ] Finish 1.3 API/runbook documentation, full regression/native tests and performance checks
  - [ ] Run the deterministic two-session FK concurrency harness in the Makefile and CI on an explicitly identified disposable cluster
- [ ] Obtain independent final coverage review, close known defects and clean up private test clusters
- [ ] Obtain owner approval before pushing or tagging the release and before sending external notices

## Medium Priority - Refactoring & API Improvements
- [ ] Automate README.md example testing (literate programming approach)
- [ ] Improve test documentation for complex/expected-failure cases

## Future Work & New Features
- [ ] Package with pgxman for easier distribution
- [ ] Report the pg_dump load-order gaps upstream (extension configuration tables, and regular-FK CHECK helpers that
      read other tables), owner approves the text first
- [ ] Optional: fail closed on direct history-table reads between a parallel restore and its final
      `sync_all_history_policies()` (restrictive pending policy); not built, owner judged it likely unnecessary

# Done

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
