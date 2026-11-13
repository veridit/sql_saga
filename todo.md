# SQL Saga - TODO

A living document of upcoming tasks. Planned work is listed first; finished work moves to Done.

## Backlog
- [ ] Batch INSERT through the `current` view like the 1.6 for_portion_of view (it still runs one `temporal_merge` per
      row; its UPDATE and DELETE use plain DML already). Owner decides whether it is needed
- [ ] Upstream PostgreSQL: design and eventually submit native support for regular-to-temporal foreign keys (a child
      without a time dimension referencing a key that exists in a temporal parent's history, including last-row delete
      and CASCADE semantics). Separate project; use the 1.4 C-trigger behavior and parity tests as the semantic
      baseline. Owner agreed 2026-10-26: practical narrow helpers in sql_saga, upstream changes for another time
- [ ] Upstream parallel-restore report and patch (section below)
- [ ] Automate README.md example testing (literate programming approach)
- [ ] Improve test documentation for complex/expected-failure cases
- [ ] Package with pgxman for easier distribution
- [ ] Optional: fail closed on direct history-table reads between a parallel restore and its final
      `sync_all_history_policies()`; owner judged it likely unnecessary
- [x] **Shelved 2026-11-12: temporal-check deferral experiment (STATBUS-591).** No application speedup, not adopted.
      `prototype/defer-temporal-checks` is kept for reference only. Do not merge, release or resume it.

StatBus side (their repository, for reference): pin 1.5 and build Remove on the view delete protocol (STATBUS-611);
switch the six regular-to-temporal addendum references (`external_ident`, `tag_for_unit`, `unit_notes` x
`legal_unit_id`, `establishment_id`) to `ON DELETE CASCADE`, with test 142 asserting it.

## Backlog detail - Upstream PostgreSQL work on the parallel-restore bug (by delegated agents)

Parallel `pg_restore -j` of a database using sql_saga fails intermittently because `pg_dump` does not record the
load order of extension configuration tables that reference each other (`doc/decision-log.md`, 2026-10-13 decision).
The split restore in README.md is the supported workaround. The restore harness keeps the parallel case as a known
failure so an upstream fix is noticed (7/20 failures in the 1.5 CI run).
- [ ] Delegate to an agent: finalize the `pgsql-bugs` report from the research in `tmp/upstream_pg_dump_fk_order.md`
      and the minimal two-table reproducer in `doc/decision-log.md`; re-run the reproducer on current PostgreSQL
      `master` and 18 before sending
- [ ] Owner approves the report text, then it is sent
- [ ] Kick off a second agent to write the PostgreSQL patch (keep the own TABLE as first archive dependency of TABLE
      DATA and add the referenced tables' TABLE DATA entries), with a pg_dump TAP test, for `pgsql-hackers`
- [ ] When a fixed PostgreSQL is released, turn the harness known failure into a supported case and update README.md
- [ ] Optional, separate report: CHECK constraints that call functions reading other tables
      (`tmp/upstream_pg_dump_check_order.md`, draft not sent). No longer affects sql_saga since 1.3, which uses
      constraint triggers; owner decides whether to send it

# Done

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
- Not in 1.6: the `current` view's INSERT still runs one `temporal_merge` per row (backlog)

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
