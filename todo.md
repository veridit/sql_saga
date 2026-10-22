# SQL Saga - TODO

A living document of upcoming tasks. Planned work is listed first; finished work moves to Done.

## Now - Version 1.4: fix the security and correctness defects found in review, ship CASCADE and TRUNCATE (2026-10-22)

1.3 is tagged and stays. 1.4 is the follow-up release that fixes what review found. StatBus runs 1.0 and moves to
1.4 directly. Order: security first, then correctness, then re-review, then packaging and acceptance, then ship.
Review: `tmp/review_v14_cascade.md` (findings F1-F10). Reproductions: `tmp/journal.md`.

**Security (release-blocking).**
- [x] F1: every sql_saga FK trigger function (uk_delete, uk_update, uk_truncate, regular_fk_check, fk_insert/update)
      verifies it is a managed trigger before acting: catalog row exists, trigger name/table/child table match, args
      equal the canonical args. Otherwise 42501. Also validate column counts against MAX_FK_COLS. Negative test with a
      forged trigger (delete path and the read oracle). Reproduced: attacker deleted another role's row; 1.3 NO ACTION
      args give a key-existence oracle on unreadable tables. DONE: regular_fk_verify_trigger in all four regular-FK
      trigger functions; temporal fk_insert/update_check_c run with caller rights (no escalation). Test 139
- [x] F2: run the cascade DELETE (and prepare its plan) as the child table's owner, as native RI does; child triggers
      then fire as the child owner. Test: child trigger records current_user = child owner. Reproduced: fired as postgres.
      DONE: every lookup/cascade planned and run as the queried table's owner, fire_triggers false (native). Test 139
- [x] F3: every SECURITY DEFINER DDL API checks the caller owns the table it changes (pg_has_role(caller, relowner,
      'USAGE'), as add_system_versioning does), plus REFERENCES on the referenced table for FKs. Affects add/drop era,
      unique key, foreign key, for_portion_of/current views, synchronize trigger. Present since 1.0. Test with a
      non-owner role. Document the behavior change in doc/upgrade.md
      DONE: C require_owner/require_references (outer user, role membership, ACL_REFERENCES) in every management
      function; drop_unique_key/drop_era CASCADE still remove dependent FKs of other owners (native DROP CASCADE).
      Test 140; README "Who may manage a table"; doc/upgrade.md "What changes in 1.4"
- [x] F8: prepare FK plans so a parent writer needs no USAGE on the child schema (follows from F2). DONE, test 139 part 5

**Correctness.**
- [x] F4: partitioned child tables: no `ONLY` for a partitioned child (native rule) in cascade, reference lookup and
      TRUNCATE check; decide inheritance children (refuse or ONLY consistently). Tests
      DONE: add_foreign_key refuses a partitioned referencing table (cloned triggers, unvalidated ATTACH; sql_saga
      supports no partitioned tables); a partition may reference. ONLY on both sides everywhere (lookups, cascade,
      TRUNCATE, add-time validation), as native: inheritance children not covered. Test 141.
      UPGRADE: the 1.4 update must refuse (name them) existing regular FKs whose referencing table is partitioned
- [x] F5: predicated unique keys: include the predicate in key lookups, or refuse add_regular_foreign_key against
      predicated keys. Test
      DONE: add_regular_foreign_key refuses a predicated key (0A000), as the temporal path already does. Test 142.
      UPGRADE: the 1.4 update must refuse (name them) existing regular FKs that reference a predicated key
- [x] F6: temporal_merge with delay_constraints => false cascades on a timeline move; force deferral when a CASCADE
      regular FK references the target, or document exactly. Test
      DONE: the referenced-side regular FK triggers on the target are always deferred during the merge;
      delay_constraints => false keeps only native constraints immediate. Test 137 part 4 (non-overlapping move)
- [x] F9: README says defer by the FK name; the constraint is `<fk>_uk_delete`. Fix wording
      DONE: README names `<fk>_uk_delete` (foreign_keys.uk_delete_trigger)
- [ ] F10 nits: error text "lookup" for the DELETE; wrong "as native" comments; lazy cascade-plan preparation

**Review and release.**
- [ ] Second independent review of all 1.4 changes (security fixes included) until APPROVE
- [ ] 1.4 packaging: control and native version 1.4, freeze 1.3 artifacts in versions/, catalog_format decision,
      direct update scripts from 1.0, 1.1, 1.2 and 1.3 (generator), catalog CHECK and uk_truncate_trigger migration
      with triggers created for existing regular FKs
- [ ] doc/upgrade.md for 1.4 (ownership behavior change, new triggers, CASCADE)
- [ ] Acceptance: full suite incl. benchmarks, generator and native units, upgrade matrix from released tags
      1.0/1.1/1.2/1.3, restore matrix with split 20/20, two-session concurrency
- [ ] Push, green Linux CI, tag v1.4
- [ ] StatBus: upgrade from 1.0 to 1.4, switch six addendum FKs to CASCADE, test 142 asserts it

**Done toward 1.4 (local, not pushed).**
- [x] ON DELETE CASCADE core, concurrency, restore, benchmark, docs (d076080..f46579f)
- [x] TRUNCATE of a referenced parent refused like native FKs (73e61d2, test 138, fast 123/123)
- [x] Independent review: BLOCK with F1-F10; F1, F2, F3 reproduced on a private cluster

## Backlog (after 1.4)
- [ ] Upstream parallel-restore report and patch (section below)
- [ ] Automate README.md example testing (literate programming approach)
- [ ] Improve test documentation for complex/expected-failure cases
- [ ] Package with pgxman for easier distribution
- [ ] Optional: fail closed on direct history-table reads between a parallel restore and its final
      `sync_all_history_policies()`; owner judged it likely unnecessary

## Backlog detail - Upstream PostgreSQL work on the parallel-restore bug (after 1.4, by delegated agents)

Parallel `pg_restore -j` of a database using sql_saga fails intermittently because `pg_dump` does not record the
load order of extension configuration tables that reference each other (`doc/decision-log.md`, 2026-10-13 decision).
The split restore in README.md is the supported workaround. The restore harness keeps the parallel case as a known
failure so an upstream fix is noticed.
- [ ] Delegate to an agent: finalize the `pgsql-bugs` report from the research in `tmp/upstream_pg_dump_fk_order.md`
      and the minimal two-table reproducer in `doc/decision-log.md`; re-run the reproducer on current PostgreSQL
      `master` and 18 before sending
- [ ] Owner approves the report text, then it is sent
- [ ] Kick off a second agent to write the PostgreSQL patch (keep the own TABLE as first archive dependency of TABLE
      DATA and add the referenced tables' TABLE DATA entries), with a pg_dump TAP test, for `pgsql-hackers`
- [ ] When a fixed PostgreSQL is released, turn the harness known failure into a supported case and update README.md
- [ ] Optional, separate report: CHECK constraints that call functions reading other tables
      (`tmp/upstream_pg_dump_check_order.md`, draft not sent). No longer affects sql_saga 1.3, which uses constraint
      triggers; owner decides whether to send it

## Reference - the StatBus ON DELETE CASCADE request (2026-10-21; work tracked under Version 1.4 above)

**Need.** A non-temporal child row that is an addendum to a temporal parent (it has no meaning without the parent) should go with the parent. StatBus has three such tables, `external_ident`, `tag_for_unit` and `unit_notes`, which reference `legal_unit.id` and `establishment.id` (temporal, unique only with the validity range). From sql_saga 1.3 they use `add_foreign_key` (regular-to-temporal), because 1.3's child-side constraint trigger is what makes `pg_dump`/`pg_restore --single-transaction` work (STATBUS-524). Their references to plain tables already use native `FOREIGN KEY ... ON DELETE CASCADE`. For the temporal parents, the only options today are `NO ACTION`/`RESTRICT`, so StatBus ships `NO ACTION` until this exists.

**Today.** `sql_saga.fk_actions` lists `CASCADE`, `SET NULL` and `SET DEFAULT`, but the `sql_saga.foreign_keys` catalog refuses them for both `update_action` and `delete_action` (`CHECK (delete_action NOT IN ('CASCADE', 'SET NULL', 'SET DEFAULT'))`, `sql_saga--1.3.sql`). The parent side is the `uk_delete` trigger (`sql_saga.uk_delete_check_c()`, `src/sql_saga.c`). It refuses a `DELETE` only when a child still references the key and no other parent row has it, so `temporal_merge` and plain DML may reshape a parent's timeline.

**StatBus follow-up once released.** StatBus switches its six regular-to-temporal addendum references (`external_ident`, `tag_for_unit`, `unit_notes` x `legal_unit_id`, `establishment_id`) from `NO ACTION` to `CASCADE` in a migration, and test 142 asserts the cascade.

# Done

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
