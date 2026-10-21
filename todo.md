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
- [x] Replace regular-FK child CHECKs with deferred constraint triggers and validate lifecycle/merge/restore behavior (efd2aed, independent core/lifecycle G4, exact staged fast suite119/119)
  - [x] Queue checks for surviving rows after a deferred key check followed by a payload-only UPDATE
  - [x] Prevent concurrent parent deletion from leaving an orphan under REPEATABLE READ
  - [x] Refuse ambiguous constraint-name deferral before merge DML
  - [x] Refuse noncanonical child-trigger replacements and preserve unrelated same-name constraints
  - [x] Adapt historical helper/CHECK fixtures without weakening pairing or enforcement assertions
  - [x] Reuse one private child-trigger creator and report automatic rebuilds with retained firing modes
- [x] Follow supported column type changes in managed views, with clear refusals for era columns and user dependants (5052c5f, 132, independent G4)
  - [x] Report pending views truthfully after DROP or TYPE and prove rollback leaves no pending rows
- [x] Invalidate native planner/executor metadata after target type changes, including rollback and other-session DDL (133, independent G4)
  - [x] Use structured fingerprints so quoted names cannot collide with encoded column lists
- [x] Freeze the original tagged 1.2 install and direct-update artifacts without changing released files
- [x] Accept released 1.2 in the read-only preflight, with independent frozen-layout and unchanged-fingerprint validation (effc130)
- [x] Package the real 1.3 control/native version and verify install/native/safeguard tests plus all direct paths (e994fe0, private 3/3)
- [x] Build direct 1.0, 1.1 and 1.2 to 1.3 update scripts (58c76bd, independent actual three-baseline G4)
  - [x] Authenticate frozen inputs and test deterministic generation, SQL splitting and signature checks (5c3b0ad, independent G4, 43/43 and GNU make wiring)
  - [x] Generate unchanged-signature replacements without ACL replay and require explicit new-routine privileges (actual upgraded ACL parity passes; the historical 1.0 grant path has its separately reviewed canonical-ACL guard)
  - [x] Preserve 1.1/1.2 stale-prefix managed-view identity, grants, comments, dependents and extension membership during UPDATE; refuse 1.0 user dependents precisely
- [x] Order XOR/view/FK repairs and the catalog-format migration before the final integrity gate
  - [x] Independently review and execute the actual three assembled migrations, including exact CHECK attribution and absence/ambiguity refusals
  - [x] Verify every automatic repair/create/drop NOTICE describes exactly what the migration did (independent final-fragment audit, including final preamble-only correction)
  - [x] Prove all-baseline routine/family ACL parity, original event-trigger firing modes and atomic rollback on the specified refusals (final dd32e40 matrix against guarded package62754bd: 1.0 468/468, 1.1 478/478, 1.2 426/426; wrapper exit0).
  - [x] Resolve the reopened direct-1.0 privilege conflict without guessing intent. Released 1.0 has no original pg_init_privs for the eight routines, and historical GRANTs can hide named site grants from pg_dump. The canonical-ACL guard now fails closed with explicit owner-controlled normalization/update/replay outside the extension script, not a new switch or automatic privilege replay (62754bd, final all-baseline acceptance passes).
    - [x] Specify and independently review exact-eight normalized ACL refusal, read-only guard ordering, owner-controlled recovery and actual customized dump/restore evidence (3b6d171 RED, independent 56 tests with nine intended failures).
    - [x] Reproduce and specify refusal of extra user overloads reached by the historical six-name grant loop, without changing or granting privileges on those user routines (same RED, actual atomic refusal and manual rename/update/rename-back dump/restore prototype).
    - [x] Complete generator-only GREEN and independent actual-install G4, preserving historical grants verbatim and 1.1/1.2 scripts byte-identically (62754bd, 56/56 units, seven atomic refusals, nine independent actual dump/restores, canonical initial privileges equal fresh 1.3). The maintained history-ACL manual route also passes in the final matrix.
    - [x] Retain maintained 1.0 hardened/custom refusal and explicit owner recovery, grant-option dump/restore, and complete multiline hardened ACL inventory with a later-row negative control (dd32e40, independent G2). Intentional PUBLIC helper exceptions are pinned to the reviewed 117 signature list, not whatever a fresh install exposes.
    - [x] Prove the explicit owner/admin workaround for released 1.0 health_checks rejecting normalization GRANTs when history ACLs exist: suspend only sql_saga's own health-check event trigger during those statements and restore its exact prior mode before UPDATE (dd32e40, independent addendum). The current guarded package passes one-transaction recovery and injected late-error ACL/mode/version rollback. Named-grant dump/restore and O/A/R/D behavior have separate, explicitly scoped evidence. The runbook is reviewed and the complete matrix passes; no broad replication-role bypass is used.
  - [x] Correct and re-review the exact ACL oracle for the same hardened defaults as the core fixture and intentional removal of attributable regular-FK helper routines, retaining multi-helper/retained-helper/unrelated quoted-name/grant-option negative controls (bd2ebbc, fix8 independent G2 and actual focused controls; final three-baseline matrix passes).
  - [x] Retain the healthy 1.0 base-GRANT-before-managed-views bootstrap regression in the maintained tag matrix (46dcf17, independently reviewed; all three focused baseline probes pass)
  - [x] Preserve the original custom current-function binding during managed-view ADD/DROP/TYPE rebuilds and current-view writes (b71ce1e, independent G4, actual 121/121 fast tests and final three-baseline acceptance; caller path preserved, two read-only invoker clock helpers explicitly whitelisted in 117)
  - [x] Correct the maintained matrix's empty-trigger-attribute renderer, controlled pre-sort XOR construction, stale-view base grants and lowercase event-test database names (59fe389, independent five-change G2, actual fixture evidence and final three-baseline matrix pass)
  - [x] Name unmanaged user history policies and give an actionable manual move/drop-and-sync repair without touching the policy automatically (7d6dbef, RED136/G2 and independent private G4; quoted executable repairs, full policy snapshots, unchanged missing/stale diagnostics and checker ACL)
  - [x] Review the released-1.2 tampered-helper conversion refusal as a baseline-specific valid diagnostic, retaining exact object names, manual repair and atomicity checks (59fe389, independent G2, actual55000 and executable HINT retained)
- [x] Test old dumps at their own version, wrong-version restore rejection, orphan refusal and atomic upgrade rollback (final guarded-package restore matrix15/15, every supported split case20/20; actual restore exit0).
- [x] Validate tag-built StatBus-shaped databases through upgrade, single-transaction restore and split parallel restore (final dd32e40 matrix against guarded package62754bd: 1.0 468/468, 1.1 478/478, 1.2 426/426; actual upgrade and restore exit0, KEEP=1 evidence retained). Unsupported whole-archive parallel restore still has the documented upstream catalog-load-order limitation, observed2/20 failures; it is not counted as supported acceptance.
- [x] Measure native cache correctness-check cost with actual matched builds: one-row merges +0.4165ms (+7.61%);1000-row medians +2.12% with wider noise (documented limits)
- [x] Finish 1.3 API/runbook documentation and full assembled-package regression/native tests
  - [x] Review and commit the 1.3 runbook contract, preserving the old-dump restore block verbatim (3cd6315)
  - [x] Run reviewed-source native unit tests (5/5)
  - [x] Pass the complete real-1.3 regression and benchmark suite in an isolated snapshot before the binding fix (132/132, tmp/coord/full13_0004/test_all_green.log)
  - [x] Rerun focused and full assembled-package regression after the binding, policy and privilege corrections (ae23109, private final package: 134/134 including all benchmarks, generator50/50, native5/5; tmp/coord/post_binding_full/test_all.log)
  - [x] Integrate the eight reviewed intentional benchmark102 native-FK NOTICE lines and immediately rerun the affected test (f5cd490, 001/102 2/2)
  - [x] Compare generated API documentation with the committed copy after final package regression (byte-identical, ae23109)
  - [x] Build/install the committed guarded package in a fresh private snapshot (62754bd, tmp/coord/acl_guard_package_0146): generator56/56, native5/5, fast121/121, actual control/native1.3 and API identity. Source/native/regression inputs and installed fresh/1.1/1.2 SQL are unchanged from the prior134/134 package; this is equivalence evidence, not a second full benchmark run.
  - [x] Reconcile runbook with actual final migration/restore outcomes, preserving the marked old-dump restore block (05bf941 reviewed canonical-ACL and health-trigger recovery wording; post-pass edit removes only the pending-result sentence, with the protected block hash unchanged).
  - [x] Integrate reviewed bounded two-session FK concurrency harness in Makefile and disposable CI (d4bba39, c8c2862; independent 35/35 plus timeout/signal/poisoned-environment controls; Ubuntu CI execution awaits push approval)
  - [x] Execute the public Makefile concurrency target on the integrated candidate (35/35, tmp/v13_designBtests/rfc_make_run.log)
- [x] Obtain independent final coverage review, close known scoped defects and clean up private test clusters
  - [x] Obtain Fable's source/test coverage map with missing execution evidence explicitly distinguished from defects
  - [x] Update that map after actual released-tag upgrade and restore acceptance (tmp/fable_release_evidence_final.md and tmp/v13_upgrade_harness/logs/final2_claims.txt, with actual exit codes and log-line references; historical evidence is distinguished from current guarded-package results).
  - [x] Stop remaining task-owned clusters and inspect the final scoped git diff/status (Rat removed146 owned databases/43 roles under exact logged prefixes, retained WORK/logs/dumps and stopped5475; coordinator process inspection shows only the untouched shared5432 server. Product inputs are unchanged from guarded package62754bd, frozen versions hashes match, API output is unchanged, protected restore block hash is unchanged).
- [ ] Obtain owner approval before pushing or tagging the release and before sending external notices

Final local acceptance evidence: tmp/v13_upgrade_harness/logs/final2_upgrade_full.log (SHA256 c2bff34423475ec42a9ef99d54ad1884269af2335e484d0edafad06f79f14e7a), final2_restore_full.log (df858b707cfa170b713146aa094f2047a22ccad4e31804a5db9a08968caa59d1), and final2_claims.txt (7a73cb7781a0bfbe32bf245ed46fcac15d5527203d3f9abf7ce2c2a7d1dbb2d5). Acceptance harness is dd32e40, guarded package source62754bd. Only documentation/tracker commits follow the frozen acceptance harness; product/package inputs are unchanged. Local implementation and acceptance are complete; no push, release tag or external notice has been made. Linux CI execution remains a post-approval gate before tagging.

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
