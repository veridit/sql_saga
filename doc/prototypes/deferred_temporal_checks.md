# Transaction-local temporal FK deferral prototype

Status: experimental branch `prototype/defer-temporal-checks`, STATBUS-591.
This is **not a release** and not part of the published `v1.4` tag. The owner decides
whether to adopt it after reviewing measurements. StatBus is not modified here.

**Final measured source: `956ce96`.** Concurrent toggle/defer pair medians are
3.524/2.167 seconds including COMMIT, a 38.5% wall-time reduction over six paired
rounds. Sequential deferral is 10.5% slower and ordinary enabled checks remain faster.
See **Final-source remeasurement** below for costs and full-release controls.
Full Linux CI **42403855569** passed on packaging follow-up `1018d3e`, with the
measured runtime SQL unchanged. This closes the prototype validation gate, not adoption.

## Proposed public call

```sql
BEGIN;
CALL sql_saga.defer_temporal_checks('app.parent'::regclass, 'app.child'::regclass);
-- Multiple temporal_merge calls, or ordinary authorized DML.
COMMIT;
```

Call once near the beginning of an explicit transaction. Listing either endpoint
selects the sql_saga foreign-key relationship, including its child check and
referenced-side checks. Repeated calls accumulate their selections. Tables without
sql_saga foreign keys and empty arrays are harmless. NULL tables are rejected.

PostgreSQL queues the existing deferrable constraint triggers and native temporal
FK checks. It validates them at COMMIT against the then-visible final state, or
earlier if the caller explicitly runs `SET CONSTRAINTS ... IMMEDIATE`. A failed
COMMIT aborts the transaction. Its existing FK SQLSTATE and diagnostics are retained.
This changes **when**, not **whether**, checks run. It is not an unchecked bulk load.

The existing `disable_temporal_triggers` and `enable_temporal_triggers` APIs are
unchanged. Unlike disabling and re-enabling triggers, this call issues no shared-table
DDL. Trigger-toggle DDL is transactional too. Its demonstrated concurrency problem
is the shared-table lock held until commit, not visibility of an uncommitted change
in another session.

## Executor integration

A wrapper issuing only SET CONSTRAINTS is insufficient: released temporal_merge
forces the constraints it selected IMMEDIATE after each merge call. The rollback-only
reproducer in `tmp/defer_initial_proof.log` demonstrated error 23503 inside that reset,
not at transaction end.

The prototype records explicitly selected constraint OIDs with SET LOCAL in
`sql_saga.temporal_merge.commit_deferred_constraints`. The executor excludes those
OIDs from its end-of-call IMMEDIATE reset. Its default behavior for other constraints
is unchanged. The registry is only a checking-moment preference, not an authorization
credential, and does not replace PostgreSQL's trigger queue. Do not set this internal
parameter directly. An explicit IMMEDIATE check drains the queue but does not cancel
the transaction's opt-in for later merge calls.

Both the registry and native constraint modes follow SAVEPOINT rollback and disappear
at transaction end. The API is SECURITY INVOKER, needs no management elevation and
confers no table privileges. Normal DML permissions and RLS continue to apply.
The caller needs schema USAGE, EXECUTE on the procedure and SELECT on sql_saga's FK
and unique-key catalogs, as it does for existing invoker merge facilities.

Constraint names are schema-qualified. PostgreSQL SET CONSTRAINTS addresses all
constraints with a matching name in that schema. The prototype refuses an ambiguous
selection rather than silently deferring unrelated constraints. It also rejects
missing, disabled or non-deferrable expected checks. Concurrent schema changes are
not an intended workload. There is no custom C check queue or arbitrary elevated SQL.

## Important boundaries

- Regular-to-temporal NO ACTION checks test final key existence. A temporary gap that
  is repaired before COMMIT is accepted. Invalid final references are rejected.
- Regular-to-temporal CASCADE remains final-key based. With this opt-in, its deferred
  trigger performs the delete at validation time when the key is absent. Code that
  needs to observe the cascade earlier must force IMMEDIATE or not opt in. This is
  deliberate timing behavior, not equivalence to observing every intermediate statement.
- TRUNCATE protection stays immediate. PostgreSQL TRUNCATE triggers are statement-level,
  whereas deferrable constraint triggers must be AFTER ROW. temporal_merge uses
  DELETE/UPDATE/INSERT on managed targets, so it does not need a deferred TRUNCATE guard.
  Truncating temporary planner tables is unrelated.
- Uniqueness, range validity, NOT NULL, synchronization triggers and arbitrary user
  triggers are not deferred by the new call. Native CASCADE action timing is not
  rewritten. Only the registered FK checks are selected.
- Independence matters. Same-key writes, dependencies on another batch's uncommitted
  parents, GiST/page contention and normal FK row locks can still wait or fail. The
  target is removal of toggle-induced table-lock serialization for disjoint keys,
  not a promise that every concurrent transaction is contention-free.
- Deferred validation still performs per-event trigger work, not one deduplicated
  set-based check per key. Commit latency and trigger-queue memory can grow with a batch.
- PostgreSQL MVCC applies. "Final state" means the state visible under the current
  transaction's isolation rules, with existing RI race detection, not omniscient
  visibility of other sessions' uncommitted writes.

## Reproducible checks and measurements

Run tests before benchmarking so they do not compete for resources. All shell commands
below are single-line commands and save their output.
Use an isolated test PostgreSQL installation for these commands: `make install` on
this branch installs experimental SQL still labelled 1.4. Create the explicitly named
disposable database and install btree_gist/sql_saga in it before running the harness.
Do not install this prototype over an application's extension installation.

```sh
make install && make test TESTS="001_install 157_deferred_temporal_checks" 2>&1 | tee tmp/defer_focused.log
make install && make test fast 2>&1 | tee tmp/defer_fast.log
make test-native 2>&1 | tee tmp/defer_native.log
make test-generator 2>&1 | tee tmp/defer_generator.log
python3 devops/test_deferred_temporal_checks.py --database sql_saga_defer_proto
python3 devops/test_deferred_temporal_checks.py --database sql_saga_defer_proto --performance --rows 8000 --rounds 6
```

The experiment requires an explicitly named disposable database, uses unique owned
schemas, bounded independent psql sessions and no extra Python dependencies. It
compares trigger toggling, new deferral and ordinary enabled checks on the same
fixture. Each batch merges 8,000 parent source rows and 8,000 native-child source rows.
Parents start with two timeline segments, forcing genuine referenced-side DELETE
checks when the merge coalesces them. Setup is excluded from timings. Body, COMMIT,
whole-pair wall time, actual plan DELETE counts, final integrity and load are retained.
Concurrency acceptance requires both deferred sessions to finish merge work before
either commits and no ShareRowExclusive/AccessExclusive locks on shared fixture tables.
Temporary-table locks are not claimed to disappear.

## Initial measured result at a62f05a, November 7, 2026

This section preserves the initial source-specific evidence. It is not the final
candidate's measurement. The later planner-ordering fix changes product SQL, so all
60 pairs were repeated on `956ce96`, reported separately below. Do not pool these runs
or attribute the initial timings to the final source.

**The prototype removes the demonstrated toggle-lock bottleneck, but is not an
unconditional speed improvement or an adoption decision.** Both disjoint-key sessions
completed their merges before either committed, with zero ShareRowExclusive or
AccessExclusive locks on shared fixture tables. In the toggle control, the second
session was observed waiting on `Lock` until the first committed.

### Workload and environment

- Code and harness: `a62f05a8bfc1b433ede79037e99165bc1684c8ef` on the prototype branch.
- macOS 26.6.2, arm64, 10 logical CPUs, PostgreSQL 18.3 from Postgres.app.
  `work_mem=4MB`, `shared_buffers=128MB`, `max_locks_per_transaction=256`,
  `shared_preload_libraries=pg_stat_monitor`, `fsync=on`, `synchronous_commit=on`.
- Each session merges 8,000 parent source rows and 8,000 native-child source rows
  for 8,000 entities. A pair therefore processes **32,000 source rows for 16,000
  entities**, not 8,000 total rows. Each entity also has a regular child reference.
  The initial parent has two adjacent, different-payload segments per entity.
- Every one of the 120 measured session batches, including the controls below,
  generated exactly **8,000 parent DELETE operations**. All 60 pair trials passed
  final orphan, count, payload, native temporal coverage and synchronization assertions.
- Six main rounds, rotating the three mode orders, each with sequential and concurrent
  pairs: 36 timed pairs and 72 session batches. Setup, reset, ANALYZE and correctness
  barriers are outside the timed regions. Whole-pair wall time includes both commits.
  Body includes BEGIN, mode selection, merges and, for toggle, re-enabling triggers.
- Our builds and other test suites had stopped before timing. Other machine activity
  remained substantial: one-minute load averages were **19.7 to 44.8** in the main run
  and **14.2 to 19.7** in the later controls. These are loaded-machine observations,
  not production capacity predictions or evidence of a small statistically stable delta.

### Main measurements

Pair-wall values are medians and ranges over six trials in each cell. Body and COMMIT
values are separate medians over the twelve individual session batches in each cell.
Do not add separately computed medians and expect the median pair wall time.

| Scheduling | Mode | Pair wall median, s | Pair wall range, s | Session body median, s | Session COMMIT median, ms |
|---|---|---:|---:|---:|---:|
| Sequential | Toggle | 4.117 | 3.590 to 4.875 | 2.046 | 5.9 |
| Sequential | Defer | 4.368 | 4.096 to 5.280 | 1.584 | 566.6 |
| Sequential | Enabled | 4.045 | 3.766 to 4.978 | 2.002 | 7.5 |
| Concurrent | Toggle | 3.876 | 3.721 to 5.732 | 3.106 | 5.2 |
| Concurrent | Defer | 2.574 | 2.382 to 2.719 | 1.935 | 578.6 |
| Concurrent | Enabled | 2.479 | 2.092 to 2.856 | 2.409 | 5.8 |

Compared with toggling, concurrent deferral reduced median pair wall time by **33.6%**,
from 3.876 to 2.574 seconds, giving **50.6% greater aggregate throughput**: approximately
4,128 to 6,217 entities/s, or 8,256 to 12,434 source rows/s. Deferral beat toggling in
**all six matched concurrent rounds**. Their individual wall reductions were 30.4%,
52.6%, 32.0%, 36.0%, 33.4% and 36.9%. Even the slowest deferred pair, 2.719 seconds,
was faster than the fastest toggle pair, 3.721 seconds, in this experiment.

The cost is real. Sequential pair wall time increased **6.1%** by the ratio of medians,
4.117 to 4.368 seconds. Five of six matched sequential rounds were slower, with changes
from -5.9% to +19.8%, and a median paired increase of 11.0%. Concurrent per-session
COMMIT increased from 5.2 to **578.6 ms**, a 526.3 to 763.4 ms deferred range. These are
client-observed commit times, not a claim that all elapsed time is exclusively FK CPU.
The faster body is not itself the benefit: the decisive comparison includes COMMIT.

**Ordinary enabled checks are a serious alternative.** On the same main run their
concurrent median pair wall was 2.479 seconds. Deferral's 2.574 seconds was 3.8% higher.
They already permit disjoint-key concurrency for this workload. They do not permit
the separate tested delete-then-restore gap spanning multiple merge calls, because
the released executor forces checks IMMEDIATE after each call. If application batches
are valid after every merge, removing toggles while retaining ordinary enabled checks
may avoid the bottleneck without this new API. That application-specific condition
has not been established here.

### Released-executor control without opt-in

The baseline database used the exact executor body from immutable `v1.4`, not a
reimplementation. The prototype database used the exact body from `a62f05a`. Both
identities were compared byte-for-byte against git before timing. C and native sources
are unchanged. The baseline database otherwise used the same fixture and branch
catalog definitions, so this is a controlled **executor-only** comparison, not a claim
of two independently built complete releases.

Three interleaved blocks ran two rounds per database with ordinary enabled checks,
in baseline/prototype, prototype/baseline, baseline/prototype order: 24 further pairs.
All passed. This checks the default path, not the new opt-in path.

| Scheduling | Released executor median pair wall, s | Prototype executor median pair wall, s | Observed change |
|---|---:|---:|---:|
| Sequential | 4.026 | 4.047 | +0.5% |
| Concurrent | 2.266 | 2.338 | +3.2% |

The three block-median changes were inconsistent: sequential -10.4%, +2.8%, +6.3%,
and concurrent -5.4%, approximately 0.0%, +16.6%. The ranges overlap. This is not proof
of zero overhead, nor enough evidence to attribute the small aggregate delta to code
rather than the changing machine load. An idle-machine repeat is needed if a narrow
default-path regression ceiling is an adoption requirement.

### Correctness evidence and reproducibility

- At the initial measured source, local fast regression passed **142/142**, native unit
  tests **5/5**, generator tests **28/28** plus Makefile wiring. Existing fast tests exercise unchanged default
  behavior. New test 157 and the independent-session harness exercise the opt-in.
- The large-run harness passed all six same-key commit race schedules across READ
  COMMITTED and REPEATABLE READ. Exactly one conflicting transaction committed in
  each schedule, the other returned 23503 or 40001, no orphan survived, and both
  sessions remained reusable. Requested child-first COMMIT was observed waiting on
  the parent's row lock, illustrating that request order is not completion order.
- Invalid immediate DML and actual deferred COMMIT produced identical primary FK
  diagnostics and SQLSTATE 23503. An unopted session remained immediate while another
  deferred. The regular-parent TRUNCATE guard still failed immediately with 0A000.
- Test 157 additionally covers repaired gaps across merge calls, native FK commit
  rejection, parent-key updates, obsolete child tuples, savepoints, selected and
  unselected name collisions, disabled-check rejection, DML privileges and RLS.
- Regular CASCADE keeps final-key deletion semantics, but **its observable execution
  time moves to validation/COMMIT under this opt-in**. The test observes one child
  before COMMIT and none afterward when the final parent key disappears. If the
  incoming requirement means preserving earlier cascade visibility exactly, this
  prototype does **not** meet that stricter requirement. Native action timing is
  not rewritten. This distinction needs an explicit adoption decision.

`doc/prototypes/deferred_temporal_checks_2026_11_07.csv` preserves every measured pair,
including both session bodies, commits and one-minute load, rounded to six decimals.
All 60 rows were checked against the original JSON. The `main` group is the six-round
comparison. `control_<block>_<database>` groups are the interleaved executor controls.
All groups use 8,000 source rows per session per target, two targets and two sessions.
`concurrent=0` means the two sessions ran sequentially, `1` means concurrent.

Local full reports and transcripts are in `tmp/defer_perf_8000_main/`,
`tmp/defer_control_{1,2,3}_{baseline,proto}/`, `tmp/defer_fast_release_candidate.log`,
`tmp/defer_native.log` and `tmp/defer_generator_final.log`. Representative parent-key,
regular-reference and native-reference probes used EXPLAIN (ANALYZE, BUFFERS): each
used an index, with 4 shared-hit blocks and 0 reads at the top node. These are not
captured internal SPI plans, and those individual lookup timings are not batch results.

After measurements, all five globally installed 1.4 install/update SQL scripts were
regenerated from immutable `v1.4` and restored byte-for-byte. A fresh disposable
database installed 1.4, had no prototype procedure or executor registry reference,
and passed check_integrity. It was then dropped. Existing application databases
were not modified. Evidence: `tmp/defer_release_install_verify.log` and
`tmp/defer_release_artifact_hashes.log`. The released install SQL SHA-256 is
`cbb4682b601d42223661149416120bcbf8f2b2cb4d2c44f33f64c8786ca7fda7`.
The two owned experiment databases were also dropped after preserving their reports.
No extra PostgreSQL cluster was started for this experiment.

## Final-source remeasurement, November 7, 2026

### Exact source and workload

Final measured runtime code is `956ce96b9931a68625d61005db1283279e2fab0f`.
Packaging follow-up `1018d3e` includes the refreshed planner in every upgrade script
and adds an explicit generator regression. It changes no `src/` or native runtime
code, and the complete fresh-install SQL retains the measured SHA-256 below.
Two exact-commit Linux attempts on its predecessor exposed a pre-existing unordered
`string_agg` over key columns joined to `pg_attribute`: test 063's generated SQL and
EXPLAIN filter could print equivalent AND operands in a different order. The minimal
fix preserves declared key/column order with ordinality and aggregate ORDER BY in
both explicit-key and fallback construction. A rollback-only proof passed under
three join strategies. Existing expected outputs needed **no changes**. All timing
runs below were repeated after this product change, rather than reusing old numbers.

The fixture, 8,000 entities per session, 32,000 source rows per pair, two targets,
two sessions, modes, rotating order and timing boundaries are the same as above.
The full main run has six balanced rounds, 36 timed pairs. Three further blocks
compare full released SQL with final prototype SQL, 24 pairs. Every one of these
60 pairs passed final orphan/count/payload/coverage/synchronization assertions.
Every one of the 120 session batches generated exactly 8,000 parent DELETEs.
No slow trials were removed.

All local builds and other test suites stopped before timing. PostgreSQL and server
settings are unchanged. One-minute host load ranged **16.5 to 30.7** during the main
run and **15.9 to 23.3** during controls on the 10-CPU Mac. These remain loaded-machine
measurements, not production capacity predictions.

### Main result including COMMIT

Pair wall medians/ranges have six trials each. Body and COMMIT medians have twelve
session observations each, computed separately, not added to obtain pair wall.

| Scheduling | Mode | Pair wall median, s | Pair wall range, s | Session body median, s | Session COMMIT median, ms |
|---|---|---:|---|---:|---:|
| Sequential | Toggle | 3.565 | 3.452 to 3.658 | 1.771 | 4.9 |
| Sequential | Defer | 3.940 | 3.814 to 4.048 | 1.454 | 507.1 |
| Sequential | Enabled | 3.670 | 3.579 to 3.825 | 1.834 | 4.7 |
| Concurrent | Toggle | 3.524 | 3.458 to 3.627 | 2.649 | 4.4 |
| Concurrent | Defer | 2.167 | 2.015 to 2.282 | 1.620 | 515.4 |
| Concurrent | Enabled | 2.080 | 1.855 to 2.278 | 2.025 | 5.5 |

Concurrent deferral reduced median pair wall time by **38.5%** relative to toggling,
with **62.6% more aggregate throughput**: approximately 4,541 to 7,383 entities/s,
or 9,081 to 14,766 source rows/s. It won **all six paired concurrent rounds**, with
individual reductions of 37.5%, 42.4%, 38.5%, 40.1%, 36.4% and 38.6%.
Even the slowest deferred pair was faster than the fastest toggle pair.

The costs are measurable too. Sequential deferral was **10.5% slower** by the ratio
of medians, and slower in all six paired rounds, with increases from 7.5% to 15.8%
and a median paired increase of 10.4%. Concurrent per-session COMMIT rose from
4.4 to **515.4 ms**, with a deferred range of 498.2 to 537.0 ms. Faster transaction
bodies alone would be a misleading benefit claim.

Ordinary enabled checks completed the concurrent pair in **2.080 s** versus
deferral's 2.167 s, so deferral was **4.2% higher**. Ordinary checks already permit
this fixture's disjoint-key concurrency. They are the simpler candidate if the
application does not require a repaired gap spanning multiple merge calls.

### Full released-SQL controls without opt-in

Unlike the initial executor-only controls, these used a fresh database with the
**full immutable released v1.4 SQL**, and a separate database with final prototype
SQL. Both used the same unchanged C/native libraries. Live planner/executor bodies
were compared byte-for-byte against the corresponding complete install scripts
before and after timing. Prototype API presence, release API absence and script
hashes were checked too. This includes the default-path cost of the planner fix.

Each block ran two sequential and two concurrent pairs per database, with ordinary
enabled checks only. Database order was release/prototype, prototype/release,
release/prototype across the three blocks. Setup is excluded in both databases.

| Scheduling | Released v1.4 median pair wall, s | Final prototype median pair wall, s | Change |
|---|---:|---:|---:|
| Sequential | 3.687 | 3.642 | -1.2% |
| Concurrent | 1.979 | 2.053 | +3.8% |

Sequential block changes were -3.8%, -2.8% and -1.2%. Concurrent changes were
**+5.6%, +4.3% and +1.4%**, slower in all three blocks. The concurrent ranges overlap
(release 1.945 to 2.063 s, prototype 2.016 to 2.135 s), but the consistent direction
must not be dismissed as proof of no regression. Treat the observed **3.8% possible
default-path slowdown** as an adoption cost until a less-loaded repeat can distinguish
code cost from host/scheduling effects. This experiment does not prove zero overhead.

### Final correctness and CI gate

- Final runtime source passed **142/142 local fast regressions**, **5/5 native tests**,
  **28/28 generator tests plus Makefile wiring**, and **27/27 migration-safety scanner
  assertions**. Packaging follow-up `1018d3e` passes **29/29 generator tests plus wiring**,
  including the new all-four-updates planner regression, observed failing before the fix.
  Focused 001/063/157 also passed. Tests used a clean disposable database
  and privately staged SQL. An initial reused-database run had stale fixtures and
  was not counted as acceptance.
- The repeated 8,000-row harness again proved both deferred sessions finish merges
  before either commits, zero strong locks on shared fixture tables, toggle-induced
  lock waiting, unopted-session immediacy, real COMMIT 23503 diagnostic parity,
  session reuse, immediate TRUNCATE 0A000, and six same-key READ COMMITTED/REPEATABLE
  READ races with exactly one commit and no orphans.
- Linux CI **42401605316** on `956ce96` passed all 142 regressions, the independent-session
  harness and supported restores, but **failed** fresh-versus-upgraded equivalence in
  all four upgrade paths. The planner's source file `2k` was absent from the generator
  manifest, leaving upgraded databases with the old planner. This is a real packaging
  omission, not harmless fingerprint noise. An in-memory proof repaired all four scripts,
  then the new regression failed before the permanent manifest fix in `1018d3e`.
  Exact-commit full CI **42403855569** on `1018d3e318f9dd5469bbcff6b4886fe8706df3cd`
  completed **successfully on November 7, 2026**, in 27m55s. The actual run conclusion,
  head SHA and every required step were verified, and the complete 3,228-line log checked.
  It passed **142/142 regressions, 5/5 native tests, 29/29 generator tests plus wiring**,
  the independent-session harness, supported restores, and **all four genuine old-binary
  1.0/1.1/1.2/1.3 upgrades** with exact routine/body/ACL and semantic catalog equivalence.
  Function-attribution concurrency passed **8/8** and every regular-FK concurrency check
  passed. Locally, the repaired private package also passed real 1.1, 1.2 and 1.3 upgrades
  and supported restores, without weakening any fingerprint.
- Earlier full Linux CI 42395614558 on `94499d5` passed restore, all four genuine
  released-version upgrades and both concurrency suites. Full-log review then
  found BSD-only grep syntax producing GNU errors incorrectly treated as a safe
  migration scan. The scanner now uses portable boundaries, strips literals even
  inside executable DO blocks, retains executable SQL and fails closed on scanner
  errors. The 27 counterexamples cover unsafe SQL, safe literals and errors. Linux run
  42401605316 passed all 27 assertions and all four real no-CASCADE scans. Its upgrade
  failures were the separate planner-manifest omission above.
- The existing unsupported parallel-restore case is explicitly excluded by the
  restore harness, not silently counted green. Final CI observed **9/20 failures** there
  from catalog load order (the earlier successful CI observed 12/20). Serial and documented
  split restore are the supported paths. All six split cases passed **20/20 each** in
  the final CI. Local SQL-only upgrades covered 1.1/1.2/1.3, but 1.0 requires genuine
  released 1.1 binaries for its query-function anchor. The complete old-binary matrix
  passed in final Linux CI, not as a locally claimed pass.
- Strict earlier regular-CASCADE action visibility remains **not met**, despite
  correct final-key behavior. This is an adoption decision, not something a green
  correctness suite can waive.

### Final evidence and environment cleanup

`doc/prototypes/deferred_temporal_checks_final_2026_11_07.csv` contains all 60 final
pairs. Every row was checked against its original JSON, including both session body
and COMMIT times and load. Its SHA-256 is
`9ee0dc9c44164d8d491b38ea581f6d260592b21d0e6ddc5d29363f2ffce9c66e`.
The original `deferred_temporal_checks_2026_11_07.csv` remains historical a62 evidence.
The final `main` group has 36 pairs; `control_<block>_<database>` has the 24 full-SQL
controls. `concurrent=0/1` means sequential/concurrent scheduling.

Local evidence is retained in `tmp/defer_final_perf_8000_main/`,
`tmp/defer_final_control_{1,2,3}_{baseline,proto}/`,
`tmp/defer_final_measurement_summary.log`, `tmp/defer_order_fast_clean.log`,
`tmp/defer_order_native.log`, `tmp/defer_order_generator.log`,
`tmp/defer_order_safety.log`, `tmp/defer_manifest_generator_{red,green}.log`,
`tmp/defer_manifest_upgrade_1_{1,2,3}.log`, `tmp/defer_planner_update_proof.log`
and the `tmp/defer_ci_42401605316_*` and `tmp/defer_ci_42403855569_*` artifacts.
The read-only final audit in `tmp/defer_handoff_audit.log` rechecked CI identity and
required step outcomes, all four genuine old-binary/equivalence paths, all 60 CSV
rows against raw JSON, unchanged runtime SQL and all five global release hashes.
The final live-body identity logs are `tmp/defer_final_live_code_identity{,_after}.log`.
Representative EXPLAIN (ANALYZE, BUFFERS) key/reference probes use indexes with
5/4/4 shared hits and zero reads at the respective top nodes. These remain separate
lookup probes, not captured internal SPI plans or end-to-end results.

The final private install SQL SHA-256 is
`08c487cfe443e2d87d996853b8cc78905da334f23fa11fcef578296649834191`.
All five globally installed SQL artifacts still match immutable released v1.4 after
these private tests. The full-release disposable database verified prototype API
absence, released executor identity and integrity. All three new final-test databases
and only the owned 17 MB private staging directory were removed after preserving
evidence. The later packaging-test stage (18 MB) and its three upgrade fixture DB/role
prefixes were also cleaned after their successful runs. The original regression database's
extension-control-path setting was reset. Application databases and other scratch work
were untouched. No extra PostgreSQL cluster was used.

## Adoption gate

Correctness, concurrent progress, repeated measured throughput and commit cost must
all be reported. A lack of benefit is a valid result. Application-specific batch
selection, dependency scheduling and a production-sized trial remain outside this
repository. Any adopted release needs a new version and an explicit supported upgrade
path. The experimental development build retains version 1.4 only for local testing,
so do not distribute its generated SQL as released 1.4. Restore the globally installed
SQL from immutable v1.4 after the experiment, without altering application databases.

Recommendation: **keep this as a measured, opt-in candidate, not a release yet**.
The experiment supports a concurrency benefit over trigger toggling when commit-scoped
semantics are actually needed. It does not support making deferral the default or
claiming it is faster than ordinary enabled checks. Before adoption, decide whether
delayed regular CASCADE visibility is acceptable, establish whether real batches need
cross-merge gaps, and run the actual application table set and dependency scheduler
with its commit-latency and memory budget. No StatBus changes, master merge or new tag
have been made by this prototype work.
