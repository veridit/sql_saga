# Transaction-local temporal FK deferral prototype

Status: experimental branch `prototype/defer-temporal-checks`, STATBUS-591.
This is **not a release** and not part of the published `v1.4` tag. The owner decides
whether to adopt it after reviewing measurements. StatBus is not modified here.

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

## Measured result, November 7, 2026

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

- Final local fast regression: **142/142**, native unit tests **5/5**, generator tests
  **28/28** plus Makefile wiring. Existing fast tests exercise unchanged default
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
