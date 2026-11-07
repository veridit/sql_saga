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

```sh
make install && make test TESTS="001_install 157_deferred_temporal_checks" 2>&1 | tee tmp/defer_focused.log
make install && make test fast 2>&1 | tee tmp/defer_fast.log
make test-native 2>&1 | tee tmp/defer_native.log
make test-generator 2>&1 | tee tmp/defer_generator.log
python3 devops/test_deferred_temporal_checks.py --database sql_saga_defer_proto
python3 devops/test_deferred_temporal_checks.py --database sql_saga_defer_proto --performance --rows 8000 --rounds 5
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

## Adoption gate

Correctness, concurrent progress, repeated measured throughput and commit cost must
all be reported. A lack of benefit is a valid result. Application-specific batch
selection, dependency scheduling and a production-sized trial remain outside this
repository. Any adopted release needs a new version and an explicit supported upgrade
path. The experimental development build retains version 1.4 only for local testing,
so do not distribute its generated SQL as released 1.4. Restore the globally installed
SQL from immutable v1.4 after the experiment, without altering application databases.
