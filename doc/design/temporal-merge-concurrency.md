# temporal_merge under concurrent writers

Status: design for 1.15 (hug 7rzgv5), reviewed (findings applied; review notes in hug 7rzgv5). Implements:
`src/2m_temporal_merge.sql`, `src/2l_temporal_merge_execute.sql`, `src/0g_for_portion_of_trigger.sql`. Tests:
`isolation/specs/` (`make test-isolation`).

## The contract

At READ COMMITTED, PostgreSQL's default, a `temporal_merge` call, or an `UPDATE` through a `for_portion_of` view,
running at the same time as other writes to the same entities ends with the result of running the transactions one
after the other, in commit order, without an error the caller must retry. At REPEATABLE READ and SERIALIZABLE the
call either gives that result or fails as a plain statement does there: a serialization failure (40001), or a key
violation for rows a concurrent transaction inserted after this transaction's snapshot.
Writes to different entities do not wait for each other, except by a rare shared lock stripe (below).

"Entity" is the value of the identity columns `temporal_merge` resolves (`primary_identity_columns`, or the
discovered primary or natural key), the same grouping the planner uses.

## Why 1.14 breaks it

`temporal_merge` plans from one snapshot of the target, then runs `DELETE`, `UPDATE` and `INSERT` statements that
find target rows by entity key and old range and write the values the plan computed. Nothing is locked before the
plan is made, and nothing checks the number of rows each statement changed. Under READ COMMITTED, an `UPDATE` that
waits for a concurrent transaction re-checks only its `WHERE` clause against the row's newest version, so:

- a concurrent change of another column passes the re-check and is overwritten by the stale planned values
  (lost update);
- a concurrently deleted row matches nothing, and the planned `INSERT`s of the leftover pieces still run
  (the deleted entity comes back);
- a concurrent `temporal_merge` of the same entity has already inserted its own pieces; ours collide with them
  (exclusion constraint violation).

The full study, with the isolation tester runs, is `doc/upstream/for-portion-of-revert.md`.

## The protocol: lock the entities, then plan

`temporal_merge` takes, in `src/2m_temporal_merge.sql`, before the planner reads the target, in statements of their
own:

1. **Advisory transaction locks on the keys the source names.** Each source row names its entity by the identity
   columns, and may also carry natural-key values. Every writer locks every key it names: one lock for the identity
   when it is not NULL, and one per natural key with at least one non-NULL column, over its non-NULL columns (a
   natural key may have mutually exclusive nullable columns; the planner matches it on the columns present, and the
   lock does the same). Two writers that touch the same
   entity therefore share a lock, whichever key each uses to name it, and also writers that create the same new
   entity. The lock is `pg_advisory_xact_lock(<target table oid as int4>, <stripe>)`, the two-key form, which
   PostgreSQL keeps apart from single-`bigint` advisory locks (StatBus's), with `<stripe> =
   hash_record_extended(ROW(<key kind>, <values cast to the target's column types>), 0) & 65535`, one of 65,536
   stripes per table (below). Casting to the target's types first makes the hash follow the target's equality
   (`1` and `1.0` as `numeric` hash alike), and `hash_record_extended` does not depend on `DateStyle` or
   `TimeZone`. The table oid is reinterpreted as a signed `int4` (`oid::bigint::bit(32)::int`), so no oid overflows.
   All locks of a call are taken in one statement in ascending stripe order.
2. **`FOR UPDATE` on every target row of those keys**: the rows whose identity, or any natural key on its non-NULL
   columns, equals one the source names, found with typed comparisons that use the key indexes. `FOR UPDATE`, not `FOR NO KEY UPDATE`:
   the executor changes the range column, which is part of the temporal key, and deletes rows, so it needs the
   strongest row lock in the end; taking it first avoids an upgrade that would wait behind foreign key checks
   (verified: an `UPDATE` of the range column waits for a concurrent `FOR KEY SHARE`).

After both locks are held, the planner reads the target in a new statement. At READ COMMITTED each statement of a
volatile PL/pgSQL routine takes a new snapshot (also inside `CALL` in a transaction block), and the native planner
reads through SPI in its own statement, so the plan is made from everything committed before the locks were granted,
and nothing that changes those rows can commit until this transaction does. That is the serial result. The locks
and the planner call must stay separate statements: nothing may carry a snapshot from before the locks into the
planner.

At REPEATABLE READ and SERIALIZABLE the planner reads the transaction's snapshot, which may predate a concurrent
writer that committed while this call waited for its locks. A row that changed after the snapshot makes `FOR UPDATE`
raise 40001. Rows inserted after the snapshot (a concurrently created entity or piece) are not visible, and the
merge's own insert then fails on the temporal key (23P01), as a plain `INSERT` racing a concurrent insert fails with
a key violation at these levels in PostgreSQL. sql_saga does not add a layer of its own there.

### What the locks cover

| Concurrent writer | Covered by |
|---|---|
| another `temporal_merge` or `for_portion_of` view update of the same entity | advisory lock (1) |
| plain `UPDATE` or `DELETE` of an existing row of the entity | row lock (2) |
| a new entity created by two `temporal_merge` calls | advisory lock (1) |
| plain `INSERT` of a row of the entity by another writer | **not covered**: the `WITHOUT OVERLAPS` key then raises an exclusion violation for whichever commits second. This is documented: write an entity through `temporal_merge` (or the view) if other sessions do so concurrently. |

### Entities found by a natural key

The rule in (1) needs no lookup before locking: a source row that names an entity only by its natural key locks that
natural-key value, and any other writer of the same entity that carries the same natural-key value, as identity
writers do when they write that column, locks it too. A writer that names the entity only by identity and does not
carry its natural-key columns does not share a lock with a natural-key writer; it does lock the entity's rows (2), and
a natural-key writer locks the same rows: every row of each entity its natural-key predicate finds, by identity,
because a natural key outside the identity may be NULL on some of the entity's pieces (before the key starts, after it
is retired; hug 40nwvn). Existing entities therefore still serialize, and the planner reads the same whole entities. A
multi-column key finds its rows through the source's distinct keys, materialized first: `(a, b) IN (subquery)` was
planned as a nested loop over one column's index on the source view's estimates (48 s for 28,578 keys, hug t8b97s). The
remaining case, two writers creating one new entity, one by identity alone and one by natural key alone, ends in an
exclusion violation on the natural key, not in silent damage. Source rows whose keys are all NULL (new entities with
generated identities and no natural key) take no lock: they cannot collide on a key.

### Lock order and deadlocks

All advisory locks of a call are taken in one statement in ascending order, then the row locks in one statement
ordered by key. Two `temporal_merge` calls on one table never wait for each other in a cycle. Deadlocks remain
possible, and PostgreSQL detects them (40P01), in two cases: two transactions that each merge into two tables in
opposite orders, and a transaction that inserted a row referencing an entity (a foreign key check holds `FOR KEY
SHARE` on the parent row) and then merges that entity while another merge of it waits on that row.

### Lock table size

Advisory locks live in the shared lock table, sized `max_locks_per_transaction * (MaxBackends +
max_prepared_transactions)`. To bound one call's use regardless of batch size, lock keys are folded into 65,536
stripes per table: two different entities share a stripe with probability 1/65,536 per pair, which only makes one
wait for the other. A batch of 1,000 entities takes at most 1,000 locks; StatBus runs with
`max_locks_per_transaction = 2048`. When the pool is exhausted PostgreSQL raises `out of shared memory` with the hint
to raise `max_locks_per_transaction`; the README says so with the recommended batch size of about 1,000 entities.

### The for_portion_of view

The view's row trigger queues, per changed row, the new values of the changed columns and the **old** values of the
unchanged ones, read from `OLD`, which the view statement read before any lock. Locking inside `temporal_merge`
would come too late: the stale unchanged values are already queued, and the merge would write them back (scenario
e). So the change queued must not carry unchanged values at all.

`UPDATE_FOR_PORTION_OF` already keeps the target's value for any column absent from the source (its payload is the
target's payload overlaid with the source's), and treats a present NULL as a value. The row trigger therefore
queues the identity, the period and the columns the statement changed, and the apply trigger merges each set of
rows that changed the same columns through a source that has exactly those columns. A statement changes the same
columns in every row (its `SET` list), except that a row whose new value equals its old one does not count as
changed; such rows join the set of the columns they did change. Sets are merged in queue order; rows of different
sets never overlap (each row's period lies within its own `OLD` row). The unchanged columns are then read under
`temporal_merge`'s locks, from committed data.

`DELETE_FOR_PORTION_OF` through the view queues no column values, so it needs no change. A set whose only change is
the period (no data column changed) is merged with no data columns: the slice keeps its values.

A row trigger cannot see the statement's `SET` list: it compares `NEW` with `OLD`. `SET price = 2` on a row whose
`OLD.price` is already 2 looks like "price not set". If another transaction committed `price = 5` after the view
statement read the row, the merge keeps 5 where running the transactions in order would give 2. This is accepted and
documented: a column is changed through the view only when its new value differs from the one the statement read.
It is the price of `INSTEAD OF` triggers, which PostgreSQL gives no re-check (EvalPlanQual) of the view's row; plain
tables do not have it.

### Checking the executor's row counts

With the locks, every planned `UPDATE` and `DELETE` must change exactly the rows the plan names. After each statement
the executor compares its row count with the number of plan rows the statement covers and raises when they differ
(SQLSTATE XX000, "the target changed under the plan"). Under the protocol this happens only when something other
than a concurrent writer removes rows from the statement: a user `BEFORE` trigger that returns NULL, or a row-level
security policy whose `UPDATE`/`DELETE` clause is narrower than its `SELECT` clause. The error's hint names both. It
turns any gap into an error instead of silent damage. `INSERT`s need no check: the exclusion constraint covers
them.

## Cost

One statement takes the advisory locks, and one per key the row locks, each set-based and served by the key's index
(a single column is matched with `= ANY (ARRAY(...))`, which the planner always serves from the index; `IN (subquery)`
was planned as a hash join over the whole table). Measured on PostgreSQL 18.3, a 200,000-row target with an identity
and a natural key, a 1,000-row `MERGE_ENTITY_UPSERT` batch, median of 10 calls after a warm-up, two runs each:

| | 1.14 | 1.15 |
|---|---|---|
| per call, natural keys not used | 498 ms, 533 ms | 506 ms, 520 ms |
| the lock statements alone | | 9 ms advisory, 3 + 6 ms row locks |

The difference is within the run-to-run spread. With the natural key discovered (the default), the native planner
took 6 to 7.6 s per call in 1.14: its target read ORed the key sets in one WHERE, which PostgreSQL served with a
BitmapOr over the GiST exclusion index. 1.15 reads each key set as its own index-served branch and combines them by
`ctid` (hug aqqsgb); the planning phase then takes 84 to 165 ms.

## Alternatives considered

- **Raise a serialization failure on conflict** (PostgreSQL's FOR PORTION OF v2): correct but pushes retries onto
  READ COMMITTED callers, which Andres Freund argued against upstream; StatBus imports run at READ COMMITTED.
- **Lock the whole table** (`SHARE ROW EXCLUSIVE`): simplest and serial, but concurrent imports of different
  entities would queue behind each other.
- **Row locks only**: misses new entities (f) and does not order two merges of the same entity before they plan.
- **Re-plan after waiting**: needs detecting that a wait happened inside the executor's statements; locking first
  is simpler and makes the plan right the first time.
