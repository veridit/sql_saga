# FOR PORTION OF in PostgreSQL 19: why it was reverted, and what it means for sql_saga

Status: study for hug task vhhp80 (epic 0t4v31), 2026-11-24. Tested on sql_saga 1.14 and PostgreSQL 18.3.

## Summary

PostgreSQL committed `UPDATE/DELETE ... FOR PORTION OF` for version 19 on 1 April 2026 (commit `8e72d914c52`,
Paul Jungwirth's patch, committed by Peter Eisentraut). A review by Andres Freund in September showed that
under READ COMMITTED, PostgreSQL's default isolation level, two concurrent temporal writes to the same entity can
silently lose one of them, and that the documented workaround did not prevent it. Peter Eisentraut reverted the
feature and its 22 follow-up fixes on 15 September 2026 (commit `a4b26b8f7cd` on master, `a9d2f728240` on
`REL_19_STABLE`). PostgreSQL 19 (tag `REL_19_0`, 16 November 2026) ships without it. Paul posted a new patch series on
31 October 2026 that, for now, raises a serialization failure when it detects the anomaly.

We ran the same kinds of races against sql_saga's `temporal_merge` and its `for_portion_of` view. sql_saga has
the same class of bug, in three cases silently:

- a plain `UPDATE` of another column, committed while `temporal_merge` waits, is overwritten on every piece;
- a `DELETE` of the entity, committed while `temporal_merge` waits, is partly undone: the updated slice and
  everything after it come back;
- the same lost update happens through the `for_portion_of` view.

The other races fail loudly (an exclusion constraint error or a serialization failure) instead of producing the
serial result. A prototype lock protocol (an advisory lock per entity plus `SELECT ... FOR UPDATE` of the entity's
rows, taken before the write) gives the serial result in the six READ COMMITTED scenarios where sql_saga is the
waiting side. It is a prototype, not a settled design: moving the locks inside `temporal_merge` does not cover the
view path, and natural keys, other writers and lock limits are open (see below). The bug and the fix are hug task
7rzgv5. This matters for the upstream conversation: we can only speak credibly about temporal writes
once sql_saga has a tested answer to the question that sank FOR PORTION OF.

## The discussion

Thread "FOR PORTION OF code review" on pgsql-hackers, 10 to 15 September 2026
(<https://www.postgresql.org/message-id/flat/vquveff5flfpsgsd55dkjqplhphziah7a7kggnemzfv5krrhet%40jxp5ubrpmxhy>).

| Date | Who | Message | Point |
|---|---|---|---|
| 09-10 | Andres Freund | [review](https://www.postgresql.org/message-id/vquveff5flfpsgsd55dkjqplhphziah7a7kggnemzfv5krrhet%40jxp5ubrpmxhy) | Concurrency: the code comment and docs admit lost updates at READ COMMITTED; he finds that unacceptable and doubts the `SELECT FOR UPDATE` workaround, because leftover rows are new rows, not linked to the original by an update chain that a lock could follow. Code quality: an unprotected type cache entry, setup repeated per row instead of once, the executor's state temporarily rewritten (`mtstate->operation = CMD_INSERT`), statement triggers fired per leftover row, the target expression planned at execution time. |
| 09-11 | Andres Freund | [AI-assisted findings](https://www.postgresql.org/message-id/yfers7d2kizq6ockz4nu7e7avq3gj5anpd7ilxkazyvicv4qgy%403cesow2bbhnx) | A crash with whole-row references; PL/pgSQL variables and named parameters rejected as bounds; `pg_get_functiondef()` output that does not replay; two query-level memory leaks; `DO ALSO` rules not rejected and not deparsed; AFTER triggers on leftovers firing while the outer statement still runs. Several are general infrastructure faults. |
| 09-11 | Nathan Bossart (release team) | [request](https://www.postgresql.org/message-id/aqP9rRDHpKHkbluf%40nathan) | Asks Peter and Paul to respond. |
| 09-11 | Paul Jungwirth | [ack](https://www.postgresql.org/message-id/CA%2BrenyUDPDJ9gRZxcuOV7HLE1JTS%2B4EcAhWrih3p-hHpODHjvA%40mail.gmail.com) | Working on patches. |
| 09-11 | Andres Freund | [workaround fails](https://www.postgresql.org/message-id/txcba757fwn5ptrjeazk4afbshf55e3rx2drb3seypqfqpvxia%40bm4vp2duvnvn) | The documented `SELECT ... FOR UPDATE` with the portion's time filter can lock nothing: the row it would lock no longer matches the time filter after the re-check. Dropping the time filter from the lock may work around the obvious case, at the cost of more deadlocks (our prototype does that, per entity). Attached `for-portion-of-lost.spec`. |
| 09-11 | Andres Freund | [DELETE breaks even full-key locks](https://www.postgresql.org/message-id/6lbh27m3epz5glwbpepdwh6xxi4adry2dyg27s2z7dc7xkzq5k%40ceyuortazlv2) | With `DELETE FOR PORTION OF` there is nothing left to serialize on: once the deleting transaction commits, waiting `FOR UPDATE` scans finish without locking a live row. Attached `for-portion-of-lost-delete2.spec`. A fix needs real work, for example ON CONFLICT's speculative insertion or a dirty-snapshot scan, or the feature must refuse READ COMMITTED. |
| 09-11 | Paul Jungwirth | [proposal](https://www.postgresql.org/message-id/CA%2BrenyUVthah-kRX1FgZfH%3DbuZ2bqUmMTZVx6mHt%3D-ba%2Bp2r%2Bw%40mail.gmail.com) | Agrees it is the most serious issue. Proposes raising a serialization failure when detected, as MERGE and cross-partition UPDATE already can under READ COMMITTED. |
| 09-11 | Andres Freund | [rejects that](https://www.postgresql.org/message-id/udycredxnko7yhop4dfixlryxbb5sbb45hv5azo5rkzvmhxquv%407ylf3nuvc5ea) | READ COMMITTED is widely used precisely so that applications need not retry; the feature should have working concurrency. Sketch: find the relevant rows with something like `ExecCheckIndexConstraints()` extended to several conflicts, lock them, check visibility at higher levels; probably needs a `WITHOUT OVERLAPS` index. |
| 09-12 | Peter Eisentraut | [timing](https://www.postgresql.org/message-id/5061be14-4d3c-4966-b6d3-c07efddbd382%40eisentraut.org) | Solutions can be explored, but not in the week before the last beta. |
| 09-14 | Melanie Plageman (release team) | [question](https://www.postgresql.org/message-id/CAAKRu_Z%3DJMj8ahVtGSP8Fh00MK4ph1y8i52_Qt1jW%3DLXeUUEGg%40mail.gmail.com) | What is the proposal? |
| 09-14 | Andres Freund | [not ready](https://www.postgresql.org/message-id/fsis3wtdcrcijt6j22nvefngqkh5dwpil5z5xoxrxcfh3nzsqo%40hqx5uf6kahyq) | Not ready; incremental work in 20 is fine, a clean start also reasonable. |
| 09-14 | Peter Eisentraut | [revert](https://www.postgresql.org/message-id/62bf70cc-dc33-4188-8070-a8c177bb61de%40eisentraut.org) | He was nudging toward a revert. |
| 09-15 | Peter Eisentraut | [done](https://www.postgresql.org/message-id/a11201e5-287e-43e1-8466-4669c522942e%40eisentraut.org) | Reverted. |

After the revert, Paul posted a new series, "UPDATE/DELETE FOR PORTION OF", on
[31 October](https://www.postgresql.org/message-id/CA%2BrenyULDQq3x-qYuADAenfqbFts9HaM66zWROZO8X%2B2bJf2GA%40mail.gmail.com)
and rebased it on [11 November](https://www.postgresql.org/message-id/CA%2BrenyU1_U1_0x1hW7UbmAT0%3Dgeh0ozU26aJ_TW6zV%3Dz%3DFHPXg%40mail.gmail.com).
It fixes the reported open items, raises a serialization failure for the READ COMMITTED anomalies "for now", says
avoiding them entirely "may require a WITHOUT OVERLAPS index", and adds support for views with rules and
`INSTEAD OF` triggers. Two prerequisite fixes have their own threads (whole-row references, a trigger memory
leak). No reply from Andres Freund or Peter Eisentraut on the new series as of 24 November.

## The problem, in one picture

A temporal write reads the entity's current rows, computes new pieces, then deletes, updates and inserts. Under
READ COMMITTED, PostgreSQL protects a plain `UPDATE ... WHERE` by re-checking each row it had to wait for (the
"EvalPlanQual" re-check) against the latest committed version. That protects the one row it waited for. It does
not protect a temporal write, because:

1. the decision covers several rows (the entity's whole timeline), not one;
2. another transaction's leftover pieces are new rows, invisible to the waiting statement's snapshot, and not
   linked to the row it was waiting for;
3. a deleted row leaves nothing to wait on.

Andres Freund's direction for PostgreSQL, finding the conflicting rows through the `WITHOUT OVERLAPS` index and
locking them before deciding, follows the same principle as our prototype below (lock what the decision depends on
before making it), at a finer grain: his would find conflicting rows by range through the index, including rows
the snapshot cannot see, where ours locks the whole entity and then takes a fresh snapshot.

## sql_saga under the same races

`temporal_merge` plans from a snapshot of the target, then runs `DELETE`, `UPDATE` and `INSERT` statements that
match target rows by entity key and old range and write the planned values. It takes no row locks before
planning and does not check the number of rows each statement changed. The `for_portion_of` view queues the
change and calls `temporal_merge`, so it behaves the same way.

The specs are in [concurrency/](concurrency/) (`gen.py` writes them, `run.sh DBNAME` runs them with PostgreSQL's
isolation tester against a throwaway database). Fixture: entity 1, price 0, note `orig`, valid 2020 to 2030, with
a temporal primary key and a `for_portion_of` view. "Serial result" is what running the two transactions one
after the other gives.

| Scenario | Isolation | Serial result | sql_saga 1.14 | With prototype locks |
|---|---|---|---|---|
| a. two portion updates, disjoint slices (PostgreSQL's documented case) | READ COMMITTED | both slices changed | second fails: exclusion constraint violation | serial result |
| a | REPEATABLE READ | | second fails: serialization failure (retry) | |
| b. plain `UPDATE SET note`, committed while a portion update waits | READ COMMITTED | note `s1` on every piece | **note `s1` lost on every piece, no error** | serial result |
| b, reverse order (portion update commits first) | READ COMMITTED | note `s1` on every piece | plain UPDATE changes only the piece it saw | same (plain PostgreSQL READ COMMITTED behaviour) |
| b | REPEATABLE READ | | the waiting side fails: serialization failure | |
| c. `DELETE` of the entity, committed while a portion update waits | READ COMMITTED | no rows | **entity comes back for 2025 to 2030, no error** | serial result |
| c | REPEATABLE READ | | serialization failure | |
| d. Andres Freund's delete2 shape: portion delete, then two portion updates | READ COMMITTED | all three changes | the middle session fails: exclusion constraint violation | serial result |
| e. as b, through the `for_portion_of` view | READ COMMITTED | note `s1` on every piece | **note `s1` lost on every piece, no error** | serial result |
| f. two sessions create the same new entity, overlapping time | READ COMMITTED | second patches the first | second fails: exclusion constraint violation | serial result |

Reading the 1.14 column:

- **b, c, e are silent data loss**, the same class of bug that got FOR PORTION OF reverted. In b the
  `UPDATE` waits for the other transaction, re-checks only "same key, same old range", which still holds,
  and writes the values it planned from the old snapshot. The planned values include the columns the merge
  does not change, so it rewrites `note` back to `orig`. In c the `UPDATE` finds the row deleted and changes
  nothing, and the planned `INSERT`s (the updated slice and the leftover after it) still run.
- **a, d, f are safe but not serial**: the exclusion constraint (`WITHOUT OVERLAPS`) catches the overlap the stale
  plan would create. The caller must retry, which READ COMMITTED callers do not expect.
- **REPEATABLE READ** (run for a, b and c) ends every race in a serialization failure. SERIALIZABLE is expected to
  be at least as strict; it was not run.
- b in reverse order is ordinary PostgreSQL READ COMMITTED behaviour for a plain `UPDATE` (rows inserted by a
  concurrent transaction are not seen) and is out of sql_saga's control.

The prototype adds, before each temporal write, `pg_advisory_xact_lock(<table>, <entity>)` and
`SELECT ... FOR UPDATE` of the entity's rows. At READ COMMITTED each statement takes a new snapshot, so the
planner's later read sees everything committed before the lock was granted, and nothing can change the entity's
existing rows until commit. The advisory lock also covers entities that have no rows yet (scenario f), which row
locks cannot. In d the locks make s3 wait for s2, so that run commits s2 before s3.

What the prototype did not settle, and the fix (hug task 7rzgv5, sql_saga 1.15, commit 3284e2e7) does: see
`doc/design/temporal-merge-concurrency.md`. The points as found:

- **The view path.** The prototype takes the lock before the `UPDATE` through the view. The view's row trigger
  copies the unchanged columns from `OLD`, read with the view statement's snapshot, into the change it queues, and
  `temporal_merge` runs later. A lock taken inside `temporal_merge` would come after the stale `note = 'orig'` is
  already queued and would not fix e. The view must lock before reading `OLD`, or re-read the unchanged columns, or
  queue only the changed columns.
- **Natural keys.** When the entity is found by a business key (`natural_identity_columns`), its id is unknown
  before planning, so the lock must be on the key values the writer uses, the same way for every writer.
- **Other writers.** Plain `INSERT`, `UPDATE` and `DELETE` take no advisory lock. Row locks cover existing rows, but
  a plain `INSERT` of a new row for the entity is not covered.
- **Deadlocks.** Sorting the advisory locks by key prevents cycles between merges. Row locks against plain
  multi-row updates, foreign key checks or user triggers can still deadlock.
- **Lock key and limits.** `hashtext('t')` stands in for a real key: use the table's oid and a hash of the full key
  (composite and non-integer keys), and state that hash collisions only cause extra waiting. One advisory lock per
  entity uses the shared lock table, so batches of thousands of entities can reach `max_locks_per_transaction`.
- **Row counts.** The executor should raise when an `UPDATE` or `DELETE` changes a different number of rows than
  planned. On its own that turns c into an error, not the serial result: it is a safety net, not the fix.

With per-entity locks, disjoint entities do not block each other, so concurrent batch imports stay parallel.

## What this means for the upstream conversation

- **Fixed for 1.15** (hug 7rzgv5): `temporal_merge` locks every key its source names, then the rows, then plans;
  the view merges only changed columns; the executor checks its row counts. The isolation specs now live in
  `isolation/specs/` and run in CI. Arriving upstream with the bug that sank FOR PORTION OF would have cost
  credibility; we can now arrive with a tested answer.
- sql_saga is evidence for Andres Freund's position that working READ COMMITTED behaviour is achievable
  for temporal writes: lock the entity's timeline, then plan. That argues against the serialization failure in
  Paul's current series, so it should be offered as data for his next step, not as a counter-proposal. It is
  entity-level locking at the SQL level, coarser than what the server can do with an index-driven protocol.
- Measured cost (1,000-row batch, 200,000-row target): 506/520 ms per call with the locks against 498/533 ms
  without, within run-to-run spread; the lock statements take about 18 ms together.

## Sources

- Thread: <https://www.postgresql.org/message-id/flat/vquveff5flfpsgsd55dkjqplhphziah7a7kggnemzfv5krrhet%40jxp5ubrpmxhy>
- Revert: `a4b26b8f7cd` (master), `a9d2f728240` (`REL_19_STABLE`), both 2026-09-15, in
  <https://git.postgresql.org/gitweb/?p=postgresql.git>
- New series: <https://www.postgresql.org/message-id/CA%2BrenyULDQq3x-qYuADAenfqbFts9HaM66zWROZO8X%2B2bJf2GA%40mail.gmail.com>
- Andres Freund's isolation specs: attachments 203634 (`for-portion-of-lost.spec`) and 203637
  (`for-portion-of-lost-delete2.spec`) in the thread
