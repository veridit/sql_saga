# Provenance columns: who changed each piece of history, and when

Status: **design, not built**. Hug task f86twg. Owner and StatBus decisions of 2026-11-21 are recorded at the end.
This is the smallest subset that gives StatBus correct writer and time columns; what it leaves out is listed under
"Not in this subset".

## The situation

StatBus tables carry `edit_by_user_id` (who), `edit_at` (when) and `edit_comment` (why) on every row of an era. All
three are ephemeral columns today, so coalescing ignores them. Probes on 1.8 (test 161, `edit_by DEFAULT
current_user`; the admin wrote the rows, then a regular user edits) show that writer and time are wrong in most
write paths:

| # | What the user does | Writer and time today | Correct |
|---|---|---|---|
| 1 | Splits a year through the `for_portion_of` view | untouched pieces: writer kept, but the remainder after the split gets a new time | writer and time kept |
| 2 | Re-saves unchanged data | nothing is written | nothing is written |
| 3 | Sets 2025 to the admin's 2024 value | merged with 2024 into one row, credited to the admin | two rows, the new one the user's |
| 4 | Direct `UPDATE` with `SET edit_by = 'admin'` | admin (forged) | the user |
| 5 | Direct `UPDATE` that does not touch `edit_by` | admin (a `DEFAULT` only fills an `INSERT`) | the user |
| 6 | The changed middle piece of case 1 | admin (the view row starts from the old row) | the user |
| 7 | `temporal_merge` PATCH from a source without `edit_by` | the new piece: admin | the user |
| 8 | A source that supplies `edit_by` | the supplied value | the user |

The comment already behaves as StatBus wants, so it stays a plain ephemeral column. Through the view, a changed piece
gets whatever the user left in the comment field: untouched, it keeps the previous comment (the user felt it still
covers the change); modified, the new text; wiped, NULL. Untouched pieces keep their own comment. (Probe 2026-11-21.)

## The rule

Writer and time are ephemeral columns with one difference from the comment: the user must not choose their value.

| | Writer, time (provenance) | Comment (ephemeral) |
|---|---|---|
| Untouched piece (moved by a split or trim) | keeps its value | keeps its value |
| Changed or new piece, user supplied nothing | the column's `DEFAULT` | the previous value |
| Changed or new piece, user supplied a value | the column's `DEFAULT` (supplied value ignored) | the supplied value |

"Changed" means what the planner already means by it: the non-ephemeral data of the piece differs from the piece it
replaces, or the piece is new.

Why the `DEFAULT` and not a computed column: PostgreSQL refuses `GENERATED ALWAYS AS (current_user)` and
`AS (statement_timestamp())` ("generation expression is not immutable"; checked on 18), because a computed column may
only depend on its own row. And an untouched piece must keep its old writer, which a recomputed column never does.
The `DEFAULT` is evaluated as the role running the write; sql_saga never changes role (D0/D1), so the result does not
depend on how an application picks that role.

## Interface

```sql
SELECT sql_saga.set_era_ephemeral_columns('legal_unit', 'valid', ARRAY['edit_by_user_id', 'edit_at', 'edit_comment']);
SELECT sql_saga.set_era_provenance_columns('legal_unit', 'valid', ARRAY['edit_by_user_id', 'edit_at']);
SELECT sql_saga.set_era_provenance_columns('legal_unit', 'valid', NULL);   -- turn it off
```

- A provenance column must be listed in the era's `ephemeral_columns` and must have a `DEFAULT`; otherwise
  registration refuses, naming the column. Removing a column from `ephemeral_columns` while it is a provenance
  column is refused too.
- Stored in `sql_saga.era` as `provenance_columns name[]`: a catalog change, so it ships in release 1.9 with a
  `catalog_format` bump, update scripts and the old-dump procedure, as 1.8 did.
- Rename follows; dropping a provenance column is refused while it is registered (as for the other era columns).
- No `add_era` parameter in this subset: one setter, like `set_era_ephemeral_columns`.

## How it works

**The planner** (both implementations: `src/2k_temporal_merge_plan.sql` and `native/src/sweep.rs`):

- an untouched piece keeps the target's provenance values (already the case);
- a changed or new piece takes no provenance value from the target or the source, so the column's `DEFAULT` fills
  it (cases 1, 3, 6, 7, 8);
- two adjacent pieces merge only when their data and their provenance values are equal (decision 3).

**One managed trigger, `<era>_provenance`**, `BEFORE INSERT OR UPDATE FOR EACH ROW`, created by
`set_era_provenance_columns`, makes direct writes follow the same rule:

- `UPDATE` whose non-ephemeral data is unchanged (only the period moved, or nothing changed): the provenance columns
  keep OLD's values, whatever the statement set. This also covers the executor's own updates of untouched pieces;
- `UPDATE` that changes data, and every `INSERT`: the provenance columns get their `DEFAULT` (cases 4 and 5);
- except an `INSERT` of an untouched piece by `temporal_merge` (the remainder after a split): the executor writes
  those rows in a separate `INSERT` statement and marks it with a transaction-local setting, and the trigger keeps
  the values the planner gave it;
- a `NOT NULL` provenance column whose `DEFAULT` gives NULL refuses the write with
  `provenance column "edit_by_user_id" of "public.legal_unit": its DEFAULT gave NULL for the writing role postgres`
  instead of the generic not-null error.

## Not in this subset

**Deliberate forgery.** `temporal_merge` runs as the caller, so the marker it sets for untouched `INSERT`s is a
setting the caller could also set by hand, and then insert a row with another writer's name. So this subset makes
the columns right on every normal write path and stops mistakes (cases 4 and 5 above), but a user who deliberately
sets that marker can still forge. That is the same status as sql_saga's existing `rebuilding_system_versioning_views`
setting, a guard against mistakes and never an authorization. Closing it means running the executor's writes inside
sql_saga's C caller context, as the management functions do: a separate, larger change, only if StatBus needs the
columns to be proof against a hostile writer.

## Migrations and backfills

Only the table owner can write provenance values directly, by disabling the managed trigger inside the migration's
transaction:

```sql
BEGIN;
ALTER TABLE public.legal_unit DISABLE TRIGGER valid_provenance;
UPDATE public.legal_unit SET edit_by_user_id = 42 WHERE ...;
ALTER TABLE public.legal_unit ENABLE TRIGGER valid_provenance;
COMMIT;
```

`ALTER TABLE ... DISABLE TRIGGER` requires ownership of the table and takes an `ACCESS EXCLUSIVE` lock, so no other
session writes while the trigger is off; a failure rolls the disable back with the rest. A trigger left disabled is
reported by `check_integrity()` with the repair `ALTER TABLE ... ENABLE TRIGGER`.

## Cost

- One row trigger per write on registered tables: a few assignments, to be measured with benchmarks 100 and 101.
- One more `INSERT` statement in `temporal_merge` when a split leaves an untouched remainder.
- More rows where a user's change makes a piece equal to a neighbour written by someone else (case 3).
- Planner changes in both implementations; one catalog column; release 1.9.

## Tests

`sql/161_provenance_columns.sql`, written first; the expected output is the behaviour above. On 1.8 it fails where
the table says today's behaviour is wrong. Covered: cases 1 to 8; the comment kept, modified and wiped on a changed
piece and kept on untouched pieces; a direct `UPDATE` that only moves the period keeps the writer; registration
refusals (not ephemeral, no `DEFAULT`); a `NOT NULL` provenance `DEFAULT` that gives NULL; the backfill procedure and
`check_integrity()` reporting a disabled trigger.

## Decisions (owner with StatBus, 2026-11-21)

1. Name `provenance_columns`, setter `set_era_provenance_columns`.
2. A supplied writer is always replaced by the column's `DEFAULT`. StatBus makes its `DEFAULT` name the real writer on
   every write path before adopting 1.9 (its import worker runs as `postgres` today); sql_saga's rule does not depend
   on how.
3. Pieces with different provenance are never merged.
4. Untouched pieces keep both writer and time.
5. Backfills only by the table owner with the trigger disabled in the migration's transaction, as above.
6. `edit_comment` stays an ephemeral column: the user keeps, modifies or wipes it in the UI; a changed piece gets
   what the user left, untouched pieces keep their own.
7. Provenance columns are ephemeral columns that sql_saga fills from their `DEFAULT`; the smallest subset above is
   what to build.
