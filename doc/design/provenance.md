# Provenance columns: who changed each piece of history, and when

Status: **proposal, not built**. Hug task f86twg. Needs the owner's answers to the questions at the end before any code.

## The situation

StatBus tables carry `edit_by_user_id` (who) and `edit_at` (when) on every row of an era. StatBus wants these to
say who last changed the *data* of that piece of history. Today they are plain columns with a `DEFAULT`, declared
ephemeral so that coalescing ignores them. That is not enough, and probes on 1.8 (2026-11-18, `edit_by DEFAULT
current_user`; the admin wrote both rows, then a regular user edits) show it is wrong in most write paths:

| # | What the user does | What the writer column says today | Correct |
|---|---|---|---|
| 1 | Splits a year through the `for_portion_of` view | carried pieces: writer admin; but the remainder after the split gets a **new time** (the planner supplies the writer from the old row, the time from the column's `DEFAULT`) | admin, old time |
| 2 | Re-saves unchanged data | nothing is written | nothing is written |
| 3 | Sets 2025 to the admin's 2024 value | data changes, but the row still says admin (the earlier 1.7 probe also saw such pieces coalesce into one row, credited to one writer) | the user; two rows, not one (question 3) |
| 4 | Direct `UPDATE` on the base table with `SET edit_by = 'admin'` | admin (forged) | the user |
| 5 | Direct `UPDATE` on the base table, not touching `edit_by` | admin (`DEFAULT` only fills an `INSERT`) | the user |
| 6 | The changed middle piece of the split in case 1 | **admin** (the view row starts from OLD, so the old writer travels in) | the user |
| 7 | `temporal_merge` PATCH from a source without `edit_by` | the new piece: **admin** (target ephemeral values are merged under the source's) | the user |

So today the columns are only right for an `INSERT` of a brand new row. Everything that changes existing history
keeps the previous writer, unless the caller sets the column, and then any value is accepted.

## Goal

Declare on the era which columns are provenance columns. sql_saga then guarantees, for every write path (views,
`temporal_merge`, plain `INSERT`/`UPDATE` on the base table):

- a piece whose data the statement changed, or that the statement created, gets fresh values;
- a piece that only moved because its neighbour was split or trimmed (a *carried* piece) keeps its values;
- the caller cannot choose the values.

Not the goal: row-level security. StatBus may use the columns in policies, but sql_saga only records.

## Proposed interface

```sql
SELECT sql_saga.add_era('lu', 'valid_range', 'valid',
    provenance_columns => ARRAY['edit_by_user_id', 'edit_at']);
-- or, on an existing era:
SELECT sql_saga.set_era_provenance_columns('lu', 'valid', ARRAY['edit_by_user_id', 'edit_at']);
SELECT sql_saga.set_era_provenance_columns('lu', 'valid', NULL);   -- turn it off
```

- **Fresh values come from each column's own `DEFAULT`**, evaluated as the writing role, once per row
  (StatBus already has `DEFAULT auth.uid()` and `DEFAULT statement_timestamp()`). No function names in the catalog,
  nothing new to validate. A provenance column must have a `DEFAULT`, or registration refuses.
- Provenance columns are **implicitly ephemeral**: they never take part in "did the data change?". They must not
  also be listed in `ephemeral_columns` (refused, to keep one meaning per column).
- Stored in `sql_saga.era` as `provenance_columns name[]`. That is a catalog change, so it ships in a release with a
  `catalog_format` bump (1.9), update scripts and the old-dump procedure, like 1.8.
- Rename and drop of a provenance column follow the era's other column lists (rename following; drop refused while
  registered).

## How it works

Three parts.

**1. One managed trigger fills the columns.** `BEFORE INSERT OR UPDATE FOR EACH ROW` on the base table, created by
`add_era`/`set_era_provenance_columns`, named like sql_saga's other managed triggers. For each row:

- *written by sql_saga's executor* (see 2): leave the values as written (the old piece's for a carried piece, `DEFAULT` for a changed one);
- *otherwise*: overwrite every provenance column with its `DEFAULT`, whatever the statement supplied
  (cases 4 and 5 become correct; forging is impossible);
- an `UPDATE` that changes no non-provenance column is a no-op for provenance too (keep OLD values), so a re-save
  stays a re-save (case 2).

**2. sql_saga marks its own writes, unforgeably.** The trigger cannot tell a carried `INSERT` from a user's
`INSERT` by looking at the row (no OLD; probe 2 shows carried pieces are written as both `UPDATE` and `INSERT`). The
executor knows: it is the planner that decided which pieces are carried. Writes stay ordinary statements run as the
caller, so D0/D1 hold: PostgreSQL authorizes them, no elevated role.

Which rows are carried is decided per piece by the planner: the diff already separates pieces whose data changed
from pieces whose data equals their ancestor's. The executor does not write them in separate statements: its
`UPDATE`s are batched by `statement_seq`, which groups by update effect (a data change with no time change and a
shrink of a carried piece share one statement, `src/2l_temporal_merge_execute.sql`), and its `INSERT`s are one
statement. So the mark must travel with each row.

Simplest form: the executor writes a carried piece with the provenance values the planner gives it (the old
piece's), and a changed piece with the provenance columns set to `DEFAULT` in the statement itself. The trigger then
only needs to know "is this row written by sql_saga's executor right now", once per statement, and can trust the
values in NEW for those rows: they are either the old piece's or the `DEFAULT` just computed. That one bit is the
in-backend marker: a flag in sql_saga's C library that `temporal_merge_execute` sets around its DML and clears in
every exit path, readable by the trigger. User SQL cannot set it (not a GUC, not a table), and a user trigger that
fires during the executor's DML runs inside the window, so the flag is suspended while caller-supplied code runs, as
the management caller stack already is (`caller_context` in `src/sql_saga.c`).

**3. The planner keeps changed and carried pieces apart.**

- New and changed pieces get no provenance values from the target or the source (cases 6 and 7). The trigger fills
  them.
- Carried pieces keep the target's values (already the case).
- Coalescing: today two adjacent pieces with equal data become one row, and their provenance is lost (case 3).
  With provenance columns, pieces merge only when their data *and their provenance* are equal; otherwise they stay
  separate rows. See question 3.

Both planners must do this: the PL/pgSQL one (`src/2k_temporal_merge_plan.sql`) and the native one
(`native/src/sweep.rs`, ephemeral merge and `coalesce_segments`).

## Cost

- One row trigger per write on registered tables (StatBus: a handful of tables). It is a few assignments; to be
  measured with benchmark 100 and 101 before and after.
- More rows where a user's edit makes a piece equal to a neighbour written by someone else (case 3).
- Planner change in two implementations, one more C function, one catalog column, release 1.9.

## Tests (written first, failing on 1.8)

`sql/161_provenance_columns.sql` (written; fails on 1.8 exactly in the rows above, re-save already right), one scenario per row, plus: registration refuses a column
without `DEFAULT` and a column also listed as ephemeral; turning provenance off removes the trigger; a user cannot
forge the marker (calls the trigger path from SQL, sets every plausible GUC); `check_integrity()` reports a missing
or altered managed trigger.

## Questions for the owner

1. **Name.** `provenance_columns` on the era, and `set_era_provenance_columns(...)`. Good?
2. **Source values.** If an import or `temporal_merge` source supplies `edit_by_user_id` (say, an import on behalf of
   another user), is it overwritten like a direct `UPDATE` (proposal: yes, always; the column records who actually
   wrote) or kept?
3. **Coalescing.** User sets 2025 to exactly the admin's 2024 value. Keep two rows with different writers (proposal),
   or merge them and credit the merged row to the user who just wrote?
4. **`edit_at` on carried pieces.** Proposal: carried pieces keep both the old writer and the old time. Agree?
5. **Direct writes to the base table by StatBus code that sets these columns on purpose** (migrations, backfills):
   with this design that is impossible except as the table owner with the trigger disabled. Is that acceptable?
