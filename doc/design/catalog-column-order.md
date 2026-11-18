# Catalog consistency: key and foreign key column order, view shape (1.3 audit)

Status: findings of the 1.3 coverage audit (2026-10-20, on v1.2), all fixed in 1.3 and current in 1.8.
Code: `src/1k_foreign_key_pairing.sql`, `src/2f_rename_following.sql`, `src/2h_health_checks.sql`,
`src/2o_updatable_view_column_follow.sql`, `src/2a_add_foreign_key.sql`. Tests: 124, 126, 128, 129, 132.
Related decision: "Use Constraint Triggers for Regular-to-Temporal Foreign Keys" in `doc/decision-log.md`.

## A1: XOR key column order (reported by StatBus, STATBUS-519)

Rename following rewrote `unique_keys.column_names` of every unique key with `mutually_exclusive_columns` on any
DDL that reached it (on 1.0 even on GRANT), **sorted by name**, while the native constraint kept the order given to
`add_unique_key`. From then on catalog and constraint disagreed, and 1.2's `check_integrity()`, which compares
`conkey` with `column_names || range` in order, called a healthy key noncanonical. The 1.1 to 1.2 update, ending
with that check, refused every database holding such a key.

Rule since 1.3: the order of a key's columns is the order of its native constraint
(`__internal_unique_key_constraint_columns`). Rename following rewrites the catalog list only when the **set** of
names changed (a real rename), in place, and follows the rename in `mutually_exclusive_columns` too. The update
scripts repair the order before any other step, with the old rename-following trigger disabled, because that
trigger re-sorts on the update's own DDL.

## A2: column order of regular foreign key checks

`add_regular_foreign_key` built the child check's argument list with `string_agg` without `ORDER BY` over a join
with `pg_attribute`. On wider tables the planner chose a merge join sorted by name, so the check passed the child
columns alphabetically while the helper expected key order. A valid row could be refused, depending on the
database's statistics. The 1.3 constraint-trigger design removed the check; every column list is built in
ordinality order.

## Foreign key pairing

A foreign key pairs its i-th child column (`foreign_keys.column_names[i]`) with the i-th column of the referenced
key **in the order of the key's native constraint**. After A1 sorted a key's catalog list, `add_foreign_key` only
accepted the sorted list and paired the child columns with it, so foreign keys created afterwards were mispaired:
valid references refused, swapped rows accepted. Since 1.3:

- `add_foreign_key` and `drop_unique_key` compare column lists as sets and order the child columns against the
  constraint order;
- attribution and `check_integrity()` compare what the managed objects implement (native `confkey`, the
  referenced-side trigger arguments) with the constraint order, and report a mispaired foreign key with the exact
  `drop_foreign_key` / `add_foreign_key` statements. A mispairing cannot be repaired without knowing the intent, so
  the update refuses it before changing anything.

## A3: updatable views follow their base table's columns

A `for_portion_of` or `current` view was created as `AS TABLE t`, which freezes its column list. `ALTER TABLE ...
ADD COLUMN` on the base afterwards is a normal operation, but `check_integrity()` required every base column in the
view, so healthy databases failed it. Since 1.3 the managed views follow ADD, DROP and RENAME COLUMN and supported
ALTER COLUMN TYPE: they are rebuilt with their grants, column grants, comments, defaults, user triggers and options
kept, and `updatable_view.generated_columns` is recomputed in the same step (README.md, "Advanced Usage").

## Restore order (class B)

The child side of a regular foreign key was an inline CHECK that read the parent table. pg_dump creates it before
loading data, so a child table whose data sorts before the parent's failed even a serial restore. The fix is the
1.3 decision to use a post-data constraint trigger. Old dumps (1.0, 1.1) are restored at their own extension
version, then updated (`doc/upgrade.md`, "Restore at the dump's own extension version").
