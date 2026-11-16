# Manual schema changes of a system-versioned table

Status: implemented in 1.1, current in 1.7. Owner-approved 2026-10-14 (rule), answers 2026-10-15.
Code: `src/1i_add_system_versioning.sql` (`rebuild_system_versioning_views`, `__internal_check_history_columns`),
`src/sql_saga.c` (`write_history`, `CheckHistoryTupleCompatibility`). Tests: 027, 056.
User documentation: README.md, "Manual schema changes".

## Rule

A change to the columns of a table with history is kept consistent **manually**: the owner changes the base table
`B` and its history table `H` explicitly, then calls `sql_saga.rebuild_system_versioning_views(B)`. sql_saga never
propagates table DDL to the history. It checks consistency and either accepts the result or refuses with the exact
fix (owner principle: actionable fail fast; nothing is glossed over or repaired silently).

## Invariants

1. Columns correspond by **name**, never by attribute number.
2. Every live user column of `B` exists in `H` with the same type, typmod and collation. `H` may have more columns.
3. A history row written from `B` gets NULL in every column found only in `H`.
4. `B_with_history` exposes the union of both tables' columns: `B`'s columns in `B`'s order, then history-only
   columns in `H`'s order. The current-row branch emits typed NULLs for history-only columns.
5. The gated view, `B_with_history` and the four as-of functions keep the owner, ACL, options, access gate, RLS
   branches and policy check of the history access design (`history-access.md`).
6. A rebuild is idempotent.
7. `without_columns` is a projection exclusion for one call, never stored: it releases view and function
   dependencies so a column can be dropped or renamed. A later call without the name brings the column back if it
   still exists.
8. A write that needs history fails before inserting when a `B` column is missing from `H` or has another type. It
   never silently drops a value.

Compatibility is the same row-type comparison `add_system_versioning` applies to a pre-existing history table:
`attname`, `atttypid`, `atttypmod` and `attcollation` of every live user attribute. Domains, typmods
(`varchar(20)` against `varchar(30)`) and collations are significant. Generated, default, not-null and identity
properties are not compared, because the writer supplies a value or NULL; a history-only `NOT NULL` column without a
default therefore fails with PostgreSQL's own constraint error.

## Rebuild

`rebuild_system_versioning_views(table_oid regclass, without_columns name[] DEFAULT '{}')`:

1. refuses NULL, a table without system versioning, and a caller who is not a member of the base owner role (42501);
2. refuses a `without_columns` entry that is NULL, listed twice, absent from both tables, or the system-time range
   column;
3. serializes on the table and sets the transaction-local flag `sql_saga.rebuilding_system_versioning_views` to the
   table's OID, so drop protection and the health check let exactly this family's views and functions be replaced;
   the flag is restored on success and on error;
4. drops the four as-of functions, then `B_with_history`, then the gated view, never with CASCADE: a user object
   depending on them stops the rebuild with PostgreSQL's dependency error;
5. recreates them from the current columns with the same generator `add_system_versioning` uses, so they get new
   OIDs, then synchronizes the history policies;
6. reports one NOTICE naming what was rebuilt.

Everything runs in the caller's transaction; an error rolls the family back.

## Writer

`write_history` projects the old base row onto the history row by name: it checks every live base column against
`H`, copies each history column that exists in `B`, leaves history-only columns NULL, and writes the closed system
time range into the history's range column. A mismatch raises before insertion:

```
ERROR:  sql_saga: column <c> of base table <B> is missing from history table <H>
HINT:  Run: ALTER TABLE <H> ADD COLUMN <c> <type>; SELECT sql_saga.rebuild_system_versioning_views('<B>');
```

and, for a different type, typmod or collation, the same with `ALTER TABLE <H> ALTER COLUMN <c> TYPE <type>`.

## Dump and restore

The rebuilt objects are ordinary schema objects and are dumped as they are. The rebuild stores no state, so a
restore of a stable final state needs no rebuild. A dump taken between the two rebuilds of a drop or rename
workflow faithfully contains the transient projection.
