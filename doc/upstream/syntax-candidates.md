# Candidate SQL syntaxes for temporal merge and regular-to-temporal foreign keys

Status: exploration for hug task axdept (epic 0t4v31), 2026-11-24. Not a specification and not a grammar patch: three
candidate shapes, each written out for every temporal_merge case, to make a first conversation with PostgreSQL's
temporal authors concrete. The full proposal (task v6s4xq) comes only after their interest.

Builds on [temporal-model-map.md](temporal-model-map.md) (what exists) and
[for-portion-of-revert.md](for-portion-of-revert.md) (the concurrency question every candidate must answer).

## The running example

```sql
CREATE TABLE unit (
    id        int,
    name      text,
    region    text,
    edit_by   text,                -- audit column: written, but never a reason to keep two pieces apart
    valid_at  daterange,
    PRIMARY KEY (id, valid_at WITHOUT OVERLAPS)
);
-- staged import: each row says "this entity had these values over this period"
CREATE TABLE import (id int, name text, region text, edit_by text, valid_at daterange);
```

Terms used below:

- **slice**: the period a source row carries (`import.valid_at`).
- **portion semantics**: inside a slice, the change applies per time point; target time outside every slice is left
  as it was (the leftovers of FOR PORTION OF).
- **coalescing**: adjacent rows of one entity whose values are equal become one row.

The eight temporal_merge modes are the product of two choices plus two special cases:

| what may happen to an entity / how values combine | UPSERT (NULL is a value) | PATCH (NULL keeps old) | REPLACE (all columns) |
|---|---|---|---|
| existing time only (`*_FOR_PORTION_OF`) | `UPDATE_FOR_PORTION_OF` | `PATCH_FOR_PORTION_OF` | `REPLACE_FOR_PORTION_OF` |
| existing time, gaps and new entities (`MERGE_ENTITY_*`) | `MERGE_ENTITY_UPSERT` | `MERGE_ENTITY_PATCH` | `MERGE_ENTITY_REPLACE` |

plus `INSERT_NEW_ENTITIES` and `DELETE_FOR_PORTION_OF`, and the delete modes `DELETE_MISSING_TIMELINE` (inside the
source's entities, remove time no slice covers) and `DELETE_MISSING_ENTITIES` (remove entities not in the source).

(UPSERT and REPLACE differ in what an *absent* column means: UPSERT keeps the target value, REPLACE sets it to NULL.
In SQL, where a statement names its columns, "absent" is "not in the SET list", so REPLACE is "SET every column".)

## Candidate A: MERGE with a per-row portion

Extend standard `MERGE` with one clause, `FOR PORTION OF <target period> USING <source period expression>`. Each
source row then acts only on its slice, per time point, and the `WHEN` clauses apply to sub-periods instead of rows:

- `WHEN MATCHED`: the parts of the slice where the entity has target rows;
- `WHEN NOT MATCHED`: the parts of the slice where it has none (a gap, or a new entity);
- `WHEN NOT MATCHED BY SOURCE`: target time of the entity that no slice covers (and, for an entity absent from the
  source, all of its time).

```sql
-- MERGE_ENTITY_UPSERT
MERGE INTO unit AS t
USING import AS s ON t.id = s.id
FOR PORTION OF t.valid_at USING s.valid_at
WHEN MATCHED THEN UPDATE SET name = s.name, region = s.region, edit_by = s.edit_by
WHEN NOT MATCHED THEN INSERT (id, name, region, edit_by) VALUES (s.id, s.name, s.region, s.edit_by);
```

The period column is not listed in `INSERT`: it is the sub-period being filled. All modes:

| temporal_merge | Candidate A |
|---|---|
| `MERGE_ENTITY_UPSERT` | as above |
| `MERGE_ENTITY_PATCH` | `UPDATE SET name = COALESCE(s.name, t.name), region = COALESCE(s.region, t.region), ...` |
| `MERGE_ENTITY_REPLACE` | `UPDATE SET` every non-key column, setting those the source does not supply to NULL |
| `UPDATE_FOR_PORTION_OF` | `WHEN MATCHED` only (no `WHEN NOT MATCHED`: gaps and new entities are skipped) |
| `PATCH_FOR_PORTION_OF` / `REPLACE_FOR_PORTION_OF` | as the two above, `WHEN MATCHED` only |
| `DELETE_FOR_PORTION_OF` | `WHEN MATCHED THEN DELETE` |
| `INSERT_NEW_ENTITIES` | `WHEN NOT MATCHED AND NOT EXISTS (SELECT FROM unit u WHERE u.id = s.id) THEN INSERT ...`, or a new condition `WHEN NOT MATCHED ENTITY` |
| `DELETE_MISSING_TIMELINE` | `WHEN NOT MATCHED BY SOURCE AND <entity is in the source> THEN DELETE` (needs a way to say "entity in source", see open questions) |
| `DELETE_MISSING_ENTITIES` | `WHEN NOT MATCHED BY SOURCE AND <entity not in the source> THEN DELETE` (sql_saga allows this with the `MERGE_ENTITY_*` modes only; A would allow it with any set of clauses) |

Coalescing and ignored columns are a property of the table, not the statement (see "Table-level declarations").

Fit: MERGE is already the standard's "apply a source to a target" statement, with the three cases temporal_merge
needs and `RETURNING merge_action()` for feedback. SQL:2011 defines FOR PORTION OF for UPDATE and DELETE (in
PostgreSQL it was reverted from 19 and is under review again as v2); letting MERGE use it with a per-row period is a
small step in the grammar and a large one in the executor.

## Candidate B: a merge statement with mode keywords

A new statement that names the temporal_merge choices directly:

```sql
MERGE TEMPORAL INTO unit
USING import
KEY (id)
PERIOD valid_at
[ ENTITIES { EXISTING | NEW | ALL } ]                     -- FOR_PORTION_OF / INSERT_NEW_ENTITIES / MERGE_ENTITY
[ VALUES { UPSERT | PATCH | REPLACE | DELETE } ]          -- how values combine
[ DELETE MISSING { TIMELINE | ENTITIES | TIMELINE AND ENTITIES } ];
```

| temporal_merge | Candidate B |
|---|---|
| `MERGE_ENTITY_UPSERT` | `ENTITIES ALL VALUES UPSERT` |
| `MERGE_ENTITY_PATCH` | `ENTITIES ALL VALUES PATCH` |
| `MERGE_ENTITY_REPLACE` | `ENTITIES ALL VALUES REPLACE` |
| `UPDATE_FOR_PORTION_OF` | `ENTITIES EXISTING VALUES UPSERT` |
| `PATCH_FOR_PORTION_OF` | `ENTITIES EXISTING VALUES PATCH` |
| `REPLACE_FOR_PORTION_OF` | `ENTITIES EXISTING VALUES REPLACE` |
| `DELETE_FOR_PORTION_OF` | `ENTITIES EXISTING VALUES DELETE` |
| `INSERT_NEW_ENTITIES` | `ENTITIES NEW` |
| delete modes | `DELETE MISSING TIMELINE`, `DELETE MISSING ENTITIES`, `DELETE MISSING TIMELINE AND ENTITIES` |

Fit: short, and a one-to-one image of temporal_merge, so easy to explain and to test against sql_saga. Some
combinations mean nothing (`ENTITIES NEW VALUES PATCH`, `ENTITIES ALL VALUES DELETE`) and must be rejected. It is
a second merge statement next to standard MERGE, with columns matched by name instead of an explicit SET list,
which the standard has avoided.

## Candidate C: table declarations plus plain statements

Keep statements standard and move the temporal behaviour into the table:

```sql
ALTER TABLE unit ALTER PERIOD valid_at SET (COALESCE, IGNORE (edit_by));
```

then use the standard statements per slice, with FOR PORTION OF taking a per-row period from a join:

```sql
UPDATE unit AS t FOR PORTION OF valid_at (s.valid_at)   -- per-row bounds: forbidden in the v2 patch
SET name = s.name, region = s.region
FROM import AS s WHERE t.id = s.id;
INSERT INTO unit ... -- for the gaps: needs an "insert where not covered" form, no standard one exists
```

(The reverted grammar takes `FOR PORTION OF col FROM a TO b` or `FOR PORTION OF col (range)`.)

| temporal_merge | Candidate C |
|---|---|
| `*_FOR_PORTION_OF` | `UPDATE`/`DELETE ... FOR PORTION OF` with per-row bounds from `FROM` |
| `MERGE_ENTITY_*` | the UPDATE above, plus an INSERT of the uncovered parts of each slice (no standard form) |
| `INSERT_NEW_ENTITIES` | `INSERT ... SELECT ... WHERE NOT EXISTS` |
| delete modes | `DELETE ... FOR PORTION OF` the complement of the source's coverage (needs range aggregation in the bounds) |
| coalescing, ignored columns | the table declaration |

Fit: the smallest grammar change, and each piece is useful alone. But the user must write and order several
statements, which is exactly what temporal_merge exists to avoid, and per-row bounds are what the v2 patch forbids:
its commit message requires constant bounds, without column references or subqueries.

## Table-level declarations (shared by A, B and C)

Whatever the statement, two properties belong to the table, because every write should respect them:

- **coalescing**: after any write, adjacent rows of one entity with equal values are merged. This keeps a timeline
  canonical, so "did anything change?" is a row comparison. FOR PORTION OF leaves split rows even when the values end
  up equal.
- **columns ignored for equality** (sql_saga's `ephemeral_columns`): audit and provenance columns are written but are
  no reason to keep two pieces apart. sql_saga carries the old values onto untouched pieces, and fills changed ones
  from the writer, or from their `DEFAULT` when the writer does not supply them (`doc/design/provenance.md`).

A possible form: `PERIOD valid_at ... WITH (COALESCE, IGNORE (edit_by, edit_at))` in `CREATE TABLE`, or the
`ALTER TABLE` above.

## The regular-to-temporal foreign key

| Form | Example | Notes |
|---|---|---|
| A: `AT ANY TIME` | `FOREIGN KEY (product_no) REFERENCES products (product_no, PERIOD valid_at) AT ANY TIME` | names the period being projected away; reads as the rule |
| B: key only | `FOREIGN KEY (product_no) REFERENCES products (product_no)` accepted when `products` has a temporal key | shortest, but silently changes the meaning of an existing error |
| C: `EXISTS` | `FOREIGN KEY (product_no) REFERENCES products (product_no) EXISTS IN PERIOD valid_at` | explicit; new keywords |

Semantics for all three: the key must exist in at least one row; the referenced side refuses to delete or re-key the
last row of a referenced key (`NO ACTION`/`RESTRICT`) or deletes the referencing rows then (`CASCADE`). This is what
sql_saga's `add_regular_foreign_key` does today, with tests and a concurrency harness. Form A is the recommended one:
it is explicit about the period and extends the existing `PERIOD` foreign key syntax.

## Comparison

| | A: MERGE + per-row portion | B: MERGE TEMPORAL with modes | C: declarations + plain statements |
|---|---|---|---|
| fit with the standard | builds on MERGE (SQL:2003) and FOR PORTION OF (SQL:2011) | new statement beside MERGE | builds on FOR PORTION OF; gaps have no standard form |
| covers all temporal_merge cases | yes, delete modes need "entity in source" | yes, one-to-one | no single statement for MERGE_ENTITY_* or the delete modes |
| user writes | one statement, explicit SET list | one statement, columns by name | several statements, ordered by the user |
| implementation in PostgreSQL | large: MERGE executor applies actions per sub-period; reuses FOR PORTION OF leftovers and MERGE concurrency | large: new statement, but it can be planned as A internally | smaller pieces, but per-row bounds hit the v2 restrictions |
| concurrency answer | MERGE locks the target rows it matches, but at READ COMMITTED it can still raise a serialization failure, and concurrent `WHEN NOT MATCHED` inserts are unprotected (a unique or exclusion violation): per-entity locking is needed | same as A | each statement alone has the FOR PORTION OF problem |
| testable against sql_saga | yes, each example maps to a temporal_merge call, once bugs rtwrp6 and 2hxkwx in the delete modes are fixed | yes, directly, with the same caveat | partly |
| open questions | see below | naming; columns by name | gap insertion; ordering |

**Recommendation: A**, with table-level declarations for coalescing and ignored columns, and form A for the
regular-to-temporal foreign key. It asks the least new syntax of the standard for the most coverage, and each
example can be checked against temporal_merge, so sql_saga's regression tests can become its conformance tests once
the delete-mode bugs (rtwrp6, 2hxkwx) are fixed.

## Open questions for candidate A

1. **"Entity in the source"** for `DELETE_MISSING_TIMELINE`: `WHEN NOT MATCHED BY SOURCE` today means "target row
   with no source row". Temporally it must distinguish "entity in the source, this time not covered" from "entity
   absent". Options: two conditions (`BY SOURCE TIME` / `BY SOURCE ENTITY`), or a predicate.
2. **Overlapping source slices** for one entity: temporal_merge resolves them by `row_id` order. MERGE forbids
   one target row matching two source rows; temporally the natural rule is per time point, which needs an order.
3. **New entities whose key is generated** (identity column) and spread over several source rows: temporal_merge's
   `founding_id_column`. MERGE has no way to say that two source rows are the same new entity.
4. **Feedback per source row**: `RETURNING merge_action()` reports per target row; import pipelines want per source
   row status (`APPLIED`, `SKIPPED_*`).
5. **Concurrency**: the statement must give serial results at READ COMMITTED (bug 7rzgv5 for sql_saga; the reason
   FOR PORTION OF was reverted). Locking per entity before planning gives serial results in sql_saga's prototype
   for the tested races; the view path, natural keys and other writers are still open
   ([for-portion-of-revert.md](for-portion-of-revert.md)).
6. **Statement triggers and leftovers**: the FOR PORTION OF review objected to firing statement triggers per leftover
   row; a temporal MERGE writes many leftovers per statement and needs a clear trigger model.
