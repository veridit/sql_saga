# Temporal INSERT, UPDATE, DELETE and MERGE: principles, design space and syntax

Status: draft for hug task v6s4xq (epic 0t4v31), 2026-11-25. Research only; nothing here has been sent upstream.
Supersedes candidate A of [syntax-candidates.md](syntax-candidates.md), which covered MERGE alone.

Every result table below is real output: [repro/temporal_dml_examples.sql](repro/temporal_dml_examples.sql) runs each
example with sql_saga 1.15's `temporal_merge`, and [repro/temporal_dml_examples.out](repro/temporal_dml_examples.out)
is its output. Each example also names the sql_saga regression test that holds the same behaviour.

The goal is consistency and clarity: one word for one concept, the same clause meaning the same thing in every
statement. The number of new keywords is not the measure.

## Vocabulary

The words sql_saga uses, which this design keeps:

| Word | Meaning |
|---|---|
| **entity** | One thing tracked over time (a unit, a person). Its rows share an identity. |
| **identity** | The columns that name an entity and never change: the primary key without its period (`id`). |
| **natural key** | Other columns that name an entity (`code`). Unique over time, and bound to one entity both ways: a value belongs to one entity, and an entity holds one value (NULL before the key starts and after it is retired). |
| **timeline** | An entity's rows over time. At most one row per point in time (`WITHOUT OVERLAPS`). May have gaps. |
| **whole timeline** | All of an entity's time. |
| **portion** | A part of the time axis given by a period; the part of a timeline inside it. |
| **piece** | One stored row of a timeline: an entity's values over one period. |
| **ephemeral column** | A column that is written but is no reason to keep two pieces apart (`edit_by`, `edit_at`). |
| **coalesce** | Merge adjacent pieces of one entity whose values are equal, ephemeral columns aside. |
| **source** | The rows a MERGE applies (an import). Each source row states one entity's values over its period. |

## Where this comes from

This is not a new idea, and its history explains the shape proposed here.

- **TSQL2 and SQL/Temporal (1992 to 2001).** Richard Snodgrass and others designed TSQL2 and brought it to the SQL
  committees, where it became a working draft, Part 7 "SQL/Temporal". Its central idea was a statement modifier:
  `VALIDTIME` before any statement asks for *sequenced* semantics, meaning the statement applies at each point in
  time separately; `NONSEQUENCED VALIDTIME` treats the period as an ordinary column. Böhlen and Jensen later gave
  statement modifiers a formal semantics (ATSQL). The ISO project was cancelled in 2001 after disagreement in the
  committee on where temporal support should go; the TSQL2 approach, with its hidden timestamp columns, was strongly
  criticised by Hugh Darwen and Chris Date.
  ([Snodgrass's account](https://rts.cs.arizona.edu/initiatives/indexOLD.html),
  [Böhlen and Jensen, "Temporal Statement Modifiers"](https://files.ifi.uzh.ch/boehlen/Papers/BJS00.pdf),
  [Darwen and Date, "An Overview and Analysis of Proposals Based on the TSQL2 Approach"](https://www.dcs.warwick.ac.uk/~hugh/TTM/OnTSQL2.pdf))
- **SQL:2011** took a small, explicit subset instead: periods made of visible columns, `FOR PORTION OF` on
  `UPDATE` and `DELETE` with one period per statement, and `WITHOUT OVERLAPS` keys. No hidden columns, no modifier.
- **PostgreSQL.** A 2016 patch from the same research group (Dignös, Gamper, Böhlen, Moser) added `NORMALIZE` and
  `ALIGN` for temporal queries; it was returned with feedback in 2017 and the authors split it up
  ([commitfest 839](https://commitfest.postgresql.org/patch/839/)). PostgreSQL 18 shipped `WITHOUT OVERLAPS` keys and
  temporal foreign keys; `UPDATE/DELETE FOR PORTION OF` was committed for 19 and reverted over concurrency
  ([for-portion-of-revert.md](for-portion-of-revert.md)).

What this design takes from each: the sequenced meaning from SQL/Temporal, but spelled with SQL:2011's visible periods
and `FOR PORTION OF` instead of hidden columns and a prefix; and from the revert, the requirement that every
statement gives the serial result at READ COMMITTED.

## Principles

1. **Pointwise meaning.** A temporal statement means, at each point in time, what the plain statement means on the
   table's state at that point. (This is sequenced semantics.)
2. **A piece is the same at every point of its period**, so anything that reads a row reads it pointwise already:
   `WHERE region = 'North'` and `SET name = 'x'` need no temporal syntax. Only *which time* a statement covers needs
   syntax, and that syntax is `FOR PORTION OF`.
3. **The default scope is the whole timeline.** A statement without `FOR PORTION OF` acts on the whole timeline of
   every entity it selects. `FOR PORTION OF` narrows it to a portion. The same rule in `UPDATE`, `DELETE` and
   `MERGE`.
4. **One period per statement, or one per source row.** `UPDATE` and `DELETE` take one period (as SQL:2011).
   `MERGE` takes the period of each source row. A statement never needs to know how a timeline is split into pieces.
5. **The timeline is canonical.** After every statement, adjacent pieces of one entity with equal values (ephemeral
   columns aside) are one piece. This is a property of the table, declared once, not a clause of each statement. So
   "did anything change?" is a row comparison, and two ways of writing the same history store the same rows.
6. **Entities are named by keys.** A source row names its entity by its identity and its natural keys. A NULL key
   names nothing. All keys a row names must find the same entity, or the statement fails: a row states one entity's
   facts. A natural key holds one value per entity (principle of sql_saga 1.15, hug 40nwvn).
7. **Two questions decide every case: is the entity known, and is the time known?** A temporal MERGE has one clause
   for each answer, on the target side and on the source side (the matrix below).
8. **Nothing is removed unless the statement says so.** Time or entities the source does not mention are kept,
   unless a `BY SOURCE` clause names them.
9. **One meaning per word.** `FOR PORTION OF`, `ENTITY`, `TIMELINE`, `MATCHED` and `BY SOURCE` mean the same in every
   statement they appear in.
10. **The serial result at READ COMMITTED.** A temporal statement locks the entities it names, by every key, before
    it reads their timelines, so concurrent statements give the result of running one after the other, without a
    retry (sql_saga 1.15, [doc/design/temporal-merge-concurrency.md](../design/temporal-merge-concurrency.md)).

## The design space

### Plain statements

| Statement | Entities | Time | At each point |
|---|---|---|---|
| `INSERT` | the rows' entities, new or known | each row's period | adds the row; a point the entity already has is a key violation, as today |
| `UPDATE ... WHERE` | those `WHERE` selects | whole timeline | sets the values |
| `UPDATE ... FOR PORTION OF p ... WHERE` | those `WHERE` selects | the portion `p` | sets the values; pieces split at `p`'s bounds |
| `DELETE ... WHERE` | those `WHERE` selects | whole timeline | removes the entity |
| `DELETE ... FOR PORTION OF p ... WHERE` | those `WHERE` selects | the portion `p` | removes the time; a gap stays |
| any statement that writes or tests the period column itself | as written | as written | nonsequenced: acts on the rows as stored (the escape hatch) |

`INSERT` keeps its SQL meaning: it adds time. A row that overlaps time the entity already has is a key violation, as
in PostgreSQL 18 today. "Add only the time the target lacks" is a comparison of a source with a target, which is what
`MERGE` is for (example M8 below).

### MERGE: the two questions

A temporal `MERGE` matches each source row to an entity (by `ON`) and, inside the row's period, each point to the
entity's timeline. That gives five cases, two of which are new:

| Case | Entity | Time | Clause | Actions |
|---|---|---|---|---|
| matched | in the target | in its timeline | `WHEN MATCHED` | `UPDATE`, `DELETE`, `DO NOTHING` |
| timeline not matched | in the target | not in its timeline (a gap, or before or after it) | `WHEN TIMELINE NOT MATCHED` | `INSERT`, `DO NOTHING` |
| entity not matched | not in the target | | `WHEN ENTITY NOT MATCHED` | `INSERT`, `DO NOTHING` |
| timeline not matched by source | in the source | target time no source row of it covers | `WHEN TIMELINE NOT MATCHED BY SOURCE` | `UPDATE`, `DELETE`, `DO NOTHING` |
| entity not matched by source | not in the source | its whole timeline | `WHEN ENTITY NOT MATCHED BY SOURCE` | `UPDATE`, `DELETE`, `DO NOTHING` |

The standard clauses keep their meaning, pointwise: `WHEN NOT MATCHED` is both target-side cases (no target row at
this point), and `WHEN NOT MATCHED BY SOURCE` (PostgreSQL 17) is both source-side cases. The qualified clauses split
them. The subject of each clause is what failed to match: the entity, or its timeline.

In a `TIMELINE NOT MATCHED` insert, the entity is known: the new piece takes the entity's identity and natural keys,
and its period is the uncovered part of the source row's period. In an `ENTITY NOT MATCHED` insert the entity is new:
its identity comes from the column's `DEFAULT` (a sequence), evaluated once per new entity (see open question 3).

### sql_saga's modes in the matrix

Each `temporal_merge` mode is one choice per case. `SET` lists replace the value modes: what sql_saga calls UPSERT,
PATCH and REPLACE is which columns the `SET` names and how.

| sql_saga | `MATCHED` | `TIMELINE NOT MATCHED` | `ENTITY NOT MATCHED` | `... BY SOURCE` |
|---|---|---|---|---|
| `MERGE_ENTITY_UPSERT` | `UPDATE SET c = s.c` | `INSERT` | `INSERT` | |
| `MERGE_ENTITY_PATCH` | `UPDATE SET c = COALESCE(s.c, t.c)` | `INSERT` | `INSERT` | |
| `MERGE_ENTITY_REPLACE` | `UPDATE SET` every column | `INSERT` | `INSERT` | |
| `UPDATE_FOR_PORTION_OF` | `UPDATE SET c = s.c` | | | |
| `PATCH_FOR_PORTION_OF` | `UPDATE SET c = COALESCE(s.c, t.c)` | | | |
| `REPLACE_FOR_PORTION_OF` | `UPDATE SET` every column | | | |
| `DELETE_FOR_PORTION_OF` | `DELETE` | | | |
| `INSERT_NEW_ENTITIES` | | | `INSERT` | |
| `delete_mode => DELETE_MISSING_TIMELINE` | | | | `TIMELINE NOT MATCHED BY SOURCE THEN DELETE` |
| `delete_mode => DELETE_MISSING_ENTITIES` | | | | `ENTITY NOT MATCHED BY SOURCE THEN DELETE` |

An empty cell is no clause: those points are left as they are (sql_saga reports `SKIPPED_NO_TARGET` or
`SKIPPED_FILTERED` for a source row that no clause applies to).

The matrix also shows combinations sql_saga has no mode for, which the grammar gives for free:

| Combination | Meaning |
|---|---|
| `TIMELINE NOT MATCHED` and `ENTITY NOT MATCHED` insert, no `MATCHED` clause | add only the time the target lacks, never change known time (M8) |
| `ENTITY NOT MATCHED BY SOURCE THEN UPDATE SET status = 'closed'` | mark entities the source no longer has, instead of deleting them |
| `TIMELINE NOT MATCHED BY SOURCE THEN UPDATE ...` | mark time the source no longer states |
| `MATCHED AND s.x IS DISTINCT FROM t.x THEN UPDATE` | the standard `AND` condition on a clause, pointwise |

Combinations that mean nothing are rejected by the clause/action table above: an `INSERT` in a `BY SOURCE` clause
(there is no source row), an `UPDATE` or `DELETE` in a `NOT MATCHED` clause (there is no target row).

## Syntax

### Table declarations

```sql
CREATE TABLE unit (
    id       int GENERATED BY DEFAULT AS IDENTITY,
    code     text,
    name     text,
    region   text,
    edit_by  text,
    valid_at daterange,
    PRIMARY KEY (id, valid_at WITHOUT OVERLAPS),          -- the identity: shipped in PostgreSQL 18
    NATURAL KEY (code, valid_at WITHOUT OVERLAPS),        -- proposed: unique over time, bound to one entity both ways
    TIMELINE (valid_at) COALESCE EPHEMERAL (edit_by)      -- proposed: canonical timeline, edit_by ignored when comparing
);
```

- `NATURAL KEY` is a `UNIQUE ... WITHOUT OVERLAPS` key plus the two rules of principle 6. Both rules can be written
  today as exclusion constraints (`EXCLUDE (code WITH =, id WITH <>)` and `EXCLUDE (id WITH =, code WITH <>)`), which
  is how sql_saga 1.15 enforces them; the declaration names the intent and lets the server use it to match entities.
- `TIMELINE (valid_at) COALESCE EPHEMERAL (...)` declares principle 5 for the table. sql_saga's equivalent is
  `add_era(..., ephemeral_columns => ...)` with `temporal_merge` coalescing.

### UPDATE and DELETE

As in SQL:2011 and PostgreSQL's FOR PORTION OF patch:

```sql
UPDATE unit FOR PORTION OF valid_at FROM '2022-01-01' TO '2023-01-01'
SET region = 'East', edit_by = 'imp' WHERE code = 'B';           -- a portion (E10)

UPDATE unit SET region = 'West', edit_by = 'imp' WHERE id = 1;    -- the whole timeline (E9)

DELETE FROM unit FOR PORTION OF valid_at FROM '2023-01-01' TO '2027-01-01'
WHERE id = 1;                                                  -- a portion (E5)

DELETE FROM unit WHERE id = 2;                                 -- the whole timeline: the entity
```

### MERGE

```sql
MERGE INTO target [ AS t ]
    [ FOR PORTION OF period_column ( source_period_expression ) ]
USING source [ AS s ] ON entity_condition
WHEN MATCHED [ AND condition ] THEN { UPDATE SET ... | DELETE | DO NOTHING }
WHEN TIMELINE NOT MATCHED [ AND condition ] THEN { INSERT ... | DO NOTHING }
WHEN ENTITY NOT MATCHED [ AND condition ] THEN { INSERT ... | DO NOTHING }
WHEN NOT MATCHED [ AND condition ] THEN { INSERT ... | DO NOTHING }                -- both of the above
WHEN TIMELINE NOT MATCHED BY SOURCE [ AND condition ] THEN { UPDATE SET ... | DELETE | DO NOTHING }
WHEN ENTITY NOT MATCHED BY SOURCE [ AND condition ] THEN { UPDATE SET ... | DELETE | DO NOTHING }
WHEN NOT MATCHED BY SOURCE [ AND condition ] THEN { UPDATE SET ... | DELETE | DO NOTHING }  -- both of the above
```

- `FOR PORTION OF valid_at (s.valid_at)` is written where `UPDATE` writes it, after the target, with the source
  row's period in place of a constant one. Without it, `MERGE` acts on whole timelines (principle 3).
- `entity_condition` is equality on keys of the target: `t.id = s.id`, `t.code = s.code`, or several joined by
  `OR`. Each source row must find at most one entity (principle 6).
- An `INSERT` does not list the period column: it is the part of the source row's period being filled.

## Examples

The target before every example ([temporal_dml_examples.sql](repro/temporal_dml_examples.sql)):

| id | code | name | region | edit_by | valid_from | valid_until |
|---|---|---|---|---|---|---|
| 1 | A | Alpha | North | load | 2020-01-01 | 2024-01-01 |
| 1 | A | Alpha | South | load | 2024-01-01 | 2026-01-01 |
| 1 | A | Alpha | South | load | 2027-01-01 | infinity |
| 2 | B | Beta | North | load | 2020-01-01 | infinity |

Entity 1 has a gap in 2026. The import of most examples renames entity 1 over [2023, 2027), giving no region, and
brings a new entity C from 2025:

| row_id | id | code | name | region | edit_by | valid_at |
|---|---|---|---|---|---|---|
| 1 | 1 | A | Alpha2 | | imp | [2023-01-01,2027-01-01) |
| 2 | | C | Gamma | West | imp | [2025-01-01,infinity) |

### M1. Upsert: known time updated, NULL is a value; gap and new entity inserted

```sql
MERGE INTO unit AS t FOR PORTION OF valid_at (s.valid_at)
USING import AS s ON t.id = s.id OR t.code = s.code
WHEN MATCHED THEN UPDATE SET name = s.name, region = s.region, edit_by = s.edit_by
WHEN NOT MATCHED THEN INSERT (code, name, region, edit_by) VALUES (s.code, s.name, s.region, s.edit_by);
```

`MERGE_ENTITY_UPSERT` (E1, test 064 scenario 7):

| id | code | name | region | edit_by | valid_from | valid_until |
|---|---|---|---|---|---|---|
| 1 | A | Alpha | North | load | 2020-01-01 | 2023-01-01 |
| 1 | A | Alpha2 | | imp | 2023-01-01 | 2027-01-01 |
| 1 | A | Alpha | South | load | 2027-01-01 | infinity |
| 2 | B | Beta | North | load | 2020-01-01 | infinity |
| 10 | C | Gamma | West | imp | 2025-01-01 | infinity |

The three pieces from 2023 to 2027 (two updated, one filling the gap) have equal values and are one row (principle 5).
Entity C got the identity 10 from the sequence.

### M2. Patch: as M1, but a NULL keeps the value at each point

```sql
WHEN MATCHED THEN UPDATE SET name = COALESCE(s.name, t.name), region = COALESCE(s.region, t.region), edit_by = s.edit_by
```

`MERGE_ENTITY_PATCH` (E2, test 064 scenario 1):

| id | code | name | region | edit_by | valid_from | valid_until |
|---|---|---|---|---|---|---|
| 1 | A | Alpha | North | load | 2020-01-01 | 2023-01-01 |
| 1 | A | Alpha2 | North | imp | 2023-01-01 | 2024-01-01 |
| 1 | A | Alpha2 | South | imp | 2024-01-01 | 2026-01-01 |
| 1 | A | Alpha2 | | imp | 2026-01-01 | 2027-01-01 |
| 1 | A | Alpha | South | load | 2027-01-01 | infinity |
| 2 | B | Beta | North | load | 2020-01-01 | infinity |
| 10 | C | Gamma | West | imp | 2025-01-01 | infinity |

`t.region` is the target's value at each point, so North and South are each kept where they were. The gap has no
target value to keep: it gets the source's NULL.

### M3. Update a portion of known time only

```sql
MERGE INTO unit AS t FOR PORTION OF valid_at (s.valid_at)
USING import AS s ON t.id = s.id OR t.code = s.code
WHEN MATCHED THEN UPDATE SET name = s.name, region = s.region, edit_by = s.edit_by;
```

`UPDATE_FOR_PORTION_OF` (E3, test 064 scenario 8). No `NOT MATCHED` clause: the gap stays a gap and entity C is not
created (sql_saga reports `SKIPPED_NO_TARGET` for row 2):

| id | code | name | region | edit_by | valid_from | valid_until |
|---|---|---|---|---|---|---|
| 1 | A | Alpha | North | load | 2020-01-01 | 2023-01-01 |
| 1 | A | Alpha2 | | imp | 2023-01-01 | 2026-01-01 |
| 1 | A | Alpha | South | load | 2027-01-01 | infinity |
| 2 | B | Beta | North | load | 2020-01-01 | infinity |

With `COALESCE` in the `SET`, this is `PATCH_FOR_PORTION_OF` (E4, test 064 scenario 3).

### M4. Delete a portion

```sql
MERGE INTO unit AS t FOR PORTION OF valid_at (s.valid_at)
USING import AS s ON t.id = s.id OR t.code = s.code
WHEN MATCHED THEN DELETE;
```

`DELETE_FOR_PORTION_OF` (E5, test 064 scenario 6). Entity 1 loses [2023, 2026); entity C is not created:

| id | code | name | region | edit_by | valid_from | valid_until |
|---|---|---|---|---|---|---|
| 1 | A | Alpha | North | load | 2020-01-01 | 2023-01-01 |
| 1 | A | Alpha | South | load | 2027-01-01 | infinity |
| 2 | B | Beta | North | load | 2020-01-01 | infinity |

### M5. New entities only

```sql
MERGE INTO unit AS t FOR PORTION OF valid_at (s.valid_at)
USING import AS s ON t.id = s.id OR t.code = s.code
WHEN ENTITY NOT MATCHED THEN INSERT (code, name, region, edit_by) VALUES (s.code, s.name, s.region, s.edit_by);
```

`INSERT_NEW_ENTITIES` (E6, test 064 scenario 5). Entity 1 is known, so row 1 does nothing (`SKIPPED_FILTERED`), and
its gap stays a gap: that is what distinguishes `ENTITY NOT MATCHED` from `NOT MATCHED`. Entity C is created as in M1.

### M6. The source states whole timelines: time it does not cover is removed

```sql
MERGE INTO unit AS t FOR PORTION OF valid_at (s.valid_at)
USING import AS s ON t.id = s.id OR t.code = s.code
WHEN MATCHED THEN UPDATE SET name = COALESCE(s.name, t.name), region = COALESCE(s.region, t.region), edit_by = s.edit_by
WHEN NOT MATCHED THEN INSERT (code, name, region, edit_by) VALUES (s.code, s.name, s.region, s.edit_by)
WHEN TIMELINE NOT MATCHED BY SOURCE THEN DELETE;
```

`MERGE_ENTITY_PATCH` with `DELETE_MISSING_TIMELINE` (E7, test 163). Entity 1 keeps only [2023, 2027); entity 2, not
in the source, is untouched:

| id | code | name | region | edit_by | valid_from | valid_until |
|---|---|---|---|---|---|---|
| 1 | A | Alpha2 | North | imp | 2023-01-01 | 2024-01-01 |
| 1 | A | Alpha2 | South | imp | 2024-01-01 | 2026-01-01 |
| 1 | A | Alpha2 | | imp | 2026-01-01 | 2027-01-01 |
| 2 | B | Beta | North | load | 2020-01-01 | infinity |
| 10 | C | Gamma | West | imp | 2025-01-01 | infinity |

### M7. The source states every entity: entities it does not name are removed

As M2, with `WHEN ENTITY NOT MATCHED BY SOURCE THEN DELETE`: `DELETE_MISSING_ENTITIES` (E8, test 163). The result is
M2 without entity 2.

### M8. Add only the time the target lacks (no sql_saga mode)

```sql
MERGE INTO unit AS t FOR PORTION OF valid_at (s.valid_at)
USING import AS s ON t.id = s.id OR t.code = s.code
WHEN NOT MATCHED THEN INSERT (code, name, region, edit_by) VALUES (s.code, s.name, s.region, s.edit_by);
```

Entity 1 gets [2026, 2027) with name Alpha2 and no region; its known time is not changed; entity C is created. This is
the "insert that fills the uncovered time" asked about for `INSERT`, and it needs no change to `INSERT`.
(sql_saga has no mode for it; the expected result is derived from M1's gap row, not run.)

### M9. A whole timeline, without a portion

```sql
UPDATE unit SET region = 'West', edit_by = 'imp' WHERE id = 1;
```

E9 (`PATCH_FOR_PORTION_OF` over all time). Every point of entity 1 gets West. The two pieces before the gap now have
equal values (`edit_by` is ephemeral) and become one; the gap stays:

| id | code | name | region | edit_by | valid_from | valid_until |
|---|---|---|---|---|---|---|
| 1 | A | Alpha | West | imp | 2020-01-01 | 2026-01-01 |
| 1 | A | Alpha | West | imp | 2027-01-01 | infinity |
| 2 | B | Beta | North | load | 2020-01-01 | infinity |

### M10. An entity named by its natural key

```sql
UPDATE unit FOR PORTION OF valid_at FROM '2022-01-01' TO '2023-01-01'
SET region = 'East', edit_by = 'imp' WHERE code = 'B';
```

E10 (`PATCH_FOR_PORTION_OF`, test 165). Entity 2, found by code B, is split around 2022:

| id | code | name | region | edit_by | valid_from | valid_until |
|---|---|---|---|---|---|---|
| 2 | B | Beta | North | load | 2020-01-01 | 2022-01-01 |
| 2 | B | Beta | East | imp | 2022-01-01 | 2023-01-01 |
| 2 | B | Beta | North | load | 2023-01-01 | infinity |

### M11. Behaviours every temporal MERGE must define

| Case | sql_saga (example, test) | Proposed rule |
|---|---|---|
| one new entity over two source rows with the same natural key | one generated id 10 for both rows (E11, test 089 scenario 11) | rows naming a new entity by the same key values are one entity: `DEFAULT` evaluated once per entity |
| two source rows overlapping in time for one entity | the later `row_id` wins where they overlap, and PATCH composes them: region East from row 1 stays under row 2's NULL (E12, test 060 scenario 25) | an error, as standard MERGE's cardinality rule, unless the statement gives an order (open question 2) |
| a source row identical to the target | nothing written, `SKIPPED_IDENTICAL` (E13, test 060) | nothing written; follows from principle 5 |
| identity and natural key finding different entities | `ERROR` feedback for that row, the batch goes on (E14, test 165) | the statement fails (principle 6); per-row capture is open question 4 |
| concurrent writers of one entity | serial result at READ COMMITTED (isolation specs, test 164) | the same (principle 10) |

## Open questions

1. **Clause names.** `WHEN TIMELINE NOT MATCHED` and `WHEN ENTITY NOT MATCHED` name what failed to match. Alternatives:
   `WHEN NOT MATCHED IN TIMELINE` / `WHEN NOT MATCHED ENTITY`, or a predicate (`WHEN NOT MATCHED AND ENTITY EXISTS`).
   The proposed form is the one where target and source sides read alike.
2. **Overlapping source rows.** Standard MERGE refuses a target row matched by two source rows. Pointwise, that is two
   source rows at one point of one entity. sql_saga applies them in `row_id` order; SQL has no statement-level order.
   Proposal: an error, unless an order is given, e.g. `USING import AS s ORDER BY s.row_id`.
3. **New entities without any key.** sql_saga's `founding_id_column` (test 062) groups source rows that create one
   new entity when they share no key. A clause for it, e.g. `ENTITY BY (s.founding_id)`, is needed only if such
   imports must be one statement.
4. **Per-row outcomes.** sql_saga reports a status per source row (`APPLIED`, `SKIPPED_*`, `ERROR`) and does not
   abort for one bad row. SQL fails the statement. `RETURNING merge_action()` with the source row's columns covers the
   written pieces; skipped and refused rows have no standard channel. PostgreSQL's `COPY ... ON_ERROR` is the nearest
   precedent.
5. **REPLACE.** With a `SET` list, "replace" is "name every column", and a column not named keeps its value. sql_saga's
   two planners disagree on `MERGE_ENTITY_REPLACE` when the source lacks a target column: the default planner keeps it
   (so REPLACE behaves as UPSERT), the PL/pgSQL planner clears it, natural key included (hug y7hbe9,
   [repro/replace_absent_column_planners.sql](repro/replace_absent_column_planners.sql)). The rule must be settled in
   sql_saga before it is proposed anywhere.
6. **Statement triggers and leftovers.** The FOR PORTION OF review objected to statement triggers firing per leftover
   row. A temporal MERGE writes many pieces per statement and needs one trigger model for all of them.

## Comparison with the earlier candidates

[syntax-candidates.md](syntax-candidates.md) compared three shapes for MERGE alone and recommended A (MERGE with a
per-row portion). This document keeps A and changes four things:

- it covers `INSERT`, `UPDATE` and `DELETE` with the same rules, so `MERGE` is not a separate world;
- it replaces "entity in the source" (A's open question 1) with the two named cases on each side;
- it makes the whole timeline the default scope and the portion the narrowing, for every statement;
- it moves keys (`NATURAL KEY`) and canonical timelines (`TIMELINE ... COALESCE EPHEMERAL`) into the table, where
  sql_saga has them, so statements stay short.
