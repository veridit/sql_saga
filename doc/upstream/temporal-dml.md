# Temporal INSERT, UPDATE, DELETE and MERGE: principles, design space and syntax

Status: draft for hug task v6s4xq (epic 0t4v31), 2026-11-25, revised after an independent review
(tmp/review_temporal_dml.md). Research only; nothing here has been sent upstream. Supersedes candidate A of
[syntax-candidates.md](syntax-candidates.md), which covered MERGE alone.

Every result table below is real output: [repro/temporal_dml_examples.sql](repro/temporal_dml_examples.sql) runs each
example against sql_saga 1.15 (with `temporal_merge`, or plain SQL where sql_saga has no mode), and
[repro/temporal_dml_examples.out](repro/temporal_dml_examples.out) is its output. Each example also names the sql_saga
regression test that holds the same behaviour.

The goal is consistency and clarity: one word for one concept, the same clause meaning the same thing in every
statement. The number of new keywords is not the measure.

## Vocabulary

The words sql_saga uses, which this design keeps. Each has one meaning here.

| Word | Meaning |
|---|---|
| **entity** | One thing tracked over time (a unit, a person). Its rows share an identity. |
| **identity** | The columns that name an entity and never change: the primary key without its period (`id`). |
| **natural key** | Other columns that name an entity (`code`). Unique over time, and bound to one entity both ways: a value belongs to one entity, and an entity holds one value (NULL before the key starts and after it is retired). |
| **timeline** | An entity's time: the union of its pieces' periods. At most one piece per point (`WITHOUT OVERLAPS`). May have gaps. |
| **piece** | One stored row: an entity's values over one period. |
| **portion** | A period on the time axis that a statement is limited to (`FOR PORTION OF`). |
| **gap** | Time inside or beside a timeline where the entity has no piece. |
| **ephemeral column** | A column that is written but is no reason to keep two pieces apart (`edit_by`, `edit_at`). |
| **canonical** | A timeline whose adjacent pieces always differ in some non-ephemeral value. |
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

What this design takes from each: the sequenced meaning from SQL/Temporal, spelled with SQL:2011's visible periods and
`FOR PORTION OF` instead of hidden columns and a prefix; and from the revert, the requirement that every statement
gives the serial result at READ COMMITTED.

## Principles

1. **Pointwise meaning.** A temporal statement means, at each point in time, what the plain statement means on the
   table's state at that point. At a point where an entity has no piece, that state has no row for it. (This is
   sequenced semantics.)
2. **A piece is the same at every point of its period**, so an expression over the target row and the source row is
   already pointwise: `WHERE region = 'North'` and `SET name = 'x'` need no temporal syntax. Other relations a
   statement reads (subqueries, joins) are read as stored. Only *which time* a statement covers needs syntax, and that
   syntax is `FOR PORTION OF`.
3. **All time by default; `FOR PORTION OF` limits the time.** Without `FOR PORTION OF` a statement covers every point.
   `WHERE` selects pieces, as today, so a statement acts on the points where it holds, never on whole entities by
   implication. `DELETE ... WHERE id = 2` removes an entity because `id = 2` holds at all its points.
4. **One portion per statement, or one per source row.** `UPDATE` and `DELETE` take one portion (as SQL:2011).
   `MERGE` takes each source row's period. No statement needs to know how a timeline is split into pieces.
5. **Timelines are canonical.** After every statement that changes pieces, adjacent pieces of one entity with equal
   values (ephemeral columns aside) are one piece. This is declared once for the table, not in each statement. So "did
   anything change?" is a row comparison, and two ways of writing the same history store the same rows.
6. **Entities are named by keys.** A row names its entity by its identity and its natural keys, at any time of the
   entity's timeline. A NULL key names nothing. All keys a row names must find the same entity, or the statement
   fails: a row states one entity's facts. A natural key value belongs to one entity forever, so a retired code is
   never given to a new entity, and a source row with a retired code names the old entity. A row with no key at all is
   a new entity of its own.
7. **Two questions, asked from each side.** Does the source row's entity exist in the target? Does it have a piece at
   this point? And from the source side: does any source row name this target entity? Does any cover this point? Each
   answer is one `MERGE` clause (the matrix below).
8. **Nothing is removed unless the statement says so.** Time or entities the source does not mention are kept,
   unless a `BY SOURCE` clause names them.
9. **One meaning per word.** `FOR PORTION OF`, `ENTITY`, `TIMELINE`, `MATCHED` and `BY SOURCE` mean the same in every
   statement they appear in.
10. **The serial result at READ COMMITTED.** Concurrent temporal statements give the result of running one after the
    other, without a retry. How is the server's choice (Andres Freund sketched locking conflicting ranges through the
    `WITHOUT OVERLAPS` index); sql_saga 1.15 shows it is achievable by locking the entities a statement names, by every
    key, before reading their timelines ([doc/design/temporal-merge-concurrency.md](../design/temporal-merge-concurrency.md)).

## The design space

### Plain statements

| Statement | Selects | Time | Effect |
|---|---|---|---|
| `INSERT` | the rows given | each row's period | adds the pieces; a point the entity already has is a key violation, as today |
| `UPDATE ... WHERE` | the pieces `WHERE` holds for | all time | sets the values on them |
| `UPDATE ... FOR PORTION OF p ... WHERE` | the pieces `WHERE` holds for | the portion `p` | sets the values inside `p`; pieces split at `p`'s bounds |
| `DELETE ... WHERE` | the pieces `WHERE` holds for | all time | removes them (the entity, if `WHERE` holds at all its points) |
| `DELETE ... FOR PORTION OF p ... WHERE` | the pieces `WHERE` holds for | the portion `p` | removes their time inside `p`; a gap stays |
| a statement that writes the period column itself | as written | as written | acts on the rows as stored: the escape hatch, no splitting |

A `WHERE` on the period column (`WHERE valid_at && '[2021,2022)'`) is an ordinary filter: a piece's period is the
same at every point of it.

`INSERT` keeps its SQL meaning: it adds time. A row that overlaps time the entity already has is a key violation, as in
PostgreSQL 18 today. "Add only the time the target lacks" compares a source with a target, which is `MERGE`'s job
(example M8).

### MERGE: how a source row is matched

A temporal `MERGE` matches in two steps:

1. **Entity.** `ON` is evaluated against every piece of every target entity, at any time. The source row's entity is
   the one entity with a piece for which `ON` holds. More than one is an error (principle 6). None means the entity is
   new.
2. **Time.** Inside the source row's period, the points where that entity has a piece are *matched*; the other points
   are *timeline not matched*. A source row whose period lies entirely outside the timeline is all timeline not
   matched.

On the source side, an entity is *named by the source* if step 1 resolves some source row to it, whatever the clause
conditions later decide. A point of its timeline is *covered* if some such row's period contains it.

That gives five cases:

| Case | Entity | Point | Clause | Actions |
|---|---|---|---|---|
| matched | found | in its timeline | `WHEN MATCHED` | `UPDATE`, `DELETE`, `DO NOTHING` |
| timeline not matched | found | not in its timeline | `WHEN TIMELINE NOT MATCHED` | `INSERT`, `DO NOTHING` |
| entity not matched | new | | `WHEN ENTITY NOT MATCHED` | `INSERT`, `DO NOTHING` |
| timeline not matched by source | named by the source | not covered by any of its rows | `WHEN TIMELINE NOT MATCHED BY SOURCE` | `UPDATE`, `DELETE`, `DO NOTHING` |
| entity not matched by source | not named by the source | its whole timeline | `WHEN ENTITY NOT MATCHED BY SOURCE` | `UPDATE`, `DELETE`, `DO NOTHING` |

The standard clauses keep their meaning, pointwise: `WHEN NOT MATCHED [BY TARGET]` is both target-side cases (no
target row at this point), and `WHEN NOT MATCHED BY SOURCE` (PostgreSQL 17) is both source-side cases. The qualified
clauses split them; the word before `NOT MATCHED` names what failed to match. Clauses are tried in written order and
the first whose kind and `AND` condition apply wins, per point: a `MATCHED AND` condition on `t.` columns is evaluated
per piece, so one source row can take different clauses on different pieces (example M12).

**Scope of `FOR PORTION OF` in MERGE.** `FOR PORTION OF valid_at (s.valid_at)` says that each source row speaks for its
own period: it sets the time of step 2, and so of the target-side clauses. The `BY SOURCE` clauses are about what the
source does not speak for, so they range over the whole timelines of the entities (named or not named by the source).
Without `FOR PORTION OF`, each source row speaks for all time.

**Inserts and keys.** In a `TIMELINE NOT MATCHED` insert the entity is known: the new piece takes the entity's identity
and natural keys, and its period is the uncovered part of the source row's period. Listing a key column with another
value is an error. In an `ENTITY NOT MATCHED` insert the entity is new: a listed identity column is used as given; an
unlisted one comes from its `DEFAULT`, evaluated once per new entity (source rows naming a new entity by the same key
values are one entity). An `INSERT` does not list the period column.

**Keys in a MATCHED update.** A natural key column may be set where the entity holds NULL for it (the key starts); a
value different from the one the entity holds is an error (principle 6).

### Differences from standard MERGE

- **Cardinality, relaxed.** Standard MERGE refuses one target row matched by two source rows. Pointwise, two source rows
  at disjoint times on one piece are fine: they split it. Two source rows at the same point of one entity are an error
  unless the statement gives an order (open question 2).
- **Cardinality, tightened.** Standard MERGE lets one source row match several target rows. Here a source row that
  finds several *entities* is an error, because it states one entity's facts. It may match several pieces of its one
  entity, as it must.
- **`ON` names an entity.** `ON` is equality on the target's keys (`t.id = s.id`, `t.code = s.code`, or several joined
  by `OR`). Any other condition belongs in the clauses' `AND` conditions.

### sql_saga's modes in the matrix

Each `temporal_merge` mode is one choice per case. `SET` lists replace the value modes: what sql_saga calls UPSERT,
PATCH and REPLACE is which columns the `SET` names and how.

| sql_saga | `MATCHED` | `TIMELINE NOT MATCHED` | `ENTITY NOT MATCHED` | `... BY SOURCE` |
|---|---|---|---|---|
| `MERGE_ENTITY_UPSERT` | `UPDATE SET c = s.c` | `INSERT` | `INSERT` | |
| `MERGE_ENTITY_PATCH` | `UPDATE SET c = COALESCE(s.c, t.c)` | `INSERT` | `INSERT` | |
| `MERGE_ENTITY_REPLACE` | `UPDATE SET` every column the source has (open question 5) | `INSERT` | `INSERT` | |
| `UPDATE_FOR_PORTION_OF` | `UPDATE SET c = s.c` | | | |
| `PATCH_FOR_PORTION_OF` | `UPDATE SET c = COALESCE(s.c, t.c)` | | | |
| `REPLACE_FOR_PORTION_OF` | `UPDATE SET` every column the source has (open question 5) | | | |
| `DELETE_FOR_PORTION_OF` | `DELETE` | | | |
| `INSERT_NEW_ENTITIES` | | | `INSERT` | |
| `delete_mode => DELETE_MISSING_TIMELINE` | | | | `TIMELINE NOT MATCHED BY SOURCE THEN DELETE` |
| `delete_mode => DELETE_MISSING_ENTITIES` | | | | `ENTITY NOT MATCHED BY SOURCE THEN DELETE` |

An empty cell is no clause: those points are left as they are (sql_saga reports `SKIPPED_NO_TARGET` or
`SKIPPED_FILTERED` for a source row that no clause applies to).

The matrix also gives combinations sql_saga has no mode for:

| Combination | Meaning |
|---|---|
| `NOT MATCHED` insert, no `MATCHED` clause | add only the time the target lacks, never change known time (M8) |
| `ENTITY NOT MATCHED BY SOURCE THEN UPDATE SET status = 'closed'` | mark entities the source no longer has, instead of deleting them |
| `TIMELINE NOT MATCHED BY SOURCE THEN UPDATE ...` | mark time the source no longer states |
| `MATCHED AND t.x IS DISTINCT FROM s.x THEN UPDATE` | the standard `AND` condition on a clause, pointwise (M12) |

Combinations that mean nothing are rejected by the clause/action table: an `INSERT` in a `BY SOURCE` clause (there is
no source row), an `UPDATE` or `DELETE` in a `NOT MATCHED` clause (there is no target row).

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
    CANONICAL (valid_at) IGNORING (edit_by)               -- proposed: adjacent equal pieces are one; edit_by ignored
);
```

- `NATURAL KEY` is a `UNIQUE ... WITHOUT OVERLAPS` key plus the two rules of principle 6. Both rules can be written
  today as exclusion constraints (`EXCLUDE (code WITH =, id WITH <>)` and `EXCLUDE (id WITH =, code WITH <>)`), which
  is how sql_saga 1.15 enforces them; the declaration names the intent and lets the server use it to match entities.
- `CANONICAL (valid_at) IGNORING (...)` declares principle 5 for the table. sql_saga's equivalent is
  `add_era(..., ephemeral_columns => ...)` with `temporal_merge` coalescing. (Not `COALESCE`, which is a function in
  the same statements.)

### UPDATE and DELETE

As in SQL:2011 and PostgreSQL's FOR PORTION OF patch:

```sql
UPDATE unit FOR PORTION OF valid_at FROM '2022-01-01' TO '2023-01-01'
SET region = 'East', edit_by = 'imp' WHERE code = 'B';         -- a portion (M10)

UPDATE unit SET region = 'West', edit_by = 'imp' WHERE id = 1; -- all time of entity 1 (M9)

UPDATE unit SET name = 'Renamed', edit_by = 'imp'
WHERE region = 'North';                                        -- the North pieces only (M11)

DELETE FROM unit FOR PORTION OF valid_at FROM '2023-01-01' TO '2027-01-01'
WHERE id = 1;                                                  -- a portion

DELETE FROM unit WHERE id = 2;                                 -- all time of entity 2: the entity
```

### MERGE

```sql
MERGE INTO target [ AS t ]
    [ FOR PORTION OF period_column ( source_period_expression ) ]
USING source [ AS s ] ON key_condition
WHEN MATCHED [ AND condition ] THEN { UPDATE SET ... | DELETE | DO NOTHING }
WHEN TIMELINE NOT MATCHED [ AND condition ] THEN { INSERT ... | DO NOTHING }
WHEN ENTITY NOT MATCHED [ AND condition ] THEN { INSERT ... | DO NOTHING }
WHEN NOT MATCHED [ BY TARGET ] [ AND condition ] THEN { INSERT ... | DO NOTHING }   -- both of the above
WHEN TIMELINE NOT MATCHED BY SOURCE [ AND condition ] THEN { UPDATE SET ... | DELETE | DO NOTHING }
WHEN ENTITY NOT MATCHED BY SOURCE [ AND condition ] THEN { UPDATE SET ... | DELETE | DO NOTHING }
WHEN NOT MATCHED BY SOURCE [ AND condition ] THEN { UPDATE SET ... | DELETE | DO NOTHING }  -- both of the above
```

`FOR PORTION OF valid_at (s.valid_at)` is written where `UPDATE` writes it. Its one extension is that the period may
refer to the source row; in `UPDATE`, SQL:2011 and PostgreSQL's patch take a period that does not refer to any row.

## Examples

The target before every example:

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

The three pieces from 2023 to 2027 (two updated, one filling the gap) have equal values and are one piece (principle
5). The gap piece took entity 1's identity (it is a `TIMELINE NOT MATCHED` insert); entity C got identity 10 from the
`DEFAULT`.

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

`t.region` is the target's value at each point, so North and South are each kept where they were. The gap takes the
`INSERT` clause, which has no `t`, so it gets the source's NULL.

### M3. Update known time only

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

### M4. Delete each source row's period

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

`INSERT_NEW_ENTITIES` (E6, test 064 scenario 5). Entity 1 is known, so row 1 does nothing (`SKIPPED_FILTERED`) and its
gap stays a gap: that is the difference between `ENTITY NOT MATCHED` and `NOT MATCHED`. The result is the target plus
entity C (10, Gamma, West, from 2025).

### M6. The source states whole timelines: time it does not cover is removed

```sql
MERGE INTO unit AS t FOR PORTION OF valid_at (s.valid_at)
USING import AS s ON t.id = s.id OR t.code = s.code
WHEN MATCHED THEN UPDATE SET name = COALESCE(s.name, t.name), region = COALESCE(s.region, t.region), edit_by = s.edit_by
WHEN NOT MATCHED THEN INSERT (code, name, region, edit_by) VALUES (s.code, s.name, s.region, s.edit_by)
WHEN TIMELINE NOT MATCHED BY SOURCE THEN DELETE;
```

`MERGE_ENTITY_PATCH` with `DELETE_MISSING_TIMELINE` (E7, test 163). The `BY SOURCE` clause ranges over entity 1's
whole timeline, outside the source row's period too (see "Scope of FOR PORTION OF"). Entity 1 keeps only [2023,
2027); entity 2, not named by the source, is untouched:

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

### M8. Add only the time the target lacks

```sql
MERGE INTO unit AS t FOR PORTION OF valid_at (s.valid_at)
USING import AS s ON t.id = s.id OR t.code = s.code
WHEN NOT MATCHED THEN INSERT (code, name, region, edit_by) VALUES (s.code, s.name, s.region, s.edit_by);
```

sql_saga has no mode for this; E15 does it in plain SQL (the uncovered part of each import row, by multirange
difference). Entity 1 gets its 2026 gap, nothing else of it changes, and entity C is created:

| id | code | name | region | edit_by | valid_from | valid_until |
|---|---|---|---|---|---|---|
| 1 | A | Alpha | North | load | 2020-01-01 | 2024-01-01 |
| 1 | A | Alpha | South | load | 2024-01-01 | 2026-01-01 |
| 1 | A | Alpha2 | | imp | 2026-01-01 | 2027-01-01 |
| 1 | A | Alpha | South | load | 2027-01-01 | infinity |
| 2 | B | Beta | North | load | 2020-01-01 | infinity |
| 10 | C | Gamma | West | imp | 2025-01-01 | infinity |

This is the "insert that fills the uncovered time", and it needs no change to `INSERT`.

### M9. All time of one entity

```sql
UPDATE unit SET region = 'West', edit_by = 'imp' WHERE id = 1;
```

E9 (`PATCH_FOR_PORTION_OF` over all time). Every point of entity 1 gets West. The two pieces before the gap now have
equal values (`edit_by` is ignored) and become one; the gap stays:

| id | code | name | region | edit_by | valid_from | valid_until |
|---|---|---|---|---|---|---|
| 1 | A | Alpha | West | imp | 2020-01-01 | 2026-01-01 |
| 1 | A | Alpha | West | imp | 2027-01-01 | infinity |
| 2 | B | Beta | North | load | 2020-01-01 | infinity |

### M10. A portion of an entity named by its natural key

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

### M11. A WHERE that varies over time

```sql
UPDATE unit SET name = 'Renamed', edit_by = 'imp' WHERE region = 'North';
```

E16 (`PATCH_FOR_PORTION_OF` of the North pieces). Only the points where region is North change: entity 1's South
pieces keep their name (principle 3).

| id | code | name | region | edit_by | valid_from | valid_until |
|---|---|---|---|---|---|---|
| 1 | A | Renamed | North | imp | 2020-01-01 | 2024-01-01 |
| 1 | A | Alpha | South | load | 2024-01-01 | 2026-01-01 |
| 1 | A | Alpha | South | load | 2027-01-01 | infinity |
| 2 | B | Renamed | North | imp | 2020-01-01 | infinity |

### M12. A clause condition that splits a source row

```sql
MERGE INTO unit AS t FOR PORTION OF valid_at (s.valid_at)
USING import AS s ON t.id = s.id
WHEN MATCHED AND t.region = 'South' THEN UPDATE SET name = s.name, edit_by = s.edit_by
WHEN MATCHED THEN DO NOTHING;
```

With the default import, row 1 covers entity 1's North piece [2023, 2024) and South piece [2024, 2026). The first
clause applies to the South piece only: Alpha2 for [2024, 2026), Alpha elsewhere. (Not run: sql_saga has no clause
conditions; the expected result follows from the clause order rule.)

### M13. Behaviours every temporal MERGE must define

| Case | sql_saga (example, test) | Proposed rule |
|---|---|---|
| one new entity over two source rows with the same natural key | one generated id 10 for both rows (E11, test 089 scenario 11) | one entity: `DEFAULT` evaluated once per entity |
| two source rows at the same points of one entity | the later `row_id` wins where they overlap, and PATCH composes them: region East from row 1 stays under row 2's NULL (E12, test 060 scenario 25) | an error, unless the statement gives an order (open question 2) |
| a source row identical to the target | nothing written, `SKIPPED_IDENTICAL` (E13, test 060) | nothing written; follows from principle 5 |
| identity and natural key finding different entities | `ERROR` feedback for that row, the batch goes on (E14, test 165) | the statement fails (principle 6); per-row capture is open question 4 |
| concurrent writers of one entity | serial result at READ COMMITTED (isolation specs, test 164) | the same (principle 10) |

## Open questions

1. **Clause names.** `WHEN TIMELINE NOT MATCHED` and `WHEN ENTITY NOT MATCHED` name what failed to match, and read
   alike on both sides with PostgreSQL 17's optional `BY TARGET`: `WHEN ENTITY NOT MATCHED [BY TARGET]`.
   Alternatives: `WHEN NOT MATCHED ENTITY`, or a predicate (`WHEN NOT MATCHED AND ENTITY EXISTS`).
2. **Overlapping source rows.** Two source rows at one point of one entity. sql_saga applies them in `row_id` order;
   SQL has no statement-level order. Proposal: an error, unless an order is given, e.g.
   `USING import AS s ORDER BY s.row_id`.
3. **New entities without any key.** sql_saga's `founding_id_column` (test 062) groups source rows that create one
   new entity when they share no key. A clause for it, e.g. `ENTITY BY (s.founding_id)`, is needed only if such
   imports must be one statement.
4. **Per-row outcomes.** sql_saga reports a status per source row (`APPLIED`, `SKIPPED_*`, `ERROR`) and does not
   abort for one bad row. SQL fails the statement. `RETURNING merge_action()` with the source row's columns covers the
   written pieces; skipped and refused rows have no standard channel. PostgreSQL's `COPY ... ON_ERROR` is the nearest
   precedent.
5. **Absent columns and NULL.** A column the source has and sets to NULL is a value: UPSERT and REPLACE write it, PATCH
   keeps the target's value. A column the source does not have says nothing, in every mode, and keeps the target's
   value. With a `SET` list this is visible: "replace" names every column the source has, and a column not named keeps
   its value. sql_saga's native planner, the reference implementation, follows this rule; its PL/pgSQL planner clears
   absent columns under REPLACE (a natural key included) and is being corrected to match (hug y7hbe9,
   [repro/replace_absent_column_planners.sql](repro/replace_absent_column_planners.sql)).
6. **Triggers, RETURNING and canonical timelines.** The FOR PORTION OF review objected to statement triggers firing per
   leftover row. A temporal statement writes and merges many pieces. Open: which rows row triggers and `RETURNING` see
   (the pieces written, or the canonical pieces after merging); whether a statement that writes the period column
   itself (the escape hatch) is merged too; one trigger model for all of it.

## Comparison with the earlier candidates

[syntax-candidates.md](syntax-candidates.md) compared three shapes for MERGE alone and recommended A (MERGE with a
per-row portion). This document keeps A and changes four things:

- it covers `INSERT`, `UPDATE` and `DELETE` with the same rules, so `MERGE` is not a separate world;
- it replaces "entity in the source" (A's open question 1) with a two-step match and two named cases on each side;
- it states all time as the default scope and `FOR PORTION OF` as the limit, for every statement;
- it moves keys (`NATURAL KEY`) and canonical timelines (`CANONICAL ... IGNORING`) into the table, where sql_saga has
  them, so statements stay short.
