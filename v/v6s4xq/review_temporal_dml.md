# Review: doc/upstream/temporal-dml.md

Reviewer: independent design review, 2026-11-25. I checked it against repro/temporal_dml_examples.sql/.out,
for-portion-of-revert.md, SQL:2011 and PostgreSQL 17/18 MERGE. All 13 run examples match the real output. The
problems are in the principles and the MERGE rules, not in the tables.

## Must-fix

### 1. Principle 3 ("whole timeline of every entity it selects") contradicts principles 1 and 2 (Principles, Plain statements)
Problem: `UPDATE unit SET name = 'x' WHERE region = 'North'`. Under principle 1 (pointwise), only the points where
region is North change, so only entity 1's 2020-2024 piece and entity 2 change. Principle 3 and the plain-statements
table say WHERE selects *entities*, and the statement acts on their *whole timeline*. That would rename entity 1's
South pieces too. `DELETE ... WHERE` says it "removes the entity", so `DELETE FROM unit WHERE region = 'North'` would
delete entity 1 entirely, South time included. The two readings give different results, and the document implies
both. Only the pointwise reading is coherent with principles 1, 2 and 4 and with today's PostgreSQL. With the pointwise
reading, a statement without FOR PORTION OF is exactly today's row-wise UPDATE/DELETE (plus coalescing). The
entity-selection reading would make a plain UPDATE without FOR PORTION OF act differently from how it does today. The
examples only use `WHERE id = 1` / `code = 'B'`, which are constant over the timeline, so they hide the conflict.
Fix: reword principle 3 as "The default scope is all time. Without FOR PORTION OF a statement covers every point.
FOR PORTION OF narrows the time. WHERE (and MERGE's matching) select pieces pointwise, never whole entities." In the
table: Entities column "the pieces WHERE selects", and DELETE "removes the selected pieces (all of the entity only if
WHERE holds at all its points)". Add an example with a WHERE that varies over time (`WHERE region = 'North'`) to show it.

### 2. MERGE's FOR PORTION OF vs `TIMELINE NOT MATCHED BY SOURCE` (M6, principle 3)
Problem: M6 writes `FOR PORTION OF valid_at (s.valid_at)` and then deletes entity 1's [2020,2023) and [2027,inf),
which lie outside every source row's portion. If FOR PORTION OF "narrows to a portion" (principle 3), the BY SOURCE
clause has nothing it can reach: by definition the portion is where the source *does* cover. So in M6, FOR PORTION OF
narrows the target-side clauses but not the source-side ones. One clause, two scopes, which breaks principle 9.
Fix: state that MERGE's FOR PORTION OF sets the time of the *source-side match* (each source row speaks for its own
period), and that the BY SOURCE clauses range over the whole timeline of the matched entities (TIMELINE) or of all
entities (ENTITY). Or let MERGE take an optional statement-level scope for BY SOURCE (e.g.
`FOR PORTION OF valid_at (s.valid_at) WITHIN '[2020,2030)'`). Either way, say it in the MERGE bullets, not only by example.

### 3. "Entity matched" vs pointwise ON is undefined when keys vary over time (MERGE: the two questions)
Problem: ON is a row predicate over target *pieces*. A natural key may be NULL on some pieces (before or after it is
held) or differ from the key the source gives. Example: entity 1 has code NULL for [2020,2022) and A after. A source
row `code = 'A'` for [2020,2021) matches entity 1 at 2022+, but not at the points it covers. Is this MATCHED (the
entity is known, ON holds on some piece) or TIMELINE NOT MATCHED, and does the INSERT then write code A over a time
where the entity held no code? The same goes for a source row whose period lies entirely outside the entity's
timeline: the document implies TIMELINE NOT MATCHED, but only if ON is evaluated over all time, not pointwise.
Fix: define the matching in two steps and say so. (a) Entity resolution: ON is evaluated against every piece of
every entity, at any time. An entity is matched if ON holds for any of its pieces. More than one entity is an error.
(b) Time: the points of the source period where that entity has a piece are MATCHED, the rest are TIMELINE NOT
MATCHED. Then say what happens to the key columns in a MATCHED UPDATE or TIMELINE insert when the source key
differs from the piece's key value at that point (error, or the key is written as a value).

### 4. Three meanings of "TIMELINE", and COALESCE as both a function and a table clause (Vocabulary, Syntax)
Problem: "timeline" is an entity's rows (Vocabulary). `TIMELINE (valid_at)` is a table property meaning
"canonical/coalesced". `TIMELINE NOT MATCHED` means "time not covered". The table clause also uses `COALESCE`, which
the same document uses in every PATCH example as the SQL function `COALESCE(s.c, t.c)`. That is two meanings for each
of two words, against the document's own principle 9.
Fix: rename the table clause, e.g. `PERIOD (valid_at) CANONICAL EXCLUDING (edit_by)` or
`COALESCED (valid_at) IGNORING (edit_by)`, and keep "timeline" only for "an entity's time".

## Should-fix

### 5. TIMELINE NOT MATCHED insert silently overrides the INSERT column list (MERGE bullets, M1, M8)
Problem: M1/M8 write `INSERT (code, name, region, edit_by)` without `id`. Under standard INSERT semantics, `id`
would come from DEFAULT, giving entity 1's gap a new identity (a new entity). The text says the server instead takes
the entity's identity and natural keys. So the visible column list does not say what is written. It is also
undefined what happens if the list names `id` or `code` with a different value. And in ENTITY NOT MATCHED, a source
id that names no entity (s.id = 5) is discarded for DEFAULT, because the list cannot carry id without the same
override.
Fix: say it explicitly. In TIMELINE NOT MATCHED, the identity and natural-key columns are filled from the matched
entity, and listing them with a different value is an error. In ENTITY NOT MATCHED, a listed identity column is used
as given, and DEFAULT is used only when it is not listed. Also consider `INSERT DEFAULT KEYS` or similar to make this
visible.

### 6. Principle 2 overstates "anything that reads a row reads it pointwise"
Problem: this holds for the target row's own columns. It does not hold for subqueries or joins to other temporal
tables (`WHERE EXISTS (SELECT 1 FROM region_rate r WHERE r.region = unit.region AND r.rate > 5)`), or for
`USING source` when the source is itself temporal. Sequenced semantics would need those aligned in time too
(TSQL2's hard part).
Fix: limit the claim to expressions over the target row and the source row. State that other relations are read
nonsequenced (as stored) unless a future extension says otherwise.

### 7. "Tests the period column" makes the escape hatch too wide (Plain statements, last row)
Problem: `UPDATE ... FOR PORTION OF p ... WHERE valid_at && '[2021,2022)'` is common and legal in the PostgreSQL
patch. The table says any statement that *tests* the period column is nonsequenced, which would change the meaning
of FOR PORTION OF statements because of their WHERE clause.
Fix: make only *writing* the period column (SET valid_at, or INSERT/UPDATE of it without FOR PORTION OF)
nonsequenced. A WHERE on the period is an ordinary pointwise-invariant row filter: a piece's period is the same at
every point of it.

### 8. Principle 5 (coalesce after *every* statement) vs nonsequenced statements, RETURNING and triggers
Problem: if the table coalesces after a plain nonsequenced `INSERT`/`UPDATE`, the server rewrites rows the user wrote
explicitly. That changes ctid, `RETURNING` and row-trigger counts, and can surprise FK and history logic. Open
question 6 covers triggers only for leftovers.
Fix: state whether coalescing applies to nonsequenced writes. State which rows `RETURNING` and row triggers see
(the written pieces before coalescing, or after). Fold this into open question 6.

### 9. Cardinality: the document both relaxes and tightens standard MERGE without saying so (MERGE bullets, M11)
Problem: (a) Standard MERGE forbids one *target row* being matched by two source rows. Pointwise, two source rows at
disjoint times on one piece are fine (they split it). That is a relaxation, and it is correct, but it is not stated.
(b) Standard MERGE allows one *source row* to match several target rows. Here a source row that matches several
entities is an error (principle 6). That is a tightening and should be stated as such, with the reason.
(c) "`entity_condition` is equality on keys of the target" restricts ON, which in the standard is any predicate.
Fix: add a short "Differences from standard MERGE" list with these three points. For (c), say whether a general ON
is rejected or is treated as nonsequenced.

### 10. Clause precedence when qualified and unqualified clauses overlap (MERGE syntax)
Problem: `WHEN NOT MATCHED` overlaps both `TIMELINE NOT MATCHED` and `ENTITY NOT MATCHED`, and `NOT MATCHED BY SOURCE`
overlaps both BY SOURCE variants. With `AND` conditions, standard MERGE takes the first WHEN clause of the right
kind whose condition holds. It is not said that this rule applies here, or whether it applies per point. For
`MATCHED AND <cond on t>`, the condition is evaluated per piece. So one source row can take different clauses on
different pieces, and some points may fall through to no clause. The document says "pointwise" for MATCHED but not
for the others.
Fix: state that the clauses are tried in written order and the first applicable one wins, per point (per piece for
target-side conditions). Clauses whose conditions use only `s.` columns or entity-level facts give one decision per
source row. Add one example of a MATCHED AND condition that splits a source row.

### 11. M8 is derived, not run
Problem: every other result is real output. M8's table is not shown and the result is "derived". It is the
showcase for a combination sql_saga lacks.
Fix: give its expected table explicitly and mark it clearly as expected, not run. Better, produce it with a
two-step sql_saga recipe (e.g. a temp copy of the import cut to the gaps, then `MERGE_ENTITY_UPSERT`), so it is
backed by output too.

### 12. ENTITY vs TIMELINE NOT MATCHED BY SOURCE: "in the source" needs a definition
Problem: is an entity "in the source" if a source row matches it by ON, even when every clause's AND condition
rejected that row? Or if the row's period lies outside the timeline (TIMELINE NOT MATCHED with DO NOTHING)? This
decides whether TIMELINE ... BY SOURCE DELETE removes that entity's time.
Fix: define it as "some source row resolves to the entity (step 3a)", independent of clause conditions, and say so.

## Notes

13. **"portion" has two senses** in the Vocabulary (a part of the time axis, and the part of a timeline inside it).
    That is acceptable, but the prose then uses "portion" for the source period too. Consider keeping "portion" for
    the axis and saying "the timeline's part in the portion" explicitly.
14. **Standard spelling.** SQL:2011 writes `FOR PORTION OF <period> FROM x TO y` over a declared `PERIOD FOR`, while
    PostgreSQL's patch uses range columns and also accepts `FOR PORTION OF col (range_expr)`. In UPDATE the patch
    requires bounds that do not reference the target row. MERGE's `(s.valid_at)` references the source row, which is
    new and should be named as the one extension of FOR PORTION OF that MERGE needs.
15. **PostgreSQL 17 spelling.** PG 17 accepts `WHEN NOT MATCHED [BY TARGET]`. Mention that the target-side qualified
    clauses could read `WHEN ENTITY NOT MATCHED [BY TARGET]`, so both sides read alike (helps open question 1).
16. **Principle 10 vs upstream direction.** sql_saga locks per entity by key (advisory lock plus FOR UPDATE).
    Andres Freund sketched index-driven locking of conflicting ranges, and Paul's v2 series raises serialization
    failures. Principle 10 states the goal, which is right. Say that the mechanism is the server's choice and that
    sql_saga's protocol is evidence that it is achievable, not the proposal.
17. **Principle 7** says "two questions", but the matrix has 5 cases from two questions on two sides. Saying "two
    questions, asked from each side" would read better. "Is the time known" is vague: "does the entity have a piece
    at this point".
18. **Pointwise over gaps** should be spelled out for a PostgreSQL reader: at a point where the entity has no piece,
    the "table state at that point" has no row for it. That is why MATCHED and t.* are absent there and why M2's gap
    gets the source NULL. One sentence after principle 1 would answer the most likely first question.
19. **New entities with no key at all** (identity NULL, natural keys NULL) become one new entity per source row.
    Combining rows needs open question 3. Say this in principle 6, which currently only says "a NULL key names nothing".
20. **Natural key reuse.** "A value belongs to one entity" over all time means a retired code can never be given to a
    new entity, and that a source row with a retired code reactivates the old entity (ENTITY is matched). That is a
    strong rule, possibly surprising upstream. State it as a consequence.
21. **Mode matrix, REPLACE.** "UPDATE SET every column" is stated as fact while open question 5 says the planners
    disagree. Mark the cell "(see open question 5)".

## Fidelity check (examples vs repro .out)

| Example | Matches .out | Remark |
|---|---|---|
| M1/E1 | yes | Correct only under the identity override of finding 5. |
| M2/E2 | yes | The gap uses the INSERT clause (s.region NULL), not COALESCE, so no t row is needed. Correct. |
| M3/E3, E4 | yes | |
| M4/E5 | yes | |
| M5/E6 | yes (table not shown in doc) | Show it or say it equals the target plus C. |
| M6/E7 | yes | Scope conflict, finding 2. |
| M7/E8 | yes | "M2 without entity 2" checked. |
| M8 | not run | Finding 11. |
| M9/E9 | yes | E9 is PATCH over [-inf,inf) with name NULL, so it is equivalent to SET region only. |
| M10/E10 | yes | |
| M11 E11-E14 | yes | E14's message is "matches multiple distinct target entities", which fits principle 6. |
