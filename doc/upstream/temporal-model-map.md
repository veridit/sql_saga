# sql_saga's temporal model against PostgreSQL: a map of three areas

Status: study for hug task 9fxqkc (epic 0t4v31), 2026-11-24. Sources are cited inline; PostgreSQL facts were
checked against the PostgreSQL git tree (master at 2026-11-24, `REL_18_STABLE`, `REL_19_STABLE`) and pgsql-hackers.

## What PostgreSQL has

| Feature | Status | Where |
|---|---|---|
| Temporal `PRIMARY KEY`/`UNIQUE (..., valid_at WITHOUT OVERLAPS)` on a range or multirange column | shipped in 18 | commit `fc0438b4e80` (2024-09-17) |
| Temporal foreign keys `FOREIGN KEY (k, PERIOD valid_at) REFERENCES p (k, PERIOD valid_at)`, `NO ACTION` only | shipped in 18 | commit `89f908a6d0a` (2024-09-17); `RESTRICT` removed again in `b83e8a2ca2e` (2025-03-10) |
| `range_minus_multi`/`multirange_minus_multi` (the leftover arithmetic) and a "Temporal Tables" chapter in the docs | shipped in 19 | `5eed8ce50ce`, `e4d8a2af07f` |
| `UPDATE/DELETE ... FOR PORTION OF` | committed for 19, **reverted** 2026-09-15; new series v2 on pgsql-hackers since 2026-10-31 | [for-portion-of-revert.md](for-portion-of-revert.md) |
| Temporal foreign key actions `RESTRICT`, `CASCADE`, `SET NULL`, `SET DEFAULT` | patch series on pgsql-hackers since 2026-08-31 | [Paul Jungwirth, "Temporal foreign key actions"](https://www.postgresql.org/message-id/CA%2BrenyUts-ksRKL3V%2B9kTnRCAJdb8Xc3T1L%2ByCVscNZQKT4Yvw%40mail.gmail.com) |
| `MERGE ... WHEN NOT MATCHED BY SOURCE` (non-temporal) | shipped in 17 | `0294df2f1f8` |
| SQL:2011 `PERIOD FOR name (start_col, end_col)` | not supported; PostgreSQL uses one range column instead | not in `gram.y` |
| System time (`SYSTEM_TIME`, `FOR SYSTEM_TIME AS OF`) | not supported; the docs point to triggers and extensions | `doc/src/sgml/ddl.sgml`, "System Time" |

Paul Jungwirth writes the patches; Peter Eisentraut reviews and commits them and is a member of the SQL standards
committee, ISO/IEC JTC 1/SC 32/WG 3 (he said so at the [PGCon 2019 developer meeting](https://wiki.postgresql.org/wiki/PgCon_2019_Developer_Meeting)
and publishes reports from its meetings). Paul's foreign-key-actions mail says of CASCADE and SET NULL that he is
interested in "getting this into Postgres and also adding it to the standard". He wants two small preliminary
patches in version 20 and could do the actions as an extension if only those are accepted. Both men work on what
sql_saga does, so the overlap is direct.

Other databases, for comparison (from vendor documentation, not re-checked for this study): Db2 for z/OS has business
time with `FOR PORTION OF` and temporal foreign keys (Paul's mail cites tests run there by Markus Winand, and notes
temporal foreign keys are a syntax error on Db2 for Linux); MariaDB has application-time periods with
`FOR PORTION OF` and `WITHOUT OVERLAPS` keys but no temporal foreign keys; Teradata has "sequenced valid-time"
forms of its DML statements, with one period of applicability per statement; SQL Server has system versioning only;
Oracle has Flashback for system time and Temporal Validity for period queries. None of the last three has portion
DML.

## Area 1: a foreign key from a regular table to a temporal table

**The problem.** An order row is not temporal: it has no validity period, it just refers to a product. The product
table is temporal: product 5 has several rows over time, with a temporal key `(product_no, valid_at WITHOUT OVERLAPS)`.
The rule wanted is "product 5 exists at some time". PostgreSQL cannot express it:

```sql
CREATE TABLE products (product_no int, price int, valid_at daterange,
                       PRIMARY KEY (product_no, valid_at WITHOUT OVERLAPS));
CREATE TABLE orders (id int, product_no int REFERENCES products (product_no));
-- ERROR:  there is no unique constraint matching given keys for referenced table "products"
```

(Run on PostgreSQL 18.3.) A plain foreign key needs a unique key on `product_no` alone, which a temporal table does not
have. A temporal foreign key needs a period on the referencing side, which the order does not have. Users fall back to
triggers, a separate non-temporal "identity" table, or no constraint.

**What sql_saga does.** `sql_saga.add_regular_foreign_key()` (since 1.0; a constraint trigger since 1.3; `ON DELETE
CASCADE` since 1.4). The child side is a `DEFERRABLE INITIALLY IMMEDIATE` constraint trigger that requires some parent
row with the key; the parent side refuses deleting or re-keying the *last* row of a referenced key, or cascades it. A
temporal_merge that reshapes the parent's timeline never trips it, because the key keeps existing. The concurrency
contract (no orphan under any interleaving at READ COMMITTED or REPEATABLE READ) is held by
`devops/test_regular_fk_concurrency.sh`; behaviour by tests 045, 125, 129, 137 to 139, 141 to 143, 147 and 148.

Doing this outside core costs. In 1.0 to 1.2 the child check was a `CHECK` calling a function that reads the parent;
`pg_dump` creates CHECK constraints before loading data, so a restore could load orders before products and fail even
serially (decision log, 1.3). A native constraint would be created after the data like any foreign key, and would get
the server's row-locking protocol instead of a trigger's.

**What PostgreSQL offers and is working on.** Nothing, and nothing in progress that we found: the temporal foreign key
work covers temporal to temporal. The SQL standard has no such constraint either.

**What a native form could look like** (the syntax exploration, task axdept, will compare forms):

```sql
CREATE TABLE orders (id int, product_no int,
    FOREIGN KEY (product_no) REFERENCES products (product_no, PERIOD valid_at) AT ANY TIME);
```

The semantics fit the temporal model Paul's mail describes as shared by Snodgrass, Date/Darwen/Lorentzos and
Johnston/Weis, where a period stands for one row per time point (his mail cites SQL:2023 Foundation 11.8 for how
the standard matches temporal references). "Exists at any time" is the projection of those rows onto the key.

## Area 2: FOR PORTION OF covers one slice per statement

**The problem.** `UPDATE ... FOR PORTION OF valid_at FROM a TO b SET ...` changes one time slice, with one pair of
bounds for the whole statement. Paul's v2 commit message says so directly: the bounds must be constant, and column
references and subqueries are forbidden. Real data changes come as a batch of source rows, each with its own period:

```text
source: product 5, price 8, valid 2022-01-01 to 2023-01-01
        product 5, price 9, valid 2023-06-01 to infinity
        product 7, price 3, valid 2021-01-01 to 2022-01-01   (product 7 is new)
```

Applying that with FOR PORTION OF means one statement per source row, chosen per row between UPDATE (entity exists
over that slice), INSERT (it does not), or both (it exists over part of the slice). It also needs ordering so
statements do not see each other's leftovers, and a merge of adjacent pieces that end up equal. That is the work
temporal_merge does.

**What sql_saga does.** One `temporal_merge` call applies a whole source table: it computes the atomic time segments
of source and target per entity, resolves each segment's values by the mode, merges adjacent equal segments, and
writes the result as a minimal set of DELETEs, UPDATEs and INSERTs. The `for_portion_of` view gives the
`UPDATE ... SET valid_from, valid_until` form for single changes on top of it.

**What PostgreSQL offers and is working on.** The reverted FOR PORTION OF and its v2 series: single-slice UPDATE and
DELETE, now raising a serialization failure for concurrent conflicts at READ COMMITTED. SQL:2011 defines FOR PORTION OF
for UPDATE and DELETE only; it has no temporal MERGE and no temporal INSERT. Teradata's sequenced valid-time DML
takes one period per statement, so it does not cover per-row slices either.

## Area 3: the whole problem space of temporal_merge

Each row is a capability of temporal_merge, what the standard or PostgreSQL offers for it, and the gap.

| temporal_merge | Meaning | SQL:2011 / PostgreSQL | Gap |
|---|---|---|---|
| entity identity (`primary_identity_columns`) | which rows form one entity's timeline | `WITHOUT OVERLAPS` key | none |
| natural key lookup (`natural_identity_columns`) | find the entity by a business key when the id is unknown | `MERGE ... ON` | none for lookup; generated ids for new multi-slice entities (`founding_id_column`) have no equivalent |
| a period per source row | each source row carries its own slice | FOR PORTION OF bounds are constant per statement | **the core gap** |
| `UPDATE_FOR_PORTION_OF` | set given columns on a slice, NULL is a value | `UPDATE ... FOR PORTION OF` (one slice) | per-row slices |
| `PATCH_FOR_PORTION_OF` | as above, NULL keeps the old value | `SET c = COALESCE(new, c)` per column | per-row slices; verbose |
| `REPLACE_FOR_PORTION_OF` | replace all columns on a slice | `UPDATE ... FOR PORTION OF` setting every column | per-row slices |
| `DELETE_FOR_PORTION_OF` | remove a slice | `DELETE ... FOR PORTION OF` | per-row slices |
| `MERGE_ENTITY_UPSERT` / `_PATCH` / `_REPLACE` | as the three above, and also fill gaps and create new entities | none (no temporal MERGE, no temporal INSERT) | **gap** |
| `INSERT_NEW_ENTITIES` | insert only entities not yet present | `INSERT ... WHERE NOT EXISTS`, or `MERGE ... WHEN NOT MATCHED` | small |
| `DELETE_MISSING_TIMELINE` | for entities in the source, remove target time the source does not cover | none | **gap** (sql_saga 1.14 misses target time beside a source edge that falls inside a slice: bug rtwrp6) |
| `DELETE_MISSING_ENTITIES` | remove entities absent from the source | `MERGE ... WHEN NOT MATCHED BY SOURCE THEN DELETE` (PostgreSQL 17, non-temporal) | small (sql_saga 1.14 applies it with PATCH and REPLACE but not UPSERT: bug 2hxkwx) |
| coalescing | adjacent pieces with equal values become one row | none: FOR PORTION OF leaves the split rows even when values end up equal | **gap** |
| `ephemeral_columns` | columns (audit, provenance) ignored when deciding equality, still written | none | **gap**, part of coalescing |
| DELETE, then UPDATE (shrink before grow), then INSERT | never a transient overlap; transient gaps allowed, checked by deferrable foreign keys at commit | FOR PORTION OF is one statement; foreign keys checked at its end | an engine statement would not need it |
| per-source-row feedback (`APPLIED`, `SKIPPED_*`, `ERROR`) | import pipelines record what happened to each input row | `MERGE ... RETURNING merge_action()` | partial |
| concurrency | serial results at READ COMMITTED | FOR PORTION OF v2: serialization failure; sql_saga 1.14: silent loss in three races (bug 7rzgv5) | **open on both sides** |
| synchronized `valid_from`/`valid_until`/`valid_to` columns | start/end columns kept in step with the range | SQL:2011 `PERIOD FOR` over two columns; PostgreSQL: range column only | PostgreSQL-specific |
| system versioning, history tables | when the database knew it | SQL:2011 system time; PostgreSQL: none | separate topic |

A fuller standard feature, then, would be a **temporal MERGE**: a MERGE whose source rows each carry a period, with
the existing `WHEN MATCHED`/`WHEN NOT MATCHED`/`WHEN NOT MATCHED BY SOURCE` clauses applied per time segment, plus
a table-level rule for coalescing and the columns it ignores. Everything in the first column above maps to a clause of
that statement. That is the subject of the syntax exploration (task axdept).

## Recommendation: the smallest proposal with the best chance

The question is what to offer Paul Jungwirth and Peter Eisentraut first. Three candidates, smallest first:

1. **Help with what they are doing now: FOR PORTION OF v2 and temporal foreign key actions.** Offer our isolation
   scenarios, the results of sql_saga's entity-lock protocol once bug 7rzgv5 is fixed (correctness and rows/s with
   and without locks), and review or testing of the two patch series against a real workload (StatBus). It needs no
   syntax and no buy-in to start, and it is what the thread lacked: evidence about concurrency behaviour that works at
   READ COMMITTED. That evidence supports Andres Freund's position rather than the serialization failure in Paul's
   current series, so it is best offered as data for his next step. Cost to prepare: fix 7rzgv5 (needed anyway),
   then about a day for the benchmark and the write-up.
2. **Propose the regular-to-temporal foreign key.** It is small, it fills a gap nobody else is working on, it fits
   the standard's model, and sql_saga has shipped it with a concurrency harness since 1.3. Cost: a design note with
   syntax options and semantics (days); a core patch later is weeks and only after interest.
3. **Propose temporal MERGE.** This is the large one that matches the owner's goal. It would follow 1 and 2, built on
   the syntax exploration and on FOR PORTION OF landing first, since it reuses its leftover machinery.

Recommended order: 1, then 2 in the same conversation, with 3 named as the direction. Precondition for all three:
bugs 7rzgv5, rtwrp6 and 2hxkwx fixed and released, so sql_saga arrives with a tested answer to the question that
reverted FOR PORTION OF and with mode semantics that hold.

## Sources

- PostgreSQL commits named above: <https://git.postgresql.org/gitweb/?p=postgresql.git>
- PostgreSQL docs, Temporal Tables: `doc/src/sgml/ddl.sgml` (master), published at
  <https://www.postgresql.org/docs/19/ddl-temporal-tables.html>
- Temporal foreign key actions: <https://www.postgresql.org/message-id/CA%2BrenyUts-ksRKL3V%2B9kTnRCAJdb8Xc3T1L%2ByCVscNZQKT4Yvw%40mail.gmail.com>
- FOR PORTION OF v2: <https://www.postgresql.org/message-id/CA%2BrenyULDQq3x-qYuADAenfqbFts9HaM66zWROZO8X%2B2bJf2GA%40mail.gmail.com>
- RESTRICT discussion (2025): <https://www.postgresql.org/message-id/22c4dd72-66fa-4f76-95f2-757d754e2aad%40eisentraut.org>
- sql_saga: `README.md` (modes), `doc/api.md`, `doc/decision-log.md` ("Use Constraint Triggers for Regular-to-Temporal
  Foreign Keys", 1.3)
