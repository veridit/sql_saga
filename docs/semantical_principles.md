# `temporal_merge`: Semantical Principles

This document clarifies the core difference between the main `temporal_merge` modes. The key is that all modes are **stateless**. The final state of any atomic time segment is calculated by looking *only* at the original source and target data for that segment. There is no "carry forward" of state from previous segments.

The flawed "carry forward" logic was previously implemented for `PATCH` modes in a recursive CTE called `running_payload_cte`, but this was a non-standard interpretation and has been removed.

## Core Concepts

1.  **Absent Column:** A key/value pair is not present in the source `jsonb` data (e.g., `location` is missing).
2.  **Explicit `NULL`:** A key is present in the source `jsonb`, but its value is `null` (e.g., `"role": null`).
3.  **Ephemeral Column:** A column whose value should be updated if provided, but which should be ignored during change detection for coalescing adjacent time segments (e.g., `edit_comment`).

## Scenario 1: NULLs, Absents, and Ephemeral Columns

- **Target Data** for `[t1, t2)`:
  `{ "A": 1, "B": 2, "C": 3, "edit_comment": "Initial" }`

- **Source Data** for `[t1, t2)`:
  `{ "B": 99, "C": null, "edit_comment": "Update" }`

- **Ephemeral Columns:** `['edit_comment']`

### `MERGE_ENTITY_REPLACE`

- **Rule:** The source data completely **replaces** the target data.
- **Logic:** `final_payload := source_payload`
- **Result:** `{ "B": 99, "C": null, "edit_comment": "Update" }`
- **Analysis:** Column `A` is lost because it was absent from the source.

### `MERGE_ENTITY_UPSERT`

- **Rule:** A partial update. Absent source columns are preserved from the target. Explicit `NULL`s overwrite target data.
- **Logic:** `final_payload := target_payload || source_payload`
- **Result:** `{ "A": 1, "B": 99, "C": null, "edit_comment": "Update" }`
- **Analysis:** `A` is preserved. `B` is updated. `C` is set to `NULL`.

### `MERGE_ENTITY_PATCH`

- **Rule:** A partial update that **ignores explicit `NULL`s** in the source.
- **Logic:** `final_payload := target_payload || jsonb_strip_nulls(source_payload)`
- **Result:** `{ "A": 1, "B": 99, "C": 3, "edit_comment": "Update" }`
- **Analysis:** `A` is preserved. `B` is updated. The `NULL` for `C` is ignored, so `C` is preserved from the target.

---

## Scenario 2: Splitting a Segment & Ephemeral Columns

- **Target Data:** `[Jan-01, May-01)` -> `{ "dept": "Sales", "edit_comment": "Original" }`
- **Source Data:**
    - `[Feb-01, Mar-01)` -> `{ "dept": "Engineering", "edit_comment": "Re-org" }`
    - `[Mar-01, Apr-01)` -> `{ "edit_comment": "Data fix" }`
- **Ephemeral Columns:** `['edit_comment']`

This creates four atomic segments. All modes are stateless.

1.  **Segment `[Jan-01, Feb-01)`:** No source data.
    - **Final State:** `{ "dept": "Sales", "edit_comment": "Original" }`

2.  **Segment `[Feb-01, Mar-01)`:** Source data changes the department.
    - **Final State:** `{ "dept": "Engineering", "edit_comment": "Re-org" }`

3.  **Segment `[Mar-01, Apr-01)`:** Source data only changes an ephemeral column. The `dept` is absent from the source and is inherited from the original target data for this segment.
    - **Final State:** `{ "dept": "Sales", "edit_comment": "Data fix" }`

4.  **Segment `[Apr-01, May-01)`:** No source data.
    - **Final State:** `{ "dept": "Sales", "edit_comment": "Original" }`

### Coalescing with Ephemeral Columns

After the plan is calculated, adjacent segments with identical data payloads (excluding ephemeral columns) are merged.

- Segments 3 and 4 have the same `dept` ("Sales"). Because `edit_comment` is ephemeral, they are considered identical for coalescing.
- **Coalesced Final Plan:**
    1.  `[Jan-01, Feb-01)` -> `{ "dept": "Sales", ... }` (Original)
    2.  `[Feb-01, Mar-01)` -> `{ "dept": "Engineering", ... }` (From source)
    3.  `[Mar-01, May-01)` -> `{ "dept": "Sales", ... }` (Merged segment)

The `temporal_merge` planner will pick the data payload from the *last* atomic segment in a group, so the final `edit_comment` for the `[Mar-01, May-01)` slice will be "Data fix".

---

## Scenario 3: Source Extends Beyond Target (`overlaps` relation)

- **Target Data:** `[Jan-01, Mar-01)` -> `{ "A": 1, "B": 2 }`
- **Source Data:** `[Feb-01, Apr-01)` -> `{ "B": 99, "C": null }`

This creates three atomic time segments:
1.  `[Jan-01, Feb-01)`: (Target only)
2.  `[Feb-01, Mar-01)`: (Overlap: Target and Source)
3.  `[Mar-01, Apr-01)`: (Source only)

### `MERGE_ENTITY_UPSERT`

- **Rule:** A partial update. The source extends the entity's timeline.
- **Segment 1 `[Jan-01, Feb-01)`:** Unchanged from target: `{ "A": 1, "B": 2 }`
- **Segment 2 `[Feb-01, Mar-01)`:** `target || source` -> `{ "A": 1, "B": 99, "C": null }`
- **Segment 3 `[Mar-01, Apr-01)`:** No target data, so it becomes the source data: `{ "B": 99, "C": null }`
- **Coalesced Final Plan:**
    1.  `[Jan-01, Feb-01)` -> `{ "A": 1, "B": 2 }` (Original)
    2.  `[Feb-01, Mar-01)` -> `{ "A": 1, "B": 99, "C": null }` (Merged)
    3.  `[Mar-01, Apr-01)` -> `{ "B": 99, "C": null }` (New slice from source)

### `UPDATE_FOR_PORTION_OF`

- **Rule:** A surgical update. The source is **clipped** to the target's timeline. The portion of the source that extends beyond the target is ignored.
- **Segment 1 `[Jan-01, Feb-01)`:** Unchanged from target: `{ "A": 1, "B": 2 }`
- **Segment 2 `[Feb-01, Mar-01)`:** `target || source` -> `{ "A": 1, "B": 99, "C": null }`
- **Segment 3 `[Mar-01, Apr-01)`:** Source is ignored. This time segment does not exist in the final plan.
- **Coalesced Final Plan:**
    1.  `[Jan-01, Feb-01)` -> `{ "A": 1, "B": 2 }` (Original)
    2.  `[Feb-01, Mar-01)` -> `{ "A": 1, "B": 99, "C": null }` (Updated portion)

### `MERGE_ENTITY_REPLACE`

- **Rule:** Source replaces target in the overlap, and extends the timeline.
- **Segment 1 `[Jan-01, Feb-01)`:** Unchanged from target: `{ "A": 1, "B": 2 }`
- **Segment 2 `[Feb-01, Mar-01)`:** Replaced by source: `{ "B": 99, "C": null }`
- **Segment 3 `[Mar-01, Apr-01)`:** No target data, so it becomes the source data: `{ "B": 99, "C": null }`
- **Coalesced Final Plan:**
    1.  `[Jan-01, Feb-01)` -> `{ "A": 1, "B": 2 }` (Original)
    2.  `[Feb-01, Apr-01)` -> `{ "B": 99, "C": null }` (Segments 2 and 3 are identical and merge)

### `MERGE_ENTITY_PATCH`

- **Rule:** A partial update that ignores explicit `NULL`s and extends the timeline.
- **Segment 1 `[Jan-01, Feb-01)`:** Unchanged from target: `{ "A": 1, "B": 2 }`
- **Segment 2 `[Feb-01, Mar-01)`:** `target || jsonb_strip_nulls(source)` -> `{ "A": 1, "B": 99 }`
- **Segment 3 `[Mar-01, Apr-01)`:** No target data, so it becomes `jsonb_strip_nulls(source)` -> `{ "B": 99 }`
- **Coalesced Final Plan:**
    1.  `[Jan-01, Feb-01)` -> `{ "A": 1, "B": 2 }` (Original)
    2.  `[Feb-01, Mar-01)` -> `{ "A": 1, "B": 99 }` (Merged, `C` is ignored)
    3.  `[Mar-01, Apr-01)` -> `{ "B": 99 }` (New slice from source, `C` is ignored)

---

## Performance Characteristics & Architectural Patterns

The `temporal_merge` procedure is a powerful, set-based tool, but its performance characteristics vary significantly depending on the `mode` and the size of the data batch. Understanding these characteristics is key to designing efficient and scalable ETL processes.

### Analysis of Performance Trends

Benchmark testing reveals two key trends:

1.  **Specialized Modes Are an Order of Magnitude Faster:** Operations with a narrow, well-defined scope, such as `UPDATE_FOR_PORTION_OF`, are significantly faster (often >70,000 rows/s) than general-purpose modes like `MERGE_ENTITY_UPSERT` (~2,000-7,000 rows/s).
    -   **Underlying Cause:** The performance difference is due to the complexity of the problem being solved. A `..._FOR_PORTION_OF` mode knows it only needs to consider existing entities and can aggressively filter the data it reads from the target table. A `MERGE_ENTITY_*` mode must consider both new and existing entities, which requires a much more complex query plan to deconstruct and unify the timelines of all potentially related source and target records.

2.  **`MERGE_ENTITY_*` Modes Have Non-Linear Scaling:** The performance of general-purpose merge modes does not scale linearly with batch size. Throughput often peaks with moderately sized batches (e.g., ~1,000 rows) and then degrades as the batch size grows larger (e.g., 10,000-20,000 rows).
    -   **Underlying Cause:** This is a classic symptom of algorithmic complexity. The planner's core "deconstruct-reconstruct" algorithm involves creating a unified set of all time points from both the source and target. The number of these points, and the cost of the joins required to resolve data for each one (specifically, the join in the `atomic_segments_with_causal_info` CTE that links atomic segments back to source data), can grow faster than linearly with batch size, leading to diminishing returns.

### Architectural Strategies for High Performance

To mitigate these effects and achieve maximum throughput, consider the following architectural patterns for your ETL jobs.

#### 1. Batch Sharding & Parallelization

Since performance is often optimal with smaller batches, a highly effective strategy is to shard a large source table into multiple smaller batches and process them in parallel.

-   **Principle:** Process many small, fast batches concurrently instead of one large, slow batch.
-   **Implementation:**
    1.  **Shard Source Data:** Divide your main source table into smaller temporary tables. The sharding key **must be the entity's natural identifier**. This is critical to ensure that all records for a single conceptual entity are processed together in the same batch, avoiding race conditions and ensuring correctness.
    2.  **Process in Parallel:** Use a parallel job runner (e.g., `pg_parallel`, `GNU parallel`) and a connection pooler (e.g., PgBouncer) to execute `temporal_merge` on each temporary table in a separate session.

This pattern leverages the planner's peak performance at smaller batch sizes and makes effective use of modern multi-core processors.

#### 2. Pre-filtering and Mode Specialization

Leverage the massive performance gains of specialized modes by pre-classifying your source data.

-   **Principle:** Break down a complex merge into a series of simpler, faster operations.
-   **Implementation:**
    1.  **Stage 1: Classify Source Rows:** In a preliminary step, join your source table against the target table on the natural key. Add a column to your source table to classify each row as a potential `INSERT` (no matching entity in target) or `UPDATE` (matching entity exists).
    2.  **Stage 2: Execute Specialized Batches:** Call `temporal_merge` twice:
        -   Once for all the `INSERT` rows, using the highly optimized `INSERT_NEW_ENTITIES` mode.
        -   Once for all the `UPDATE` rows, using a fast `..._FOR_PORTION_OF` mode (e.g., `UPDATE_FOR_PORTION_OF`).

This approach ensures that the planner is always using the most efficient query path for the specific task at hand.

#### 3. Potential Future Planner Optimizations (Internal)

While the above user-side architectural patterns are the primary method for achieving scalability, future versions of `sql_saga` may incorporate further internal optimizations, such as:

-   **Temporal Filtering:** Enhancing the planner to filter the target table not just by entity, but also by the overall time range of the source batch. This would reduce the amount of history the planner needs to consider.
-   **Algorithmic Refinements:** Further optimization of the most expensive CTEs in the planner to reduce their computational complexity.

By combining these strategies, `temporal_merge` can be used to build highly performant, scalable, and robust temporal data warehouses.
