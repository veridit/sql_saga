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
