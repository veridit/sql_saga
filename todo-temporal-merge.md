# Design Document: `sql_saga.temporal_merge`

## 1. Vision & Goals

The primary goal is to provide a single, powerful, and semantically clear procedure for performing set-based `INSERT`, `UPDATE`, and `DELETE` operations on temporal tables. This function will be analogous to the standard SQL `MERGE` command but tailored specifically for the complexities of temporal data.

It will solve the critical "multi-statement transaction problem" where client-side logic cannot be reliably validated by `sql_saga`'s deferred constraint triggers due to MVCC visibility rules. By encapsulating the entire temporal modification logic into a single server-side statement, we ensure that triggers fire at the end and see a consistent, final state.

## 2. The "Plan and Execute" Architecture

The function will be built on the "Plan and Execute" pattern, which has been validated in the `statbus_speed` project. This architecture ensures correctness by separating the read and write phases of the operation.

1.  **Plan (Read-Only):** In the first phase, the function analyzes the source data and the target table's current state. It generates a complete, step-by-step execution plan of all DML operations (`INSERT`, `UPDATE`, `DELETE`) required to transition the target table to its correct new state. This phase is entirely read-only and uses complex queries to resolve all temporal overlaps and data changes.
2.  **Execute (Write-Only):** In the second phase, the function executes the pre-calculated plan. DML operations are performed in a specific **"add-then-modify"** order (`INSERT`s first, then `UPDATE`s, then `DELETE`s) to ensure that `sql_saga`'s foreign key triggers can validate the final state correctly within a single statement context.

## 3. Proposed Unified API

A single procedure, `sql_saga.temporal_merge`, will provide all functionality.

### 3.1. The `mode` ENUM: Defining Intent

The core of the API is a new `ENUM` that precisely defines the operation's behavior, making the caller's intent explicit and preventing ambiguity.

```sql
CREATE TYPE sql_saga.temporal_merge_mode AS ENUM (
    'upsert_patch',
    'upsert_replace',
    'patch_only',
    'replace_only',
    'insert_only'
);
```

**Semantic Definitions:**

| Mode                 | Use Case            | If Entity Exists...                                      | If Entity Doesn't Exist... | `NULL`s in Source Data...                  |
| :------------------- | :------------------ | :------------------------------------------------------- | :------------------------- | :----------------------------------------- |
| **`upsert_patch`**   | Standard Import     | **Patches** timeline, preserving non-overlapping history. | **Inserts** new timeline.  | Are **ignored** (existing values preserved). |
| **`upsert_replace`** | Idempotent Import   | **Replaces** timeline portions, preserving history.      | **Inserts** new timeline.  | **Overwrite** existing values.             |
| **`patch_only`**     | User Edits          | **Patches** timeline, preserving history.                | Is a **NOOP**.             | Are **ignored**.                           |
| **`replace_only`**   | Data Correction     | **Replaces** timeline portions, preserving history.      | Is a **NOOP**.             | **Overwrite** existing values.             |
| **`insert_only`**    | Append-Only Data    | Is a **NOOP**.                                           | **Inserts** new timeline.  | N/A (always inserts).                      |

### 3.2. Procedure Signature

```sql
PROCEDURE sql_saga.temporal_merge(
    p_target_table regclass,
    p_source_table regclass,
    p_id_columns text[],
    p_mode sql_saga.temporal_merge_mode,
    p_ephemeral_columns text[] DEFAULT '{}',
    p_era_name name DEFAULT NULL
);
```

### 3.3. Parameters
**Required Parameters:**
*   `p_target_table regclass`: The target temporal table.
*   `p_source_table regclass`: A source table, view, or temporary table containing the new data.
*   `p_id_columns text[]`: An array of column names that form the conceptual entity identifier.
*   `p_mode sql_saga.temporal_merge_mode`: The operational mode, as defined above.

**Optional Parameters:**
*   `p_ephemeral_columns text[] DEFAULT '{}'`: Columns to exclude from data-equivalence checks for coalescing (e.g., audit columns like `edit_comment`). This is conceptually similar to the "excluded columns" in system versioning, but is provided as a parameter because its use is specific to a data loading operation, not a persistent table property.
*   `p_era_name name DEFAULT NULL`: The name of the era to operate on. If omitted, the function will proceed if the target table has only one era. If the table has multiple eras, this parameter is required.

### 3.4. Feedback and Result Reporting
For a general-purpose `sql_saga` procedure, row-level feedback is not required. The procedure will succeed or fail atomically for the entire batch, which is simpler and aligns with standard DML commands. More complex systems (like Statbus) can build a wrapper function that provides detailed row-level feedback if necessary.


## 4. Low-Level Implementation Details (The Planner)

The planner is the core of the function's intelligence.

*   **Temporal Logic Foundation:** The planner's logic is formally based on **Allen's Interval Algebra**. It deconstructs all source and target intervals for a given entity into a set of non-overlapping "atomic" segments. It then calculates the correct data payload for each atomic segment based on the `mode` and the source/target data.
*   **Dynamic SQL and Introspection:** The procedure will be fully dynamic. It will use the PostgreSQL system catalogs (`pg_attribute`, etc.) to discover the common data columns between the source and target tables, removing the need for manual column lists and making the API robust and easy to use.
*   **Coalescing Logic:** A key performance optimization is the coalescing of adjacent atomic segments that have identical data payloads. This significantly reduces the number of final DML operations. The `p_ephemeral_columns` array is used to exclude columns from this data-equivalence check.
*   **`patch` vs. `replace` Payloads:** The planner handles the two modes differently:
    *   **`patch`**: The source data payload is constructed using `jsonb_strip_nulls`, meaning `NULL` values in the source are ignored. When patching a hole in the timeline, data from the preceding target segment is inherited.
    -   **`replace`**: The source data payload is used as-is. `NULL` values in the source will overwrite non-`NULL` values in the target.
*   **Automatic Detection of Generated Columns:** The function will automatically introspect the target table's metadata to identify columns that should not be included in `INSERT` statements, allowing their default values to be applied. This includes columns that are `SERIAL`, `IDENTITY`, `GENERATED`, or have a `DEFAULT` expression that calls `nextval()`. This removes the need for a manual `p_exclude_from_insert` parameter, making the API simpler and more robust.

## 5. Relationship to `update_for_portion_of`

The `temporal_merge` function is a strict superset of the functionality provided by the `FOR PORTION OF` views and their triggers. For example:

`UPDATE my_view FOR PORTION OF ... SET price = 100 WHERE id = 1;`

This is semantically equivalent to calling `temporal_merge` with:
*   A `p_source_table` containing a single row with `id=1` and `price=100`.
*   `p_mode = 'patch_only'`.

This suggests that the `update_portion_of` trigger function could be refactored to use `temporal_merge` internally, unifying the codebase.

## 6. Open Questions for Discussion

1.  **API Finalization:** Are the proposed procedure and parameter names clear and intuitive?
2.  **Source Data Input:** The `regclass` parameter for `p_source_table` is flexible, accepting permanent tables, views, and temporary tables. Is this sufficient, or are there other patterns to consider?
3.  **Performance:** While the PL/pgSQL prototype is expected to be highly performant due to its set-based nature, should the final version be ported to C for maximum efficiency? (Recommendation: Start with PL/pgSQL, port to C if benchmarks show it's a bottleneck).
4.  **Row-Level Feedback:** Confirm that atomic, batch-level success/failure is sufficient for the core `sql_saga` extension.

## 7. Strengths and Weaknesses Analysis

This section provides a critical review of the proposed API and architecture.

### Strengths

1.  **Architecturally Sound:** The "Plan and Execute" pattern is the definitive solution to the MVCC visibility problem for complex, multi-row temporal modifications. It guarantees that `sql_saga`'s deferred triggers validate a consistent final state, which is impossible with client-side, multi-statement logic.
2.  **Semantically Clear API:** The `mode` ENUM is the core of the design's strength. It forces the caller to be explicit about their intent (e.g., `patch` vs. `replace`), which prevents ambiguity and makes behavior predictable and easy to reason about.
3.  **Powerful and Flexible:** The API is highly adaptable. It handles composite and surrogate keys, defaulted columns (`p_exclude_from_insert`), and ephemeral columns for coalescing (`p_ephemeral_columns`). The use of `regclass` for the source table allows it to work seamlessly with permanent tables, views, and temporary tables.
4.  **High Performance:** The entire operation is set-based, leveraging PostgreSQL's strengths for data processing. The logic to coalesce adjacent, identical periods is a critical optimization that significantly reduces the number of final DML operations.
5.  **Robust Introspection:** Using system catalogs to dynamically discover common columns makes the API robust and easy to use. It eliminates the need for brittle, manual column lists in function calls.
6.  **Unification of Logic:** The concept that `temporal_merge` is a superset of `update_for_portion_of` is powerful. Refactoring the latter to use the former will reduce code duplication, minimize the maintenance burden, and ensure consistent behavior across the extension.

### Weaknesses and Mitigations

1.  **Implementation Complexity:**
    *   **Weakness:** The planner's logic, based on Allen's Interval Algebra and dynamic SQL, is inherently complex and can be challenging to debug and maintain.
    *   **Mitigation:** This complexity is a necessary trade-off for the power and correctness the function provides. A comprehensive, well-documented test suite (ported from `statbus_speed`) will be critical for ensuring correctness and preventing regressions.

2.  **No Granular Feedback (By Design):**
    *   **Weakness:** The core procedure provides atomic, batch-level success/failure. It does not return detailed, row-level status information.
    *   **Mitigation:** This is a deliberate and correct design choice for a general-purpose library function. Systems requiring granular feedback (like the Statbus import system) can implement a wrapper function that calls `temporal_merge` and then generates the necessary detailed results, keeping the core API clean and focused.

3.  **Reliance on Correct Entity Identifier Definition:**
    *   **Weakness:** The function's correctness depends on the entity identifier being correctly defined, either via a primary temporal key or the `p_id_columns` parameter. An incorrect definition will lead to logical data corruption.
    *   **Mitigation:** This is an inherent risk in a powerful API. The documentation must be exceptionally clear on the importance of defining the entity identifier correctly. The client application is responsible for providing the correct `p_id_columns`.

## 8. Relationship to `add_api` and `FOR PORTION OF` Views

The `add_api` function serves a dual purpose, creating two distinct types of views to support two different use cases:

1.  **Current-State View (for PostgREST):** It creates a simple view (e.g., `<table>_current`) that shows only the *currently valid* rows (where `valid_until = 'infinity'`). This view has `INSTEAD OF` triggers that translate `INSERT`, `UPDATE`, and `DELETE` operations into correct temporal modifications, making it easy for APIs and interactive users to manage the current state. A `DELETE` here is a "logical delete" that ends the timeline.

2.  **`FOR PORTION OF` View (for Historical Updates):** It also creates an updatable view named like `<table>__for_portion_of_<era>` (e.g., `pricing__for_portion_of_quantities`). This view is designed to emulate the SQL:2011 `FOR PORTION OF` syntax, allowing for targeted updates to specific historical periods. An `UPDATE` on this view is handled by the `update_portion_of` trigger, which splits existing historical records.

The relationship to `temporal_merge` is now clearer:

-   `temporal_merge` is a **strict superset of the `FOR PORTION OF` view's functionality**. An `UPDATE` on the `...__for_portion_of_...` view is semantically equivalent to a `'patch_only'` call to `temporal_merge` with a single source row. This confirms the plan to refactor the `update_portion_of` trigger to use `temporal_merge` internally is correct.
-   `temporal_merge` is **not a superset of the current-state view's functionality**, specifically its "logical delete" behavior. They are complementary: `temporal_merge` is for bulk/historical data management, while the `add_api` current-state view is for interactive, single-entity, current-state management.

## 9. Parameter-to-Metadata Mapping

This table details the source of information for each parameter and for the implicit data required by the `temporal_merge` function.

| Parameter / Data          | Source of Information                                                                                                                                                                                                                                                                                                                                                      |
| ------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `p_target_table`          | **User-provided.** This is the primary input, used as the key to look up all necessary configuration from `sql_saga` metadata.                                                                                                                                                                                                                                          |
| `p_source_table`          | **User-provided.** The function will introspect this table's structure using PostgreSQL's system catalogs to determine the set of data columns.                                                                                                                                                                                                                               |
| `p_mode`                  | **User-provided.** This is fundamental to the function's logic.                                                                                                                                                                                                                                                                                                             |
| `p_era_name`              | **User-provided (optional).** Used to select the correct era from `sql_saga.era` if a table has multiple eras. If omitted, it's inferred if only one era exists for the table.                                                                                                                                                                                                  |
| `p_id_columns`            | **User-provided.** This is a required parameter that defines the conceptual entity identifier.                                                                                                                                                                                                                                             |
| `p_ephemeral_columns`     | **User-provided.** This is specific to a data-loading operation (e.g., ignoring `edit_comment` for coalescing) and is not suitable for persistent metadata.                                                                                                                                                                                                                   |
| Period Columns            | **`sql_saga.era` metadata.** The function queries `sql_saga.era` using `p_target_table` and `p_era_name` to get the `valid_from_column_name` and `valid_until_column_name`.                                                                                                                                                                                                |
| Data Columns              | **Introspection of `pg_attribute`.** The function finds the intersection of column names between `p_source_table` and `p_target_table`, excluding period columns and the entity identifier.                                                                                                                                                                                    |
| Excluded-from-Insert Columns | **Introspection of `pg_attribute` and `pg_attrdef`.** The function automatically identifies columns that are `SERIAL`, `IDENTITY`, `GENERATED`, or have a `DEFAULT` with `nextval()`. These are excluded from `INSERT` statements to allow database defaults to apply. This replaces the need for a manual parameter. |
