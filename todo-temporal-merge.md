# Design Document: `sql_saga.temporal_merge`

## 1. Vision & Goals

The primary goal is to provide a single, powerful, and semantically clear procedure for performing set-based `INSERT`, `UPDATE`, and `DELETE` operations on temporal tables. This function will be analogous to the standard SQL `MERGE` command but tailored specifically for the complexities of temporal data.

It will solve the critical "multi-statement transaction problem" where client-side logic cannot be reliably validated by `sql_saga`'s deferred constraint triggers due to MVCC visibility rules. By encapsulating the entire temporal modification logic into a single server-side statement, we ensure that triggers fire at the end and see a consistent, final state.

## 2. The "Plan and Execute" Architecture

The function will be built on the "Plan and Execute" pattern, which has been validated in the `statbus_speed` project. This architecture ensures correctness by separating the read and write phases of the operation.

1.  **Plan (Read-Only):** In the first phase, the function analyzes the source data and the target table's current state. It generates a complete, step-by-step execution plan of all DML operations (`INSERT`, `UPDATE`, `DELETE`) required to transition the target table to its correct new state. This phase is entirely read-only and uses complex queries to resolve all temporal overlaps and data changes.
2.  **Execute (Write-Only):** In the second phase, the function executes the pre-calculated plan. DML operations are performed in a specific **"add-then-modify"** order (`INSERT`s first, then `UPDATE`s, then `DELETE`s) to ensure that `sql_saga`'s foreign key triggers can validate the final state correctly within a single statement context.

## 3. API Design: The "Configuration with Registered Type" Pattern

After careful consideration, the chosen API design is the **"Configuration with Registered Type"** pattern. This approach provides the best balance of safety, usability, and maintainability by combining a metadata-driven configuration with strong runtime type checking.

The design consists of a two-step process:
1.  **`add_temporal_merge_setup`:** A one-time setup procedure that creates a custom composite `TYPE` to define the expected structure of source data and registers this configuration in `sql_saga`'s metadata.
2.  **`temporal_merge`:** A streamlined, powerful procedure that dynamically verifies the source table's structure against the registered `TYPE` before performing the merge operation.

### 3.1. Setup and Teardown Procedures

- **`add_temporal_merge_setup` (Setup):**
  This procedure registers a table for use with `temporal_merge` and creates the necessary type definition.
  **Signature:**
  ```sql
  PROCEDURE sql_saga.add_temporal_merge_setup(
      p_target_table regclass,
      p_entity_id_columns text[],
      p_ephemeral_columns text[] DEFAULT '{}',
      p_era_name name DEFAULT NULL
  );
  ```
  **Actions:**
  1. Creates a new composite `TYPE` (e.g., `public.legal_unit_valid_temporal_merge_input`) that includes all columns of the target table.
  2. Stores the configuration—including the `regtype` of the new `TYPE` and ephemeral columns—in a new metadata table, `sql_saga.temporal_merge_setup`.

- **`drop_temporal_merge_setup` (Teardown):**
  This procedure removes the configuration and the associated `TYPE`.
  **Signature:**
  ```sql
  PROCEDURE sql_saga.drop_temporal_merge_setup(p_target_table regclass);
  ```

### 3.2. Core Procedure: `temporal_merge`

This is the main procedure users will call. Its first action is to perform a dynamic type check.

**Signature:**
```sql
PROCEDURE sql_saga.temporal_merge(
    p_target_table regclass,
    p_source_table regclass,
    p_mode sql_saga.temporal_merge_mode
);
```
**Dynamic Type Check:**
Before executing the merge logic, the procedure will:
1.  Look up the `source_data_type` from `sql_saga.temporal_merge_setup` for the `p_target_table`.
2.  Introspect the columns and types of the provided `p_source_table`.
3.  Compare the source table's structure with the registered `TYPE`.
4.  If they are not compatible, it will raise a clear, informative error and abort.

**Parameters:**
*   `p_target_table regclass`: The target temporal table, which must have been configured.
*   `p_source_table regclass`: A source table, view, or temp table containing the new data. Its structure must be compatible with the registered `TYPE`.
*   `p_mode sql_saga.temporal_merge_mode`: The operational mode.

### 3.3. The `mode` ENUM: Defining Intent

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

| Mode                 | `UPDATE` Behavior (Source overlaps Target)                                                                | `INSERT` Behavior (Source has no Target) |
| :------------------- | :-------------------------------------------------------------------------------------------------------- | :--------------------------------------- |
| **`upsert_patch`**   | **Patches** target data. `NULL` values in the source **do not** overwrite existing non-`NULL` values.        | **Inserts** the new entity timeline.     |
| **`upsert_replace`** | **Replaces** target data. `NULL` values in the source **will** overwrite existing non-`NULL` values.        | **Inserts** the new entity timeline.     |
| **`patch_only`**     | Same as `upsert_patch`.                                                                                   | **Ignores** the source record (NOOP).    |
| **`replace_only`**   | Same as `upsert_replace`.                                                                                 | **Ignores** the source record (NOOP).    |
| **`insert_only`**    | **Ignores** the source record (NOOP).                                                                       | **Inserts** the new entity timeline.     |

### 3.4. Use Case Scenarios

The five modes provide precise control for common data management tasks:

- **`upsert_patch` (The Idempotent Workhorse):** For nightly batch jobs from external systems with potentially incomplete data. Safely inserts new records and patches existing ones without accidental data loss from `NULL`s.
- **`upsert_replace` (The Full Corrector):** For data corrections from a trusted source, where `NULL`s are intentional and should overwrite existing data.
- **`patch_only` (The Safe UI Edit):** For user edits of *existing* entities. Guarantees that a mistyped ID will not create a new, erroneous record.
- **`replace_only` (The Targeted Correction):** For data quality scripts that need to apply fixes, including `NULL`s, to a known set of existing records.
- **`insert_only` (The Safe UI Create / Bulk Load):** For UI "create" forms or initial data loads. Guarantees that existing data will not be touched, preventing accidental modification or duplication.

### 3.5. Feedback and Result Reporting
For a general-purpose `sql_saga` procedure, row-level feedback is not required. The procedure will succeed or fail atomically for the entire batch, which is simpler and aligns with standard DML commands. More complex systems (like Statbus) can build a wrapper function that provides detailed row-level feedback if necessary.


## 4. Low-Level Implementation Details (The Planner)

The planner is the core of the function's intelligence.

*   **Temporal Logic Foundation:** The planner's logic is formally based on **Allen's Interval Algebra**. It deconstructs all source and target intervals for a given entity into a set of non-overlapping "atomic" segments. It then calculates the correct data payload for each atomic segment based on the `mode` and the source/target data.
*   **Dynamic SQL and Introspection:** The procedure will be fully dynamic. It will use the PostgreSQL system catalogs (`pg_attribute`, etc.) to discover the common data columns between the source and target tables, removing the need for manual column lists and making the API robust and easy to use.
*   **Coalescing Logic:** A key performance optimization is the coalescing of adjacent atomic segments that have identical data payloads. This significantly reduces the number of final DML operations. The `ephemeral_columns` configuration (set via `add_temporal_merge_setup`) is used to exclude columns from this data-equivalence check.
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


## 7. Implementation Plan

The implementation will proceed as follows:

1.  **Create New Files:**
    -   A new SQL file for the function definition: `src/27_temporal_merge.sql`.
    -   A new test suite: `sql/44_temporal_merge.sql`.

2.  **Port Existing Logic:**
    -   The core "Planner" logic, which is the most complex part of the implementation, will be ported from the mature `temporal_merge` function in the `statbus_speed` project. This provides a robust and well-tested foundation.

3.  **Develop Test Suite:**
    -   The `sql/42_statbus_upsert_pattern.sql` test, which validates the existing `upsert` pattern, will serve as the initial template for the new test suite. The new suite will be expanded to cover all modes defined in the `temporal_merge_mode` ENUM.

4.  **Update Build System:**
    -   The `Makefile` will be updated to include the new source and test files in the build process and test runs.

## 8. Strengths and Weaknesses Analysis

This section provides a critical review of the proposed API and architecture.

### Strengths

1.  **Architecturally Sound:** The "Plan and Execute" pattern is the definitive solution to the MVCC visibility problem for complex, multi-row temporal modifications. It guarantees that `sql_saga`'s deferred triggers validate a consistent final state, which is impossible with client-side, multi-statement logic. Furthermore, by being a procedure, it can support `OUT` parameters or other mechanisms for returning data (e.g., via temporary tables), which is not reliably possible with `INSTEAD OF INSERT` triggers on views that need to return auto-generated primary keys.
2.  **Semantically Clear API:** The `mode` ENUM is the core of the design's strength. It forces the caller to be explicit about their intent (e.g., `patch` vs. `replace`), which prevents ambiguity and makes behavior predictable and easy to reason about.
3.  **Powerful and Flexible:** The API is highly adaptable. It handles composite and surrogate keys, defaulted columns, and ephemeral columns for coalescing (configured via `add_temporal_merge_setup`). The use of `regclass` for the source table allows it to work seamlessly with permanent tables, views, and temporary tables.
4.  **High Performance:** The entire operation is set-based, leveraging PostgreSQL's strengths for data processing. The logic to coalesce adjacent, identical periods is a critical optimization that significantly reduces the number of final DML operations.
5.  **Robust Introspection:** Using system catalogs to dynamically discover common columns makes the API robust and easy to use. It eliminates the need for brittle, manual column lists in function calls.
6.  **Unification of Logic:** The concept that `temporal_merge` is a superset of `update_for_portion_of` is powerful. Refactoring the latter to use the former will reduce code duplication, minimize the maintenance burden, and ensure consistent behavior across the extension.

### Weaknesses and Mitigations

1.  **Lifecycle Management Complexity:**
    *   **Weakness:** Creating a dependent `TYPE` for each configured table introduces significant lifecycle management complexity. The system must correctly handle `DROP TABLE`, `ALTER TABLE ... RENAME`, and `ALTER TABLE ... ADD/DROP/RENAME COLUMN` events to keep the generated `TYPE` and metadata in sync with the table's schema.
    *   **Mitigation:** This is a solvable engineering challenge. The existing `drop_protection` and `rename_following` event triggers will be enhanced to manage the `sql_saga.temporal_merge_setup` metadata and the associated generated `TYPE`. This is a critical part of the implementation plan.

2.  **Implementation Complexity:**
    *   **Weakness:** The planner's logic, based on Allen's Interval Algebra and dynamic SQL, is inherently complex and can be challenging to debug and maintain.
    *   **Mitigation:** This complexity is a necessary trade-off for the power and correctness the function provides. A comprehensive, well-documented test suite (ported from `statbus_speed`) will be critical for ensuring correctness and preventing regressions.

2.  **No Granular Feedback (By Design):**
    *   **Weakness:** The core procedure provides atomic, batch-level success/failure. It does not return detailed, row-level status information.
    *   **Mitigation:** This is a deliberate and correct design choice for a general-purpose library function. Systems requiring granular feedback (like the Statbus import system) can implement a wrapper function that calls `temporal_merge` and then generates the necessary detailed results, keeping the core API clean and focused.

3.  **Reliance on Correct Entity Identifier Definition:**
    *   **Weakness:** The function's correctness depends on the entity identifier being correctly defined in the `add_temporal_merge_setup` call. An incorrect definition will lead to logical data corruption.
    *   **Mitigation:** This is an inherent risk in a powerful API. The documentation must be exceptionally clear on the importance of defining the entity identifier correctly during setup.


