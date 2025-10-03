# Architectural Decision Log

This document records significant architectural decisions made during the development of `sql_saga`.

---

## Decision: De-prioritizing Statement-Level Triggers (2025-09-01)

This section logs the decision to not pursue the implementation of statement-level triggers at this time.

### Rationale
This feature is not worth the implementation complexity for the following reasons:
1.  **Performance:** The current performance of the row-level triggers is acceptable ("not great, but not horrible").
2.  **Limited Utility:** The primary benefit of statement-level triggers is for validating multi-row DML within a *single statement*. However, the user's typical use cases involve changes across multiple tables within a single transaction. Since statement-level triggers are **not deferrable** to the end of a transaction, they would not solve this more common and complex validation problem.

### Conclusion
Based on this feedback, the "Investigate Statement-Level Triggers" and the dependent "Implement full `ON UPDATE` action support" tasks have been removed from the active `todo.md`. The current C-based, row-level triggers will remain the primary mechanism for foreign key validation. This decision can be revisited in the future if performance requirements change or if PostgreSQL adds support for deferrable statement-level triggers.

---

## Architectural Decision: Supporting Native Range Types (2025-09-01)

This section documents the exploration of different architectural strategies for allowing users to define an era using a single, native PostgreSQL range type column (e.g., `daterange`) instead of the current two-column (`valid_from`, `valid_until`) approach.

### Option A: The "Big Refactor"
-   **Description:** Fundamentally alter the entire extension. All metadata tables, C functions, and PL/pgSQL procedures would be rewritten to operate directly on a single `anyrange` column.
-   **Pros:**
    -   Architecturally "pure"; the internal model would directly reflect the user's data model.
-   **Cons:**
    -   **Massive Effort:** Requires a near-total rewrite of the codebase.
    -   **High Risk:** A rewrite of this magnitude is extremely high-risk and would likely introduce subtle regressions in well-tested components like the C triggers and `temporal_merge` planner.

### Option B: The "View Shim" (User-Managed)
-   **Description:** Require the user to manually create a view over their table that exposes `lower(range_col)` as `valid_from` and `upper(range_col)` as `valid_until`. The user would then register the *view* with `sql_saga`.
-   **Pros:**
    -   Requires zero code changes to the extension.
-   **Cons:**
    -   **Poor Ergonomics:** Pushes all the complexity onto the user, including the non-trivial task of creating `INSTEAD OF` triggers to make their view updatable.
    -   **Performance/Complexity Issues:** Can inhibit index usage and leads to complex "view-on-view" scenarios.

### Option C: The "Generated Columns" (Recommended)
-   **Description:** Overload the `add_era` function. When called with a `range_column_name`, it automatically alters the user's table to add `valid_from` and `valid_until` columns. These columns are defined as `GENERATED ALWAYS AS (lower(...)) STORED` and `GENERATED ALWAYS AS (upper(...)) STORED`. The rest of the extension continues to operate on these generated columns.
-   **Pros:**
    -   **Excellent Ergonomics:** The user experience is seamless.
    -   **Minimal Core Changes:** The complexity is almost entirely isolated to the `add_era` and `drop_era` functions. The rest of the extension, including all C code, remains untouched.
    -   **Performance:** `STORED` generated columns can be indexed, preserving all performance characteristics.
-   **Cons:**
    -   **Modifies User Table:** The function alters the user's schema, which must be clearly documented to avoid surprises.
    -   **Storage Overhead:** The generated `STORED` columns consume additional disk space.

### Option D: The "Metadata Shim"
-   **Description:** Modify the `sql_saga.era` table to store the `range_column_name`. Then, every part of the codebase (C functions, PL/pgSQL) would be modified with conditional logic to dynamically generate `lower(range_col)` and `upper(range_col)` expressions if a range column is being used.
-   **Pros:**
    -   Does not alter the user's table schema. The user's data model remains "clean".
-   **Cons:**
    -   **Pervasive & Complex Code Changes:** This approach pushes complexity deep into the extension's code. Conditional logic (`IF range_col IS NOT NULL THEN ... ELSE ...`) would need to be added to every performance-critical component, including:
        -   The C-based foreign key triggers.
        -   The `temporal_merge` planner's massive dynamic query.
        -   The triggers for the updatable views.
        This makes the extension's internal logic significantly harder to maintain and debug.
    -   **Performance Penalty & Indexing Complexity:** Using functions on a column (e.g., `lower(range_col)`) prevents the query planner from using a standard B-tree index on that column. To maintain performance, users would be **required** to create and maintain functional indexes (e.g., `CREATE INDEX ON my_table (lower(my_range_col))`). This externalizes a critical performance dependency, making the system less robust and harder to use correctly compared to the simple, standard indexes used by the "Generated Columns" approach. The query plans themselves also become more complex for the database to optimize.

### Option E: The "Synchronization Trigger"
-   **Description:** This approach is directly inspired by the existing `synchronize_valid_to_until` trigger. The user would define all three columns in their table (`my_range range`, `valid_from date`, `valid_until date`). A new, generic trigger function would be created. When applied to the table, this trigger would ensure that all three columns are kept in perfect sync on any `INSERT` or `UPDATE`. If any one of the columns is changed, the other two are automatically updated to match.
-   **Pros:**
    -   **Maximum Flexibility & API Ergonomics:** This is the most significant advantage. It provides an ideal experience for both database and API consumers:
        -   **For Database Users:** The ability to use a native `range` type is clean and idiomatic.
        -   **For PostgREST Users:** The synchronized `valid_from` and `valid_until` columns completely bypass the friction of using range types over HTTP. As documented in the [PostgREST documentation](https://docs.postgrest.org/en/v10/how-tos/working-with-postgresql-data-types.html#ranges), using ranges directly requires cumbersome filter syntax (e.g., `?duration=cs.[2023-01-01,2023-01-01]`), precise string representations for `INSERT`/`UPDATE`, and custom casting functions to receive a useful JSON object instead of a string literal. The synchronized columns provide a clean, simple, and direct RESTful interface.
    -   **Built-in Data Integrity:** The trigger is the ideal place to enforce `sql_saga`'s required `[)` semantics (inclusive lower, exclusive upper bound), guaranteeing data quality without extra user setup.
    -   **Leverages Existing Pattern:** Follows a pattern already established and proven within `sql_saga`, making it a consistent and predictable addition.
    -   **No Core Changes:** Like Option C, this requires no changes to the core of the extension (C triggers, `temporal_merge`, etc.).
    -   **Explicit Storage:** The user explicitly defines and stores all three columns, making the storage cost a clear and deliberate part of their data model.
-   **Cons:**
    -   **Requires User to Add Trigger:** This is a minor step, but the user must remember to create the trigger on their table. This can be completely mitigated by making `add_era` smart enough to do it automatically.

### Recommended Architecture: Option E
The **"Synchronization Trigger"** is the superior recommendation. It provides all the benefits of the "Generated Columns" approach (PostgREST ergonomics, performance, isolation from the core extension) while providing far greater flexibility by allowing writes to the `range` column itself. It is the most user-friendly and robust solution.
