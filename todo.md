# SQL Saga - TODO

This file tracks prioritized improvements and tasks for the `sql_saga` codebase.

## High Priority - Bugs & Core Features

- [ ] **Fix memory corruption and logic bugs in `covers_without_gaps` aggregate:**
  - **Issue:** The `covers_without_gaps` aggregate is causing a server crash due to memory corruption.
  - **Action (In Progress):** Following the "Simplify and Rebuild" strategy after multiple failed hypotheses.
    -   **Documented False Leads:**
        -   The crash was *not* caused by simple logic errors in gap detection.
        -   The crash was *not* caused by C99-style variable declarations.
        -   The crash was *not* solved by simply removing `pfree` (this created a memory leak).
        -   The crash was *not* solved by re-introducing `pfree` in various locations (this led to use-after-free errors).
        -   The crash was *not* caused by a flaw in the `copy_bound_to_state` helper function itself, but by the complex logic that was using it.
    -   **Current Strategy: Simplify and Rebuild (Logical Binary Search)**
        -   **Step 1 (Verified):** Reduced the implementation to a minimal, non-crashing stub. The server is stable.
        -   **Step 2 (Verified):** Re-introduced state initialization logic from the `no_gaps.c` prototype. The server remains stable.
        -   **Step 3 (Verified):** Re-introduced input range processing and comparison logic (with state updates commented out). The server remains stable.
        -   **Step 4 (Falsified):** Re-introducing the simplified state update logic for `covered_to` re-introduced the server crash. This approach is flawed.
        -   **Step 5 (Verified):** Reverting the transition function's logic to a more verbose implementation based on the `no_gaps.c` prototype has stabilized the server. The foreign key tests now pass without crashing.
        -   **Step 6 (Falsified):** The initial attempt to fix the memory leak by tracking allocated datums was flawed. It led to a `pfree` crash by attempting to free a pointer to temporary, non-`palloc`'d memory from a deserialized range.
        -   **Step 7 (Verified):** Corrected the memory management logic for pass-by-reference types. The server is now stable and does not crash or leak memory when processing any range type.
        -   **Step 8 (Verified):** Added a final check in `covers_without_gaps_finalfn` to correctly assess coverage after all rows are processed.
        -   **Step 9 (Falsified):** The attempt to create a generic logic for boundary inclusivity was flawed. It correctly handled continuous ranges but failed for discrete ranges, causing many tests to fail.
        -   **Step 10 (Falsified):** The attempt to use `rng_is_discrete` failed because this member does not exist in the `TypeCacheEntry` struct. This approach was based on an incorrect assumption about the PostgreSQL internals.
        -   **Step 11 (Falsified):** The attempt to differentiate logic based on discrete vs. continuous range types was flawed. While the detection mechanism was corrected, the logic itself was overly complex and failed for continuous types like `numeric`.
        -   **Step 12 (Falsified):** Reverting to a simpler logic (always inclusive) fixed discrete range tests but failed for continuous ones because the tests themselves were flawed.
        -   **Step 13 (Falsified):** The generalized logic and corrected tests still revealed a fundamental flaw. The root cause was the incorrect use of `range_cmp_bounds` for contiguity checking, which is designed for sorting and misinterprets adjacent bounds as gaps.
        -   **Step 14 (Verified):** Implemented a correct, manual contiguity check in `covers_without_gaps.c`, and enhanced the test suite. All `covers_without_gaps` tests now pass.
        -   **Step 15 (Falsified):** The hypothesis to exclude the `OLD` row was correct, but the implementation introduced a SQL syntax error (`missing FROM-clause entry for table "uk"`) by incorrectly using an outer alias inside a subquery.
        -   **Step 16 (Falsified):** The attempt to fix the SQL syntax by removing the `uk.` prefix from the `old_pk_val_clause` did not resolve the issue, indicating a deeper problem with the subquery approach.
        -   **Step 17 (Falsified):** The attempt to fix the SQL syntax by moving the exclusion logic to the `ON` clause was syntactically correct but did not fix the underlying logical error, as the tests still failed to detect violations.
        -   **Step 18 (Falsified):** Simplifying the validation query by removing the `OLD` row exclusion logic did not work. The query still failed to detect foreign key violations, indicating the issue is not with the exclusion logic but something more fundamental.
        -   **Step 19 (Falsified):** The hypothesis that refactoring `validate_foreign_key_old_row` to a `LOOP` that calls `validate_foreign_key_new_row` would work was incorrect. This change failed to detect violations and produced confusing error messages.
        -   **Step 20 (Current):** Correcting foreign key validation by inlining logic.
            1.  **Hypothesis:** The `LOOP` approach is correct, but re-using `validate_foreign_key_new_row` is flawed. Inlining the validation logic directly into `validate_foreign_key_old_row`'s `LOOP` and raising a context-appropriate error will be more robust.
            2.  **Action:** Replace the `PERFORM` call inside the `LOOP` in `validate_foreign_key_old_row` with the explicit `SELECT ... INTO okay` and `IF NOT okay THEN RAISE ...` logic from `validate_foreign_key_new_row`. This makes the function self-contained and should fix all remaining test failures.

- [x] **Make `drop_*` commands fail for incorrect parameters:**
  - **Issue:** The `drop_*` functions were not strict and would fail silently for non-existent objects, which can hide configuration errors.
  - **Fix:** The `drop_*` functions have been made strict to raise an exception for invalid parameters. The test suite's cleanup helpers have been updated to be idempotent by checking for an object's existence before attempting to drop it.

## Medium Priority - Refactoring & API Improvements

- [ ] **Improve trigger performance by converting to C:**
  - **Files:** `sql_saga--1.0.sql`, `sql_saga.c`
  - **Issue:** Many trigger functions are written in `pl/pgsql`, which can be slow.
  - **Action:** Port performance-critical trigger functions to C for better execution speed. This includes caching query plans and metadata.

- [ ] **Implement hook for DDL changes (drop/rename):**
  - **Files:** `sql_saga--1.0.sql`, `sql_saga.c`
  - **Issue:** The extension does not currently handle `DROP` or `RENAME` operations on tables or columns that are part of a temporal setup, which can leave the metadata in an inconsistent state.
  - **Action:** Implement an event trigger or hook to detect these DDL changes and either update the `sql_saga` catalog or prevent the operation.

- [ ] **Make `add_*`/`drop_*` API symmetric:**
  - **Status:** Done.
  - **Issue:** The parameters for `add_*` and `drop_*` functions were not always symmetric, making the API less intuitive.
  - **Action:** Overloaded `drop_unique_key` and `drop_foreign_key` functions have been added. They now accept the same logical parameters as their `add_*` counterparts, providing a more intuitive API.

- [ ] **Use a private prefix for internal helper functions:**
  - **File:** `sql_saga--1.0.sql`
  - **Issue:** Helper functions like `_make_name` are in the public `sql_saga` schema.
  - **Action:** Rename them with a more explicit "private" prefix (e.g., `__internal_make_name`) or move them to a separate internal schema to discourage external use.

## Low Priority - Future Work & New Features

- [ ] **Support foreign keys from non-temporal tables:**
  - **File:** `sql_saga--1.0.sql`
  - **Issue:** The current implementation only supports foreign keys between two temporal tables.
  - **Action:** Adapt `sql_saga.add_foreign_key` to allow a non-temporal table to reference a temporal table. This would require a check function to validate the key's existence at the time of insert/update.

- [ ] **Prototype a combined UPSERT/UPDATE API:**
  - **File:** `sql_saga--1.0.sql`
  - **Issue:** The current API for updating data is complex.
  - **Action:** Prototype a new `sql_saga.add_api` function or a new function that provides a simpler interface for handling both `INSERT` and `UPDATE` (UPSERT) operations on temporal data.

- [ ] **Ensure `infinity` is the default for `valid_to` columns:**
  - **File:** `sql_saga--1.0.sql`
  - **Issue:** The `add_era` function does not enforce `'infinity'` as the default value for the `stop_on_column_name` (e.g., `valid_to`).
  - **Action:** Modify `add_era` to set the default and potentially add a check constraint.

- [ ] **Implement full `ON UPDATE` action support for foreign keys:**
  - **File:** `sql_saga--1.0.sql`
  - **Issue:** The `update_action` parameter in `add_foreign_key` is not fully handled; the comment notes it should affect trigger timing.
  - **Action:** Implement the logic for `ON UPDATE` actions (`NO ACTION`, `RESTRICT`) correctly.

- [ ] **Package `sql_saga` with pgxman for distribution:**
  - **Issue:** The extension currently requires manual installation.
  - **Action:** Create configuration files and a process to package the extension using `pgxman` for easier distribution and installation.

## Learnings from Inspired Projects (`periods` and `time_for_keys`)

This section summarizes potential improvements and features adapted from the `periods` and `time_for_keys` extensions, which served as inspiration for `sql_saga`.

### From `periods` extension:

- [ ] **Implement System Versioning:** `periods` contains a complete implementation of `SYSTEM VERSIONING`, including history tables, views, and helper functions (`_as_of`, `_between`, etc.). This is a major feature that `sql_saga` currently has commented-out stubs for.
  - **Action:** Review the `periods.system_versioning` table and the `add_system_versioning` function as a blueprint for implementing this feature in `sql_saga`.

- [ ] **Adopt C-based Trigger Functions for Performance:** `periods.c` contains C implementations for `generated_always_as_row_start_end` and `write_history` triggers. This is a significant performance enhancement over `pl/pgsql`.
  - **Action:** Use `periods.c` as a reference for moving performance-critical trigger logic in `sql_saga` to C. This includes handling different tuple descriptors between the main table and history table.

- [ ] **Enhance Event Trigger Logic:** `periods` has robust `drop_protection` and `rename_following` event triggers.
  - **Action:** Compare the logic in `periods` with `sql_saga`'s event triggers to identify any missing checks or opportunities for improvement.

### From `time_for_keys` extension:

- [ ] **Analyze Alternative Foreign Key Implementation (`TRI_FKey_*`):** This represents a less dynamic, but potentially faster, approach to temporal foreign keys. Instead of a central metadata catalog, it creates specific triggers for each foreign key constraint.
  - **Action:** Analyze the performance of this approach vs. `sql_saga`'s metadata-driven approach. The recent refactoring in `sql_saga` to pass metadata as arguments to triggers might have closed the performance gap.

- [ ] **Evaluate `completely_covers` Aggregate Function:** The C function in `completely_covers.c` is specialized for `tstzrange` and is used to validate temporal foreign keys. `sql_saga` has a generic `covers_without_gaps` aggregate for `anyrange`.
  - **Action:** Compare the performance and correctness of `completely_covers` against `covers_without_gaps`. A specialized function might be faster. Determine if `sql_saga` would benefit from specialized aggregates for common range types.


## Done

- **Clarify variable naming in `add_api`:** A misleading variable name inside the `add_api` function loop was corrected for clarity.
- **Clarify parameter naming in `_make_api_view_name`:** A misleading parameter name in the `_make_api_view_name` function was corrected for clarity.
- **Fix `add_foreign_key` exception for incompatible eras:** Corrected a bug where the error message for incompatible era types was incorrect.
- **Fix bug in temporal foreign key validation for `UPDATE` and `DELETE`:** Rewrote the validation logic in `validate_foreign_key_old_row` to correctly handle `UPDATE` and `DELETE` on referenced keys.
- **Simplify foreign key validation using `covers_without_gaps`:** Refactored `validate_foreign_key_new_row` to use the `covers_without_gaps` aggregate, simplifying the code and making it consistent with `validate_foreign_key_old_row`.
