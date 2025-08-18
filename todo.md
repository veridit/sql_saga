# SQL Saga - TODO

This file tracks prioritized improvements and tasks for the `sql_saga` codebase.

## High Priority - Bugs & Core Features

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

- [ ] **Analyze Alternative Foreign Key Implementation:** The `time_for_keys` project represents a less dynamic, but potentially faster, approach to temporal foreign keys. Instead of a central metadata catalog, it creates specific triggers for each foreign key constraint. The legacy code for this was removed from `sql_saga` to avoid confusion.
  - **Action:** Analyze the performance of this approach (by reviewing the `time_for_keys` project) vs. `sql_saga`'s metadata-driven approach. The recent refactoring in `sql_saga` to pass metadata as arguments to triggers might have closed the performance gap.

- [ ] **Evaluate `completely_covers` Aggregate Function:** The C function in `completely_covers.c` is specialized for `tstzrange` and is used to validate temporal foreign keys. `sql_saga` has a generic `covers_without_gaps` aggregate for `anyrange`.
  - **Action:** Compare the performance and correctness of `completely_covers` against `covers_without_gaps`. A specialized function might be faster. Determine if `sql_saga` would benefit from specialized aggregates for common range types.


## Done

 **Remove legacy `time_for_keys` implementation:** Removed the `TRI_FKey_*` trigger functions and the `create/drop_temporal_foreign_key` helper functions. This code was part of an alternative, non-metadata-driven foreign key implementation from the `time_for_keys` project. It was removed to avoid confusion with `sql_saga`'s primary metadata-driven API. This resulted in a significant performance improvement, with the `43_benchmark` test runtime dropping from over 40 seconds to approximately 12 seconds. The analysis of this alternative approach remains as a research task, referencing the original `time_for_keys` project.
- **Fix memory corruption and logic bugs in `covers_without_gaps` aggregate:** This was a complex, multi-stage bug fix. The final solution involved:
    1.  **Memory Safety:** Replacing a flawed, simplified implementation with a more verbose but stable one based on the `no_gaps.c` prototype, then fixing memory leaks for pass-by-reference types by carefully managing `palloc`'d memory within the aggregate's context.
    2.  **Correct Logic:** Implementing a fully generalized contiguity logic that correctly handles both discrete (e.g., `integer`, `date`) and continuous (e.g., `numeric`, `timestamp`) range types. The final implementation correctly checks for gaps between adjacent boundaries by comparing datums directly and then inspecting the `inclusive` flags, rather than using the incorrect `range_cmp_bounds` function.
    3.  **Test Suite Correction:** Correcting all `covers_without_gaps` tests to be deterministic by using `ORDER BY` inside the aggregate call to guarantee sorted input, which is a requirement of the function.
    4.  **Foreign Key Validation:** Fixed `validate_foreign_key_old_row` by refactoring it to use a robust, set-based query with the `covers_without_gaps` aggregate. This was an iterative process that involved fixing the aggregate itself and ensuring correct transaction visibility semantics and correct range bound construction.
- **Clarify variable naming in `add_api`:** A misleading variable name inside the `add_api` function loop was corrected for clarity.
- **Clarify parameter naming in `_make_api_view_name`:** A misleading parameter name in the `_make_api_view_name` function was corrected for clarity.
- **Fix `add_foreign_key` exception for incompatible eras:** Corrected a bug where the error message for incompatible era types was incorrect.
- **Fix bug in temporal foreign key validation for `UPDATE` and `DELETE`:** Rewrote the validation logic in `validate_foreign_key_old_row` to correctly handle `UPDATE` and `DELETE` on referenced keys.
- **Simplify foreign key validation using `covers_without_gaps`:** Refactored `validate_foreign_key_new_row` to use the `covers_without_gaps` aggregate, simplifying the code and making it consistent with `validate_foreign_key_old_row`.
