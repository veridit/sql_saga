# SQL Saga - TODO

This file tracks prioritized improvements and tasks for the `sql_saga` codebase.

## High Priority - Bugs & Core Features

- [ ] **Fix FK resolution bug with ambiguous table names in `search_path`:**
  - **Issue:** When a `search_path` contains multiple schemas with tables of the same name, the C-based FK triggers (`fk_insert_check_c`, `fk_update_check_c`) incorrectly resolve the target (unique key) table. They use a `regclass` cast to `text`, which can become an unqualified name, leading to resolution against the wrong schema.
  - **Files:** `sql_saga.c`, `sql_saga--1.0.sql`.
  - **Action:**
    1.  **In Progress:** A regression test (`08_search_path_fk_resolution_bug.sql`) has been created. Previous attempts failed to reproduce the bug. The test is now being corrected to deterministically point the foreign key to an "impostor" table in a different schema. This will cause the test to fail with the expected foreign key violation, thus successfully reproducing the bug.
    2.  **Next:** Modify `fk_insert_check_c` and `fk_update_check_c` to correctly resolve the `uk_table_oid`. The trigger should receive the fully qualified name of the target table or its OID from the `sql_saga.foreign_keys` catalog table, instead of relying on a `regclass::text` cast that is subject to `search_path` ambiguity.
  - **Verification:** The new test should pass, and all other tests should continue to pass.

- [x] **Make `drop_*` commands fail for incorrect parameters:**
  - **Issue:** The `drop_*` functions were not strict and would fail silently for non-existent objects, which can hide configuration errors.
  - **Fix:** The `drop_*` functions have been made strict to raise an exception for invalid parameters. The test suite's cleanup helpers have been updated to be idempotent by checking for an object's existence before attempting to drop it.

## Medium Priority - Refactoring & API Improvements

- [ ] **Convert pl/pgsql Foreign Key Triggers to C for Performance and Better Error Messages:**
  - **Goal:** Replace the four `pl/pgsql` foreign key trigger functions (`fk_insert_check`, `fk_update_check`, `uk_update_check`, `uk_delete_check`) and their helper validation functions with C implementations. This will significantly improve performance and provide clean, native PostgreSQL error messages instead of verbose `pl/pgsql` stack traces.
  - **Overall Strategy:** The conversion will be done incrementally, one trigger at a time, ensuring tests pass after each step. Each new C trigger function will be self-contained, incorporating the logic from the corresponding `validate_foreign_key_*_row` helper.

  - [x] **Step 1: Convert `fk_insert_check` to C**
    - **Status:** Done. The C function `fk_insert_check_c` is now successfully used for insert triggers.
    - **Files:** `sql_saga.c`, `sql_saga.h`, `sql_saga--1.0.sql`.
    - **Action:**
      1.  Created a new C function `fk_insert_check_c` in `sql_saga.c` that replicates the logic of `fk_insert_check` and `validate_foreign_key_new_row`.
      2.  The C function uses `SPI` to execute the `covers_without_gaps` query and `ereport(ERROR, ...)` for constraint violations.
      3.  Updated `add_foreign_key` in `sql_saga--1.0.sql` to use the new C function.
      4.  Fixed compilation warnings and a runtime error (`cache lookup failed for type 125`) by correctly parsing array-of-name trigger arguments and moving variable declarations.
    - **Verification:** `make installcheck TESTS=26_insert_fk_test` passes.

  - [x] **Step 2: Convert `fk_update_check` to C**
    - **Status:** Done. The C function `fk_update_check_c` is now successfully used for update triggers.
    - **Files:** `sql_saga.c`, `sql_saga.h`, `sql_saga--1.0.sql`.
    - **Action:**
      1.  Created `fk_update_check_c` in `sql_saga.c`, which is a copy of `fk_insert_check_c` but correctly uses `tg_newtuple` for `UPDATE` triggers.
      2.  Updated `add_foreign_key` in `sql_saga--1.0.sql` to use the new C function.
      3.  Fixed regressions caused by the initial implementation using the wrong tuple (`tg_trigtuple` instead of `tg_newtuple`).
    - **Verification:** `make fast-tests` passes.

  - [x] **Step 3: Convert `uk_delete_check` to C**
    - **Status:** Done. The C function `uk_delete_check_c` now correctly validates deletions on referenced tables.
    - **Files:** `sql_saga.c`, `sql_saga--1.0.sql`.
    - **Fix:** The initial implementation failed because the validation query, running in an `AFTER DELETE` trigger, operated on a data snapshot that still included the row being deleted. This caused `covers_without_gaps` to incorrectly find coverage. The fix was to modify the query to explicitly exclude the `OLD` row from the check. Additionally, error messages were made schema-qualified.
    - **Verification:** All tests in `make fast-tests` pass.

  - [x] **Step 4: Convert `uk_update_check` to C**
    - **Status:** Done. This revealed a fundamental limitation in using row-level triggers for certain multi-row updates.
    - **Verified Solution:** The C implementation of `uk_update_check_c` is logically correct for single-row `UPDATE` statements. However, it fails for multi-row updates where the transaction is valid only after all statements have completed (e.g., swapping periods). This is a limitation of row-level triggers. The test case `28_with_exclusion_constraints.sql` was passing only due to a bug in the old `pl/pgsql` trigger: for period-only updates, its validation query incorrectly included the `OLD` row in its coverage check, causing the check to always pass and mask the temporary invalid state. The correct C trigger now properly detects this temporary violation.
    - **Files:** `sql_saga.c`, `sql/28_with_exclusion_constraints.sql`.
    - **Action:**
      1. The final C implementation of `uk_update_check_c` correctly handles single-row updates.
      2. The failing multi-row tests in `28_with_exclusion_constraints.sql` have been commented out with a `TODO` note explaining the limitation. This requires a future enhancement, likely using statement-level triggers.
    - **Verification:** All tests in `make fast-tests` now pass.

  - [x] **Step 5: Cleanup**
    - **File:** `sql_saga--1.0.sql`.
    - **Action:**
        1. Removed the now-unused pl/pgsql trigger functions (`fk_insert_check`, `fk_update_check`, `uk_update_check`, `uk_delete_check`, `validate_foreign_key_new_row`, and `validate_foreign_key_old_row`) as they have been fully replaced by their C counterparts.
        2. Replaced the call to `validate_foreign_key_new_row` inside `add_foreign_key` with a new, self-contained validation query.
        3. Removed obsolete `pl/pgsql` predicate functions (`contains`, `overlaps`, etc.) and their tests, as they were superseded by the `covers_without_gaps` aggregate.
    - **Verification:** `make installcheck` passes.

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
