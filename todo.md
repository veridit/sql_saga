# SQL Saga - TODO

This file tracks prioritized improvements and tasks for the `sql_saga` codebase.

## High Priority - Bugs & Core Features

- [x] **Fix `add_foreign_key` exception for incompatible eras:**
  - **File:** `sql_saga--1.0.sql`
  - **Issue:** The `RAISE EXCEPTION` for incompatible era types in `sql_saga.add_foreign_key` will fail at runtime because it references a non-existent field and uses the same variable for both era names in the message.
  - **Fix:** Correct the field access and ensure the two different era names are displayed in the error message.

- [ ] **Fix bug in temporal foreign key validation for `UPDATE`:**
  - **Files:** `sql_saga--1.0.sql`, `completely_covers.c`
  - **Issue:** When updating the temporal range of a primary key, the system fails to block the update even if it leaves referencing foreign keys "dangling". This was identified in `25_update_pk_test.sql` where `UPDATE`s on the `houses` table should have failed due to existing records in the `rooms` table.
  - **Action:** Investigate the validation logic, likely within the `completely_covers` or `no_gaps` aggregate functions, to correctly handle boundary conditions and prevent invalid updates.

- [ ] **Make `drop_*` commands fail for incorrect parameters:**
  - [x] `drop_era`
  - [x] `drop_unique_key`
  - [ ] `drop_foreign_key`
  - [ ] `drop_api`
  - **Issue:** The `drop_*` functions should raise errors for invalid parameters (e.g., non-existent keys or tables) instead of silently doing nothing. This will make the API more robust and predictable.

## Medium Priority - Refactoring & API Improvements

- [ ] **Improve trigger performance by converting to C:**
  - **Files:** `sql_saga--1.0.sql`, `sql_saga.c`
  - **Issue:** Many trigger functions are written in `pl/pgsql`, which can be slow.
  - **Action:** Port performance-critical trigger functions to C for better execution speed. This includes caching query plans and metadata.

- [ ] **Implement hook for DDL changes (drop/rename):**
  - **Files:** `sql_saga--1.0.sql`, `sql_saga.c`
  - **Issue:** The extension does not currently handle `DROP` or `RENAME` operations on tables or columns that are part of a temporal setup, which can leave the metadata in an inconsistent state.
  - **Action:** Implement an event trigger or hook to detect these DDL changes and either update the `sql_saga` catalog or prevent the operation.

- [ ] **Clarify variable naming in `add_api` function:**
  - **File:** `sql_saga--1.0.sql`
  - **Issue:** Inside the loop in `sql_saga.add_api`, a variable named `table_oid` is used to store a table *name* (`relname`), which is misleading.
  - **Fix:** Rename the variable to `table_name` to improve readability.

- [ ] **Clarify parameter naming in `_make_api_view_name` function:**
  - **File:** `sql_saga--1.0.sql`
  - **Issue:** The function `_make_api_view_name` takes a `table_oid` parameter which is actually a table name.
  - **Fix:** Rename the parameter to `table_name` for clarity.

- [ ] **Make `add_*`/`drop_*` API symmetric:**
  - **File:** `sql_saga--1.0.sql`
  - **Issue:** The parameters for `add_*` and `drop_*` functions are not always symmetric, making the API less intuitive. For example, `add_foreign_key` has many parameters while `drop_foreign_key` uses just `key_name`.
  - **Action:** Refactor the API to make `add` and `drop` operations take similar identifying parameters.

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

This section summarizes potential improvements and features that can be adapted from the `periods` and `time_for_keys` extensions, which served as inspiration for `sql_saga`. Once these ideas are evaluated and either integrated or discarded, the corresponding source files should be removed from the repository.

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

- [ ] **Evaluate `completely_covers` Aggregate Function:** The C function in `completely_covers.c` is specialized for `tstzrange` and is used to validate temporal foreign keys. `sql_saga` has a generic `no_gaps` aggregate for `anyrange`.
  - **Action:** Compare the performance and correctness of `completely_covers` against `no_gaps`. A specialized function might be faster. Determine if `sql_saga` would benefit from specialized aggregates for common range types.

- [ ] **Review Referential Action Logic:** The `TRI_FKey_restrict` function contains a specific SQL pattern (`WITH` clause and a `LEFT OUTER JOIN`) for checking for referencing rows.
  - **Action:** Analyze this query pattern. It might be a more optimized way to check for violations than the current logic in `sql_saga.validate_foreign_key_old_row`.
