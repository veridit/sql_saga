# SQL Saga - TODO

A living document of upcoming tasks.
Tasks are checked ✅ when done and made brief.
Keep a journal.md that tracks the state of the current ongoing task and relevant details.

## High Priority - Bugs & Core Features

- [x] **Implement `sql_saga.temporal_merge` (Set-Based Upsert API):** Provided a single, high-performance, set-based function for `INSERT`/`UPDATE`/`DELETE` operations on temporal tables. The API is simplified via `regclass` parameters, era introspection, and auto-detection of defaulted columns. This is the official solution for bulk data modifications.

- [x] **Improve `rename_following` to support column renames:** The event trigger now correctly detects when a column in a foreign key is renamed and automatically updates all relevant metadata, including the foreign key name, column list, and associated trigger names.

- [x] **Foreign key validation fails for tables in different schemas:** Fixed. The `fk_update_trigger` is now created with a dynamic column list that includes `valid_to` (if present) to ensure validation fires correctly for synchronized columns without being an overly-broad row-level trigger.

- [x] **Support identifiers with quotes inside:** Verified API functions handle quoted identifiers correctly.
- [x] **Cache query plans:** Cached query plans to improve trigger performance.
- [x] **Refactor core to use `(schema, table)` instead of `oid`:** Made event triggers robust against `DROP`.
- [x] **(Breaking Change) Adopted `[valid_from, valid_until)` period semantics:** Refactored the extension to use the standard `[)` inclusive-exclusive period convention, renaming all temporal columns accordingly.

- [x] **Implement System Versioning:** Ported the complete System Versioning feature from `periods`, including history tables, C-based triggers (`generated_always_as_row_start_end`, `write_history`), and the `add/drop_system_versioning` API.

## Medium Priority - Refactoring & API Improvements

- [x] **Support Predicates for Temporally Unique Keys:** Extended `add_unique_key` to support a `WHERE` clause for creating partial unique keys (e.g., `WHERE legal_unit_id IS NOT NULL`). This uses a unique index with a predicate instead of a unique constraint.

- [x] **Refactor `temporal_merge` to a procedure for better ergonomics:** Converted the `temporal_merge` function into a procedure. Instead of returning a set of results, it now creates a temporary table `__temp_last_sql_saga_temporal_merge` with the feedback, simplifying the calling pattern.
- [ ] **Refactor `add_api` to `add_updatable_views` for clarity:** The name `add_api` is too generic. Rename `add_api` to `add_updatable_views` and `drop_api` to `drop_updatable_views` to make it clear that these functions create and manage the specialized updatable views for interacting with temporal data.

- [ ] **Refactor `update_portion_of` to use `temporal_merge`:** Unify the codebase by refactoring the `update_for_portion_of` view's trigger to be a simple wrapper around `temporal_merge` with `mode = 'patch_only'`. This will reduce code duplication and ensure consistent behavior.

- [x] **Temporal Merge with Dependent Row Support:** Refactored `temporal_merge` to correctly handle batches containing dependent operations (e.g., an `INSERT` of a new entity and subsequent `UPDATE`s to it). This was achieved by changing the API to accept a `p_founding_id_column` and implementing internal, multi-stage ID propagation logic, making the function truly set-based and robust.

- [x] **Add option to back-fill generated IDs into source table:** Extended `temporal_merge` with a new parameter `p_update_source_with_assigned_entity_ids`. When `true`, the procedure will update the source table with any generated surrogate or composite key values for newly inserted entities. This simplifies multi-step import processes by removing the need for manual ID propagation between steps.

- [x] **Ensure Symmetrical APIs:** Refactored `drop_unique_key` and `drop_foreign_key` to be unambiguous by renaming the `_by_name` variants. Aligned tests to use the more intuitive symmetrical API calls by default.
- [x] **Standardize System Versioning Column Naming:** Renamed system versioning columns to `system_valid_from` and `system_valid_until` to be consistent with application-time `valid_from`/`valid_until` semantics.

- [x] **Refactor test suite:** Made all tests self-contained and idempotent, resolving all regressions.

- [x] **Complete the `regclass` -> `(schema, table)` refactoring:** Removed all `oid` columns from metadata tables, making event triggers robust against `DROP`.

- [x] **Provide convenience trigger `synchronize_valid_to_until`:** Added trigger to help manage human-readable inclusive end-dates.

- [x] **Refactor `add_api` to not require a primary key:** Refactored `add_api` to use a single-column temporal unique key as the entity identifier if no primary key is present. Fixed `update_portion_of` trigger to correctly preserve identifier columns during updates.

- [x] **Fix event trigger regressions:** Resolved bugs in `rename_following` and `health_checks` event triggers that were exposed by refactoring. The triggers now correctly handle `search_path` issues and reliably update metadata for renamed objects.

- [x] **Refactor build system and fix all regressions:** Overhauled the `Makefile` to ensure reliable, incremental builds. Refactored the SQL source into a modular structure and resolved all test failures that were exposed by the new build process.

- [x] **Support foreign keys from non-temporal tables:** Provided full foreign key support from standard tables to temporal tables. The `add_foreign_key` function now automatically creates a `CHECK` constraint on the referencing table and `UPDATE`/`DELETE` triggers on the referenced temporal table to provide full `RESTRICT`/`NO ACTION` semantics.

- [ ] **Investigate Statement-Level Triggers:**
  - **Goal:** Replace the `uk_update_check_c` and `uk_delete_check_c` row-level triggers with statement-level triggers.
  - **Benefit:** This would provide correct validation for complex, single-statement DML operations that modify multiple rows (e.g., `UPDATE ... FROM ...`, `MERGE`). This is the architecturally correct way to handle multi-row validation.
  - **Limitation:** This would not solve the problem of multi-statement transactions that are only valid at commit time (e.g., two separate `UPDATE` statements swapping periods), as statement-level triggers are not deferrable to the end of the transaction. This limitation should be documented.

## Low Priority - Future Work & New Features

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

- [x] **Enhance Event Trigger Logic:** `periods` has robust `drop_protection` and `rename_following` event triggers.
  - **Action:** Compared logic in `periods` with `sql_saga`'s event triggers. `sql_saga`'s logic is a correct subset of `periods`, and includes a performance optimization for `rename_following` that `periods` lacks. No further changes needed.

### From `time_for_keys` extension:

- [x] **Analyze Alternative Foreign Key Implementation:** The `time_for_keys` project represents a less dynamic, but potentially faster, approach to temporal foreign keys. Instead of a central metadata catalog, it creates specific triggers for each foreign key constraint. The legacy code for this was removed from `sql_saga` to avoid confusion.
  - **Action:** Analysis complete. The `time_for_keys` approach uses `pl/pgsql` triggers that re-plan validation queries on every row, making it significantly less performant than `sql_saga`'s current C-based triggers with cached query plans. The recent refactoring in `sql_saga` to pass all metadata as arguments has already adopted the best part of this design while implementing it in a much more performant way. No changes are warranted.

- [x] **Evaluate Alternative Gap-Coverage Functions:** `periods` uses a `pl/pgsql` implementation for temporal validation, and `time_for_keys` has a `no_gaps` function specialized for `daterange`. `sql_saga`'s C-based `covers_without_gaps` aggregate for `anyrange` is a deliberate improvement.
  - **Action:** Analysis complete. The `covers_without_gaps` function in `sql_saga` is more performant than the `periods` implementation and more generic and correct than the `time_for_keys` version. It successfully passes a more comprehensive test suite ported from `time_for_keys`, fixing regressions that were present in the `periods` logic. No changes are needed.
