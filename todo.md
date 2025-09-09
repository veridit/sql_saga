# SQL Saga - TODO

A living document of upcoming tasks.
Tasks are checked ✅ when done and made brief.
Keep a journal.md that tracks the state of the current ongoing task and relevant details.

## High Priority - Bugs & Core Features
- [x] **Fix `temporal_merge` to preserve surrogate keys when using a natural key identity:** When `p_identity_columns` is a natural key, the procedure fails to propagate the table's surrogate key (e.g., a `serial` PK) to new historical slices, generating incorrect new keys instead. It also incorrectly attempts to `UPDATE` the surrogate key column, causing `NOT NULL` violations.
- [x] **Fix `temporal_merge` `valid_to` consistency on slice split:** When splitting a time slice, `temporal_merge` incorrectly inherits `valid_to` from the source instead of recalculating it from the new, shorter `valid_until`. This causes a trigger constraint violation when `valid_to` synchronization is active.
- [x] **Fix `add_era` to validate range column type:** The function must verify that the column passed to `p_synchronize_range_column` is a valid range type before creating the synchronization trigger, preventing invalid SQL generation.
- [x] **Fix `temporal_merge` executor to ignore generated columns:** The executor must not include generated columns (`GENERATED ALWAYS`) in its `UPDATE` statements to prevent "cannot update a generated column" errors.
- [x] **Fix `temporal_merge` planner bug causing `NOT NULL` violations:** When splitting a time slice, `PATCH` modes failed with a `NOT NULL` violation if the source omitted a `NOT NULL` column. Fixed the planner's introspection to build the target's data payload using all of the target's columns, not just those common to the source, ensuring inherited values are correctly carried forward. Added regression tests for both `PATCH_FOR_PORTION_OF` and `MERGE_ENTITY_PATCH`.
- [x] **Fix C-level plan cache collision:** Refactored the C-based foreign key triggers to use the trigger's OID as the plan cache key instead of the foreign key's name. This prevents stale cache hits and potential crashes when a foreign key is dropped and a new one with the same name is created in the same session, making the caching mechanism robust.
- [x] **Fix C-language FK trigger logic and regressions:** Restored the correct, complex query-building logic to the `uk_update_check_c` trigger and fixed a bug in the `COALESCE` logic in both `uk_update_check_c` and `uk_delete_check_c`. This resolves multiple regressions and the original `temporal_merge` foreign key violation, stabilizing the C-based trigger functionality.
- [x] **Deeply investigate and document trigger MVCC semantics and multi-step ETL patterns:** Created a new diagnostic test (`58_trigger_visibility.sql`) to empirically prove PostgreSQL trigger visibility rules. This confirmed that multi-step ETL processes with conflicting DML order requirements (`INSERT` vs. shrinking `UPDATE`) must temporarily disable foreign key triggers. This understanding resolved a persistent failure in the `temporal_merge` architecture test (`56_...`) and led to comprehensive documentation of the correct ETL pattern in the `README.md`.
- [x] **Validate `temporal_merge` architectural assumptions with a low-level test:** Created a new, isolated test (`57_temporal_merge_validate_timely_strategy.sql`) that uses raw SQL to prove that an `INSERT`-then-`UPDATE` sequence is a valid strategy for SCD Type 2 changes. The test was extended to also verify this strategy is compatible with `sql_saga`'s own FK triggers and is correctly implemented by the `temporal_merge` procedure itself. This foundational architecture is now fully verified.
- [x] **Refine `temporal_merge` API for clarity and consistency:** Refactored the identity and feedback parameters to use a more intuitive and consistent naming convention (`p_identity_columns`, `p_source_row_id_column`, `p_identity_correlation_column`, etc.), improving the API's robustness and ease of use.
- [x] **Fix `temporal_merge` executor to respect `DEFAULT` values during multi-stage inserts:** Corrected the introspection logic for the "Smart Merge" `INSERT` to ensure columns with `DEFAULT` values are correctly excluded, allowing the database to generate their values as intended.
- [x] **Fix `temporal_merge` planner to support `BIGINT` source row identifiers:** Changed the internal `temporal_merge_plan` and `temporal_merge_feedback` types to use `BIGINT` for source row identifiers, making the procedure robust to different integer types for source primary keys.
- [x] **Fix `MERGE_ENTITY_PATCH` to carry forward values for new entities:** Replaced the flawed payload inheritance logic for PATCH modes with a robust recursive CTE that correctly computes a "running payload". This ensures that when creating new entities from sparse source data, attribute values are correctly carried forward from one time slice to the next, aligning the implementation with the documented intent.
- [x] **Design and implement `temporal_merge` deletion semantics:** Based on the architecture outlined in `docs/temporal_merge_delete_semantics.md`, added a new `p_delete_mode` parameter to `temporal_merge` to allow for opt-in destructive deletes. This enables "source as truth" synchronization for ETL processes while maintaining safe, non-destructive behavior by default.
- [x] **Improve `temporal_merge` parameter validation:** Added server-side checks to `temporal_merge` to provide clear, immediate error messages for invalid parameters, such as `NULL` or non-existent column names, improving developer experience.
- [x] **Implement `sql_saga.temporal_merge` (Set-Based Upsert API):** Provided a single, high-performance, set-based function for `INSERT`/`UPDATE`/`DELETE` operations on temporal tables. The API is simplified via `regclass` parameters, era introspection, and auto-detection of defaulted columns. This is the official solution for bulk data modifications.

- [x] **Fix duplicated column in FK trigger `UPDATE OF` list:** Corrected `add_foreign_key` to prevent adding a synchronized `valid_to`-style column to the trigger's `UPDATE OF` list if it is already part of the era's temporal columns, resolving a "column specified more than once" error. The fix is generic and respects the `synchronize_valid_to_column` era metadata.

- [x] **Improve `rename_following` to support column renames:** The event trigger now correctly detects when a column in a foreign key is renamed and automatically updates all relevant metadata, including the foreign key name, column list, and associated trigger names.

- [x] **Foreign key validation fails for tables in different schemas:** Fixed. The `fk_update_trigger` is now created with a dynamic column list that includes `valid_to` (if present) to ensure validation fires correctly for synchronized columns without being an overly-broad row-level trigger.

- [x] **Support identifiers with quotes inside:** Verified API functions handle quoted identifiers correctly.
- [x] **Cache query plans:** Cached query plans to improve trigger performance.
- [x] **Refactor core to use `(schema, table)` instead of `oid`:** Made event triggers robust against `DROP`.
- [x] **(Breaking Change) Adopted `[valid_from, valid_until)` period semantics:** Refactored the extension to use the standard `[)` inclusive-exclusive period convention, renaming all temporal columns accordingly.

- [x] **Implement System Versioning:** Ported the complete System Versioning feature from `periods`, including history tables, C-based triggers (`generated_always_as_row_start_end`, `write_history`), and the `add/drop_system_versioning` API.
- [x] **Feat(api): Add unified synchronization for temporal columns:** Enhanced `add_era` to support `p_synchronize_valid_to_column` and `p_synchronize_range_column`. This creates a single, unified trigger to keep all temporal representations (bounds, `valid_to`, range) consistent and enables declarative metadata for synchronization.

## Medium Priority - Refactoring & API Improvements

- [x] **Add Docker installation instructions to README:** Added a comprehensive guide to `README.md` for building and installing `sql_saga` using Docker, based on usage in the `statbus` project.
- [x] **Implement final `temporal_merge` API with unambiguous naming:** Refactor `temporal_merge` to use the final, approved `MERGE_ENTITY_*` naming scheme. Correct the planner logic to be strictly orthogonal, where `p_mode` controls non-destructive merge scope and `p_delete_mode` is the sole controller of destructive actions.
- [x] **Add focused test coverage for `..._FOR_PORTION_OF` modes:** Create a new test file dedicated to validating the behavior of `PATCH_FOR_PORTION_OF`, `REPLACE_FOR_PORTION_OF`, and `DELETE_FOR_PORTION_OF` to ensure the surgical correction logic is fully covered.
- [x] **Refactor default value handling to be metadata-driven**: Implemented a robust, metadata-driven strategy for default values. For simple eras, `add_era` sets `DEFAULT 'infinity'`. For eras with synchronized columns, a metadata flag instructs the trigger to programmatically apply the default on `INSERT`. This removes the brittle `'infinity'` heuristic and adds fail-fast consistency checks for `UPDATE` operations.

- [x] **Simplify `for_portion_of` view semantics**: Refactored the `for_portion_of` view to only provide the complex "apply change to a time slice" `UPDATE` functionality. Simple `INSERT`, `UPDATE`, and `DELETE` operations are now disallowed on the view and must be performed on the base table.

- [x] **Fix regressions from optional `add_era` parameters**: Resolved failures in `04_synchronize_validity_trigger` and other tests caused by `add_era`'s new default-setting behavior interacting incorrectly with generated columns and the synchronization trigger's consistency checks.

- [x] **Make `add_era` constraints and defaults optional**: Added `p_add_defaults` and `p_add_bounds_check` parameters to `add_era` to allow users to opt out of the default integrity management. This provides flexibility for advanced use cases while maintaining a safe default.

- [x] **Set `valid_until` to `DEFAULT 'infinity'` in `add_era`**: Modified `add_era` to be type-aware, automatically setting `DEFAULT 'infinity'` and stricter `CHECK` constraints on `valid_until` columns only for data types that support infinity. This improves ergonomics while maintaining correctness for all types.

- [x] **Document Updatable Views in README:** Updated `README.md` with a comprehensive section on the `for_portion_of` and `current` updatable views, explaining their purpose, DML protocols, and security model. The runnable example test (`47_readme_usage.sql`) has been extended to cover the usage of these views.

- [x] **Support Predicates for Temporally Unique Keys:** Extended `add_unique_key` to support a `WHERE` clause for creating partial unique keys (e.g., `WHERE legal_unit_id IS NOT NULL`). This uses a unique index with a predicate instead of a unique constraint.

- [x] **Refactor `temporal_merge` to a procedure for better ergonomics:** Converted the `temporal_merge` function into a procedure. Instead of returning a set of results, it now creates a temporary table `__temp_last_sql_saga_temporal_merge` with the feedback, simplifying the calling pattern.
- [x] **Rename `add_api` to `add_updatable_views` for clarity:** Renamed `add_api` and `drop_api` to `add_updatable_views` and `drop_updatable_views` respectively, to better reflect their purpose of managing views for temporal data.

- [x] **Refactor and Finalize `for_portion_of` Updatable View:** Ported and expanded test coverage from legacy files, and formalized DML semantics. The view's behavior, including its known limitation of not coalescing adjacent identical rows from multi-row updates, is now fully verified by the test suite.

- [x] **Implement `current` Updatable View:** Created the new `current` view with a robust and explicit DML protocol for easy integration with ORMs and APIs.
    - **Phase 1: Data Model (Shared Prerequisite - Complete):**
        - [x] `updatable_view` metadata table and `updatable_view_type` enum are in place.
    - **Phase 2: Define the Goal (Tests & Docs) - Complete:**
        - [x] `README.md` is updated with the symmetrical `add/drop_current_view` API reference.
        - [x] Created the primary test file (`sql/30_updatable_view_current_basis.sql`) to define the target behavior.
    - **Phase 3: Implementation - Complete:**
        - [x] Created the `add_current_view` and `drop_current_view` functions.
        - [x] Implemented the trigger logic with an explicit `UPDATE` protocol for SCD Type 2 and soft-deletes, while disallowing ambiguous `DELETE`s.
    - **Phase 4: Verification (Basis) - Complete:**
        - [x] The `30_updatable_view_current_basis.sql` test is now passing.

- [x] **Synchronized: Full Test Coverage & Cleanup:**
    - **Prerequisite:** Phase 4 must be complete for *both* `for_portion_of` and `current` views.
    - **Phase 5: Full Test Coverage:**
        - [x] `sql/31_updatable_view_for_portion_of_full.sql` is passing, covering edge cases and ACLs for `for_portion_of` views.
        - [x] `sql/32_updatable_view_current_full.sql` is passing, now with full coverage for both empty- and non-empty-range soft-deletes.
        - [x] The two `current` view tests (`30_..._basis` and `32_..._full`) cover both `delete_as_cutoff` and `delete_as_documented_ending` modes.
        - [x] `sql/34_updatable_views_types.sql` is passing, testing both views against all supported range types.
    - **Phase 6: Deprecation & Finalization:**
        - [x] Deprecate `07_for_portion_of.sql` and `21_api_lifecycle.sql` now that their functionality is fully covered by the new, structured test suite.

- [x] **Refactor `temporal_merge` founding ID logic:** Made the source `row_id` column configurable (`p_source_row_id_column`) and implemented the `p_identity_correlation_column` to handle intra-batch dependencies for new entities.

- [x] **Create a test suite for `temporal_merge` parameter edge cases:** The test `52_temporal_merge_parameters.sql` now provides comprehensive coverage for API parameter variations, including `NULL` and empty `p_identity_columns`, non-existent column names, custom source row identifiers, and fully non-standard naming conventions.

- [x] **Add runnable test for README usage examples:** Created a self-contained test that executes the code from the `README.md` "Usage" section. This test serves as living documentation, verifying the public API and demonstrating a realistic `temporal_merge` data loading pattern with ID back-filling.

- [x] **Enhance ETL architecture test to cover "current state" pattern:** Refactored the `56_temporal_merge-minimal-copying-architecture.sql` test to demonstrate a common ETL pattern where incoming data represents the current state of an entity, valid until `infinity`. The test now uses a batch-driven loop, disables foreign key triggers during processing, and inspects feedback after each batch to fully verify that the minimal-copying architecture correctly manages complex, stateful timelines.
- [x] **Finalize advanced ETL reference implementation:** The `56_...` test is now a complete reference for a robust, minimal-copying ETL architecture. It demonstrates ID back-propagation (both intra-batch and inter-batch), dynamic metadata-driven procedures that correctly filter source data, handling of natural keys, non-temporal targets, and ephemeral metadata columns.

- [x] **Optimize `temporal_merge` with prepared statements:** Refactored the expensive planner query (`temporal_merge_plan`) to use hash-based prepared statement caching. This improves performance for repeated calls with the same parameters by avoiding redundant query planning and introspection, while keeping the main executor procedure simple and readable.
- [x] **Add happy-path test coverage for all `temporal_merge` modes and supported range types:** Created two new test files (`49_temporal_merge_modes.sql` and `50_temporal_merge_types.sql`) to verify the behavior of all merge modes and data types, improving overall test coverage.

- [x] **Clarify `temporal_merge` naming convention:** Finalized a robust and consistent naming convention for the Planner/Executor architecture. All internal `temporal_merge` objects now use a hierarchical naming scheme (e.g., `temporal_merge_plan`, `temporal_merge_plan_action`). Renamed `SKIP_IDEMPOTENT` to `SKIP_IDENTICAL` for improved clarity.

- [x] **Temporal Merge with Dependent Row Support:** Refactored `temporal_merge` to correctly handle batches containing dependent operations (e.g., an `INSERT` of a new entity and subsequent `UPDATE`s to it). This was achieved by changing the API to accept a `p_identity_correlation_column` and implementing internal, multi-stage ID propagation logic, making the function truly set-based and robust.

- [x] **Remove obsolete legacy files:** Deleted unused source files (`periods.c`, `time_for_keys.c`, etc.) from the repository to reduce clutter and prevent confusion.

- [x] **Make `temporal_merge` fully dynamic:** Analyzed `temporal_merge` for hardcoded column names. Verified through searches that all user-data columns are handled dynamically. The single identified "hardcoded" string is a private, internal implementation detail for state management, which is a correct design. No changes were needed.

- [x] **Add option to back-fill generated IDs into source table:** Extended `temporal_merge` with a new parameter `p_update_source_with_identity`. When `true`, the procedure will update the source table with any generated surrogate or composite key values for newly inserted entities. This simplifies multi-step import processes by removing the need for manual ID propagation between steps.

- [x] **Ensure Symmetrical APIs:** Refactored `drop_unique_key` and `drop_foreign_key` to be unambiguous by renaming the `_by_name` variants. Aligned tests to use the more intuitive symmetrical API calls by default.
- [x] **Standardize System Versioning Column Naming:** Renamed system versioning columns to `system_valid_from` and `system_valid_until` to be consistent with application-time `valid_from`/`valid_until` semantics.

- [x] **Refactor test suite:** Made all tests self-contained and idempotent, resolving all regressions.

- [x] **Complete the `regclass` -> `(schema, table)` refactoring:** Removed all `oid` columns from metadata tables, making event triggers robust against `DROP`.

- [x] **Refactor `add_updatable_views` to not require a primary key:** Refactored `add_updatable_views` to use a single-column temporal unique key as the entity identifier if no primary key is present. Fixed `update_portion_of` trigger to correctly preserve identifier columns during updates.

- [x] **Fix event trigger regressions:** Resolved bugs in `rename_following` and `health_checks` event triggers that were exposed by refactoring. The triggers now correctly handle `search_path` issues and reliably update metadata for renamed objects.

- [x] **Refactor build system and fix all regressions:** Overhauled the `Makefile` to ensure reliable, incremental builds. Refactored the SQL source into a modular structure and resolved all test failures that were exposed by the new build process.

- [x] **Support foreign keys from non-temporal tables:** Provided full foreign key support from standard tables to temporal tables. The `add_foreign_key` function now automatically creates a `CHECK` constraint on the referencing table and `UPDATE`/`DELETE` triggers on the referenced temporal table to provide full `RESTRICT`/`NO ACTION` semantics.


## Low Priority - Future Work & New Features

- [x] **Use `pg_temp` schema for temporary tables**: Refactored `temporal_merge` to create its feedback table in the session-local `pg_temp` schema for improved robustness and concurrency safety.
- [x] **Enforce `NOT NULL` on synchronized temporal columns**: Modified `add_era` to apply `NOT NULL` constraints on `valid_to` and `valid_range` columns when synchronization is enabled to strengthen data integrity guarantees.
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
