# SQL Saga - TODO

A living document of upcoming tasks.
Tasks are checked ✅ when done and made brief.
Keep a journal.md that tracks the state of the current ongoing task and relevant details.

## High Priority - Bugs & Core Features

- [x] **Foreign key validation fails for tables in different schemas:** Fixed. The `fk_update_trigger` is now created with a dynamic column list that includes `valid_to` (if present) to ensure validation fires correctly for synchronized columns without being an overly-broad row-level trigger.

- [x] **Support identifiers with quotes inside:** Verified API functions handle quoted identifiers correctly.
- [x] **Cache query plans:** Cached query plans to improve trigger performance.
- [x] **Refactor core to use `(schema, table)` instead of `oid`:** Made event triggers robust against `DROP`.
- [x] **(Breaking Change) Adopted `[valid_from, valid_until)` period semantics:** Refactored the extension to use the standard `[)` inclusive-exclusive period convention, renaming all temporal columns accordingly.

- [x] **Implement System Versioning:** Ported the complete System Versioning feature from `periods`, including history tables, C-based triggers (`generated_always_as_row_start_end`, `write_history`), and the `add/drop_system_versioning` API.

## Medium Priority - Refactoring & API Improvements

- [x] **Ensure Symmetrical APIs:** Refactored `drop_unique_key` and `drop_foreign_key` to be unambiguous by renaming the `_by_name` variants. Aligned tests to use the more intuitive symmetrical API calls by default.

- [x] **Refactor test suite:** Made all tests self-contained and idempotent, resolving all regressions.

- [x] **Complete the `regclass` -> `(schema, table)` refactoring:** Removed all `oid` columns from metadata tables, making event triggers robust against `DROP`.

- [x] **Provide convenience trigger `synchronize_valid_to_until`:** Added trigger to help manage human-readable inclusive end-dates.

- [ ] **Implement High-Performance, Set-Based Upsert API (The "Plan and Execute" Pattern):**
  - **Goal:** Provide official, high-performance, set-based functions for performing `INSERT OR UPDATE` and `INSERT OR REPLACE` operations on temporal tables. This should be the primary API for complex data loading.
  - **Problem:** Multi-statement transactions that perform complex temporal changes cannot be reliably validated by `sql_saga`'s `CONSTRAINT TRIGGER`s due to PostgreSQL's MVCC snapshot rules.
  - **Validated Solution (The "Statbus Pattern"):** An external project (`statbus_speed`) has validated the definitive "Plan and Execute" solution. This pattern must be implemented inside a single procedural function (ideally in C for performance) that:
    1.  **Plans (Read-Only):** Reads all source and target data and calculates a complete DML plan (`DELETE`s, `UPDATE`s, `INSERT`s) that is guaranteed to result in a consistent final timeline.
    2.  **Executes (Write-Only):** Executes the pre-calculated plan using a specific **"add-then-modify" order**. It must perform `INSERT`s of new data before `UPDATE`ing or `DELETE`ing the old data that is being replaced. This ensures `sql_saga`'s triggers can validate the final state correctly.
  - **Benefit:** A call to this function is a single top-level statement. `sql_saga`'s deferred triggers fire only at the end, validating a state that the planner has already guaranteed is correct. This is the architecturally sound solution to the multi-statement update problem and provides a path to re-enabling the tests in `28_with_exclusion_constraints.sql` by having them use this new API.

- [ ] **Investigate Statement-Level Triggers:**
  - **Goal:** Replace the `uk_update_check_c` and `uk_delete_check_c` row-level triggers with statement-level triggers.
  - **Benefit:** This would provide correct validation for complex, single-statement DML operations that modify multiple rows (e.g., `UPDATE ... FROM ...`, `MERGE`). This is the architecturally correct way to handle multi-row validation.
  - **Limitation:** This would not solve the problem of multi-statement transactions that are only valid at commit time (e.g., two separate `UPDATE` statements swapping periods), as statement-level triggers are not deferrable to the end of the transaction. This limitation should be documented.

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

- [x] **Enhance Event Trigger Logic:** `periods` has robust `drop_protection` and `rename_following` event triggers.
  - **Action:** Compared logic in `periods` with `sql_saga`'s event triggers. `sql_saga`'s logic is a correct subset of `periods`, and includes a performance optimization for `rename_following` that `periods` lacks. No further changes needed.

### From `time_for_keys` extension:

- [x] **Analyze Alternative Foreign Key Implementation:** The `time_for_keys` project represents a less dynamic, but potentially faster, approach to temporal foreign keys. Instead of a central metadata catalog, it creates specific triggers for each foreign key constraint. The legacy code for this was removed from `sql_saga` to avoid confusion.
  - **Action:** Analysis complete. The `time_for_keys` approach uses `pl/pgsql` triggers that re-plan validation queries on every row, making it significantly less performant than `sql_saga`'s current C-based triggers with cached query plans. The recent refactoring in `sql_saga` to pass all metadata as arguments has already adopted the best part of this design while implementing it in a much more performant way. No changes are warranted.

- [x] **Evaluate Alternative Gap-Coverage Functions:** `periods` uses a `pl/pgsql` implementation for temporal validation, and `time_for_keys` has a `no_gaps` function specialized for `daterange`. `sql_saga`'s C-based `covers_without_gaps` aggregate for `anyrange` is a deliberate improvement.
  - **Action:** Analysis complete. The `covers_without_gaps` function in `sql_saga` is more performant than the `periods` implementation and more generic and correct than the `time_for_keys` version. It successfully passes a more comprehensive test suite ported from `time_for_keys`, fixing regressions that were present in the `periods` logic. No changes are needed.
