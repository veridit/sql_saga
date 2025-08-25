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

## Medium Priority - Refactoring & API Improvements

- [x] **Refactor test suite:** Made all tests self-contained and idempotent, resolving all regressions.

- [ ] **Complete the `regclass` -> `(schema, table)` refactoring:**
  - **Issue:** The metadata tables `sql_saga.era` and `sql_saga.api_view` still contain `oid` columns (`audit_table_oid` and `view_oid` respectively). This is a remnant of the old design and should be removed to complete the refactoring.
  - **Action:** Replace these columns with schema and table names.
  - **Discussion Point:** The concept of an `api_view` is tied to PostgREST integration. Should we rename this metadata table and its functions (e.g., `add_api`, `drop_api`) to something more generic like `add_updatable_view` to better reflect its purpose, which is to provide a simplified view for `INSERT`, `UPDATE`, `DELETE` operations on a temporal table's history?

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

- [ ] **Implement System Versioning (with C triggers):** `periods` contains a complete implementation of `SYSTEM VERSIONING`, including history tables, C-based triggers (`generated_always_as_row_start_end`, `write_history`) for populating them, and helper functions (`_as_of`, `_between`, etc.). This is a major feature that `sql_saga` currently has commented-out stubs for.
  - **Comparison to Audit Frameworks (`pgaudit`):** System Versioning (as implemented in `periods`) and audit logging (`pgaudit`) are complementary, can be used together without conflict, and solve different problems.
    - **System Versioning** tracks the *state of the data* over time. It creates a complete, queryable history of every row, answering the question: "What did this record look like last year?" Its purpose is historical data analysis and reconstruction.
    - **Audit Frameworks (`pgaudit`)** track the *actions performed on the data*. They log the `INSERT`, `UPDATE`, `DELETE` statements themselves, answering the question: "Who deleted a record from this table yesterday?" Their purpose is security, compliance, and forensics.
    - **Combined Use Case:** For NSOs, using both provides a complete picture: `pgaudit` supplies the mandatory, unalterable log of *who made a change*, while System Versioning provides the queryable history of *what the data looked like* as a result of that change.
  - **Action:** Port the entire system versioning feature from `periods`. The existing (and currently failing) test `61_system_versioning_excluded_columns.sql` should be used to verify this functionality once implemented. This includes:
    1.  The `system_versioning` and `system_time_periods` metadata tables.
    2.  The `add_system_versioning` and `drop_system_versioning` API functions.
    3.  The C trigger functions from `periods.c` for high-performance history tracking.
  - **Design Constraints to Preserve:**
    - **History Tables are Not Temporal:** The `periods` implementation correctly prevents a history table from being a temporal table itself. This is a critical design choice that separates concerns: the main table handles "application time," while the history table tracks "system time" (the audit trail). This prevents circular dependencies and preserves the integrity of the history log.

- [x] **Enhance Event Trigger Logic:** `periods` has robust `drop_protection` and `rename_following` event triggers.
  - **Action:** Compared logic in `periods` with `sql_saga`'s event triggers. `sql_saga`'s logic is a correct subset of `periods`, and includes a performance optimization for `rename_following` that `periods` lacks. No further changes needed.

### From `time_for_keys` extension:

- [x] **Analyze Alternative Foreign Key Implementation:** The `time_for_keys` project represents a less dynamic, but potentially faster, approach to temporal foreign keys. Instead of a central metadata catalog, it creates specific triggers for each foreign key constraint. The legacy code for this was removed from `sql_saga` to avoid confusion.
  - **Action:** Analysis complete. The `time_for_keys` approach uses `pl/pgsql` triggers that re-plan validation queries on every row, making it significantly less performant than `sql_saga`'s current C-based triggers with cached query plans. The recent refactoring in `sql_saga` to pass all metadata as arguments has already adopted the best part of this design while implementing it in a much more performant way. No changes are warranted.

- [x] **Evaluate Alternative Gap-Coverage Functions:** `periods` uses a `pl/pgsql` implementation for temporal validation, and `time_for_keys` has a `no_gaps` function specialized for `daterange`. `sql_saga`'s C-based `covers_without_gaps` aggregate for `anyrange` is a deliberate improvement.
  - **Action:** Analysis complete. The `covers_without_gaps` function in `sql_saga` is more performant than the `periods` implementation and more generic and correct than the `time_for_keys` version. It successfully passes a more comprehensive test suite ported from `time_for_keys`, fixing regressions that were present in the `periods` logic. No changes are needed.
