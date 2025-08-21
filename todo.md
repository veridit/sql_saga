# SQL Saga - TODO

A living document of upcoming tasks.
Tasks are checked ✅ when done and made brief.
Keep a todo-journal.md that tracks the state of the current ongoing task and relevant details.

## High Priority - Bugs & Core Features

- [x] Cached query plan for era range_type lookups to improve trigger speed.

- [ ] **(Breaking Change)** Adopt `[)` period semantics
  **Goal:** Align with PostgreSQL's native `tsrange` and `daterange` types, making the extension more intuitive and compatible with built-in operators like `OVERLAPS`.
  **Problem:** The current `(valid_after, valid_to]` semantic (`(]`) is non-standard, requires explicit casting (`daterange(valid_after, valid_to, '(]')`), and prevents natural use of range operators.
  **Action:** This is a complete, breaking change.
    1.  Replace `valid_after` with `valid_from` (inclusive start) and `valid_to` with `valid_until` (exclusive end) throughout the entire extension.
    2.  All internal logic, metadata tables (`sql_saga.era`), C code, and tests must be updated to use the new column names and `[)` period semantics.
    3.  The `synchronize_valid_from_after` trigger will be removed. A new trigger or recommendation might be needed to handle the old `valid_to` (inclusive) from `valid_until` (exclusive) if users need it for display purposes (e.g., `valid_to = valid_until - '1 day'`).

- [ ] Change the core to use `table_schema and table_name` instead of the `table_oid` and change all relevant
  tables and variables and code. An oid is looked up in the system tables, and for DDL triggers
  it is not possible to look up since the trigger runs after the fact and can cause a rollback,
  by using text variables, they can be checked and used without causing system table lookups.

## Medium Priority - Refactoring & API Improvements

- [x] **Cache Dynamic Validation Queries:**
  - **Goal:** Improve trigger performance for large DML operations by caching the main validation query plans.
  - **Problem:** The core validation queries in `fk_insert_check_c`, `fk_update_check_c`, `uk_delete_check_c`, and `uk_update_check_c` are dynamically constructed with `psprintf` and executed with `SPI_execute` on every trigger invocation. This incurs a significant overhead for query planning, especially in statements affecting many rows.
  - **Action:** Refactor these functions to:
    1.  Build a parameterized query string (using `$1`, `$2`, etc.) instead of injecting quoted literals.
    2.  Use `SPI_prepare` to create a prepared statement.
    3.  Implement a cache (e.g., a hash table keyed by foreign key name) to store and reuse these prepared statements across function calls within a transaction, similar to the plan caching in `periods.c`.

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
  - **Action:** Port the entire system versioning feature from `periods`. This includes:
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
