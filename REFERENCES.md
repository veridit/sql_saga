# C-Language Extension Development in PostgreSQL

This document serves as a quick reference for developers working on the `sql_saga` project, summarizing key concepts from the official PostgreSQL documentation for writing C-language extensions.

## C-Language Functions

Reference: [PostgreSQL Documentation: C-Language Functions](https://www.postgresql.org/docs/current/xfunc-c.html)

### Key Concepts

*   **Dynamic Loading**: C functions are compiled into shared libraries (`.so` files) and loaded by the server on demand. Each shared library must contain a `PG_MODULE_MAGIC;` macro in one of its source files.
*   **Version 1 Calling Convention**: This is the standard for C functions.
    *   Functions must be declared as `Datum funcname(PG_FUNCTION_ARGS)`.
    *   A `PG_FUNCTION_INFO_V1(funcname);` macro call is required for each function.
    *   Arguments are accessed via `PG_GETARG_...()` macros.
    *   Results are returned via `PG_RETURN_...()` macros.
*   **Memory Management**:
    *   Use `palloc()` and `pfree()` instead of `malloc()` and `free()`.
    *   Memory allocated with `palloc()` is managed within PostgreSQL's memory contexts and is automatically freed at the end of a transaction, preventing leaks.
    *   For data that must persist across multiple function calls (e.g., in aggregates or set-returning functions), use the appropriate long-lived memory context (e.g., `multi_call_memory_ctx`).
*   **Data Types**:
    *   Data can be passed by value or by reference.
    *   Variable-length types (like `text` or ranges) have a 4-byte header containing the total length. Use `VARHDRSZ` and `SET_VARSIZE` macros to work with them.
*   **NULL Handling**:
    *   Functions can be declared `STRICT` in SQL, which means they are not called if any input is NULL. PostgreSQL handles this automatically.
    *   For non-`STRICT` functions, use `PG_ARGISNULL(arg_index)` to check for NULLs and `PG_RETURN_NULL()` to return a NULL value.

## User-Defined Aggregates

Reference: [PostgreSQL Documentation: User-Defined Aggregates](https://www.postgresql.org/docs/current/xaggr.html)

### Key Concepts

*   **State Machine**: Aggregates operate as a state machine.
    *   `stype`: The data type of the internal state. For complex states without a SQL equivalent, `internal` is used.
    *   `sfunc` (State Transition Function): Called for each input row. It takes the current state and the input value(s) and returns the new state.
    *   `finalfunc` (Final Function): Called after all rows are processed. It takes the final state and returns the aggregate result. This is optional.
*   **State Management in C**:
    *   The `AggCheckCallContext` function can be used to determine if a C function is being called as part of an aggregate.
    *   This allows for optimizations, such as modifying the state value in-place, which is safe for transition values.
    *   The second argument to `AggCheckCallContext` can retrieve the memory context for aggregate state values, which is crucial for data that must persist between calls. `covers_without_gaps.c` uses this for its state management.
*   **`finalfunc_extra`**: This option in `CREATE AGGREGATE` passes extra arguments to the final function, corresponding to the aggregate's input arguments. This is useful when the final function needs access to arguments that are constant across the aggregation group. `covers_without_gaps` uses this to pass the `target` range to its final function.

### Ordering and `covers_without_gaps`

*   **Normal Aggregates and Ordering**: For most aggregates, if an `ORDER BY` clause is provided within the aggregate call (e.g., `SUM(x ORDER BY y)`), PostgreSQL sorts the input rows before feeding them to the transition function. This process is handled by the PostgreSQL executor and is transparent to the aggregate's C functions.
*   **`covers_without_gaps` Anomaly**: The `sql_saga.covers_without_gaps` aggregate is an exception. It is a *normal* aggregate, but its internal logic *requires* that the input ranges are passed to it in sorted order based on their start bounds.
    *   **Developer Responsibility**: The C code in `covers_without_gaps_transfn` explicitly checks if the current range's start is before the previous one and raises an error if the input is unsorted.
    *   **User Responsibility**: Consequently, any SQL query using `covers_without_gaps` *must* include an `ORDER BY` clause on the range column to ensure correctly sorted input. For example:
        ```sql
        SELECT sql_saga.covers_without_gaps(my_range, target_range ORDER BY my_range)
        FROM my_table;
        ```
    *   This is a critical implementation detail for both users and developers of `sql_saga`. The aggregate does not perform the sorting; it validates that the sorting has been done.

## Constraint Definitions and Renaming

PostgreSQL provides several ways to inspect constraint definitions, and choosing the correct one is critical for robust event triggers that handle DDL changes.

*   **`pg_constraint.consrc` (Deprecated and Removed):** This column stored a human-readable text representation of a `CHECK` constraint's expression. It was removed in PostgreSQL 12 because it was not reliably updated when referenced objects were renamed, making it unsafe for use in event triggers.

*   **`pg_get_constraintdef(oid)` (For Display):** This function returns a formatted, human-readable string representing a constraint's definition (e.g., `CHECK ((col1 < col2))`). While useful for display, it is not ideal for programmatic logic because the exact formatting can change between PostgreSQL versions, and it may not accurately reflect the state of renamed objects within the same transaction that the trigger is handling. `sql_saga`'s `rename_following` trigger initially used this, leading to brittle string comparisons.

*   **`pg_constraint.conbin` and `pg_get_expr()` (Robust for `CHECK` constraints):** The `conbin` column stores the internal, parsed expression tree for a `CHECK` constraint. The `pg_get_expr(conbin, conrelid)` function decompiles this tree back into a canonical string representation (e.g., `(col1 < col2)`). This is the most reliable way to inspect a `CHECK` constraint's logic, as it is independent of display formatting. `sql_saga` now uses this method.

*   **`pg_constraint.conkey` (Robust for `UNIQUE`, `PRIMARY KEY`, `FOREIGN KEY`):** This column stores an array of the attribute numbers (`attnum`) that make up the constraint. This is the most robust way to identify these types of constraints, as it is completely independent of column names. `sql_saga` uses this method to track renamed `UNIQUE` constraints.

*   **`EXCLUDE` Constraints:** These are more complex. While they have `conkey`, they also have `conexclop` (an array of operator OIDs). Reconstructing the definition programmatically is difficult. For these, `sql_saga` still relies on a pattern match (`LIKE`) against `pg_get_constraintdef()`, which is a pragmatic compromise.

## Trigger Behavior and Data Visibility

A deep understanding of PostgreSQL's data visibility rules for triggers is essential for developing `sql_saga`. The documentation reveals that there are two distinct models of behavior, and the choice between them has profound consequences for our implementation.

### The Two Models of Data Visibility

The core distinction lies between regular `AFTER` triggers and `CONSTRAINT TRIGGER`s.

#### Model 1: Regular `AFTER` Triggers (Sees Final State)

Reference: [Visibility of Data Changes](https://www.postgresql.org/docs/current/trigger-datachanges.html)

A regular row-level `AFTER` trigger sees the final state of the data after the DML operation is complete. The documentation states: *"When a row-level `AFTER` trigger is fired, all data changes made by the outer command are already complete, and are visible to the invoked trigger function."*

The example in the documentation ([A Complete Trigger Example](https://www.postgresql.org/docs/current/trigger-example.html)) confirms this. When a regular `AFTER DELETE` trigger runs `SELECT count(*)`, it sees that the row count has been decremented.

#### Model 2: `CONSTRAINT TRIGGER`s (Sees Initial State)

Reference: [Overview of Trigger Behavior](https://www.postgresql.org/docs/current/trigger-definition.html)

`CONSTRAINT TRIGGER`s, which `sql_saga` uses, are subject to a much stricter visibility rule. The documentation is explicit: *"A query started by a constraint trigger will see the state of the database as of the start of the statement that caused the trigger to fire, regardless of the SET CONSTRAINTS mode."*

This means that even if a constraint trigger is `DEFERRABLE` and its *execution* is postponed until the end of the transaction, any query it runs operates on a data snapshot from the *past*. It is blind to the changes made by the statement that queued it, and any subsequent statements in the same transaction.

### Implications for `sql_saga`

`sql_saga`'s entire design is dictated by the trade-offs between these two models.

1.  **Why `sql_saga` MUST Use `CONSTRAINT TRIGGER`s**: The primary reason is to get the `DEFERRABLE` property. This allows `sql_saga` to validate temporal foreign keys at the end of a transaction, permitting multi-statement updates (like an `UPDATE` followed by an `INSERT` that "fills the gap") to succeed if the final state is consistent. Regular `AFTER` triggers are not deferrable and would fail immediately.

2.  **Consequence 1: The Multi-Statement Update Limitation**: By choosing `CONSTRAINT TRIGGER`s, `sql_saga` is locked into the stricter "sees initial state" visibility model. This fully explains why the multi-statement `UPDATE`s in `28_with_exclusion_constraints.sql` fail. The trigger for the first `UPDATE` is queued, but its view of the database is frozen. It cannot see the changes from the second `UPDATE`. At the end of the transaction, it executes its query against its old snapshot, sees a temporary gap, and correctly (from its perspective) reports a violation. This is a fundamental and documented limitation of PostgreSQL's trigger architecture.

3.  **Consequence 2: The `uk_delete_check_c` Logic**: This clarifies why the `uk_delete_check_c` function must explicitly exclude the `OLD` row from its validation query.
    *   A regular `AFTER DELETE` trigger (Model 1) would see the final state where the row is already gone, and would not need special logic.
    *   However, `uk_delete_check_c` is a `CONSTRAINT TRIGGER` (Model 2). Its query sees the *initial* state of the table, which includes the row that is about to be deleted. Without excluding the `OLD` row, the validation query would incorrectly find that the row still provides coverage, and the check would always pass. The current implementation is therefore correct and necessary.

## Advanced Trigger Concepts and Design Learnings

Analysis of PostgreSQL's trigger system reveals important details that directly influence `sql_saga`'s design and limitations.

### Constraint Triggers

Reference: [PostgreSQL Documentation: Overview of Trigger Behavior](https://www.postgresql.org/docs/current/trigger-definition.html)

*   **Definition**: A `CONSTRAINT TRIGGER` is a special type of `AFTER ROW` trigger whose execution timing can be controlled with `SET CONSTRAINTS`. They can be `DEFERRABLE`, allowing their execution to be postponed until the end of the transaction.
*   **Usage in `sql_saga`**: All of `sql_saga`'s temporal foreign key triggers are implemented as constraint triggers. This is what allows multi-statement updates to proceed without failing immediately, by deferring the check.
*   **The MVCC Snapshot Rule for Constraint Triggers**: There is a subtle but critical distinction between *when a trigger executes* and *what data it sees*.
    *   **Execution Time**: As described in the `CREATE TRIGGER` documentation, a `DEFERRABLE` constraint trigger's execution can be postponed until the end of the transaction. This is why multi-statement updates don't fail immediately.
    *   **Data Visibility**: However, the "Overview of Trigger Behavior" chapter provides the crucial context on data visibility: *"A query started by a constraint trigger will see the state of the database as of the start of the statement that caused the trigger to fire, regardless of the SET CONSTRAINTS mode."*
    *   **Reconciling the Two**: This means that although the trigger function *runs* at commit time, any query it executes operates on a data snapshot from the past—specifically from the beginning of the statement that originally queued it. It is blind to its own statement's changes and any subsequent statements in the transaction. This is the root cause of the validation failures for multi-statement updates seen in `28_with_exclusion_constraints_test.sql`.

### Statement-Level Triggers and `REFERENCING`

*   **Definition**: A statement-level trigger fires only once per statement, regardless of the number of rows affected. The optional `REFERENCING` clause allows the trigger to access all modified rows in "transition tables" (e.g., `OLD TABLE` and `NEW TABLE`).
*   **Use Case**: This mechanism is the correct way to validate complex changes that occur within a **single statement**. For example, an `UPDATE ... FROM ...` that modifies multiple rows could be validated correctly by a statement-level trigger that examines the final state of all affected rows in the `NEW TABLE` transition table.
*   **Limitation for `sql_saga`**: While powerful, this does not solve the multi-statement transaction problem. A statement-level trigger still fires at the end of each *statement*, not at the end of the *transaction*, so it remains blind to changes from subsequent statements within the same transaction block.

### Built-in vs. User-Defined Constraint Triggers

The core of the multi-statement update problem lies in the difference between PostgreSQL's built-in `DEFERRABLE` foreign keys and the user-defined `CONSTRAINT TRIGGER`s that `sql_saga` must use.

*   **Built-in Foreign Keys**: These are deeply integrated into the PostgreSQL executor. When deferred, their final validation can re-check the constraint against the actual, final state of the data at the end of the transaction. They have access to internal state management that is not exposed to user-defined functions. This is why they work as users intuitively expect.

*   **User-Defined `CONSTRAINT TRIGGER`s**: These are a more limited mechanism. As documented, they are bound by a strict MVCC snapshot rule: their queries always see the state of the database as of the *start of the statement* that queued them, even when execution is deferred to the end of the transaction. They cannot see the final, transaction-complete state.

*   **The Previous `pl/pgsql` Illusion**: The old `pl/pgsql` triggers in `sql_saga` did **not** have the same semantics as built-in FKs. They only *appeared* to work for the multi-statement `UPDATE` case because of a bug. For updates that only changed the time period, the validation query failed to exclude the `OLD` row from its coverage check. This effectively disabled the validation for that specific scenario, masking the underlying limitation. The correct C implementation fixed this bug, which brought the architectural limitation to light.

### Conclusion: The Multi-Statement Update Problem and The Principled Solution

From first principles, we have learned that there is a fundamental limitation in PostgreSQL's trigger architecture for validating multi-statement transactions that are only consistent at the very end. Standard `DEFERRABLE` foreign keys can handle this because they use internal mechanisms not available to user-defined triggers.

#### The Principled Solution for Single-Statement Updates

The correct way to validate a complex change affecting multiple rows within a **single DML statement** is with a statement-level trigger.

*   An `AFTER UPDATE ... FOR EACH STATEMENT` trigger with a `REFERENCING OLD TABLE AS old_rows, NEW TABLE AS new_rows` clause can see the complete "before" and "after" state of all rows modified by the statement.
*   This trigger could then identify the full set of unique keys that were affected and re-validate all foreign keys that reference them. This would correctly handle complex operations like `UPDATE ... FROM ...` or `MERGE`.

#### Why This Doesn't Solve the Multi-Statement Transaction Problem

The critical limitation is that statement-level triggers are **not deferrable** to the end of a transaction. They execute at the end of each statement. In a transaction consisting of an `UPDATE` followed by an `INSERT`, the statement-level trigger for the `UPDATE` would fire before the `INSERT` occurs, see a temporary gap in coverage, and report a violation.

#### The `sql_saga` Compromise

*   `sql_saga` uses `CONSTRAINT TRIGGER`s because they are the only user-definable trigger type that is `DEFERRABLE` to the end of the transaction.
*   However, these triggers are restricted to `FOR EACH ROW` and, crucially, operate on a data snapshot from the start of the statement, not the transaction.
*   This is a pragmatic compromise. It correctly handles the common cases of single-row changes and simple multi-statement transactions (e.g., `UPDATE` then `INSERT` to fill a gap), but it cannot support complex multi-statement `UPDATE`s as demonstrated in `28_with_exclusion_constraints.sql`.

Therefore, the decision to comment out these tests is correct. The architectural pattern they represent is fundamentally incompatible with the validation mechanisms available to user-defined triggers in PostgreSQL.

#### The Encapsulated, Set-Based API Solution

The correct and most robust way to perform complex temporal modifications is to encapsulate the logic within a single, high-level procedural function. This approach, validated by the external `statbus_speed` project, is the strategic path forward for `sql_saga`.

**The "Plan and Execute" Pattern with "Add-Then-Modify" Ordering:**

The definitive solution for complex temporal modifications is the "Plan and Execute" pattern, implemented within a single C or pl/pgsql function.

1.  **The Plan Phase (Read-Only):** The function first reads all source and target data and calculates a complete DML plan (`DELETE`s, `UPDATE`s, `INSERT`s) that is guaranteed to result in a consistent final state. This phase is pure and does not write to the database or queue any triggers.

2.  **The Execute Phase (Write-Only):** The function then executes this plan using a critical **"add-then-modify"** order. It first performs all `INSERT` operations to add new timeline segments. This creates a temporary state of overlapping periods. It then performs all `UPDATE`s and `DELETE`s to truncate or remove the old segments.

**Why This Is the Correct Solution:**

*   **Deferred Exclusion Constraints:** The temporary overlaps created by the "add-first" approach would normally fail. However, they are correctly handled by setting the table's exclusion constraints to `DEFERRED`.
*   **Working with the Snapshot Rule:** The `sql_saga` `CONSTRAINT TRIGGER`s queued by the later `UPDATE` or `DELETE` statements will see a snapshot that contains **both** the new rows (from the earlier `INSERT`s) and the old rows. The trigger's validation logic (`all_rows_in_snapshot - OLD_row + NEW_row`) will therefore be checking a set of rows that correctly represents the final, consistent state of the timeline.
*   **A Single Top-Level Statement:** All triggers are deferred until the top-level function call completes, at which point the planner has already guaranteed the timeline is consistent. The triggers act as a powerful, low-level safety net that verifies the planner's logic.

This "add-then-modify" execution strategy is the principled way to solve the multi-statement update problem. It avoids the MVCC snapshot limitation by creating an intermediate state that allows the trigger's validation logic to see the future.

## Event Triggers

Reference: [PostgreSQL Documentation: Chapter 38. Event Triggers](https://www.postgresql.org/docs/current/event-triggers.html)

This section summarizes key concepts about event triggers, focusing on their behavior and how they are used within `sql_saga`.

### Core Concepts

*   **Scope**: Unlike regular triggers, which are attached to a single table, event triggers are global to a database.
*   **Events**: They capture DDL (Data Definition Language) events, not DML (Data Manipulation Language) events.
*   **Execution Order**: If multiple event triggers are defined for the same event, they are fired in alphabetical order by name.
*   **Creation**: They are created with `CREATE EVENT TRIGGER` and require a special function that returns the `event_trigger` pseudo-type.

### Key Events and Their Timing

`sql_saga` relies on the precise timing of two key events:

#### `ddl_command_end`

*   **Fires**: *After* a DDL command has executed and its changes have been written to the system catalogs, but *before* the transaction commits.
*   **`sql_saga` Usage**: Used by the `rename_following` trigger. Because it fires after the catalog changes, the trigger can inspect the "new" state of renamed objects (like columns or constraints) and update `sql_saga`'s metadata tables accordingly.
*   **Context Function**: `pg_event_trigger_ddl_commands()` returns a list of the DDL commands that were executed.

#### `sql_drop`

*   **Fires**: Just before the `ddl_command_end` event for any command that drops objects.
*   **Critical Timing Detail**: The documentation states: *"the trigger is executed **after** the objects have been deleted from the system catalogs, so it's not possible to look them up anymore."* This is a fundamental point for understanding `sql_saga`'s protection logic.
*   **`sql_saga` Usage**: Used by the `drop_protection` trigger. The trigger's logic is a direct consequence of this timing rule:
    1.  **To detect dropped top-level objects** (like a table with an `era`): The trigger must use the `pg_event_trigger_dropped_objects()` function, which provides a list of objects that were just deleted. This is used for cleanup.
    2.  **To protect dependent objects** (like constraints or triggers created by `sql_saga`): The protection logic must query the system catalogs and check if the object is *missing*. A query using `WHERE NOT EXISTS (SELECT FROM pg_constraint ...)` is the correct and necessary way to detect if a protected constraint was dropped, because the drop has already happened when the trigger code runs.

### Writing Event Triggers in C

*   **Interface**: C functions do not receive normal arguments. Instead, they get a context pointer to an `EventTriggerData` struct.
*   **Macro**: The `CALLED_AS_EVENT_TRIGGER(fcinfo)` macro must be used to verify that the function was called as an event trigger.
*   **`EventTriggerData` struct**: This struct provides the `event` name (e.g., `"sql_drop"`) and the command `tag` (e.g., `"DROP TABLE"`).
*   **Return Value**: The function must return `NULL` via `PG_RETURN_NULL()`.

## Server Programming Interface (SPI)

Reference: [PostgreSQL Documentation: Server Programming Interface](https://www.postgresql.org/docs/current/spi.html)

### Key Concepts

*   **Execution Context**: SPI allows C functions to execute SQL commands within the server. All SPI operations must be wrapped between `SPI_connect()` and `SPI_finish()`.
*   **Error Handling**: A critical feature of SPI is its error handling model. If any command executed via SPI (e.g., `SPI_execute`) fails, **control is not returned to the C function**. Instead, the current transaction or subtransaction is immediately rolled back, and an error is raised, unwinding the stack. Documented error-return codes from SPI functions are only for errors detected within the SPI layer itself, not for errors from the executed SQL.
*   **Memory Management**: Memory for query results (like `SPI_tuptable`) is allocated in a context that is automatically freed by `SPI_finish()`. For data that needs to be returned from the function, it must be copied into the upper executor context using functions like `SPI_palloc` or `SPI_copytuple`.
