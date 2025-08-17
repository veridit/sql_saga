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
