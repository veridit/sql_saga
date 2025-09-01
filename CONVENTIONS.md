# Development Conventions

## Introduction
This document outlines the standard operating procedure for all AI agents contributing to this project. Adhering to these conventions ensures a systematic, verifiable, and robust development process.

## Tool Usage
When you suggest commands in your regular response (*NOT* inside a `SEARCH/REPLACE` block), format them inside `bash` blocks:
```bash
cmd1
cmd2
```
These commands are then presented to the user, who can accept them for execution, and the results are returned to you.

Key tools available for you to suggest:
- **`rg` (ripgrep)**: Your primary tool for fast, powerful code searching. Use it to find definitions, locate files, or understand code relationships.
- **`tree`**: List files to understand directory structures.
- **`head`**: Inspect the beginning of files to quickly understand their structure and content.
- **`ls`**: Check file sizes to determine if they should be read or inspected.
- **`psql`**: Run arbitrary SQL for debugging or inspection (e.g., `echo 'SELECT * FROM sql_saga.era;' | psql -d sql_saga_regress`).

For file system operations and large-scale edits, prefer suggesting shell commands over generating `SEARCH/REPLACE` blocks where appropriate. This is faster and more efficient.
- Use `rm` to delete files and `git mv` to move or rename them.
- For simple content replacement (e.g., replacing an entire file's contents), `echo "new content" > filename` can be used instead of a large `SEARCH/REPLACE` block.
- For large-scale, repetitive search-and-replace operations across multiple files, powerful tools like `ruplacer` and `renamer` are available and should be used.

## Coding Standards

### C Code (PostgreSQL Extensions)
- **C99 Compliance:** All C code must be compatible with the C99 standard. The build process uses the `-Wdeclaration-after-statement` flag, which will generate a warning if variable declarations are mixed with code.
  - **Rule:** Declare all variables at the beginning of a block (immediately after a `{`). Do not mix declarations and executable statements.
- **PostgreSQL Coding Conventions:** Adhere to the formatting and naming conventions outlined in the official [PostgreSQL Documentation](https://www.postgresql.org/docs/current/source.html). This includes conventions for variable names, function names, and code layout.

### SQL Conventions
- **Function/Procedure Definitions**:
    - Use the function/procedure name in the literal string quote for the body (e.g., `AS $my_function_name$`).
    - Specify `LANGUAGE plpgsql` (or other) before the body.
    - Use the long form for parameters for documentation clarity (e.g., `param_name param_type`).
- **Function Calls**: For calls with 3+ arguments, use named arguments (e.g., `arg1 => val1`).
- **String Literals for `format()`**:
    - Always prefer dollar-quoting (e.g., `format($$ ... $$)`) for the main dynamic SQL string. This avoids having to escape single quotes inside the SQL.
    - **Nesting**: When nesting dollar-quoted strings, use named dollar quotes for the outer string to avoid conflicts (e.g., `$SQL$`).
    - For `format()` calls with multiple parameters, use numbered placeholders for clarity:
      - `%1$I` for the 1st parameter as an identifier, `%2$L` for the 2nd as a literal, `%3$s` for the 3rd as a plain string, etc.
      - Keep the SQL readable by aligning numbered placeholders with inline comments that show which parameter they refer to (e.g., `... %1$I ... /* %1$I */`).
- **Table Aliases**: Prefer explicit `AS` for table aliases, e.g., `FROM my_table AS mt`.

### Build System
- **`Makefile` and Source Files**: The `Makefile` uses a glob pattern (`src/[0-9][0-9]_*.sql`) to automatically discover and concatenate SQL source files. When adding a new feature, simply create a new numbered `.sql` file in the `src/` directory (e.g., `src/31_new_feature.sql`). It will be included in the build automatically without any need to edit the `Makefile`.

### SQL Naming conventions
- `x_id` is a foreign key to table `x`
- `x_ident` is an external identifier, not originating from the database
- `x_at` is a TIMESTAMPTZ (with timezone)
- `x_on` is a DATE
- **Temporal Columns:** To ensure consistency and intuitive use, all temporal periods must follow the `[)` semantic (inclusive start, exclusive end). This aligns with the default behavior of PostgreSQL's native range types (e.g., `daterange`) and ensures compatibility with operators like `OVERLAPS`.
  - Column names for the start of a period must be named `valid_from` (or a similarly descriptive name ending in `_from`).
  - Column names for the end of a period must be named `valid_until` (or a similarly descriptive name ending in `_until`).
  - Metadata columns in `sql_saga` that store these column names will be named `valid_from_column_name` and `valid_until_column_name`.

## Guiding Principles

### 1. Maintain a Stateless Mindset
With each new message from the user, especially one containing test results or command output, you must clear all previous assumptions and hypotheses. Your analysis must be based **only** on the new information provided in the latest message. Do not refer back to your own previous hypotheses if they have been contradicted by data. Treat each interaction as a fresh start to the analysis cycle.

### 2. Follow a Hypothesis-Driven Cycle
All development work is an iterative process of forming hypotheses and verifying them with real-world data. The core principle is to replace assumption with verification.

## The Iterative Development Cycle
All development work, especially bug fixing, must follow this iterative cycle. Do not mark tasks as complete or "done" until the final step of the cycle has been successfully executed.

### 1. Formulate and State a Hypothesis
- **Action:** Before making any code changes, clearly state your hypothesis about the root cause of the problem in `tmp/journal.md`. This creates a locally persistent log of your thought process. You must also explicitly document the outcome of every hypothesis in the project's `todo.md` file, whether it is verified or falsified. This creates a permanent record of the debugging process and prevents repeating failed strategies. This is not optional; it is the most critical part of the process for complex bugs.
- **Example:** "Hypothesis: The server crash is caused by a memory leak in `covers_without_gaps.c`, where pass-by-reference datums are not being freed before new ones are allocated in the aggregate's state."

### 2. Create a Minimal Reproducing Test
- **Action:** Before proposing a fix, add a test case that isolates the bug at the lowest possible level. If one already exists, identify it. This test must fail before the fix and pass after.
- **Example:** "I will add a new test to `sql/22_covers_without_gaps_test.sql`. This test will call `covers_without_gaps` with a set of ranges that have a gap at the start of the target period, which is currently not being detected. This test is expected to fail."

### 3. Propose a Change and State the Expected Outcome
- **Action:** Propose the specific code changes (using SEARCH/REPLACE blocks). Alongside the changes, explicitly state your hope or assumption about what the change will achieve.
- **Example:** "Hope/Assumption: Applying this fix will prevent the memory leak. The new test in `22_covers_without_gaps_test.sql` will now pass, and the cascading failures in foreign key tests (25, 41, 42) will be resolved."

### 4. Gather Real-World Data
- **Action:** After the user applies the changes, request that they run the relevant tests or commands to gather empirical evidence of the change's impact.
- **Standard Command Format:** Always use the following command structure to run tests. This ensures the extension is installed before testing and that any failures are immediately diffed for analysis.
  - **Debugging Tip:** For complex SQL or C functions, prefer adding `RAISE DEBUG` statements over `RAISE NOTICE`. In the corresponding test file, temporarily wrap the relevant commands with `SET client_min_messages TO DEBUG;` and `RESET client_min_messages;`. This provides targeted diagnostic output without permanently cluttering the test results.
  - **Command:** `make install && make test ...; make diff-fail-all`
  - **Test Output Review:** You must **never** propose `SEARCH/REPLACE` blocks for `expected/*.out` files. If a test fails due to intended changes, instruct the user to review and accept the new output interactively by running `make diff-fail-all vim`.
  - **Usage:**
    - To run all tests: `make install && make test; make diff-fail-all`
    - To run fast tests (excluding benchmarks): `make install && make test fast; make diff-fail-all`
    - To run specific tests: `make install && make test TESTS="01_install 51_quoted_identifiers"; make diff-fail-all`
  - **Note on Self-Contained Tests:** The `01_install.sql` test handles setup required by older tests. Newer tests should be self-contained.
    - Include `\i sql/include/test_setup.sql` at the beginning to create necessary roles and permissions.
    - Include `\i sql/include/test_teardown.sql` at the end to clean up.
    - For tests that verify transactional behavior, the `BEGIN`/`ROLLBACK` block should wrap only the test logic, not the setup/teardown of non-transactional objects like roles.
    - When running a specific older test, `01_install` must still be run first.

### 5. Analyze Results and Verify Hypothesis
- **Action:** Carefully inspect the output of the tests or commands. Compare the actual results against the expected outcome from Step 3.
  - **If Successful:** The hypothesis is confirmed. The fix worked as expected.
  - **If Unsuccessful:**
    1.  **Analyze Failure:** The hypothesis was incorrect, or the fix was incomplete. Analyze the new data (test failures, error messages) to form a new, more accurate hypothesis.
    2.  **Revert Incorrect Changes:** If a proposed change did not solve the problem and does not have standalone merit, it must be reverted. Do not leave incorrect workarounds or convoluted code in the codebase. Propose a new set of `SEARCH/REPLACE` blocks to undo the incorrect change before proceeding.
    3.  **Return to Step 1:** Begin the cycle again with the new hypothesis.

### 6. Update Documentation and Conclude
- **Action:** Only after the fix has been successfully verified in Step 5, update the relevant documentation.
  - **For `todo.md`:** Move the task from a pending state (e.g., "High Priority") to the "Done" section. The description of the completed task should accurately reflect the *verified* solution.
  - **For other documentation:** Update any other relevant documents, such as `README.md` or design documents.

By strictly following this process, we ensure that progress is real, verifiable, and that the project's state remains consistently stable.

## General Development Principles
- **Fail Fast**:
  - Functionality that is expected to work should fail immediately and clearly if an unexpected state or error occurs.
  - Do not mask or work around problems; instead, provide sufficient error or debugging information to facilitate a solution. This is crucial for maintaining system integrity and simplifying troubleshooting, especially in backend processes and SQL procedures.
- **Declarative Transparency**:
  - Where possible, store the inputs and intermediate results of complex calculations directly on the relevant records. This makes the system's state self-documenting and easier to debug, inspect, and trust, rather than relying on dynamic calculations that can appear magical.

### The "Observe, Verify, Implement" Protocol for Complex Changes
When a task is complex or has a history of regressions (e.g., the `rename_following` trigger), a more rigorous, data-driven protocol is required to avoid speculative fixes. This protocol is a practical application of the "Observe first, then change" principle.

1.  **Establish a Baseline (Observe):**
    *   **Action:** Before writing any new logic, instrument the existing, *passing* code with diagnostic logging (`RAISE NOTICE`).
    *   **Goal:** Capture the exact inputs, outputs, and event sequences that occur during the relevant tests. This baseline data serves as a definitive specification for the new logic. The tests are expected to fail due to the new logging output.

2.  **Verify New Logic in Read-Only Mode (Verify):**
    *   **Action:** Implement the new logic, but keep it "read-only." It should calculate its decisions but only log them, *without* altering the program's control flow (e.g., no early `RETURN` statements).
    *   **Goal:** Run the tests again. Analyze the new logs to compare the baseline data against the logic's decisions. This step proves the logic is correct against real-world test data *before* it is activated.

3.  **Activate the Verified Logic (Implement):**
    *   **Action:** Only after the logic has been verified in read-only mode, remove all diagnostic logging and enable the new control flow (e.g., add the early `RETURN` statement).
    *   **Goal:** Run the tests a final time to confirm that the now-active logic works as intended and that no regressions have been introduced.

This protocol transforms debugging from a cycle of "guess-and-check" into a methodical, scientific process of data gathering and verification, dramatically reducing the number of failed attempts.

### Development Journaling
For complex, multi-step tasks like major refactoring, a detailed plan should be maintained in `tmp/journal.md`. This file outlines the sequence of steps, the actions required for each step, and the expected outcome. It serves as a roadmap for the task, ensuring a systematic approach. The journal should be cleared when you begin on a new major todo item. This hels for bug fixing and iterative development by logging hypotheses and outcomes, providing a low-level history of the debugging process.

## Known Pitfalls and Falsified Assumptions
This section documents incorrect assumptions that have been disproven through testing. Reviewing these can help avoid repeating past mistakes.

*   **MVCC and Transaction Visibility in PL/pgSQL:** The set of rows visible to a `pl/pgsql` function is determined when the function begins execution. Within a single function, a `SET`-based query and a `LOOP` that executes queries will both operate on the same data snapshot. It is a flawed assumption to think that a `LOOP` is somehow more robust to transaction visibility issues than a single complex query within the same function. If a set-based query is failing, the bug is in the query's logic, not in a fundamental limitation of MVCC for set-based operations.

*   **`regclass` Type Casting in C Triggers:** The `regclass` data type is a symbolic reference to a relation OID, not just the OID itself. When a C function using SPI executes a query involving a `regclass` column, the PostgreSQL backend may attempt to format this value, which involves looking up the object's name. This behavior can be dangerous and lead to subtle bugs.
    *   **The Problem in `sql_drop` Event Triggers:** An `sql_drop` trigger fires *after* the object has been deleted from the system catalogs. If an SPI query in the trigger function *selects* a `regclass` column that refers to a now-deleted object, the backend's attempt to look up the object's name will fail. This causes `SPI_execute` to return an error, leading to unpredictable behavior if not handled precisely. This can manifest as cascading, incorrect error messages or the appearance that queries are returning rows when they have actually failed.
    *   **The Problem in Regular Triggers:** In regular DML triggers, using `regclass` in a `WHERE` clause (e.g., `WHERE table_oid = $1`) can be sensitive to the current `search_path` and rely on implicit casting rules. This can make queries less robust.
    *   **The Solution:** For maximum robustness in any C-based trigger function using SPI:
        1.  **Always `CAST` to `oid` in SQL:** In all SPI queries, explicitly cast `regclass` columns to `oid` for both `SELECT` lists and `WHERE` clauses (e.g., `SELECT table_oid::oid ...`, `... WHERE table_oid::oid = $1`).
        2.  **Use `OIDOID` for Parameters:** When using `SPI_execute_with_args`, specify the parameter type as `OIDOID` for relation identifiers.
        3.  **Perform Lookups in C:** Fetch raw OIDs from the database. Any conversion from an OID to a textual name for error messages must be done manually in the C code (e.g., using `regclassout`) *after* the query has successfully completed.

## When the Cycle Fails: Changing Strategy
When repeated iterations of the hypothesis-driven cycle fail to resolve a persistent and complex bug (such as a memory corruption crash), it is a sign that the underlying assumptions are wrong and a change in strategy is required.

### The "Simplify and Rebuild" Approach
1.  **Formulate a new meta-hypothesis:** State that the complexity of the current implementation is the source of the problem.
2.  **Strip to a non-crashing stub:** Drastically simplify the faulty code to a minimal, stable state that is guaranteed not to crash, even if it returns an incorrect result. This changes the failure mode from a crash to a predictable test failure.
3.  **Verify stability:** Run the tests to confirm that the server is now stable and the crash is gone. This establishes a new, reliable baseline.
4.  **Incrementally re-introduce logic (Logical Binary Search):** Re-introduce functionality piece by piece, running tests at each step.
    *   Start by restoring the first half of the original logic. If the system remains stable, the bug is in the second half. If it crashes, the bug is in the first half.
    *   Continue this process, dividing the suspicious code block in half with each iteration, until the single line or small section of code causing the instability is isolated.
    *   This process is a logical "binary search" on the code's functionality.
5.  **Resume the normal cycle:** Once the root cause is identified, resume the standard hypothesis-driven cycle to fix the specific issue.
