# Development Conventions

## Introduction
This document outlines the standard operating procedure for all AI agents contributing to this project. Adhering to these conventions ensures a systematic, verifiable, and robust development process.

## Guiding Principles

### 1. Maintain a Stateless Mindset
With each new message from the user, especially one containing test results or command output, you must clear all previous assumptions and hypotheses. Your analysis must be based **only** on the new information provided in the latest message. Do not refer back to your own previous hypotheses if they have been contradicted by data. Treat each interaction as a fresh start to the analysis cycle.

### 2. Follow a Hypothesis-Driven Cycle
All development work is an iterative process of forming hypotheses and verifying them with real-world data. The core principle is to replace assumption with verification.

## The Iterative Development Cycle
All development work, especially bug fixing, must follow this iterative cycle. Do not mark tasks as complete or "done" until the final step of the cycle has been successfully executed.

### 1. Formulate and State a Hypothesis
- **Action:** Before making any code changes, clearly state your hypothesis about the root cause of the problem. You must also explicitly document the outcome of every hypothesis in the project's `todo.md` file, whether it is verified or falsified. This creates a permanent record of the debugging process and prevents repeating failed strategies. This is not optional; it is the most critical part of the process for complex bugs.
- **Example:** "Hypothesis: The server crash is caused by a memory leak in `covers_without_gaps.c`, where pass-by-reference datums are not being freed before new ones are allocated in the aggregate's state."

### 2. Create a Minimal Reproducing Test
- **Action:** Before proposing a fix, add a test case that isolates the bug at the lowest possible level. If one already exists, identify it. This test must fail before the fix and pass after.
- **Example:** "I will add a new test to `sql/22_covers_without_gaps_test.sql`. This test will call `covers_without_gaps` with a set of ranges that have a gap at the start of the target period, which is currently not being detected. This test is expected to fail."

### 3. Propose a Change and State the Expected Outcome
- **Action:** Propose the specific code changes (using SEARCH/REPLACE blocks). Alongside the changes, explicitly state your hope or assumption about what the change will achieve.
- **Example:** "Hope/Assumption: Applying this fix will prevent the memory leak. The new test in `22_covers_without_gaps_test.sql` will now pass, and the cascading failures in foreign key tests (25, 41, 42) will be resolved."

### 4. Gather Real-World Data
- **Action:** After the user applies the changes, request that they run the relevant tests or commands to gather empirical evidence of the change's impact.
- **Example:** "Please run `make install && make installcheck` to verify the fix."

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
For complex tasks, a detailed journal of hypotheses, data, and outcomes should be maintained in `journal.md`. This provides a low-level history of the development process, complementing the high-level summary in `todo.md`.

## Known Pitfalls and Falsified Assumptions
This section documents incorrect assumptions that have been disproven through testing. Reviewing these can help avoid repeating past mistakes.

*   **MVCC and Transaction Visibility in PL/pgSQL:** The set of rows visible to a `pl/pgsql` function is determined when the function begins execution. Within a single function, a `SET`-based query and a `LOOP` that executes queries will both operate on the same data snapshot. It is a flawed assumption to think that a `LOOP` is somehow more robust to transaction visibility issues than a single complex query within the same function. If a set-based query is failing, the bug is in the query's logic, not in a fundamental limitation of MVCC for set-based operations.

*   **`regclass` Type Casting in C Event Triggers:** The `regclass` data type is a symbolic reference to a relation OID, not just the OID itself. When a C function using SPI executes a query that returns a `regclass` column, the PostgreSQL backend attempts to format this value, which involves looking up the object's name. This behavior is extremely dangerous in `sql_drop` event triggers.
    *   **The Problem:** An `sql_drop` trigger fires *after* the object has been deleted from the system catalogs. If an SPI query in the trigger function selects a `regclass` column that refers to a now-deleted object, the backend's attempt to look up the object's name will fail. This causes `SPI_execute` to return an error, leading to unpredictable behavior if not handled precisely. This can manifest as cascading, incorrect error messages or the appearance that queries are returning rows when they have actually failed.
    *   **The Solution:** When writing SPI queries for `sql_drop` event triggers, **never select `regclass` columns directly**. Always explicitly `CAST` them to `oid` within the SQL query string (e.g., `SELECT table_oid::oid FROM ...`). This fetches the raw OID without triggering the problematic name lookup. Any conversion from the OID to a textual name for error messages must be done manually in the C code (e.g., using `regclassout`) *after* the query has successfully completed and returned a row.

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
