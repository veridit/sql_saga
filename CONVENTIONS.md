# AI Agent Development Conventions

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
- **Action:** Before making any code changes, clearly state your hypothesis about the root cause of the problem.
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
