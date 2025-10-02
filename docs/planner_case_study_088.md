# Planner Case Study: `088_temporal_merge_identity_discovery`, Scenario 11

This document provides a step-by-step analysis of the planner's logic for the complex multi-row `INSERT` scenario in the `088` regression test. It confirms that the current output is correct.

## Scenario Setup

A new entity ("Dee") is being created from 5 source rows with fragmented data and overlapping time periods. The mode is `MERGE_ENTITY_PATCH`.

**Source Data:**
```
 row_id | ssn | employee_nr |      email      | full_name | valid_from | valid_until 
--------+-----+-------------+-----------------+-----------+------------+-------------
      1 | 555 | E105        | dee@doe.com     | Dee Doe   | 2023-01-01 | 2024-01-01
      2 | 555 |             | dee@example.com |           | 2023-02-01 | 2023-04-01
      3 | 555 |             |                 | Sweet Dee | 2023-04-01 | 2023-06-01
      4 |     | E105        | doe@example.com |           | 2023-03-01 | 2023-05-01
      5 |     | E105        |                 | Sweet Doe | 2023-05-01 | 2023-07-01
```

## Planner Logic Walkthrough

The planner first deconstructs all source periods into a set of unique time points: `2023-01-01`, `2023-02-01`, `2023-03-01`, `2023-04-01`, `2023-05-01`, `2023-06-01`, `2023-07-01`, `2024-01-01`.

These points form 7 atomic, non-overlapping time segments. For each segment, the planner finds all covering source rows and applies the `PATCH` logic (merging payloads in `row_id` order, ignoring `NULL`s).

### 1. Segment `[2023-01-01, 2023-02-01)`
- **Covered by:** Row 1
- **Payload:** `{"email": "dee@doe.com", "full_name": "Dee Doe"}`
- **Result:** `{"email": "dee@doe.com", "full_name": "Dee Doe"}`

### 2. Segment `[2023-02-01, 2023-03-01)`
- **Covered by:** Row 1, Row 2
- **Payloads:**
    - Row 1: `{"email": "dee@doe.com", "full_name": "Dee Doe"}`
    - Row 2: `{"email": "dee@example.com"}` (NULL `full_name` is ignored)
- **Result:** `(Row 1 || Row 2)` -> `{"email": "dee@example.com", "full_name": "Dee Doe"}`

### 3. Segment `[2023-03-01, 2023-04-01)`
- **Covered by:** Row 1, Row 2, Row 4
- **Payloads:**
    - From previous segment: `{"email": "dee@example.com", "full_name": "Dee Doe"}`
    - Row 4: `{"email": "doe@example.com"}`
- **Result:** `(Row 1 || Row 2 || Row 4)` -> `{"email": "doe@example.com", "full_name": "Dee Doe"}`

### 4. Segment `[2023-04-01, 2023-05-01)`
- **Covered by:** Row 1, Row 3, Row 4
- **Payloads:**
    - Row 1: `{"email": "dee@doe.com", "full_name": "Dee Doe"}`
    - Row 3: `{"full_name": "Sweet Dee"}`
    - Row 4: `{"email": "doe@example.com"}`
- **Result:** `(Row 1 || Row 3 || Row 4)` -> `{"email": "doe@example.com", "full_name": "Sweet Dee"}`

### 5. Segment `[2023-05-01, 2023-06-01)`
- **Covered by:** Row 1, Row 3, Row 5
- **Payloads:**
    - Row 1: `{"email": "dee@doe.com", "full_name": "Dee Doe"}`
    - Row 3: `{"full_name": "Sweet Dee"}`
    - Row 5: `{"full_name": "Sweet Doe"}`
- **Result:** `(Row 1 || Row 3 || Row 5)` -> `{"email": "dee@doe.com", "full_name": "Sweet Doe"}`

### 6. Segment `[2023-06-01, 2023-07-01)`
- **Covered by:** Row 1, Row 5
- **Payloads:**
    - Row 1: `{"email": "dee@doe.com", "full_name": "Dee Doe"}`
    - Row 5: `{"full_name": "Sweet Doe"}`
- **Result:** `(Row 1 || Row 5)` -> `{"email": "dee@doe.com", "full_name": "Sweet Doe"}`

### 7. Segment `[2023-07-01, 2024-01-01)`
- **Covered by:** Row 1
- **Payload:** `{"email": "dee@doe.com", "full_name": "Dee Doe"}`
- **Result:** `{"email": "dee@doe.com", "full_name": "Dee Doe"}`

## Coalescing and Final State

The planner then merges adjacent segments with identical data payloads.

- **Segments 5 & 6** have the same payload: `{"email": "dee@doe.com", "full_name": "Sweet Doe"}`.
- They are coalesced into a single segment: `[2023-05-01, 2023-07-01)`.

The final result is 6 distinct historical rows, which exactly matches the new output.

**Conclusion:** The new output is correct and demonstrates the planner's ability to correctly apply `PATCH` logic across multiple, overlapping source rows and then coalesce the result into a minimal set of historical records.
