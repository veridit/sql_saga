# temporal_merge: Entity Identification Architecture

This document outlines the systematic rules and interaction between different key types for entity identification within the `temporal_merge` procedure.

## 1. Key Types & Roles

`temporal_merge` uses up to two sets of keys to identify entities.

- **`identity_columns` (The Stable Key):**
    - **Purpose:** The canonical, unique identifier for an entity throughout its history (e.g., a surrogate `id`). It must not change.
    - **Source Behavior:** If provided and non-`NULL` in a source row, it signals a direct operation on a known entity. If `NULL`, the entity's identity must be determined via a natural key lookup, or it is considered a new entity.
    - **API Note:** If this parameter is omitted, `sql_saga` will automatically discover the table's primary key, or the first available natural key if no primary key exists.

- **`natural_identity_keys` (The Lookup Keys):**
    - **Purpose:** A `JSONB` array of business keys used to find the stable key. A source row can provide values for any of these keys to identify an entity. This is the primary mechanism for "upsert" logic.
    - **Format:** A JSONB array of arrays, where each inner array is a set of columns for a single (potentially composite) natural key. This nested structure is required to unambiguously represent multiple, independent keys, which may themselves be composite.
        - `[['employee_nr']]`: One natural key on the `employee_nr` column.
        - `[['employee_nr'], ['email']]`: Two separate natural keys. The planner will look for a match on `employee_nr` **OR** `email`.
        - `[['first_name', 'last_name']]`: One composite natural key on `(first_name, last_name)`.
    - **Source Behavior:** The planner uses these keys in a `LEFT JOIN` to look up the corresponding stable key from the target table.
    - **API Note:** If this parameter is omitted, `sql_saga` automatically discovers all registered natural and predicated keys on the target table.

- **`natural_identity_columns` (Legacy):**
    - **Purpose:** A `TEXT[]` array specifying a single natural key.
    - **API Note:** This parameter is maintained for backward compatibility. For new development, prefer `natural_identity_keys` which supports multiple, independent lookup keys.

## 2. The "Is Identifiable" Rule

A source row is considered **identifiable** if it provides enough information for the planner to unambiguously find or create an entity. A row that is not identifiable will result in an `ERROR` status in the feedback.

The rule is:
> A source row is identifiable if it has a non-`NULL` value for its **stable key**, OR it has non-`NULL` values for at least one complete **natural key**.

## 3. Ambiguity and Error Handling

`temporal_merge` is a set-based operation designed to be fail-safe. Instead of aborting a transaction on a data error, it provides row-level feedback. Two common error scenarios are handled this way:

- **Unidentifiable Row:** A source row that provides `NULL` for all stable and natural key columns cannot be processed.
- **Ambiguous Row:** A source row that provides natural key values that match **more than one** distinct entity in the target table is ambiguous.

In both cases, the planner will generate an `ERROR` action for that source row, and the executor will report it in the feedback, allowing the rest of the batch to be processed.

## 4. Example Scenarios

- **Target Table `person`:**
    - `identity_columns`: `['id']`
    - `natural_identity_keys`: `[['employee_nr'], ['email']]`
    - Existing data: `(id: 1, employee_nr: 'E101', email: 'alice@example.com')`

- **Scenario A: Update via Stable Key**
    - Source row: `(id: 1, full_name: 'Alice Jones')`
    - Result: **Identifiable**. Planner updates person `id=1`.

- **Scenario B: Update via Natural Key**
    - Source row: `(id: NULL, employee_nr: 'E101', full_name: 'Alice Jones')`
    - Result: **Identifiable**. Planner looks up `'E101'`, finds `id=1`, and updates person `id=1`.

- **Scenario C: New Entity**
    - Source row: `(id: NULL, employee_nr: 'E102', email: 'bob@example.com', ...)`
    - Result: **Identifiable**. Planner looks up natural keys, finds no match, and inserts a new person.

- **Scenario D: Unidentifiable Entity (Row-level ERROR)**
    - Source row: `(id: NULL, employee_nr: NULL, email: NULL, full_name: 'Ghost')`
    - Result: **Not Identifiable**. The planner generates an `ERROR` action. The executor provides feedback with status `ERROR` and a descriptive message for this row.

- **Scenario E: Ambiguous Entity (Row-level ERROR)**
    - Target also contains: `(id: 2, employee_nr: 'E102', email: 'bob@example.com')`
    - Source row: `(employee_nr: 'E101', email: 'bob@example.com', ...)`
    - Result: **Ambiguous**. The source row matches two distinct entities (`id=1` and `id=2`). The planner generates an `ERROR` action and the executor provides feedback with status `ERROR` for this row.
