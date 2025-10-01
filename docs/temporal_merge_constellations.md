# `temporal_merge`: A Principled Approach to Planner Logic

## 1. Introduction

The `temporal_merge` planner is a complex piece of software that must handle numerous variations in source and target table schemas, API parameters, and data patterns. To manage this complexity, the planner's logic is modeled as a state machine.

This document outlines the two primary dimensions of this state machine:
1.  **Merge Strategy**: The static, "in-principle" configuration determined by the API parameters (`identity_columns`, `natural_identity_columns`, etc.). This defines the *strategy* for how the planner will identify and correlate entities.
2.  **Row Case**: The dynamic, "in-practice" state of each individual source row (e.g., whether `id` is `NULL`). This determines the logical path a specific row takes within the chosen strategy.

By making these states explicit, we can ensure the planner's behavior is predictable, robust, and easier to maintain.

## 2. Merge Strategies

This defines the high-level strategy for entity identification based on the parameters passed to `temporal_merge`.

| Strategy                     | `identity_columns` (Stable Key) | `natural_identity_columns` (Lookup Key) | Description                                                                                                                                                                                                                                                                                                                                                     |
|------------------------------|---------------------------------|-----------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `STRATEGY_STABLE_KEY_ONLY`   | Populated                       | `NULL`                                  | **Stable Key Only:** The `identity_columns` (e.g., a surrogate `id`) are the only way to identify an entity.                                                                                                                                                                                                                                                     |
| `STRATEGY_NATURAL_KEY_ONLY`  | `NULL`                          | Populated                               | **Natural Key Only:** The `natural_identity_columns` (e.g., `unit_ident`) are the only way to identify an entity. In this constellation, the natural key serves as both the lookup key and the stable identifier.                                                                                                                                              |
| `STRATEGY_HYBRID`            | Populated                       | Populated                               | **Hybrid Model (Surrogate + Natural Key):** This is the most common and robust pattern. A stable, surrogate key (`identity_columns`) is the canonical entity identifier. A separate natural key (`natural_identity_columns`) is used to look up entities when the stable key is not known by the source (e.g., for new entities or updates from external systems). |

## 3. Implementation: A Declarative, Partition-Based Approach

The planner was previously failing to correctly group multiple source rows that belonged to the same conceptual entity. This was due to a flawed timeline partitioning strategy that relied on an incorrect correlation ID (`corr_ent`).

- **The Flaw:** When multiple source rows for a new entity had contiguous, meeting time periods (e.g., `[Jan, Feb)` and `[Feb, Mar)`), the `time_points` CTE would incorrectly discard one of the time points at the meeting boundary. Its `DISTINCT ON (..., point)` clause, combined with an `ORDER BY ... corr_ent ASC` tie-breaker, would deterministically keep the `valid_until` point from the earlier source row and discard the `valid_from` point from the later source row. This truncated the entity's timeline, causing only the first historical segment to be created.
- **The Declarative Fix:** The planner's logic for constructing the `entity_key` has been fixed to correctly handle intra-batch dependencies for new entities. The logic now correctly prioritizes the `founding_id_column` (via the `causal_id`) when grouping source rows for new entities. This ensures that when `founding_id_column` is provided, all rows sharing the same founding ID are correctly grouped into a single conceptual entity, resolving the regressions. When `founding_id_column` is not used, the planner correctly falls back to using natural keys for grouping. The logic is now:
    - For **existing entities**, the entity key is their stable identifier (`identity_columns`).
    - For **new entities**:
        - If `founding_id_column` is provided, the entity key is the `causal_id`.
        - Otherwise, the entity key is their natural identifier (`natural_identity_columns`), falling back to the `causal_id` (from `row_id`) if no natural key is available.

This ensures that all time points and source rows for a single conceptual entity (whether new or existing) are processed together in the same partition. This stateless, declarative approach correctly solves the multi-row update and insert bugs revealed by the `088_...` test suite without requiring complex, stateful grouping CTEs.

## 4. Row Cases and Planner Logic

For each merge strategy, the planner must determine its action based on the state of the identity columns in each source row. The planner checks if the stable key (`sk`) columns are all `NULL`, and if the natural key (`nk`) columns are all `NULL`. The following table describes this logic flow, which proceeds from left to right: the planner uses the source row's `NULL` state to choose a lookup key, queries the target table, and then takes an action based on whether a matching entity was found.

| Strategy                     | All `sk` cols `NULL`? | All `nk` cols `NULL`? | Target Lookup Key | Target Found? | Planner Action & Identity Flow                                                                                                                                   |
|:-----------------------------|:----------------------|:----------------------|:------------------|:--------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `STRATEGY_HYBRID`            | No                    | *any*                 | `sk`              | Yes           | **Update by SK.** The `sk` from the source is canonical. The `nk` from the source is treated as data to be updated.                                                |
| `STRATEGY_HYBRID`            | No                    | *any*                 | `sk`              | No            | **Insert with SK.** Use `sk` from the source. Fails for `..._FOR_PORTION_OF` modes.                                                                              |
| `STRATEGY_HYBRID`            | Yes                   | No                    | `nk`              | Yes           | **Update by NK.** Discover the `sk` from the target entity. Back-fill the discovered `sk` to the source if requested.                                            |
| `STRATEGY_HYBRID`            | Yes                   | No                    | `nk`              | No            | **Insert New.** The entity is identified by its `nk`. A new `sk` is generated by the database. Back-fill the new `sk` to the source.                               |
| `STRATEGY_HYBRID`            | Yes                   | Yes                   | *none*            | N/A           | **Error.** The source row is unidentifiable.                                                                                                                     |
| `STRATEGY_STABLE_KEY_ONLY`   | No                    | N/A                   | `sk`              | Yes           | **Update by SK.** As above.                                                                                                                                      |
| `STRATEGY_STABLE_KEY_ONLY`   | No                    | N/A                   | `sk`              | No            | **Insert with SK.** As above.                                                                                                                                    |
| `STRATEGY_STABLE_KEY_ONLY`   | Yes                   | N/A                   | *none*            | N/A           | **Insert New.** The entity is identified by its causal ID (`causal_id`). A new `sk` is generated by the database. Back-fill the new `sk` to the source.         |
| `STRATEGY_NATURAL_KEY_ONLY`  | N/A                   | No                    | `nk`              | Yes           | **Update by NK.** The `nk` from the source is the canonical key.                                                                                                 |
| `STRATEGY_NATURAL_KEY_ONLY`  | N/A                   | No                    | `nk`              | No            | **Insert New.** The `nk` from the source is the canonical key.                                                                                                   |
| `STRATEGY_NATURAL_KEY_ONLY`  | N/A                   | Yes                   | *none*            | N/A           | **Error.** The source row is unidentifiable.                                                                                                                     |

## 5. Canonical Natural Key Resolution for New Entities

When multiple source rows describe different historical slices of the *same new conceptual entity*, the planner must have a robust mechanism to group them. This is common when source data is fragmented, providing different natural key attributes in different rows.

**Example Scenario from `088_temporal_merge_identity_discovery`:**
A new entity "Dee" is described by five source rows, with two natural keys (`ssn`, `employee_nr`):
- Row 1: `ssn='555'`, `employee_nr='E105'`
- Row 2: `ssn='555'`, `employee_nr=NULL`
- Row 3: `ssn='555'`, `employee_nr=NULL`
- Row 4: `ssn=NULL`, `employee_nr='E105'`
- Row 5: `ssn=NULL`, `employee_nr='E105'`

All these rows refer to the same conceptual person. The planner must unify them under a single `entity_key`.

### The `canonical_nk_json` Logic

The planner achieves this unification by computing a `canonical_nk_json` for each new entity source row. It does this in a sub-CTE (`source_rows_with_canonical_key`) using a `LATERAL` join:
1.  For each source row (`s1`), it computes its own set of non-NULL natural key attributes (`nk_json`).
2.  It then searches all other source rows (`s2_inner`) for a candidate that is a "superset" of `s1`'s natural key (`s2_inner.nk_json @> s1.nk_json`).
3.  From the candidates, it picks the "best" one to be the canonical key for `s1`.

### The Bug: Flawed "Best" Key Selection

The original logic for selecting the "best" key was flawed because it did not correctly determine which of the candidate natural keys was the most "complete" (i.e., had the most non-NULL attributes). This resulted in non-deterministic ordering, causing the planner to sometimes fail to find the most complete key. For the example above, it resulted in two distinct `entity_key` values:
- `new_entity__{"ssn": "555", "employee_nr": "E105"}`
- `new_entity__{"ssn": "555"}`

This caused the executor to attempt to create two new entities with `ssn=555`, leading to an exclusion constraint violation.

### The Fix: Deterministic Ordering via Pre-computed Array

The logic has been corrected by pre-computing an array of non-null natural key attributes for each source row (`nk_non_null_keys_array`). The `ORDER BY` clause then uses this array:
`ORDER BY array_length(s2_inner.nk_non_null_keys_array, 1) DESC, s2_inner.nk_non_null_keys_array::text DESC`

This correctly and deterministically orders the candidate keys by the number of attributes they contain, ensuring the most complete key is always chosen. With this fix, all five source rows are correctly assigned the same `canonical_nk_json` (`{"ssn": "555", "employee_nr": "E105"}`) and a single `entity_key`, resolving the bug.

**Note on Flawed Alternative Approaches:** An seemingly simpler alternative, `ORDER BY array_length(array(SELECT jsonb_object_keys(s2_inner.nk_json)), 1) DESC`, is fundamentally incorrect. Because `jsonb_object_keys` returns a `SETOF text`, its use inside a subquery that is itself inside a `LATERAL` join's `ORDER BY` clause is not guaranteed to be stable or portable. Pre-calculating the array of keys (`nk_non_null_keys_array`) in an earlier CTE and projecting it is the only robust solution.
## 6. A Note on Pathological Cases: Unifying Fragmented Entities

A critical bug was discovered in the `088_temporal_merge_identity_discovery` test, which uses five source rows with fragmented natural key information to describe a single new entity. The planner was failing to correctly merge the payloads from all five rows, and previous attempts to fix this had introduced widespread regressions for existing entities.

### The Bug: Fragmented Identity and Flawed Joins

The problem had two parts:
1.  **Fragmented Identity for Existing Entities:** When an existing entity was updated, its natural key columns were being nulled out in the final plan. This was because the planner was not correctly propagating the full, known identity of an existing entity to all of its time segments.
2.  **Flawed Join for New Entities:** The `LATERAL` join from an atomic segment to the source rows used a `causal_id` condition that incorrectly filtered out most of the relevant source rows for a new entity, preventing correct payload merging.

### The Fix: A Two-Part, Methodical Solution

The solution required fixing both issues without re-introducing regressions.
1.  **Unified Natural Keys for All Entities:** The planner now computes a canonical `jsonb` representation of the natural key (`canonical_nk_json`) for *all* rows, both source and target. This is then propagated to all time points of an entity using a `FIRST_VALUE` window function in the `time_points_unified` CTE. The `v_unified_id_cols_projection` logic then unpacks this canonical `jsonb` object to project a consistent, non-fragmented set of natural key columns for all segments of an entity. This fixed the regression.
2.  **Conditional Join Logic:** With the regression fixed, the join logic was corrected. The `v_lateral_join_sr_to_seg` variable now generates a `CASE` statement.
    - For **new entities** (`seg.is_new_entity`), it uses the correct, robust join on the unified `entity_key`: `source_row.entity_key = seg.entity_key`.
    - For **existing entities**, it uses the original, correct join on the individual identity columns.

This surgical, conditional logic ensures that the complex multi-row insert case is handled correctly without breaking the well-tested logic for existing entities.
