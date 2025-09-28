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

## 3. Implementation: The Unified Partition Key

To robustly handle all row cases, the planner's window functions rely on a single, unambiguous `TEXT` partition key. This key is dynamically constructed to guarantee that all timeline segments belonging to a single conceptual entity are processed together, while ensuring that distinct entities are kept separate.

The logic is as follows:
- **For new entities** (where `stable_identity_columns` are `NULL` and the entity does not exist in the target), the partition key is namespaced and based on the `corr_ent` (a unique identifier for a source row or a group of founding rows):
  `'new_entity__<correlation_id>'`
- **For existing entities**, the partition key is namespaced and based on a composite of their stable, canonical identifier values:
  `'existing_entity__<stable_key_val_1>__<stable_key_val_2>'`

This strategy is the core of the planner's correctness. It creates a single, stable value that window functions can `PARTITION BY`, eliminating the complexity and ambiguity of partitioning by multiple, nullable columns. This ensures that information, such as a discovered stable key, is correctly propagated across all segments of an entity's timeline without "bleeding" over to unrelated entities.

## 4. Row Cases and Planner Logic

For each merge strategy, the planner must determine its action based on the state of the identity columns (`sk` = stable key, `nk` = natural key) in each source row. The following table describes this logic flow, which proceeds from left to right: the planner uses the source row state to choose a lookup key, queries the target table, and then takes an action based on whether a matching entity was found.

| Strategy                   | Source `sk` State | Source `nk` State | Target Lookup Key | Target Found? | Planner Action & Identity Flow                                                                                                                                   |
|:---------------------------|:------------------|:------------------|:------------------|:--------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `STRATEGY_HYBRID`          | NOT NULL          | *any*             | `sk`              | Yes           | **Update by SK.** The `sk` from the source is canonical. The `nk` from the source is treated as data to be updated.                                                |
| `STRATEGY_HYBRID`          | NOT NULL          | *any*             | `sk`              | No            | **Insert with SK.** Use `sk` from the source. Fails for `..._FOR_PORTION_OF` modes.                                                                              |
| `STRATEGY_HYBRID`          | NULL              | NOT NULL          | `nk`              | Yes           | **Update by NK.** Discover the `sk` from the target entity. Back-fill the discovered `sk` to the source if requested.                                            |
| `STRATEGY_HYBRID`          | NULL              | NOT NULL          | `nk`              | No            | **Insert New.** The entity is identified by its `nk`. A new `sk` is generated by the database. Back-fill the new `sk` to the source.                               |
| `STRATEGY_HYBRID`          | NULL              | NULL              | *none*            | N/A           | **Error.** The source row is unidentifiable.                                                                                                                     |
| `STRATEGY_STABLE_KEY_ONLY` | NOT NULL          | N/A               | `sk`              | Yes           | **Update by SK.** As above.                                                                                                                                      |
| `STRATEGY_STABLE_KEY_ONLY` | NOT NULL          | N/A               | `sk`              | No            | **Insert with SK.** As above.                                                                                                                                    |
| `STRATEGY_STABLE_KEY_ONLY` | NULL              | N/A               | *none*            | N/A           | **Insert New.** The entity is identified only by its `founding_id_column`. A new `sk` is generated by the database. Back-fill the new `sk` to the source.           |
| `STRATEGY_NATURAL_KEY_ONLY`| N/A               | NOT NULL          | `nk`              | Yes           | **Update by NK.** The `nk` from the source is the canonical key.                                                                                                 |
| `STRATEGY_NATURAL_KEY_ONLY`| N/A               | NOT NULL          | `nk`              | No            | **Insert New.** The `nk` from the source is the canonical key.                                                                                                   |
| `STRATEGY_NATURAL_KEY_ONLY`| N/A               | NULL              | *none*            | N/A           | **Error.** The source row is unidentifiable.                                                                                                                     |

