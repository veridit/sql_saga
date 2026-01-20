# temporal_merge Planner Temp Tables Audit

This document provides a systematic audit of all temporary tables created in the `temporal_merge_plan` function (`src/27_temporal_merge_plan.sql`), analyzing column usage, identifying waste, and recommending optimizations.

## Executive Summary

### Key Findings

| Category | Count | Action |
|----------|-------|--------|
| **UNUSED columns** | 5 | Remove to reduce computation |
| **DEBUG-only columns** | 8 | Keep - valuable for trace system |
| **REDUNDANT columns** | 2 | Consider consolidation |
| **All columns ESSENTIAL** | ~90% | Well-optimized design |

### Actionable Optimizations

1. **Remove `temporal_columns_are_consistent`** from `all_rows` onward - consumed in early_feedback, never used after
2. **Remove `priority`** from `resolved_atomic_segments` - computed but never referenced
3. **Remove `s_causal_id`** from payload resolution - aliased but never used (keep `direct_source_causal_id` for trace)
4. **Remove `look_behind_grp` and `look_ahead_grp`** propagation - used only for window functions, then carried as dead weight
5. **Consider gating `prev_data_payload`** behind trace flag - only used in trace output

### Estimated Performance Impact

- Removing unused columns: **2-5% improvement** (less memory, fewer column copies)
- Most significant: `temporal_columns_are_consistent` flows through 6 tables unnecessarily

---

## Table-by-Table Analysis

### Overview: Data Flow

```
source_initial
    ↓
source_with_eclipsed_flag
    ↓
source_rows_with_matches ←──────┐
    ↓                           │
source_rows_with_aggregates ────┤ (side table for aggregation)
    ↓                           │
source_rows_with_discovery ─────┘
    ↓
source_rows
    ↓
source_rows_with_new_flag
    ↓
source_rows_with_nk_json
    ↓
source_rows_with_canonical_key
    ↓
source_rows_with_early_feedback
    ↓
active_source_rows ─────────────┐
    ↓                           │
target_rows ───────────────────┐│
    ↓                          ││
all_rows ←─────────────────────┘│
    ↓                           │
time_points_raw                 │
    ↓                           │
time_points_unified             │
    ↓                           │
time_points_with_unified_ids    │
    ↓                           │
time_points                     │
    ↓                           │
atomic_segments                 │
    ↓                           │
existing_segments_with_target ←─┤
new_segments_no_target ←────────┘
    ↓
resolved_atomic_segments_with_payloads
    ↓
resolved_atomic_segments_with_flag (conditional)
    ↓
resolved_atomic_segments_with_propagated_ids
    ↓
resolved_atomic_segments
    ↓
island_group
    ↓
coalesced_final_segments
    ↓
diff
    ↓
diff_ranked
    ↓
plan_with_op
    ↓
plan
```

---

## Phase 1: Source Processing Chain

### 1. `source_initial` (line ~1434)

**Purpose:** Materializes source data with computed temporal columns, payloads, and flags.

| Column | Classification | Notes |
|--------|----------------|-------|
| `source_row_id` | ESSENTIAL | Primary key, used in all joins |
| `causal_id` | ESSENTIAL | Entity grouping for new entities |
| `<identity columns>` | ESSENTIAL | Entity identification |
| `valid_from`, `valid_until` | ESSENTIAL | Temporal boundaries |
| `data_payload` | ESSENTIAL | Merge content |
| `ephemeral_payload` | ESSENTIAL | Non-coalesced content |
| `stable_pk_payload` | ESSENTIAL | Identity propagation |
| `stable_identity_columns_are_null` | ESSENTIAL | New entity detection |
| `natural_identity_column_values_are_null` | ESSENTIAL | Eclipse and grouping logic |
| `is_identifiable` | ESSENTIAL | Error validation |
| `temporal_columns_are_consistent` | ESSENTIAL | Error validation (early feedback) |
| `valid_range` | ESSENTIAL | Pre-computed for eclipse detection (~15% perf gain) |

**Verdict:** All columns ESSENTIAL.

---

### 2. `source_with_eclipsed_flag` (line ~1494)

**Purpose:** Detect source rows eclipsed by newer rows.

| Column | Classification | Notes |
|--------|----------------|-------|
| `*` (from source_initial) | ESSENTIAL | Pass-through |
| `is_eclipsed` | ESSENTIAL | Filters eclipsed rows |
| `eclipsed_by` | ESSENTIAL | Error reporting |

**Verdict:** All columns ESSENTIAL.

---

### 3-7. Source Row Processing Chain

Tables: `source_rows_with_matches`, `source_rows_with_aggregates`, `source_rows_with_discovery`, `source_rows`, `source_rows_with_new_flag`

| New Column | Added In | Classification | Notes |
|------------|----------|----------------|-------|
| `discovered_stable_pk_payload` | source_rows_with_matches | ESSENTIAL | Identity resolution |
| `discovered_id_*` | source_rows_with_matches | ESSENTIAL | Identity propagation |
| `match_count` | source_rows_with_aggregates | INTERMEDIATE | Only used to derive is_ambiguous |
| `conflicting_ids` | source_rows_with_discovery | ESSENTIAL | Error reporting |
| `is_ambiguous` | source_rows_with_discovery | ESSENTIAL | Error detection |
| `target_entity_exists` | source_rows | ESSENTIAL | New entity determination |
| `is_new_entity` | source_rows_with_new_flag | ESSENTIAL | Ubiquitous downstream |

**Potential optimization:** `is_new_entity` is just `NOT target_entity_exists` - could be inlined, but explicit column improves readability.

---

### 8-9. Natural Key and Canonical Key Resolution

Tables: `source_rows_with_nk_json`, `source_rows_with_canonical_key`

| Column | Classification | Notes |
|--------|----------------|-------|
| `nk_json` | ESSENTIAL | GIN indexed for canonical key lookup |
| `nk_non_null_keys_array` | ESSENTIAL | Deterministic ordering |
| `canonical_nk_json` | ESSENTIAL | Natural key unification |
| `grouping_key` | ESSENTIAL | Indexed, used everywhere downstream |

**Verdict:** All columns ESSENTIAL. GIN index on `nk_json` is critical for performance.

---

### 10-11. Early Feedback and Active Rows

Tables: `source_rows_with_early_feedback`, `active_source_rows`

| Column | Classification | Notes |
|--------|----------------|-------|
| `early_feedback` | ESSENTIAL | Captures errors before main pipeline |

**Indices created on `active_source_rows`:**
- Entity key columns
- `grouping_key`  
- GiST on temporal range

---

## Phase 2: Target and Segment Processing

### 12. `target_rows` (line ~1527)

**Purpose:** Extract relevant target rows for merge comparison.

| Column | Classification | Notes |
|--------|----------------|-------|
| `<identity columns>` | ESSENTIAL | Join keys |
| `valid_from`, `valid_until` | ESSENTIAL | Temporal boundaries |
| `causal_id` | ESSENTIAL | NULL for target, type compatibility |
| `stable_pk_payload` | ESSENTIAL | Entity discovery |
| `data_payload` | ESSENTIAL | Merge resolution |
| `ephemeral_payload` | ESSENTIAL | Ephemeral handling |
| `canonical_nk_json` | ESSENTIAL | Natural key unification |

**Indices created:**
- Entity key columns
- GiST on temporal range

**Verdict:** All columns ESSENTIAL.

---

### 13. `all_rows` (line ~1733)

**Purpose:** Union source and target for time point extraction.

| Column | Classification | Notes |
|--------|----------------|-------|
| `temporal_columns_are_consistent` | **UNUSED** | Only used in early_feedback, never after all_rows |

**FINDING:** `temporal_columns_are_consistent` is carried through `all_rows` → `time_points_raw` → `time_points_unified` → `time_points_with_unified_ids` → `time_points` → `atomic_segments` but **never referenced** after `source_rows_with_early_feedback`.

**Recommendation:** Remove from `all_rows` and all subsequent tables.

---

### 14-18. Time Points and Atomic Segments

Tables: `time_points_raw`, `time_points_unified`, `time_points_with_unified_ids`, `time_points`, `atomic_segments`

| Column | Classification | Notes |
|--------|----------------|-------|
| `unified_causal_id` | ESSENTIAL | Replaces causal_id for existing entities |
| `unified_stable_pk_payload` | ESSENTIAL | Coalesced across entity |
| `unified_canonical_nk_json` | ESSENTIAL | Consistent NK for entity |
| `grouping_key` | ESSENTIAL | Partition key |

**Verdict:** All unification columns ESSENTIAL. The time point deduplication and segment creation are correctly optimized.

---

### 19-20. Segment Tables

Tables: `existing_segments_with_target`, `new_segments_no_target`

| Column | Classification | Notes |
|--------|----------------|-------|
| `t_valid_from`, `t_valid_until` | ESSENTIAL | Target boundaries |
| `t_data_payload`, `t_ephemeral_payload` | ESSENTIAL | Merge resolution |
| `target_stable_pk_payload` | DEBUG | Only used in trace |
| `trace` | ESSENTIAL | Debug system (gated by p_log_trace) |

---

## Phase 3: Payload Resolution and Coalescing

### 21. `resolved_atomic_segments_with_payloads` (line ~1874)

**Purpose:** Join segments with source payloads via LATERAL.

| Column | Classification | Notes |
|--------|----------------|-------|
| `source_row_id` | ESSENTIAL | Propagation tracking |
| `contributing_row_ids` | ESSENTIAL | Final row_ids |
| `s_data_payload`, `s_ephemeral_payload` | ESSENTIAL | Merge content |
| `s_causal_id` | **UNUSED** | Aliased but never referenced |
| `direct_source_causal_id` | DEBUG | Used only in trace output |
| `s_valid_from`, `s_valid_until` | ESSENTIAL | Allen relation computation |
| `propagated_stable_pk_payload` | ESSENTIAL | Identity propagation |

**FINDING:** `s_causal_id` is aliased but never used. `direct_source_causal_id` is used only in trace output.

**Recommendation:** Remove `s_causal_id` from the LATERAL join projection. Keep `direct_source_causal_id` for trace.

---

### 22. `resolved_atomic_segments_with_propagated_ids` (line ~1920)

**Purpose:** Propagate IDs across segments using gaps-and-islands.

| Column | Classification | Notes |
|--------|----------------|-------|
| `look_behind_grp` | **REDUNDANT** | Used only for window functions, carried forward via SELECT * |
| `look_ahead_grp` | **REDUNDANT** | Same as above |
| `unified_canonical_nk_json` | ESSENTIAL | Used in coalescing |
| `propagated_contributing_row_ids` | ESSENTIAL | Final row_ids |
| `propagated_s_valid_from/until` | ESSENTIAL | Allen relation |

**FINDING:** `look_behind_grp` and `look_ahead_grp` are computed for window function partitioning, then carried through all subsequent tables via `SELECT *` but never used again.

**Recommendation:** Either:
1. Use explicit column list instead of `SELECT *` to exclude these, or
2. Accept as minor overhead (they're small integers)

---

### 23. `resolved_atomic_segments` (line ~1953)

**Purpose:** Compute final resolved payload and data hash.

| Column | Classification | Notes |
|--------|----------------|-------|
| `data_hash` | ESSENTIAL | Coalescing comparison (md5) |
| `unaffected_target_only_segment` | ESSENTIAL | Skip detection |
| `priority` | **UNUSED** | Computed but never referenced downstream |

**FINDING:** `priority` column (`CASE WHEN s_data_payload IS NOT NULL THEN 1 ELSE 2 END`) is computed but never used in `island_group`, `coalesced_final_segments`, or any subsequent table.

**Recommendation:** Remove the `priority` column computation.

---

### 24. `island_group` (line ~1981)

**Purpose:** Group adjacent segments with identical payloads for coalescing.

| Column | Classification | Notes |
|--------|----------------|-------|
| `prev_valid_until` | ESSENTIAL | Island boundary detection |
| `prev_data_hash` | ESSENTIAL | Island boundary detection |
| `prev_data_payload` | DEBUG | Only used in trace output |
| `is_island_start` | INTERMEDIATE | Only used for island_group_id |
| `island_group_id` | ESSENTIAL | GROUP BY key |

**Note:** `prev_data_payload` is only used when trace is enabled. Consider gating behind `p_log_trace`.

---

### 25. `coalesced_final_segments` (line ~2011)

**Purpose:** Aggregate adjacent identical segments.

All columns ESSENTIAL - this is the final pre-diff representation.

---

## Phase 4: Diff and Plan Generation

### 26. `diff` (line ~2046)

**Purpose:** Compare final segments against target rows.

| Column | Classification | Notes |
|--------|----------------|-------|
| `stable_identity_columns_are_null` | DEBUG | Only in trace |
| `natural_identity_column_values_are_null` | DEBUG | Only in error messages |
| `b_a_relation` | DEBUG | Never used by executor |

---

### 27. `diff_ranked` (line ~2075)

**Purpose:** Rank overlapping segments for UPDATE vs INSERT.

| Column | Classification | Notes |
|--------|----------------|-------|
| `update_rank` | ESSENTIAL | Determines UPDATE vs INSERT |

---

### 28. `plan_with_op` (line ~2099)

**Purpose:** Determine operation for each diff row.

| Column | Classification | Notes |
|--------|----------------|-------|
| `operation` | ESSENTIAL | Core executor input |
| `entity_keys_json` | ESSENTIAL | Executor join key |
| `identity_keys` | DEBUG | Redundant with entity_keys |
| `lookup_keys` | DEBUG | Redundant with entity_keys |
| `feedback` | ESSENTIAL | Error messages |

---

### 29. `plan` (line ~2214)

**Purpose:** Final execution plan with update_effect.

| Column | Classification | Notes |
|--------|----------------|-------|
| All operation columns | ESSENTIAL | Executor input |
| `update_effect` | ESSENTIAL | Statement batching |
| `s_t_relation` | DEBUG | Never used by executor |
| `b_a_relation` | DEBUG | Never used by executor |
| `identity_keys` | DEBUG | Never used by executor |
| `lookup_keys` | DEBUG | Never used by executor |
| `trace` | ESSENTIAL | Debug system |

---

## The Trace System

The `trace` column system is **ESSENTIAL** and must be preserved:

1. **Zero overhead when disabled:** When `p_log_trace = false`, trace expressions evaluate to `NULL::jsonb`
2. **Progressive enrichment:** Trace is seeded in segment tables, enriched through resolution, and accumulated through coalescing
3. **Valuable for debugging:** Captures intermediate state at each transformation stage
4. **Design pattern:** `CASE WHEN {trace} IS NOT NULL THEN {trace} || jsonb_build_object(...) ELSE NULL END`

### Columns that feed trace but are DEBUG classification:

These are correctly classified as DEBUG because their only downstream consumer is the trace system:
- `target_stable_pk_payload`
- `prev_data_payload`
- `stable_identity_columns_are_null`
- `s_t_relation`, `b_a_relation`
- `identity_keys`, `lookup_keys`

---

## Detailed Recommendations

### 1. Remove `temporal_columns_are_consistent` from `all_rows` onward

**Impact:** Flows through 6 tables, ~20 bytes per row
**Risk:** None - only used in early_feedback
**Savings:** Minor memory reduction

**Change locations:**
- `all_rows` SELECT list
- `time_points_raw` union
- Remove from `atomic_segments` and downstream

### 2. Remove `priority` from `resolved_atomic_segments`

**Impact:** Computed but never read
**Risk:** None
**Savings:** 4 bytes per segment + computation time

**Change location:** Line ~1976, remove CASE expression

### 3. Remove `s_causal_id`

**Impact:** Aliased in LATERAL but never used (`direct_source_causal_id` IS used in trace)
**Risk:** None
**Savings:** ~8 bytes per segment

**Change location:** LATERAL join in `resolved_atomic_segments_with_payloads`

### 4. Consider explicit SELECT in propagated_ids CTE

**Impact:** `look_behind_grp` and `look_ahead_grp` carried via SELECT *
**Risk:** Low - they're just integers
**Savings:** 8 bytes per segment

**Alternative:** Leave as-is for maintainability

### 5. Gate `prev_data_payload` behind trace flag

**Impact:** Only used in trace
**Risk:** Slightly more complex SQL
**Savings:** Potentially significant if payloads are large

**Recommendation:** Low priority - current approach is simpler

---

## Columns Confirmed ESSENTIAL

The vast majority of columns are correctly designed and essential:

- **Identity columns:** Flow through entire pipeline, used for joins
- **Temporal columns:** Core to the temporal merge algorithm  
- **Payload columns:** Required for merge resolution
- **Flag columns:** Control branching logic
- **Grouping columns:** Enable correct partitioning
- **Trace column:** Invaluable debugging tool

---

## Conclusion

The `temporal_merge_plan` function is well-designed with minimal waste. The identified unused columns represent minor optimization opportunities (~5% overhead). The trace system is a first-class feature that enables powerful debugging without runtime cost when disabled.

**Priority order for cleanup:**
1. `temporal_columns_are_consistent` (flows through most tables)
2. `priority` (dead code)
3. `s_causal_id`, `direct_source_causal_id` (unused aliases)
4. Window function helper columns (minor)
