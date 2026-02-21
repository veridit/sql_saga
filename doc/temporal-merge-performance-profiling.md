# temporal_merge Performance Profiling Results

**Date**: 2026-02-21
**Context**: Profiling the 5 slowest pg_regress tests in StatBus to identify optimization targets.

## Executive Summary

`sql_saga.temporal_merge_plan` is the **#1 performance bottleneck** in the StatBus import pipeline. The PLAN phase takes 180-270ms per call regardless of data volume, while the EXECUTE phase takes only 25-29ms. The 2-level cache exists but does not hit across import jobs.

**Impact**: Each import job calls temporal_merge 4 times. The PLAN phase accounts for ~930ms of the ~1,100ms processing phase per import. Across the 5 slowest tests (54 import calls total), this adds ~50 seconds of pure plan-building overhead.

## Methodology

1. **`\timing on`** via psql — wall-clock ms per SQL statement
2. **`worker.tasks.duration_ms`** — per-task breakdown within `process_tasks`
3. **Hot-patched `import_job_process_batch`** — per-step timing via `RAISE NOTICE` with `clock_timestamp()`
4. **`sql_saga.log_step_timing = 'on'`** — built-in temporal_merge PLAN/EXECUTE phase timing
5. **Cache hit testing** — two consecutive imports of same definition type

## Detailed Findings

### temporal_merge is called 4 times per legal_unit import

Each import job with legal units calls temporal_merge for these processing steps:

| Processing Step | temporal_merge calls | Notes |
|----------------|---------------------|-------|
| legal_unit | 2 (demotion check + main merge) | Heaviest step |
| physical_location | 1 | |
| primary_activity | 1 | |
| postal_location | 0 (no data → skipped) | Fast at 2-3ms |
| secondary_activity | 0 (no data → skipped) | Fast at 2-3ms |
| contact | 0 (no temporal_merge) | Fast at 2-3ms |
| statistical_variables | 0 (no temporal_merge) | Fast at 3-7ms |
| tags | 0 (no temporal_merge) | Fast at <1ms |

### PLAN vs EXECUTE phase timing (1-row import)

```
SET sql_saga.log_step_timing = 'on';
```

| Call | Target Table | PLAN (ms) | EXECUTE (ms) | PLAN % |
|------|-------------|-----------|-------------|--------|
| legal_unit (demotion) | legal_unit | 184 | 25 | 88% |
| legal_unit (main) | legal_unit | 228 | 29 | 89% |
| physical_location | location | 250 | 28 | 90% |
| primary_activity | activity | 267 | 29 | 90% |
| **Total** | | **929** | **111** | **89%** |

**The PLAN phase consumes 89% of temporal_merge time.**

### Cache is NOT hitting across import jobs

Tested with two consecutive imports using the same import definition (`legal_unit_source_dates`):

```
Import #1 (cold):  PLAN times: 179, 230, 249, 258 ms
Import #2 (warm):  PLAN times: 271, 265, 284, 267 ms  ← NO improvement
```

**Root cause**: Each import job creates a different source data table (e.g., `test_c1_data`, `test_c2_data`), giving each a different OID.

- **L1 cache** (temp table): Keyed by `source_table::oid` → always misses across jobs
- **L2 cache** (persistent): Keyed by schema structure (no OIDs) → should hit but appears not to work within the same transaction, or the schema-based key differs for some other reason

### Idempotent re-imports show no speedup

When re-importing identical data (all rows get `SKIPPED_IDENTICAL` in analysis phase), the processing phase still takes the same time:

| Import | legal_unit (ms) | physical_location (ms) | primary_activity (ms) |
|--------|----------------|----------------------|---------------------|
| First import (cold) | 470 | 268 | 272 |
| Second import (warm) | 622 | 310 | 306 |
| Idempotent re-import | 600 | 315 | 299 |

Even when temporal_merge discovers all rows are identical and skips the actual merge, the PLAN phase (introspection + SQL generation) still runs at full cost.

### What the PLAN phase actually does (2908-line function)

From examining `sql_saga.temporal_merge_plan`:

1. **Pre-cache introspection** (runs every call, even on cache hit):
   - Query `pg_class` + `pg_namespace` for target table schema/name
   - Query `sql_saga.era` for temporal boundary column info
   - Query `sql_saga.unique_keys` for mutually exclusive columns
   - Query `pg_constraint` + `pg_attribute` for primary key columns
   - Parse and normalize lookup keys (jsonb operations)
   - Generate L1 cache key (format string with 13 components)
   - Generate L2 cache key via `temporal_merge_cache_key()`
   - Compute source columns hash via `temporal_merge_source_columns_hash()`

2. **On cache miss** (currently: every call):
   - Consolidated column validation (~6 `pg_attribute` queries batched into 1)
   - Column list generation with type introspection
   - Join condition building with nullability checks
   - ~20 intermediate temp table DDL generation
   - Full SQL generation for plan, execute, and feedback phases
   - L2 cache store

3. **On cache hit** (currently: never happens across jobs):
   - L2 → L1 promotion with `{{SOURCE_TABLE}}` placeholder substitution
   - Would reduce to ~20-40ms

## Optimization Recommendations

### 1. Fix L2 cache to hit across import jobs (HIGH impact)

**Expected savings: ~800ms per import call (4× speedup on processing phase)**

The L2 cache key uses schema-based identification (no OIDs), which should match across import jobs using the same definition. Investigate why it's not hitting:

- Is `temporal_merge_cache_lookup` returning NULL within the same transaction?
- Does the source columns hash differ between jobs of the same definition?
- Is the L2 cache table (`sql_saga.temporal_merge_cache`?) being populated?

**Verification test**:
```sql
SET sql_saga.log_step_timing = 'on';
-- Add RAISE NOTICE for L1/L2 hit/miss in temporal_merge_plan
-- Run two consecutive imports of same definition type
-- Check if L2 hits on second import
```

### 2. Short-circuit temporal_merge when source has no actionable rows (MEDIUM impact)

**Expected savings: ~200-600ms per idempotent import**

Before calling `temporal_merge_plan`, check if the source table has any rows that actually need merging. For idempotent re-imports where all rows were marked `action = 'skip'` in analysis, skip the entire temporal_merge call.

This could be done in the caller (`import.process_legal_unit` etc.) or as an early return in `temporal_merge` itself:

```sql
-- Early return if source table is empty or all rows are skipped
EXECUTE format('SELECT EXISTS(SELECT 1 FROM %s LIMIT 1)', source_table) INTO v_has_rows;
IF NOT v_has_rows THEN RETURN; END IF;
```

### 3. Reduce pre-cache introspection overhead (MEDIUM impact)

**Expected savings: ~30-50ms per call even with cache hit**

The pre-cache block (steps before cache lookup) runs every call and includes multiple catalog queries. Consider:

- Caching the pre-cache introspection results (era info, PK columns, etc.) in L1
- Using a simpler L1 key that doesn't require `temporal_merge_source_columns_hash` computation
- Moving some introspection into the cache-miss block

### 4. Reduce EXECUTE phase overhead for small batches (LOW impact)

**Expected savings: ~10-15ms per call**

The EXECUTE phase creates ~25 temp tables with `ON COMMIT DROP`. For 1-row imports, this is disproportionate. Consider:

- Materializing fewer intermediate results for small batches
- Using CTEs instead of temp tables when row count is below a threshold

## Raw Profiling Data

### Per-test process_tasks timing

| Test | Duration | Import time | Analytics time | Import calls |
|------|----------|------------|---------------|-------------|
| 310_idempotent_import_source_dates | 45.4s | 24.2s | 9.8s | 4 |
| 307_test_lu_enterprise_link | 39.4s | 29.6s | 0.5s | 19 |
| 320_test_enterprise_name_preservation | 35.7s | 21.0s | 3.1s | 13 |
| 304_test_complex_happy_path | 31.4s | 23.0s | 0.9s | 10 |
| 303_import_jobs_for_norway_small_history | 28.1s | 19.8s | 1.6s | 9 |

### Per-import call timing (from `\timing on`)

Test 307 (19 import calls, 1-3 rows each):
```
1299, 1509, 1536, 1872, 1505, 1606, 1468, 1684, 1487, 1683,
1513, 1756, 1548, 1771, 1442, 1597, 248, 1667 ms
Average: ~1558ms (excluding the 248ms outlier which had 0 rows)
```

Test 310 (4 import calls):
```
Import #1 (initial, 3 jobs):  9644ms  (3 jobs × ~3.2s each)
Import #2 (idempotent):      12993ms  (3 jobs × ~4.3s — SLOWER than initial!)
Import #3 (turnover):          804ms  (1 job, small data)
Import #4 (turnover idem):     802ms  (1 job, small data)
```

### worker.tasks breakdown (test 310, initial import)

```
command                | tasks | total_ms | avg_ms | max_ms
import_job_process     |   141 |     9794 |   69.5 |   3016

Per-job breakdown (3 import jobs):
- Job 1 (LU, 76 rows):     ~50 tasks, max task 2609ms
- Job 2 (Formal ES):       ~45 tasks, max task 2773ms
- Job 3 (Informal ES):     ~46 tasks, max task 3016ms

The max-duration task in each job = the processing_data phase = temporal_merge calls.
All other tasks (analysis batches): 1-40ms each.
```

### Analytics pipeline timing (for reference — NOT the bottleneck)

```
command                              | tasks | total_ms | avg_ms
derive_statistical_history_facet_period |   168 |      342 |    2.0
statistical_unit_refresh_batch          |     1 |      197 |  197.3
derive_statistical_history_period       |   168 |      131 |    0.8
statistical_unit_flush_staging          |     1 |       43 |   42.6
statistical_unit_facet_reduce           |     1 |       39 |   39.3
derive_statistical_unit                 |     1 |       33 |   33.4
derive_statistical_history              |     1 |       20 |   20.2
derive_statistical_history_facet        |     1 |       17 |   16.9
derive_statistical_unit_facet_partition |     4 |        7 |    1.7
derive_reports                          |     1 |        6 |    5.9
collect_changes                         |     1 |        1 |    1.1
Total analytics: ~839ms
```

## Reproduction Steps

All profiling scripts are in `statbus_speed/tmp/`:

```bash
# Per-step timing with hot-patched batch processor
psql < tmp/profile_step_timing.sql

# temporal_merge PLAN/EXECUTE timing
psql < tmp/profile_temporal_merge.sql

# Cache hit/miss testing
psql < tmp/profile_cache.sql

# Full test timing (sequential, clean)
for t in 310 307 304 303 320; do
  psql < tmp/profile_${t}_timing.sql > tmp/profile_${t}_seq.log 2>&1
done

# Extract timing summary
bash tmp/extract_timing.sh tmp/profile_310_seq.log
```

---

## Resolution: Native Rust Planner (2026-02-21)

The native Rust sweep-line planner is now the **default** planner for `temporal_merge`. It replaces the PL/pgSQL plan generator entirely for the planning phase, eliminating the bottleneck identified above.

### What changed

The PL/pgSQL planner generated ~20 intermediate SQL statements (temp tables, CTEs) on every call, even with caching — the "plan phase" was essentially a SQL code generator. The native Rust planner does all planning (atomic segmentation, payload resolution, coalescing, diff, operation classification) in memory using a sweep-line algorithm. No intermediate SQL is generated. The result is written directly to `pg_temp.temporal_merge_plan`.

To fall back to PL/pgSQL: `SET sql_saga.temporal_merge.use_plpgsql_planner = true;`

### Benchmark results: 1.1M legal units + 800K establishments

| Metric | PL/pgSQL | Native Rust | Speedup |
|--------|----------|-------------|---------|
| Total time | 2426s (40min) | 237s (4min) | **10.2x** |
| LU load (1.1M rows) | 1408s @ 781 rows/sec | 105s @ 10,476 rows/sec | **13.4x** |
| ES load (800K rows) | 980s @ 816 rows/sec | 95s @ 8,382 rows/sec | **10.3x** |
| LU ms/batch scaling | 320 → 8,746 (REGRESSION) | 156 → 213 (O(1)) | **constant** |
| ES ms/batch scaling | 406 → 5,090 (REGRESSION) | 212 → 260 (O(1)) | **constant** |

### How each recommendation is addressed

**1. L2 cache across import jobs → ELIMINATED**

The native planner doesn't use the SQL-based L2 cache at all. Instead it uses a multi-entry in-process cache (`HashMap<u64, CachedState>`) that holds one entry per distinct `(target_table, mode, era, identity_columns, ...)` configuration. This cache:

- Hits across import jobs within the same backend connection (same session)
- Holds multiple entries simultaneously, so alternating between 4 target tables (the StatBus pattern) doesn't cause thrashing
- Costs ~0ms on hit (no SPI, no catalog queries, no SQL generation)
- The cache key is a hash of all configuration parameters; source table OID and column structure are checked separately for invalidation

**2. Short-circuit empty sources → STILL RELEVANT**

The native planner still reads source rows before discovering they're empty. The early-return optimization in the PL/pgSQL wrapper (`temporal_merge` procedure) should still be added for the no-rows case. However, the cost of a no-op native plan (read 0 source rows, emit 0 plan rows) is now ~2-5ms instead of ~200ms, so the urgency is much lower.

**3. Pre-cache introspection overhead → ELIMINATED**

The native planner's introspection runs only on cache miss. On cache hit, zero SPI calls are made before reading source/target rows. The introspection results (era info, PK columns, column layouts, SQL templates) are all cached in the `CachedState`.

**4. EXECUTE phase overhead → UNCHANGED**

The executor is still PL/pgSQL. It still creates ~25 temp tables with `ON COMMIT DROP`. This is now the dominant cost per call (~25-30ms). Optimizing the executor is a separate project.

### Per-connection cache architecture

The native planner maintains four levels of cached state within a backend connection:

| Cache | Key | Contents | Lifetime |
|-------|-----|----------|----------|
| `PLANNER_CACHE` | config hash (target + mode + columns) | Introspection results, column layouts, SQL templates | Connection |
| `EMIT_STMT` | (shared) | Prepared INSERT for `pg_temp.temporal_merge_plan` | Connection |
| `TARGET_READ_STMTS` | target SQL template text | Prepared SELECT for target rows | Connection |
| `SOURCE_READ_STMTS` | full source SQL text | Prepared SELECT for source rows | Connection |

All caches are multi-entry HashMaps that grow organically (bounded by the number of distinct configurations used in the session). For the StatBus pattern (4 target tables × 1 mode = 4 configs), the cache stabilizes after the first import cycle and every subsequent call is a full hit.

### Expected impact on StatBus profiling numbers

The PLAN phase that consumed 89% of temporal_merge time (929ms of 1,040ms per import) should drop to ~5-15ms on cache hit. Combined with the ~25-30ms executor, total temporal_merge time per import call should be ~30-45ms — a **20-30x improvement** over the profiled numbers.

For the 5 slowest tests (54 import calls, ~50s of plan overhead), the native planner should save ~48-49s of that, bringing the ~180s total down to ~130s.
