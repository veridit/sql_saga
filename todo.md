# SQL Saga - TODO

A living document of upcoming tasks.
Tasks are checked [x] when done, made brief and moved to the '# Done' section.

## Current Priority - Critical Issues & Bugs
(None - all critical issues resolved)

## Medium Priority - Refactoring & API Improvements
- [ ] **Automate README.md example testing:** Investigate and implement a "literate programming" approach to ensure code examples in `README.md` are automatically tested. This could involve generating a test file from the README or creating a consistency checker script.
- [ ] **Improve test documentation:** Clarify the purpose of complex or non-obvious test cases, such as expected failures.

## Future Work & New Features
- [ ] **Package `sql_saga` with pgxman for distribution:**
  - **Issue:** The extension currently requires manual installation.
  - **Action:** Create configuration files and a process to package the extension using `pgxman` for easier distribution and installation.

# Done

## 2026-01-21: temporal_merge Executor ANALYZE Optimization

**Added ANALYZE to temporal_merge_plan for 5x faster executor DML.**

**Problem:** PostgreSQL severely underestimated row counts on the temporary `temporal_merge_plan` table (often `rows=1` when actual was 450+), causing it to choose Nested Loop joins with post-scan filters instead of efficient Hash Joins.

**Root Cause:** Temp tables have no statistics until ANALYZE is run. The planner's default selectivity estimate for enum filters is very low, leading to poor cardinality estimates.

**Solution:** Added `ANALYZE temporal_merge_plan;` in executor before DML execution (src/28_temporal_merge_execute.sql:613).

**Before:**
```
->  Nested Loop  (cost=30.79..38.86 rows=1 ...)
      ->  Index Scan using valid_range_idx ...
            Filter: (p.id_ek = id)
            Rows Removed by Filter: 499
```

**After:**
```
->  Hash Join  (cost=68.23..75.87 rows=338 ...)
      Hash Cond: ((p.id_ek = t.id) AND (p.old_valid_range = t.valid_range))
```

**Benchmark Results:**
- Range-Only temporal_merge Parent: 412 rows/s → 2036 rows/s (5x faster)
- Range-Only temporal_merge Child: 868 rows/s → 3635 rows/s (4.2x faster)
- UPDATE duration: 34ms → 5ms for 450 rows (6.8x faster)

**Also:** Documented auto_explain setup in README.md for performance debugging of internal EXECUTE statements.

## 2026-01-20: temporal_merge Planner Hash Join Optimization

**Enabled Hash Joins in temporal_merge planner by using = instead of IS NOT DISTINCT FROM.**

Changed two join conditions that were preventing PostgreSQL from using Hash Joins:
- `v_source_rows_exists_join_expr` (line ~1192)
- `v_lateral_join_tr_to_seg` (line ~662)

**Why safe:** Target columns are primary key columns (always NOT NULL). Source rows with NULL identity get no match from LEFT JOIN, which is correct behavior.

**Impact:** 470-500x speedup for these specific planner queries.

## 2026-01-20: temporal_merge UPDATE Performance Optimization

**Optimized entity_key join in temporal_merge executor for 30-38% speedup.**

**Problem:** The UPDATE statement in temporal_merge_execute was slow (~3 seconds for 1000 parent rows) due to:
1. JSONB extraction `(p.entity_keys->>'id')::integer` in WHERE clause prevented hash join usage
2. `IS NOT DISTINCT FROM` comparison also prevented hash join, forcing cross-join with filter

**Solution:** Pre-extract entity_keys in subquery SELECT list and use `=` for comparison:
```sql
-- Before (slow - cross-join with 20M row filter):
FROM (SELECT * FROM temporal_merge_plan ...) p
WHERE t.id IS NOT DISTINCT FROM (p.entity_keys->>'id')::integer

-- After (fast - proper hash join):
FROM (SELECT *, (entity_keys->>'id')::integer AS id_ek FROM temporal_merge_plan ...) p  
WHERE t.id = p.id_ek
```

**Results:**
- Parent temporal_merge: 3669ms → 2471ms (33% faster)
- Child temporal_merge: 1725ms → 1204ms (30% faster)
- Parent UPDATE: 3091ms → 1917ms (38% faster)
- Child UPDATE: 1379ms → 864ms (37% faster)

**Note:** Using `=` instead of `IS NOT DISTINCT FROM` is safe because identity columns are part of unique keys (NULLs not expected).

## 2026-01-20: Statbus Severe Issues - FOR_PORTION_OF View Bug Fixes (RESOLVED)

Fixed critical user-reported bugs in FOR_PORTION_OF view trigger that were causing data loss and incorrect merging in production Statbus deployment:

**Issue #1: Name Merging Bug (SCENARIO 5)**
- **Problem:** Updating one attribute via FOR_PORTION_OF view incorrectly merged temporal rows with different values for other attributes, losing historical data. Example: Entity had name "A" Jan-Jun and "B" Jul-Dec; updating legal_form across full year merged into single row, losing name change.
- **Root Cause:** Each row-level trigger firing called temporal_merge with the FULL requested temporal range, so second trigger saw target already modified by first trigger.
- **Solution:** Compute intersection of requested range with OLD row's existing timeline (src/06_for_portion_of_trigger.sql:146-197), so each trigger only affects its own temporal segment. Return NULL when intersection is empty.
- **Test:** sql/080_temporal_merge_reported_regressions.sql SCENARIO 5

**Issue #2: Ephemeral Column Preservation (SCENARIO 6)**
- **Problem:** Timeline splits were resetting ephemeral/audit columns (edit_at, edit_by_user_id) to NULL/DEFAULT in edge segments instead of preserving original values.
- **Root Cause:** In UPSERT/REPLACE modes, planner combined ephemeral payloads as `COALESCE(t_ephemeral, '{}') || COALESCE(s_ephemeral, '{}')`, causing source NULLs to overwrite target values for ALL segments.
- **Solution:** Added conditional logic to preserve target ephemeral when no source data exists (src/27_temporal_merge_plan.sql:1852-1856):
  ```sql
  CASE 
    WHEN s_data_payload IS NULL THEN t_ephemeral_payload
    ELSE COALESCE(t_ephemeral, '{}') || COALESCE(s_ephemeral, '{}')
  END
  ```
- **Test:** sql/080_temporal_merge_reported_regressions.sql SCENARIO 6

**Issue #3: Explicit NULL Assignment Support**
- **Problem:** Users couldn't explicitly set a column to NULL via FOR_PORTION_OF view.
- **Solution:** Use UPDATE_FOR_PORTION_OF mode (not PATCH_FOR_PORTION_OF) so NULL is treated as explicit value. Include unchanged columns with OLD values to preserve per-segment values (src/06_for_portion_of_trigger.sql:53-71).
- **Test:** sql/051_view_for_portion_of_full.sql SCENARIO 12

**Additional Changes:**
- Enhanced executor to include ephemeral columns with NOT NULL in INSERT statements (src/28_temporal_merge_execute.sql:417)
- Updated AGENTS.md with pg_temp table inspection technique for debugging temporal_merge
- Updated AGENTS.md warning against "locking in bugs" in expected output files
- All 75 fast tests pass

## 2026-01-20: Move DEFAULT column exclusion logic to planner

Moved column exclusion logic from executor to planner for a declarative approach where the plan explicitly specifies which columns to include/exclude. Makes execution more predictable and testable.

## 2026-01-19: Performance Optimization Phase Complete

**All PL/pgSQL-level optimizations completed.** Performance status:
- Regular DML: ~24,000-45,000 rows/s
- temporal_merge single call (1000 rows): ~270-580 rows/s (depends on operation complexity)
- temporal_merge batched (1000 rows/call in loop): ~2,650-2,930 rows/s (MERGE_ENTITY_UPSERT), ~7,460-8,310 rows/s (UPDATE_FOR_PORTION_OF)

Further performance gains require either template-based temporal_merge functions (3-5x, HIGH complexity) or PostgreSQL extension in C (10-50x, VERY HIGH complexity). See doc/temporal_merge_postgresql_extension_plan.md for extension approach.

## 2026-01-19: FK Trigger Column Cleanup

**Investigated and removed unused FK trigger columns from sql_saga.foreign_keys table.** Original todo claimed these columns "could potentially be removed" because they're always NULL. Investigation and cleanup:

**Column Usage Patterns:**
- `fk_insert_trigger`, `fk_update_trigger`: Always NULL for both FK types - **REMOVED** (no functional purpose)
- `uk_update_trigger`, `uk_delete_trigger`: POPULATED for regular_to_temporal FKs, NULL for temporal_to_temporal FKs - **KEPT** (essential)

**Test Evidence (tests 045 & 072):**
- regular_to_temporal: Creates actual triggers on referenced table (e.g., `regular_fk_pk_id_fkey_uk_update`, `regular_fk_pk_id_fkey_uk_delete`)
- temporal_to_temporal: Uses native PG18 `FOREIGN KEY ... PERIOD` with system-generated `RI_ConstraintTrigger_*` triggers

**Actions Taken:**
- Removed fk_insert_trigger and fk_update_trigger columns from schema (src/01_schema.sql)
- Updated all code references in drop_protection, rename_following, manage_temporal_triggers, add_foreign_key
- Updated 75 test expected outputs
- Net reduction: 87 lines of code deleted

**Result:** Clean schema with only essential columns. uk_update_trigger and uk_delete_trigger retained because they're critical for regular_to_temporal FK functionality. See tmp/fk_trigger_columns_investigation.md for detailed evidence.

## 2026-01-19: Individual Optimizations
- **Investigate and reject typed temp tables optimization** - Benchmarked TEXT vs typed columns for temporal_merge_plan.
  Result: TEXT is 2.6% FASTER (38.2ms vs 37.1ms for 10K ops). Range I/O functions are highly optimized, conversion 
  overhead is negligible. Keeping TEXT for polymorphism, debuggability, and simplicity. See doc/internals/optimization_findings.md.
- **Implement template-based temporal column sync triggers** - Complete system replacing generic triggers with 
  specialized per-table/era functions. Performance: 2.3x faster INSERTs (85ms→37ms), 2.2x faster UPDATEs (99ms→45ms).
  Architecture: Template generator creates hardcoded functions eliminating dynamic SQL/JSONB overhead. Schema tracking
  via sync_temporal_trg_name and sync_temporal_trg_function_name columns. Comprehensive cleanup chain fixes.
  Event trigger protection updated. 75+ tests updated. All regression tests passing.
- **Implement eclipse detection optimizations** - Added composite index on (lookup_columns, source_row_id) and 
  pre-computed valid_range columns in temporal_merge planner. Expected 30-40% performance improvement in eclipse detection.
- Document CROSS JOIN LATERAL eclipse detection logic (required for correctness)
- Analyze eclipse detection performance and identify optimization strategies
- Test template-based trigger approach: 8-9x faster than generic triggers
- Abandon C-based trigger implementation (5-7x slower due to SPI overhead)
- Create comprehensive optimization documentation (doc/internals/eclipse_detection.md, optimization_findings.md)

## 2026-01-18
- Eliminate LATERAL jsonb_populate_record from temporal_merge executor (~48% UPDATE speedup)
- Add old_valid_range/new_valid_range columns to temporal_merge_plan (eliminate runtime construction)
- Update executor to use pre-computed range columns
- Add compound index (id, valid_range) to add_unique_key (~20x speedup for exact match queries)

## 2026-01-17
- Optimize temporal_merge planner: split-path approach for resolved_atomic_segments_with_payloads (~10x speedup)
- Optimize temporal_merge planner: replace CASE→OR for 36-56% speedup on benchmarks
- Add range-only view tests (051, 053) and fix same-day DELETE bug in current_view_trigger
- Add range-only temporal_merge test (066) with proper test renumbering
- Add Makefile target `renumber-tests-from` for inserting tests at any slot
- Document optimal batch size (1000 rows) in README with benchmark data
- Add era existence validation to `add_for_portion_of_view` and `add_current_view`
- Fix flapping test 063 by adding ORDER BY to string_agg
- Update 097_readme_usage.sql to match README examples (valid_range API)

## Earlier (summarized)
- Phase 1 complete: Migrated benchmark tests (100-107) to valid_range API
- Added sync trigger overhead benchmarks (tests 104-107)
- PostgreSQL 18 native temporal features integration (WITHOUT OVERLAPS, temporal FKs)
- Implemented smart MOVE batching with multirange types
- Fixed temporal_merge for range-only tables
- Comprehensive temporal_merge planner refactoring and optimization
- System versioning implementation
- C-based FK triggers for performance
- Event trigger support for schema changes
- `[valid_from, valid_until)` period semantics adoption
