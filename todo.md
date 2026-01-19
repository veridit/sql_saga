# SQL Saga - TODO

A living document of upcoming tasks.
Tasks are checked [x] when done, made brief and moved to the '# Done' section.

## Current Priority - Refactoring & API Improvements
- [ ] **Consider removing obsolete FK trigger columns from schema:** After migrating to native PostgreSQL 18 temporal FKs, the following columns in `sql_saga.foreign_keys` are always NULL for temporal_to_temporal FKs and could potentially be removed:
  - `fk_insert_trigger` - Always NULL (no longer needed with native FKs)
  - `fk_update_trigger` - Always NULL (no longer needed with native FKs)
  - `uk_update_trigger` - Always NULL (no longer needed with native FKs)
  - `uk_delete_trigger` - Always NULL (no longer needed with native FKs)
  - However, these are still used for `regular_to_temporal` FKs and kept for backward compatibility. Removal would complicate upgrades and lose historical schema documentation.
- [ ] **Automate README.md example testing:** Investigate and implement a "literate programming" approach to ensure code examples in `README.md` are automatically tested. This could involve generating a test file from the README or creating a consistency checker script.
- [ ] **Improve test documentation:** Clarify the purpose of complex or non-obvious test cases, such as expected failures.
- [ ] Use existing values when splitting and making a split segment with insert. I.e. edit_at should not be nulled and set by now(), it should be preserved.

## Future Work & New Features
- [ ] **Package `sql_saga` with pgxman for distribution:**
  - **Issue:** The extension currently requires manual installation.
  - **Action:** Create configuration files and a process to package the extension using `pgxman` for easier distribution and installation.

# Done

## 2026-01-19: Performance Optimization Phase Complete

**All PL/pgSQL-level optimizations completed.** Performance status:
- Regular DML: ~24,000-45,000 rows/s
- temporal_merge single call (1000 rows): ~270-580 rows/s (depends on operation complexity)
- temporal_merge batched (1000 rows/call in loop): ~2,650-2,930 rows/s (MERGE_ENTITY_UPSERT), ~7,460-8,310 rows/s (UPDATE_FOR_PORTION_OF)

Further performance gains require either template-based temporal_merge functions (3-5x, HIGH complexity) or PostgreSQL extension in C (10-50x, VERY HIGH complexity). See doc/temporal_merge_postgresql_extension_plan.md for extension approach.

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
