# SQL Saga - TODO

A living document of upcoming tasks.
Tasks are checked [x] when done, made brief and moved to the '# Done' section.

## Current Focus: valid_range API Transition & Performance

### Phase 2: Performance Optimization
- [ ] **Consider C-based sync trigger** - Current PL/pgSQL sync trigger causes 2.4-3.8x overhead
  for INSERT/UPDATE on temporal keys. A C trigger could eliminate most of this overhead.

### Phase 3: temporal_merge Performance Investigation
Performance analysis from benchmarks (after CASE→OR and split-path optimizations):
- Regular DML: ~24,000-45,000 rows/s
- temporal_merge (no batching): ~75-155 rows/s (200-300x slower!)
- temporal_merge (batch 1000): ~2,800-3,000 rows/s (optimal batch size)

Current bottlenecks from pg_stat_monitor:
- **UPDATE execution: 13ms per row** (13,095ms for 1000 rows) - Major bottleneck!
  - Uses LATERAL jsonb_populate_record() for each row
  - Missing compound index on (id, valid_range)
  - 1.4M buffer hits for 1000 rows
- resolved_atomic_segments_with_payloads: Optimized with split-path approach
- source_with_eclipsed_flag: ~199ms (CROSS JOIN LATERAL)

Completed optimizations (2026-01-18):
- [x] **Eliminated LATERAL jsonb_populate_record** - Direct JSONB extraction provides ~48% UPDATE speedup
- [x] **Added old_valid_range/new_valid_range to plan** - Pre-computed ranges eliminate runtime construction
- [x] **Updated executor to use range columns** - No more daterange() construction at execution time

Completed optimizations (2026-01-18):
- [x] **Eliminated LATERAL jsonb_populate_record** - Direct JSONB extraction provides ~48% UPDATE speedup
- [x] **Added old_valid_range/new_valid_range to plan** - Pre-computed ranges eliminate runtime construction
- [x] **Updated executor to use range columns** - No more daterange() construction at execution time
- [x] **Added compound index on (id, valid_range)** - ~20x speedup for exact match queries

Remaining optimizations:
- [ ] **Consider typed temp tables** - Eliminate range type casting (e.g., ::daterange) overhead
- [ ] **Review CROSS JOIN LATERAL in source_with_eclipsed_flag** - ~200ms for eclipse detection

## Medium Priority - Refactoring & API Improvements
- [ ] **Consider removing obsolete FK trigger columns from schema:** After migrating to native PostgreSQL 18 temporal FKs, the following columns in `sql_saga.foreign_keys` are always NULL for temporal_to_temporal FKs and could potentially be removed:
  - `fk_insert_trigger` - Always NULL (no longer needed with native FKs)
  - `fk_update_trigger` - Always NULL (no longer needed with native FKs)
  - `uk_update_trigger` - Always NULL (no longer needed with native FKs)
  - `uk_delete_trigger` - Always NULL (no longer needed with native FKs)
  - However, these are still used for `regular_to_temporal` FKs and kept for backward compatibility. Removal would complicate upgrades and lose historical schema documentation.
- [ ] **Automate README.md example testing:** Investigate and implement a "literate programming" approach to ensure code examples in `README.md` are automatically tested. This could involve generating a test file from the README or creating a consistency checker script.
- [ ] **Improve test documentation:** Clarify the purpose of complex or non-obvious test cases, such as expected failures.
- [ ] Use existing values when splitting and making a split segment with insert. I.e. edit_at should not be nulled and set by now(), it should be preserved.

## Low Priority - Future Work & New Features
- [ ] **Package `sql_saga` with pgxman for distribution:**
  - **Issue:** The extension currently requires manual installation.
  - **Action:** Create configuration files and a process to package the extension using `pgxman` for easier distribution and installation.

# Done

## 2026-01-18
- Add compound index (id, valid_range) to add_unique_key for temporal_merge performance
- Optimize temporal_merge executor: eliminate LATERAL jsonb_populate_record (~48% UPDATE speedup)
- Add old_valid_range/new_valid_range columns to temporal_merge_plan
- Update executor to use pre-computed range columns

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
