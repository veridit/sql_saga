# Agent Guidelines for sql_saga

This PostgreSQL extension provides temporal table management. This document provides quick-reference guidelines for AI coding agents working on this codebase.

## Quick Reference

- **API Documentation:** See `doc/api.md` for complete function signatures without implementation details
- **Conventions:** See `CONVENTIONS.md` for detailed code style and architecture patterns
- **Tasks:** See `todo.md` for current project status and pending work

## Build, Test, and Lint Commands

### Building
```bash
make                                    # Build extension (C library + SQL)
make install                            # Install extension to PostgreSQL
make clean                              # Clean build artifacts
```

### Testing

**CRITICAL: ALWAYS run tests immediately after making changes to test files!**
```bash
# After editing a test file, ALWAYS run this immediately:
make test TESTS="001_install YOUR_TEST"; make diff-fail-first | head -n 30

# DO NOT just edit and assume it works. Test first, then iterate.
```

**Full test suite:**
```bash
make install && make test; make diff-fail-all              # Run all tests
make install && make test fast; make diff-fail-all         # Run fast tests (exclude benchmarks)
make install && make test benchmark; make diff-fail-all    # Run benchmark tests only
make install && make test TESTS="001_install 052_era"; make diff-fail-all  # Run specific tests

# View test diffs
make diff-fail-all                      # Show all test failures (plain diff)
make diff-fail-all vim                  # Review failures interactively with vimdiff
make diff-fail-first vim                # Review first failure only

# Accept new test output
make expected                           # Update expected/*.out from results/ (all tests from last run)
make expected TESTS="test1 test2"      # Update specific tests only
```

### Running Single Test
```bash
# Most tests require 001_install to run first
make install && make test TESTS="001_install YOUR_TEST"; make diff-fail-all
# Newer self-contained tests can run independently (include test_setup.sql)
```

### Efficient Test Logging with tee

**IMPORTANT: Always use `tee` when running test suites** to save output for later inspection without re-running:

```bash
# Save full test output to tmp/*.log and show tail
make test fast 2>&1 | tee tmp/test_fast.log | tail -20
make test benchmark 2>&1 | tee tmp/test_benchmark.log | tail -20
make test TESTS="001_install 080_test" 2>&1 | tee tmp/test_080.log | tail -10

# After tests complete, you can inspect the saved log without re-running:
grep "not ok" tmp/test_fast.log           # Find failing tests
grep "ERROR" tmp/test_fast.log            # Find errors
grep -A 5 "SCENARIO 6" tmp/test_fast.log  # Search specific content
less tmp/test_fast.log                    # Browse full output
```

**Why this matters:**
- Running full test suite takes 30+ seconds
- If something fails, you can investigate the log immediately
- Avoids wasteful re-running of tests just to see earlier output
- Can search/analyze full output after tests complete

**Common workflow:**
```bash
# 1. Run tests with logging
make install && make test fast 2>&1 | tee tmp/test_fast.log | tail -20

# 2. If failure, find which test without re-running
grep "not ok" tmp/test_fast.log

# 3. Investigate specific test output
grep -A 50 "not ok 58.*080_" tmp/test_fast.log

# 4. Check for specific errors
grep "ERROR" tmp/test_fast.log | head -20
```

### Database Commands
```bash
psql -d sql_saga_regress                                 # Connect to test database
echo 'SELECT * FROM sql_saga.era;' | psql -d sql_saga_regress  # Run query
psql -d sql_saga_regress < tmp/verify_fix.sql            # Execute script
```

### C Development (LSP Setup)
```bash
make compile_commands.json              # Generate LSP compilation database
```
This creates `compile_commands.json` with PostgreSQL include paths from `pg_config`, enabling clangd and other LSPs to resolve headers correctly.

## Code Style Guidelines

### SQL Code

**Function/Procedure Definitions:**
- Use function name in dollar-quote: `AS $function_name$`
- Specify `LANGUAGE plpgsql` before body
- Use long-form parameters: `param_name param_type`
- Use named arguments for calls with 3+ arguments: `arg1 => val1`

```sql
CREATE FUNCTION public.example(email text) RETURNS void LANGUAGE plpgsql AS $example$
BEGIN
    SELECT * FROM some_table st WHERE st.email = example.email;
END;
$example$;
```

**Dynamic SQL with format():**
- Always use dollar-quoting: `format($$ ... $$)` to avoid escaping
- Use named dollar-quotes for nesting: `$SQL$`, `$jsonb_expr$`
- Use numbered placeholders: `%1$I` (identifier), `%2$L` (literal), `%3$s` (string)
- Prefer `EXECUTE ... USING` for arrays over `%L` interpolation

```sql
EXECUTE format($$
    UPDATE public.%1$I AS dt SET last_completed = %2$L
    WHERE dt.row_id = ANY($1)
$$, v_table_name /* %1$I */, v_priority /* %2$L */)
USING p_batch_ids;
```

**Table Aliases:**
- Always use explicit `AS`: `FROM my_table AS mt`
- Data table imports use: `AS dt`

**Temporal Logic:**
- Order chronologically: `start <= point AND point < end`
- Avoid: `point >= start`

**Temporary Tables:**
```sql
IF to_regclass('pg_temp.my_temp') IS NOT NULL THEN DROP TABLE my_temp; END IF;
CREATE TEMP TABLE my_temp (...) ON COMMIT DROP;
```

**Test Isolation with SAVEPOINTs:**
```sql
SAVEPOINT scenario_A;
-- test logic
RELEASE SAVEPOINT scenario_A;

-- For expected failures:
SAVEPOINT expect_error;
SELECT my_buggy_function();  -- Should fail
ROLLBACK TO SAVEPOINT expect_error;
SELECT 'Transaction still active';
```

### SQL Naming Conventions
- `x_id` - Foreign key to table `x`
- `x_ident` - External identifier (not from database)
- `x_at` - TIMESTAMPTZ (with timezone)
- `x_on` - DATE
- `valid_from`, `valid_until` - Temporal periods use `[)` semantics (inclusive start, exclusive end)

### C Code (PostgreSQL Extensions)

**C99 Compliance:**
- Declare ALL variables at block start (after `{`)
- Never mix declarations and statements (triggers `-Wdeclaration-after-statement`)
- Follow PostgreSQL coding conventions

**regclass Handling in C:**
- Always cast `regclass` to `oid` in SQL: `SELECT table_oid::oid ...`
- Use `OIDOID` parameter type in `SPI_execute_with_args`
- Do OID-to-name lookups in C (after query completes) using `regclassout`

### File Organization

**Source Files:**
- SQL: `src/[0-9][a-z]_*.sql` (uses Na_ pattern: `0a_`, `0b_`, ..., `1a_`, etc.)
  - This provides 26 positions per digit group instead of 10 with pure numeric prefixes
  - Auto-discovered by Makefile glob pattern
- C files: `src/*.c` and `src/*.h`
- Tests: `sql/[0-9][0-9][0-9]_*.sql` (auto-discovered)
- Expected output: `expected/[testname].out`
- Benchmark outputs: `expected/performance/*.csv` and `*.perf`
- New tests should be self-contained (include `test_setup.sql` and `test_teardown.sql`)

**Adding Features:**
- Create numbered SQL file: `src/3e_my_feature.sql` (auto-included in build)
- No Makefile edits needed

**Key Source Files:**
- `src/0b_schema.sql` - Core catalog tables (`era`, `unique_keys`, `foreign_keys`)
- `src/2k_temporal_merge_plan.sql` - temporal_merge planner
- `src/2l_temporal_merge_execute.sql` - temporal_merge executor
- `src/sql_saga.c` - C extension code (FK triggers, history table support)

### Test Structure

**Test Categories:**
- `001_install` - Extension installation (required by older tests)
- `002-009` - Era management tests
- `010-019` - Unique key tests
- `020-029` - System versioning tests
- `030-039` - Aggregate function tests
- `040-049` - Foreign key tests
- `050-059` - View tests (for_portion_of, current)
- `060-099` - temporal_merge tests
- `100+` - Benchmark tests

**Self-Contained Test Template:**
```sql
\i sql/include/test_setup.sql

BEGIN;
-- Test schema and data setup
-- Test scenarios using SAVEPOINTs
COMMIT;

\i sql/include/test_teardown.sql
```

## Development Workflow

### Hypothesis-Driven Development Cycle

**ALL development must follow this cycle:**

1. **Hypothesize** - State hypothesis in `tmp/journal.md`
2. **Isolate** - Create/identify reproducing test
3. **Prototype** - Create non-destructive verification script (`tmp/verify_fix.sql`)
4. **Observe** - Run prototype: `psql -d sql_saga_regress < tmp/verify_fix.sql`
5. **Analyze** - Confirm hypothesis or return to step 1
6. **Implement** - Apply permanent changes
7. **Validate** - Run full regression tests
8. **Conclude** - Update `todo.md`

**Key Principles:**
- Never skip verification steps
- Use `EXPLAIN (ANALYZE, BUFFERS)` for performance verification
- Maintain stateless mindset - base analysis on latest data only
- Embrace falsifiability - plans are provisional until proven

### Debugging Tools

**Inspecting pg_temp Tables (CRITICAL for debugging temporal_merge):**

When debugging temporal_merge behavior, you can inspect intermediate temp tables by querying them within a transaction before they're dropped:

```sql
-- In a test file (e.g., sql/999_debug_test.sql):
BEGIN;

-- Execute the operation that calls temporal_merge
UPDATE my_table__for_portion_of_valid
SET some_column = value, valid_from = '2023-01-01', valid_until = '2026-01-01'
WHERE id = 1;

-- Immediately query pg_temp tables before COMMIT drops them
TABLE pg_temp.source_rows;           -- What source data was provided?
TABLE pg_temp.target_rows;           -- What was in the target?
TABLE pg_temp.resolved_atomic_segments;  -- What segments were created?
TABLE pg_temp.island_group;          -- How were segments grouped for coalescing?
TABLE pg_temp.coalesced_final_segments;  -- What was produced after coalescing?

COMMIT;
```

Then run the test and view output:
```bash
make install && make test TESTS="999_debug_test"
make diff-fail-first  # View the full table contents in the diff output
```

**Key temp tables to inspect:**
- `source_rows` - Input data after initial processing
- `target_rows` - Existing target table data
- `resolved_atomic_segments` - After payload resolution (shows data_payload)
- `island_group` - After grouping adjacent segments with same payload
- `coalesced_final_segments` - Final merged segments before diff
- `diff` - Comparison between final segments and target
- `plan_with_op` - Planned operations (INSERT/UPDATE/DELETE)

**When to use:**
- Understanding why rows are being merged unexpectedly
- Debugging payload resolution logic
- Tracing how source data flows through the planner
- Investigating why segments aren't being split correctly

**Important notes:**
- Tables are created with `ON COMMIT DROP`, so query them BEFORE commit
- Use `BEGIN...COMMIT` to keep transaction open during inspection
- Can temporarily add to existing tests for debugging
- `enable_trace = true` is NOT required - that's for advanced join debugging

**Add diagnostic output:**
- Prefer `RAISE DEBUG` over `RAISE NOTICE`
- Wrap test sections with:
```sql
SET client_min_messages TO DEBUG;
-- test commands
RESET client_min_messages;
```

**temporal_merge GUCs for debugging:**
```sql
SET sql_saga.temporal_merge.log_plan = true;     -- Log execution plan
SET sql_saga.temporal_merge.log_feedback = true; -- Log row-by-row feedback
SET sql_saga.temporal_merge.log_sql = true;      -- Log generated SQL
SET sql_saga.temporal_merge.enable_trace = true; -- Enable trace column
```

## RAISE NOTICE Usage Guidelines

**Core Principle:** "When we do a side effect, we inform the caller about this, since that may be surprising, and the informational text matches EXACTLY what was done."

### Three Types of Messages

**1. Informational Notices (RAISE NOTICE):**
Use `RAISE NOTICE` to inform callers about side effects that may be surprising. The message must describe exactly what action was performed.

**Message format:** All RAISE NOTICE messages must include the `sql_saga: ` prefix to clearly identify the subsystem generating the message.

**When to use:**
- Table creation, modification, or deletion
- Index creation or rebuilds
- Constraint additions or modifications
- Data migrations or transformations
- Any operation that changes system state beyond the primary function purpose

**Examples:**
```sql
-- Good: Describes the exact side effect with sql_saga prefix
RAISE NOTICE 'sql_saga: Created temporal foreign key constraint % on table %', constraint_name, table_name;
RAISE NOTICE 'sql_saga: Rebuilt unique constraint % to include temporal dimensions', constraint_name;
RAISE NOTICE 'sql_saga: Migrated % rows from % to new temporal format', row_count, table_name;

-- Bad: Vague or internal detail
RAISE NOTICE 'Processing constraint...';
RAISE NOTICE 'Step 3 of migration completed';
```

**2. Debug Messages (RAISE DEBUG):**
Use `RAISE DEBUG` for internal diagnostics, troubleshooting, and development information that normal users don't need to see.

**When to use:**
- Tracing execution flow
- Logging intermediate calculations
- Debugging query plans or performance
- Internal state inspection
- Development diagnostics

**Examples:**
```sql
-- Good: Internal diagnostic information
RAISE DEBUG 'Query plan selected % segments for merge', segment_count;
RAISE DEBUG 'Temporal merge trace: %', trace_data;
RAISE DEBUG 'Function entered with parameters: table=%, period=%', table_name, time_period;

-- Bad: User-relevant side effects (should be NOTICE)
RAISE DEBUG 'Created index on table %', table_name;
```

### Rationale

**Why distinguish these message types:**
- **Side effects can be surprising:** Users calling a function may not expect schema changes, constraint additions, or data migrations
- **Exact description builds trust:** When the message precisely matches the action, users can verify the operation succeeded as intended
- **Clean separation of concerns:** User-facing notifications vs. developer diagnostics serve different audiences
- **Debugging flexibility:** `client_min_messages` can control debug output without hiding important user notifications

**Message content requirements:**
- **NOTICE messages:** Must be user-friendly and match exactly what was done
- **DEBUG messages:** Can include technical details, trace data, and development artifacts
- **WARNING messages:** Should identify the problematic condition and provide actionable guidance
- **All types:** Should include relevant identifiers (table names, constraint names, counts) for context

**3. Warning Messages (RAISE WARNING):**
Use `RAISE WARNING` to alert users about assumption violations, suboptimal configurations, or potentially problematic usage patterns that don't prevent operation but may cause issues.

**When to use:**
- Schema incompatibilities or suboptimal design patterns
- Performance problems or missing indexes
- Data type mismatches that force skipping features
- Configuration issues that violate best practices
- Situations where user assumptions may be incorrect

**Examples:**
```sql
-- Good: Schema compatibility warnings
RAISE WARNING 'Table "%" has a simple PRIMARY KEY that does not include temporal columns. This schema is incompatible with SCD Type 2 history.', table_name;
RAISE WARNING 'Table "%" has a GENERATED ALWAYS AS IDENTITY column ("%"). This schema is incompatible with SCD Type 2 history.', table_name, column_name;

-- Good: Performance warnings with specific guidance
RAISE WARNING 'Performance warning: The source relation % lacks a GIST index on its temporal columns.', table_name
USING HINT = 'For better performance, consider creating an index, e.g., CREATE INDEX ON tablename USING GIST (range_column);';

-- Good: Data quality/configuration issues
RAISE WARNING 'Synchronization column "%" on table % has an incompatible data type (%). It must match the era subtype (%). Skipping synchronization.', col_name, table_name, actual_type, expected_type;

-- Bad: Side effects (should be NOTICE)
RAISE WARNING 'Created index on table %', table_name;
```

### Development Journaling

Use `tmp/journal.md` for complex tasks:
- Log hypotheses and outcomes
- Clear journal between major tasks: `echo -n > tmp/journal.md`
- Never use SEARCH/REPLACE on ephemeral files

## Error Handling

**Fail Fast Philosophy:**
- Fail immediately on unexpected states
- Provide clear error messages with context
- Never mask or work around problems
- Use descriptive error messages for debugging

**CRITICAL: Never "Lock In" Bugs in Expected Output**

When a test fails showing incorrect behavior:

❌ **NEVER DO THIS:**
```bash
# Bad: Accepting broken behavior
make expected TESTS="failing_test"  # This locks in the bug!
```

✅ **ALWAYS DO THIS:**
1. **Read the test carefully** - Understand what the CORRECT behavior should be
2. **Edit the expected output** - Manually set it to show CORRECT behavior
3. **Fix the code** - Modify implementation until test passes with correct output
4. **Verify** - Ensure test passes with the corrected expected output

**Example:**
```
Test shows: edit_by_user_id = 1 (wrong - using default)
Expected:   edit_by_user_id = 100 (correct - preserved from original)

Action: Edit expected/*.out to show "100", then fix the code to make it happen
```

**Why this matters:**
- Locking in bugs means they're forgotten and become "features"
- Tests should document CORRECT behavior, not current bugs
- Expected output files are specifications, not snapshots

## Important Constraints

- **Never** propose SEARCH/REPLACE for `expected/*.out` files
- **Never** add `E` prefix to psql echo strings: use `echo '...\n...'` not `echo E'...\n...'`
- **Never** remove `trace` column from temporal_merge output (zero overhead when disabled)
- **Never** suggest multi-line shell commands (environment splits on newlines)
- **Always** run `make install` before tests

## Architecture Overview

### Core Concepts

**Eras:** A temporal period definition on a table, tracked in `sql_saga.era` catalog
- `range_column_name` - The authoritative range column (e.g., `valid_range daterange`)
- Optional synchronized columns: `valid_from`, `valid_until`, `valid_to`
- `ephemeral_columns` - Columns excluded from coalescing comparison (e.g., audit columns)

**Unique Keys:** Temporal uniqueness enforced via `WITHOUT OVERLAPS` constraint
- Tracked in `sql_saga.unique_keys` catalog
- Types: `primary`, `natural`, `predicated`

**Foreign Keys:** Temporal foreign key relationships
- Types: `temporal_to_temporal`, `regular_to_temporal`
- Implemented via C triggers for performance
- Tracked in `sql_saga.foreign_keys` catalog

### temporal_merge Architecture

The `temporal_merge` procedure is the core data loading mechanism:

1. **Planner** (`src/2k_temporal_merge_plan.sql`):
   - Uses Allen's Interval Algebra for temporal relationships
   - Creates atomic time segments from source and target data
   - Resolves payloads based on merge mode
   - Coalesces adjacent identical segments
   - Generates execution plan with `plan_op_seq`

2. **Executor** (`src/2l_temporal_merge_execute.sql`):
   - Executes DELETE → UPDATE → INSERT order
   - Uses deferred constraints for temporary gaps
   - Provides row-by-row feedback

**Merge Modes:**
- `MERGE_ENTITY_UPSERT` - Insert or partial update (NULL = explicit value)
- `MERGE_ENTITY_PATCH` - Insert or partial update (NULL = keep target)
- `MERGE_ENTITY_REPLACE` - Insert or complete replacement
- `UPDATE_FOR_PORTION_OF` - Surgical update on time slice
- `PATCH_FOR_PORTION_OF` - Surgical patch on time slice
- `REPLACE_FOR_PORTION_OF` - Surgical replace on time slice
- `DELETE_FOR_PORTION_OF` - Surgical delete on time slice
- `INSERT_NEW_ENTITIES` - Insert only new entities

**Delete Modes:**
- `NONE` - No deletions
- `DELETE_MISSING_TIMELINE` - Delete timeline gaps not in source
- `DELETE_MISSING_ENTITIES` - Delete entities not in source
- `DELETE_MISSING_TIMELINE_AND_ENTITIES` - Both

### Performance Benchmarks

From recent optimizations:
- Regular DML: ~24,000-45,000 rows/s
- temporal_merge batched: ~2,650-8,310 rows/s
- Optimal batch size: ~1,000 rows per call

## PostgreSQL 18 Temporal Constraints and Foreign Keys

### Execution Strategy: `with_temporary_temporal_gaps`

**Architectural Decision** (proven by test 073):
sql_saga uses DELETE→UPDATE→INSERT execution order to enable native PostgreSQL 18 temporal foreign keys.

**Why This Strategy:**
- Creates temporary **gaps** (tolerated by DEFERRABLE FKs)
- Avoids temporary **overlaps** (would violate NOT DEFERRABLE unique)
- Enables native `FOREIGN KEY ... PERIOD` syntax
- Preserves optimizer awareness of FK relationships

```sql
-- Example: Timeline split with DELETE-first strategy
SET CONSTRAINTS ALL DEFERRED;  -- Defer FK checks

DELETE FROM parent WHERE id = 1;                    -- Creates gap
INSERT INTO parent VALUES 
    (1, '[2024-01-01,2024-05-01)'),                 -- Closes gap (partial)
    (1, '[2024-05-01,infinity)');                   -- Closes gap (complete)

SET CONSTRAINTS ALL IMMEDIATE;  -- FK check passes (no gaps remain)
```

### PostgreSQL 18 Constraint Requirements

**Unique Constraints:** NOT DEFERRABLE
- Temporal unique constraints use `WITHOUT OVERLAPS`
- Must be NOT DEFERRABLE (immediate checking prevents overlaps)
- Can be referenced by native temporal FKs

**Foreign Key Constraints:** DEFERRABLE
- Native temporal FKs use `FOREIGN KEY ... PERIOD` syntax  
- Must be DEFERRABLE to tolerate temporary gaps
- Checked at transaction end (gaps must be closed by then)

```sql
-- Parent table
CREATE TABLE parent (
    id int,
    valid daterange,
    UNIQUE (id, valid WITHOUT OVERLAPS)  -- NOT DEFERRABLE (default)
);

-- Child table with native temporal FK
CREATE TABLE child (
    parent_id int,
    valid daterange,
    FOREIGN KEY (parent_id, PERIOD valid) 
        REFERENCES parent (id, PERIOD valid)
        DEFERRABLE  -- Required for gap tolerance
);
```

### Execution Order Details

**Operation Order:** DELETE → UPDATE → INSERT
- Defined by enum order in `sql_saga.temporal_merge_plan_action`
- Planner generates `plan_op_seq` based on this ordering
- Executor processes operations sequentially by `plan_op_seq`

**UPDATE Effect Order:** NONE → SHRINK → MOVE → GROW
- Defined by enum order in `sql_saga.temporal_merge_update_effect`  
- NONE first: data-only updates, no timeline impact
- SHRINK before GROW: contract before expanding (minimizes gap duration)
- Within each operation type, sorted by this effect order

### Implementation Details

**Schema** (`src/0b_schema.sql`):
- Documents `with_temporary_temporal_gaps` strategy
- Enum orders define execution sequence
- Comments explain rationale and constraints

**Planner** (`src/2k_temporal_merge_plan.sql`):
- Generates `plan_op_seq` using operation and effect ordering
- Automatically uses DELETE-first based on enum order

**Executor** (`src/2l_temporal_merge_execute.sql`):
- Processes plan by `plan_op_seq` (respects DELETE-first order)
- Sets `CONSTRAINTS ALL DEFERRED` during execution
- Validates constraints at transaction end

**Constraints** (`src/1j_add_unique_key.sql`, `src/2a_add_foreign_key.sql`):
- Unique: NOT DEFERRABLE (prevents overlaps immediately)
- FK: DEFERRABLE (tolerates gaps temporarily)

### Trade-offs and Benefits

✅ **Benefits:**
- Native FK syntax with optimizer awareness
- Declarative constraint definitions
- Proven PostgreSQL 18 compatibility
- Simpler than trigger-based FKs

⚠️ **Requirements:**
- Operations must close gaps by transaction end
- FK checks deferred (not immediate feedback)
- Must use DELETE-first order (not INSERT-first)

## Common Pitfalls

1. **MVCC in PL/pgSQL** - Set-based queries and loops see same snapshot within function
2. **regclass in triggers** - Cast to oid explicitly to avoid catalog lookup issues
3. **IGNORE NULLS** - PostgreSQL doesn't support it; use gaps-and-islands pattern with SUM() window functions
4. **Test isolation** - Use SAVEPOINTs for transaction management in tests
5. **Schema incompatibility** - Tables with simple PRIMARY KEY or GENERATED ALWAYS AS IDENTITY are incompatible with SCD Type 2

## CI/CD

**GitHub Actions** (`.github/workflows/regression.yml`):
- Runs on push, pull_request, and manual dispatch
- Tests against PostgreSQL 11-16 using `pgxn/pgxn-tools` container
- Command: `pg-build-test` (make && make install && make installcheck)

**Local Pre-commit Hooks:**
- Enable with: `git config core.hooksPath devops/githooks`
- Prevents committing files in `tmp/` directory

## Reference Commands

```bash
# Explore codebase
rg "pattern"                            # Search code
tree src/                               # View structure
head -20 file.sql                       # Preview file

# Development
make install && make test fast; make diff-fail-all     # Standard dev cycle
make diff-fail-all vim                  # Review and accept changes
psql -d sql_saga_regress < tmp/verify_fix.sql  # Verify hypothesis

# Test utilities
make renumber-tests-from SLOT=066       # Shift tests from 066 onwards to make room
```

## Additional Resources

- Full conventions: `CONVENTIONS.md`
- Project tasks: `todo.md`
- API documentation: `doc/api.md`
- PostgreSQL docs: https://www.postgresql.org/docs/current/
