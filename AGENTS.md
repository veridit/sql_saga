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

### Database Commands
```bash
psql -d sql_saga_regress                                 # Connect to test database
echo 'SELECT * FROM sql_saga.era;' | psql -d sql_saga_regress  # Run query
psql -d sql_saga_regress < tmp/verify_fix.sql            # Execute script
```

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
- SQL: `src/[0-9][0-9]_*.sql` (auto-discovered by Makefile)
- Tests: `sql/[0-9][0-9][0-9]_*.sql` (auto-discovered)
- Expected output: `expected/[testname].out`
- New tests should be self-contained (include `test_setup.sql` and `test_teardown.sql`)

**Adding Features:**
- Create numbered SQL file: `src/34_my_feature.sql` (auto-included in build)
- No Makefile edits needed

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

**temporal_merge tracing (advanced):**
```sql
SET sql_saga.temporal_merge.enable_trace = true;
CALL sql_saga.temporal_merge(...);
SET sql_saga.temporal_merge.enable_trace = false;
-- Note: This adds trace jsonb for debugging table joins, not usually needed
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

## Important Constraints

- **Never** propose SEARCH/REPLACE for `expected/*.out` files
- **Never** add `E` prefix to psql echo strings: use `echo '...\n...'` not `echo E'...\n...'`
- **Never** remove `trace` column from temporal_merge output (zero overhead when disabled)
- **Never** suggest multi-line shell commands (environment splits on newlines)
- **Always** run `make install` before tests

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

**Schema** (`src/01_schema.sql`):
- Documents `with_temporary_temporal_gaps` strategy
- Enum orders define execution sequence
- Comments explain rationale and constraints

**Planner** (`src/27_temporal_merge_plan.sql`):
- Generates `plan_op_seq` using operation and effect ordering
- Automatically uses DELETE-first based on enum order

**Executor** (`src/28_temporal_merge_execute.sql`):
- Processes plan by `plan_op_seq` (respects DELETE-first order)
- Sets `CONSTRAINTS ALL DEFERRED` during execution
- Validates constraints at transaction end

**Constraints** (`src/19_add_unique_key.sql`, `src/20_add_foreign_key.sql`):
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
```

## Additional Resources

- Full conventions: `CONVENTIONS.md`
- Project tasks: `todo.md`
- PostgreSQL docs: https://www.postgresql.org/docs/current/
