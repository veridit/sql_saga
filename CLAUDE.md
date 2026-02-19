# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

sql_saga is a PostgreSQL extension (C + PL/pgSQL) for temporal table management, designed for National Statistical Offices. It provides temporal foreign keys, updatable views, system versioning, and a high-performance bulk data loading procedure (`temporal_merge`).

## Essential References

- **`AGENTS.md`** — Comprehensive AI agent guidelines (build commands, code style, architecture, debugging, pitfalls). Read this first for any non-trivial task.
- **`CONVENTIONS.md`** — Development workflow, hypothesis-driven cycle, coding standards.
- **`doc/api.md`** — Complete function signatures.
- **`todo.md`** — Current project tasks.

## Build and Test Commands

```bash
make install && make test fast; make diff-fail-all              # Standard dev cycle (excludes benchmarks)
make install && make test TESTS="001_install YOUR_TEST"; make diff-fail-all  # Single test (older tests need 001_install)
make install && make test; make diff-fail-all                   # All tests including benchmarks
make diff-fail-all                                              # Show all test failures
make expected                                                   # Update expected output from last run
make install && make test fast 2>&1 | tee tmp/test_fast.log | tail -20  # With logging (preferred)
```

**Database interaction:**
```bash
psql -d sql_saga_regress                                        # Connect to test database
psql -d sql_saga_regress < tmp/verify_fix.sql                   # Run verification script
```

## Architecture

**Core catalog tables** (`src/0b_schema.sql`): `sql_saga.era`, `sql_saga.unique_keys`, `sql_saga.foreign_keys` track temporal metadata.

**Eras** define temporal periods on tables via a range column (e.g., `valid_range daterange`) with optional synchronized `valid_from`/`valid_until` columns. Ephemeral columns are excluded from coalescing.

**temporal_merge** is the core bulk loading mechanism:
- **Planner** (`src/2k_temporal_merge_plan.sql`): Uses Allen's Interval Algebra to create atomic time segments, resolve payloads, coalesce adjacent identical segments, and generate an execution plan.
- **Executor** (`src/2l_temporal_merge_execute.sql`): Executes in DELETE → UPDATE → INSERT order (enables native PostgreSQL 18 temporal FKs by creating temporary gaps rather than overlaps).

**C extension** (`src/sql_saga.c`): FK trigger implementations, history table support, SPI-based operations. `covers_without_gaps.c` provides a GIST coverage aggregate.

**Source file naming**: `src/[0-9][a-z]_*.sql` (e.g., `0a_`, `1g_`, `2k_`) — auto-discovered by Makefile. New features just need a new numbered file.

**Test file naming**: `sql/[0-9][0-9][0-9]_*.sql` — auto-discovered. Ranges: 002-009 era, 010-019 unique keys, 020-029 versioning, 030-039 aggregates, 040-049 FKs, 050-059 views, 060-099 temporal_merge, 100+ benchmarks.

## Critical Rules

- **Never edit `expected/*.out` directly** — use `make diff-fail-all vim` to review, or `make expected` to accept.
- **Never lock in bugs** — if tests show wrong behavior, fix the code, don't accept wrong expected output.
- **Always `make install` before `make test`** — the extension must be rebuilt first.
- **Shell commands must be single-line** — the execution environment splits on newlines.
- **Never add `E` prefix to psql echo strings** — `\n` works without it.
- **C99 compliance** — declare all variables at block start; no mixed declarations/statements.
- **regclass in C/SPI** — always cast to `oid` in SQL queries, use `OIDOID` parameter type.
- **Hypothesis-driven development** — verify fixes with `tmp/verify_fix.sql` before applying permanent changes; journal in `tmp/journal.md`.

## SQL Conventions

- Dollar-quote function bodies with function name: `AS $function_name$`
- Named arguments for 3+ params: `func(arg1 => val1, arg2 => val2, arg3 => val3)`
- Dollar-quoted `format()`: `format($$ ... $$)` with numbered placeholders `%1$I`, `%2$L`, `%3$s`
- Explicit table aliases: `FROM my_table AS mt`, data imports use `AS dt`
- Temporal ordering: `start <= point AND point < end` (chronological)
- Naming: `x_id` (FK), `x_ident` (external ID), `x_at` (timestamptz), `x_on` (date), `valid_from`/`valid_until` (`[)` semantics)
- RAISE NOTICE with `sql_saga: ` prefix for side effects; RAISE DEBUG for internals
- Temp tables: `IF to_regclass('pg_temp.x') IS NOT NULL THEN DROP TABLE x; END IF;`

## Debugging temporal_merge

```sql
-- Inspect intermediate temp tables within a transaction (before ON COMMIT DROP):
TABLE pg_temp.source_rows;
TABLE pg_temp.target_rows;
TABLE pg_temp.resolved_atomic_segments;
TABLE pg_temp.coalesced_final_segments;
TABLE pg_temp.diff;
TABLE pg_temp.plan_with_op;

-- GUCs:
SET sql_saga.temporal_merge.log_plan = true;
SET sql_saga.temporal_merge.enable_trace = true;
```

## Git Setup

```bash
git config core.hooksPath devops/githooks    # Pre-commit hook prevents tmp/ commits
```
