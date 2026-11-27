---
id: js3wx3
title: "Run the regression tests on the PL/pgSQL planner as a separate step"
state: "done"
type: feature
priority: ""
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzzzzzzq"
created: 2026-11-27
updated: 2026-11-27
model: "claude-opus"
branch: "master"
worktree: "/Users/jhf/ssb/.jcode/scratch/saga_master"
done_at: "2026-11-27T02:45"
---

## Goal
make test-plpgsql runs the fast regression tests with the PL/pgSQL planner against the same expected output, as its own CI job.

## Purpose
Owner's decision 2026-11-27: the PL/pgSQL planner is a test oracle. Running every behaviour test on it, not only 8 comparison tests, finds its divergences; the regular loop stays fast.

## Principle
Behaviour tests only: no benchmarks (speed is the native planner's job). One expected output per test, the same for both planners; a difference is a bug in one of them, or noise to remove from the test, never a second expected file. Native-only tests (native cache, packaging) are listed and skipped.

## Details
First run 2026-11-27 (PGOPTIONS -c sql_saga.temporal_merge.use_plpgsql_planner=true, make test fast): 144 of 153 pass in 2 min 38 s. The 9 others: (1) 066, 088, 091: error CONTEXT names the planner function, noise; make those tests planner-neutral (VERBOSITY terse or SHOW_CONTEXT never). (2) 067: PL/pgSQL error messages print the table name plus a stray s ('source table %s' in RAISE), a bug. (3) 126, 132: 'permission denied for table temporal_merge_plan_cache' when a second role merges in the same session, then wrong results: the session plan cache is a temp table owned by the first role, a bug. (4) 133: after a column rename the PL/pgSQL plan cache is stale ('column payload does not exist'), a bug. (5) 164: 'operator does not exist: numeric = text' for a numeric key given as text in the source, a bug. (6) 097: native cache counters, native-only. Plan: fix (2)-(5) in the PL/pgSQL planner with the existing tests as the failing tests; make (1) neutral; mark 097, 115, 133-native parts native-only; add make test-plpgsql and a parallel CI job.

## Depends on

## Acceptance
- [x] make test-plpgsql passes, benchmarks excluded, native-only tests skipped by a written list
- [x] each of the PL/pgSQL bugs (2)-(5) fixed; make test fast still passes
- [x] CI runs it as a separate job; the main job's time is unchanged

## Notes


check 1: 18fc10f: make test-plpgsql 152/152 pass (fast list minus PLPGSQL_NATIVE_ONLY=097), output in tmp/plpgsql
check 2: f8b2e33: 126/132 (per-role cache), 133 (schema fingerprint in key), 164 (source keys cast to target type) red before on PL/pgSQL run, green after; 067 green; make test fast 153/153
check 3: 18fc10f: separate job test-plpgsql in regression.yml; main job steps unchanged
Reflection (open, 2026-11-27, ram): nothing misled; the brief named each failing test, which made red/green direct
## History
- 2026-11-27T02:25 todo -> doing/active (take)
- 2026-11-27T02:45 doing/active -> done (done); was doing_since: 2026-11-27T02:25
