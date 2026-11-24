---
id: aqqsgb
title: "Native planner takes 6 s per 1,000 rows when a natural key is discovered"
state: "doing"
type: bug
priority: "medium"
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzzzzy"
created: 2026-11-24
updated: 2026-11-24
parent: "pj849j"
doing_since: "2026-11-24T15:55"
stage: "active"
model: "claude-opus-5-5"
branch: "master"
worktree: "/Users/jhf/ssb/sql_saga"
---

## Goal
temporal_merge plans a 1,000-row batch against a 200,000-row target in under 200 ms whether or not natural keys are discovered.

## Purpose
Found while benchmarking 7rzgv5: with one natural key registered (and discovered by default), the native planner's PLAN phase is 6.0-7.6 s for 1,000 source rows on a 200k-row target; with natural_identity_columns => ARRAY[] it is 90 ms. Same on 1.14 and 1.15. StatBus registers natural keys on its tables, so its imports may pay this.

## Principle
Measure first (EXPLAIN of the native reader's target query, timing per native phase), fix the cause, keep both planners' results identical; benchmark before and after.

## Details
Repro: doc/upstream/repro/native_planner_natural_key_slow.sql (psql -f against a *_jhf db with sql_saga; prints PLAN/EXECUTE phase times with sql_saga.log_step_timing). Table bl(id, code) with primary key id and natural key code, 200k rows; source 1,000 rows with id and code. Suspect: native/src/reader.rs target filter for lookup_key_sets (build_target_filter / try_build_parameterized_filter) producing a non-indexable predicate, or correlate_entities in sweep.rs being quadratic.

## Depends on

## Acceptance
- [x] PLAN phase for the repro is under 200 ms with the natural key discovered, measured before and after
- [ ] make test fast and the 101/102 benchmarks pass

## Notes


check 1: Repro doc/upstream/repro/native_planner_natural_key_slow.sql: PLAN phase 6.0-7.6 s before, 84-165 ms after (PL/pgSQL planner 356 ms); d04b7b42
## History
- 2026-11-24T15:55 todo -> doing/active (take)
