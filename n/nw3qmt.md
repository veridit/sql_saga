---
id: nw3qmt
title: "Native planner loses numeric precision"
state: "doing"
type: bug
priority: "high"
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzy"
created: 2026-11-22
updated: 2026-11-22
doing_since: "2026-11-22T03:18"
stage: "active"
model: "claude-opus-5-5"
branch: "feature/provenance-columns"
worktree: "/Users/jhf/ssb/sql_saga"
---

## Goal
temporal_merge with the native planner stores numeric values exactly as given

## Purpose
The native planner rounds numeric data through a 64-bit float: 12345678901234567890.123456789012345678900 is stored as 12345678901234567000. Silent data corruption in the default planner

## Principle
Carry numeric as its exact text; no float conversion of decimal types; both planners give identical results

## Relevant details
Situation: native/src/reader.rs parse_typed_value parses numeric/decimal/money/real/double via text.parse::<f64>() into a JSON number. Seen 2026-11-22 on 1.10 (arr_jhf scratch db): native stores 12345678901234567000, PL/pgSQL stores the exact value. Present since v1.0 (commit 05615a7). Change: for numeric and decimal, keep the exact digits (serde_json arbitrary_precision is already enabled in Cargo.toml, so Number::from_str keeps them), never through f64. Why: StatBus stores money and statistics in numeric columns, and temporal_merge with the default native planner is StatBus's import path. Cost: a small change in reader.rs, plus a regression test comparing both planners on large-scale and high-scale numerics. Risk: equality of data payloads in coalescing must compare 1.10 and 1.1 as equal, as PostgreSQL numeric does. Question: none.

## Depends on

## Acceptance
- [x] A regression test stores 12345678901234567890.123456789012345678900 and 1.10 exactly under both planners
- [ ] ~~Coalescing treats 1.1 and 1.10 as the same value under both planners~~ (not relevant: Wrong premise: both planners already keep 1.1 and 1.10 as separate pieces (measured 2026-11-22 on the 1.11 candidate, table cz in scratch db arr_jhf), and that is correct, since numeric scale is part of the stored value. The real requirement is that the planners agree, which test 162 now checks., sabertooth, 2026-11-22)
- [x] Fast suite green; make test-native green in CI

## Notes


## History
- 2026-11-22T03:18 todo -> doing/active (take)
