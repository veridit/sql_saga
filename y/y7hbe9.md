---
id: y7hbe9
title: "PL/pgSQL planner clears target columns the source does not have"
state: "doing"
type: "bug"
priority: ""
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzzzzzz"
created: 2026-11-25
updated: 2026-11-25
parent: "0t4v31"
doing_since: "2026-11-26T22:39"
stage: "active"
model: "claude-opus"
branch: "master"
worktree: "/Users/jhf/ssb/.jcode/scratch/saga_master"
---

## Goal
The PL/pgSQL planner keeps a target column the source table does not have, in every mode, as the native planner does.

## Purpose
A column the source has and sets to NULL is a value; a column it does not have says nothing. The PL/pgSQL planner confuses the two in REPLACE and clears data, natural keys included.

## Principle
Owner's decision 2026-11-26: the native (Rust) planner is the reference implementation and the PL/pgSQL planner must match it; it was the reference once and is behind here. No change to the native planner or to StatBus behaviour.

## Details
Found drafting doc/upstream/temporal-dml.md (open question 5). Repro: doc/upstream/repro/replace_absent_column_planners.sql. Target entity 1: North [2020,2024), South [2024,2026); source has no region column: (Alpha2, [2023,2027)). Native: region North / South kept per piece, the new time [2026,2027) NULL. PL/pgSQL: one piece [2023,2027) with region NULL and code NULL (a natural key cleared). Seen for MERGE_ENTITY_REPLACE and REPLACE_FOR_PORTION_OF. StatBus uses the REPLACE modes on the native planner, so it already gets the rule. Fix in src/2k_temporal_merge_plan.sql, where REPLACE builds the payload over all target columns instead of the source's. Then check every mode for the same confusion. A source column that is present and NULL keeps its meaning per mode (REPLACE/UPSERT: a value; PATCH: keep).

## Depends on

## Acceptance
- [x] A regression test runs REPLACE, UPSERT and PATCH (merge and portion modes) with a source lacking a target column, on both planners; it fails on the PL/pgSQL planner before the fix
- [ ] Both planners give the native result for every case of that test, and make test fast passes

## Notes

StatBus uses MERGE_ENTITY_REPLACE/REPLACE_FOR_PORTION_OF in import procedures (legal_unit, location, contact, statistical variables, legal relationship), on the native planner: so the native behaviour (absent column kept) is what StatBus relies on. Changing native would change StatBus imports; check their source tables carry every column before deciding.


check 1: test 168 (3 sources x 9 modes, both planners): before the fix PL/pgSQL differs in 10 of 27 cases (and refused a source without the natural key column); commit 6d3856a
## History
- 2026-11-26T22:39 todo -> doing/active (take)
