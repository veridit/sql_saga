---
id: 40nwvn
title: "PL/pgSQL planner drops the natural key a source row changes"
state: "doing"
type: bug
priority: ""
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzzzzzh"
created: 2026-11-24
updated: 2026-11-24
parent: "pj849j"
doing_since: "2026-11-24T17:02"
stage: "active"
model: "claude-opus-5-5"
branch: "master"
worktree: "/Users/jhf/ssb/sql_saga"
---

## Goal
Natural key values follow time like data: a merge never changes a natural key outside the source's time or one the source leaves NULL.

## Purpose
Found with mfky60 (grid doc/upstream/repro/natural_key_time_varying_grid.sql, same on 1.14). A natural key value may differ over time for one entity (target id 1: code A [2020,2025), Q [2025,2030)). Source [2024,2026) naming id=1: the native planner (default) writes one code over the whole source time, A over [2025,2026) where the target holds Q, in PATCH_FOR_PORTION_OF and MERGE_ENTITY_*: a silent rewrite of a stored natural key value the source did not give. PL/pgSQL refuses such rows ('Planner failed to generate a plan'). A row naming only code A or Q under MERGE_ENTITY_* fails the whole call with an exclusion violation in both.

## Principle
Decide first what a natural key column is in a merge: an identifying key (fixed per entity; changing it is not a merge) or data that may change over time (sql_saga lets a natural key value differ over time). Then make both planners follow it, test on both, expected hand-written.

## Details
Repro: tmp/nkchange.sql and tmp/nkchange2.sql (copy into doc/upstream/repro or the test). Native carrying Z into the later split piece [2026,2030) is also questionable: PATCH_FOR_PORTION_OF should only touch the source's time. The PL/pgSQL source-to-segment join (v_lateral_join_sr_to_seg_existing) compares every entity key column; joining by grouping_key made PL/pgSQL drop target time instead.

## Depends on

## Acceptance
- [ ] Owner decision recorded: natural key as identifier or as time-varying data
- [ ] Test for id=1 code=Z and for a natural key that changes over time gives the same hand-written result from both planners

## Notes

Severity raised, blocks 1.15. Target id 1: code A [2020,2025), code Q [2025,2030); source id=1 code NULL [2024,2026), PATCH_FOR_PORTION_OF. After mfky60 (7ae460c4) the PL/pgSQL planner plans DELETE of the Q row [2025,2030) and writes only [2024,2025): data loss (1.14 skipped the row). Native (1.14 and now) writes code A over [2025,2026), where the target had Q. Cause in both planners: natural key columns are entity-constant (entity_keys/group lookup keys, excluded from data). The schema allows a natural key to differ over time for one entity (the guard binds a value to one id, not one value per id), so the only answer consistent with it is per-time values: the target's value at each time, overridden by the source's non-NULL value inside the source's time; adjacent pieces with different values do not coalesce. Repro doc/upstream/repro/natural_key_change_over_time_plan.sql.

CORRECTION of the previous note: the 'PL/pgSQL plans DELETE of the Q row' result came from a scratch database still holding my reverted join experiment (make install does not replace functions in an existing database). Fresh database, grid doc/upstream/repro/natural_key_time_varying_grid.sql, HEAD 7ae460c4 identical to 1.14: target code A [2020,2025), Q [2025,2030); source [2024,2026). Native (default) with id=1 and code NULL, A or Q writes one code over the whole source time, e.g. A over [2025,2026) where the target holds Q, in PATCH_FOR_PORTION_OF, MERGE_ENTITY_PATCH and MERGE_ENTITY_REPLACE: it rewrites a stored natural key value the source did not give. PL/pgSQL refuses those rows ('Planner failed to generate a plan'), no write. id=- code=A or Q under MERGE_ENTITY_*: exclusion violation in both (the natural key finds the entity, but the row is built with the target's other value).


## History
- 2026-11-24T17:02 todo -> doing/active (take)
