---
id: 6qhssr
title: "Decide whether to retire the PL/pgSQL planner"
state: todo
type: spike
priority: ""
assignee: []
labels: []
group: ""
rank: "zzzzzzzh"
created: 2026-11-26
updated: 2026-11-26
---

## Goal
The owner decides whether the PL/pgSQL planner stays as a test oracle or is removed, from the costs and findings below.

## Purpose
Every planner change is made twice; the native planner is the reference since 2026-11-26, and the PL/pgSQL one keeps diverging.

## Principle
Decide from evidence of what the second planner catches, not from the cost of one fix.

## Details
Situation: src/2k_temporal_merge_plan.sql is 3,091 lines of PL/pgSQL beside 5,855 lines of Rust in native/. The native planner is the default and the reference; the PL/pgSQL one runs only with sql_saga.temporal_merge.use_plpgsql_planner = true, which 8 regression tests set to compare the two. It is not a fallback: the update refuses a missing native library. Findings in 1.15/1.16 from comparing them: native wrong and PL/pgSQL right (composite natural key refused as ambiguous, review 2 S1); PL/pgSQL wrong (y7hbe9 absent columns, 4h7png entity keys, XOR feedback N1); both wrong (mfky60). Cost of keeping: each behaviour change written and tested twice (y7hbe9 was 22 lines). Options: (a) keep it as a test oracle, internal, the native planner the reference; (b) remove it, and turn the comparison tests into hand-written expected outputs, which keep the native results but lose the second opinion on new cases. Question for the owner: keep (a) or remove (b)?

## Depends on

## Acceptance
- [ ] The owner has chosen (a) or (b), recorded in a note

## Notes
