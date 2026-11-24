---
id: 40nwvn
title: "PL/pgSQL planner drops the natural key a source row changes"
state: todo
type: bug
priority: ""
assignee: []
labels: []
group: ""
rank: "zzzzzzh"
created: 2026-11-24
updated: 2026-11-24
parent: "pj849j"
---

## Goal
Natural key values follow time like data: a merge never changes a natural key outside the source's time or one the source leaves NULL.

## Purpose
Found with mfky60. Target id 1 code A; source id=1 code=Z. Native writes code Z over the row's time (and carries it to the later piece of the split row); PL/pgSQL writes code A, ignoring Z, because natural key columns are entity key columns there, unified to the target's value (time_points_unified) and excluded from the data payload. When the natural key changes over time (A then Q) and the source names only id=1, PL/pgSQL loses the target time after the source (1.14: SKIPPED_NO_TARGET; with mfky60 it applies, but drops [2026,2030)).

## Principle
Decide first what a natural key column is in a merge: an identifying key (fixed per entity; changing it is not a merge) or data that may change over time (sql_saga lets a natural key value differ over time). Then make both planners follow it, test on both, expected hand-written.

## Details
Repro: tmp/nkchange.sql and tmp/nkchange2.sql (copy into doc/upstream/repro or the test). Native carrying Z into the later split piece [2026,2030) is also questionable: PATCH_FOR_PORTION_OF should only touch the source's time. The PL/pgSQL source-to-segment join (v_lateral_join_sr_to_seg_existing) compares every entity key column; joining by grouping_key made PL/pgSQL drop target time instead.

## Depends on

## Acceptance
- [ ] Owner decision recorded: natural key as identifier or as time-varying data
- [ ] Test for id=1 code=Z and for a natural key that changes over time gives the same hand-written result from both planners

## Notes
