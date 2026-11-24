---
id: mfky60
title: "Planners disagree when a source row has both an identity and a natural key"
state: "doing"
type: bug
priority: ""
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzzzzz"
created: 2026-11-24
updated: 2026-11-24
parent: "pj849j"
doing_since: "2026-11-24T16:30"
stage: "active"
model: "claude-opus-5-5"
branch: "master"
worktree: "/Users/jhf/ssb/sql_saga"
---

## Goal
Both planners resolve a source row by every key it names and refuse, per row, a row whose keys name different entities.

## Purpose
Found while checking aqqsgb (grid tmp/idnk_grid.sql, target id 1/A, 2/B, 3/C, natural key code). Native (default): id=9 code=B writes to entity 2 and drops id 9 silently; id=1 code=B fails the whole call with the natural key's exclusion violation. PL/pgSQL: with any natural key present it never matches by identity, so id=1 alone gives SKIPPED_NO_TARGET (PATCH_FOR_PORTION_OF) or an exclusion violation (MERGE_ENTITY_PATCH); id=1 code=Z likewise; conflicts get 'Planner failed to generate a plan'. Same on 1.14.

## Principle
A natural key is bound to one primary key (mnntde), so a row whose keys find different entities, or whose identity is new while its natural key finds an entity, is contradictory: refuse it per row with ERROR feedback, never guess. A key that finds nothing beside one that finds an entity is a new natural key value for that entity (id=1 code=Z changes entity 1's code). Identity and natural keys are equal ways to name an entity, as in the lock step (2m). Same feedback from both planners.

## Details
PL/pgSQL (src/2k): v_keys_for_filtering and v_source_rows_exists_join_expr use only lookup_keys when present; add the identity key set to both, and the conflict to early feedback. Native (sweep.rs correlate_entities): tries natural keys only when the identity did not match; collect matches from the identity and every key set. New test 165 runs the grid on both planners, expected hand-written first.

## Depends on

## Acceptance
- [x] Test 165: the 9-row grid (identity only, natural key only, both agreeing, both conflicting, new identity with known natural key, ...) in PATCH_FOR_PORTION_OF, MERGE_ENTITY_PATCH and INSERT_NEW_ENTITIES gives identical, hand-written feedback and rows from both planners
- [ ] make test fast passes; StatBus call shapes (primary_identity_columns with discovered or given natural keys) checked against the new rule

## Notes


check 1: Test 165 (8 cases x 3 modes; id=1 code=Z moved to 40nwvn): native matches the hand-written table (tmp/165_tables.out), PL/pgSQL differs in 0 rows; 7ae460c4
## History
- 2026-11-24T16:30 todo -> doing/active (take)
