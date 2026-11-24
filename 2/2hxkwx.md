---
id: 2hxkwx
title: "MERGE_ENTITY_UPSERT ignores DELETE_MISSING_ENTITIES"
state: "doing"
type: bug
priority: "high"
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzzzzq"
created: 2026-11-24
updated: 2026-11-24
parent: "pj849j"
doing_since: "2026-11-24T14:08"
stage: "active"
model: "claude-opus-5-5"
branch: "master"
worktree: "/Users/jhf/ssb/sql_saga"
---

## Goal
With delete_mode DELETE_MISSING_ENTITIES, every MERGE_ENTITY_* mode deletes target entities absent from the source.

## Purpose
Found while checking mode semantics for the upstream map (9fxqkc). README says DELETE_MISSING_ENTITIES deletes absent entities 'when used with MERGE_ENTITY_* modes'. PATCH and REPLACE do; UPSERT, documented as the main workhorse, silently keeps them, so a full refresh with UPSERT leaves entities that should be gone.

## Principle
Failing test first with hand-written expected output. One rule for all MERGE_ENTITY_* modes; decide and document what the *_FOR_PORTION_OF and INSERT_NEW_ENTITIES modes do with delete modes (refuse or apply), rather than leaving it to a scan-scope accident.

## Details
Repro: doc/upstream/repro/delete_missing_entities_upsert.sql. Cause: src/2k_temporal_merge_plan.sql (about line 1075) scans the whole target only when mode IN ('MERGE_ENTITY_PATCH', 'MERGE_ENTITY_REPLACE'); MERGE_ENTITY_UPSERT (added later as the workhorse) falls into the source-entities-only scan and never sees absent entities. Likely the same for DELETE_MISSING_TIMELINE_AND_ENTITIES. Existing tests (sql/069) use REPLACE only.

## Depends on

## Acceptance
- [x] A regression test with hand-written expected output fails on 1.14 for UPSERT with DELETE_MISSING_ENTITIES and DELETE_MISSING_TIMELINE_AND_ENTITIES, and passes after the fix
- [x] The combination of every mode with every delete mode is either applied as documented or refused with a clear error, covered by tests
- [x] make test fast passes

## Notes


Grid of all 8 modes x 4 delete modes on both planners (doc/upstream/repro/mode_delete_mode_grid.sql), 1.14: (1) UPSERT ignores DELETE_MISSING_ENTITIES in both planners (scan rule mode IN (PATCH, REPLACE) is copied in src/2k_temporal_merge_plan.sql:1075 and twice in native/src/reader.rs). (2) Delete modes with non-MERGE_ENTITY modes are arbitrary: INSERT_NEW_ENTITIES + DELETE_MISSING_TIMELINE deletes existing entity 1 entirely (both planners); DELETE_FOR_PORTION_OF + DMT/DMTE crashes the PL/pgSQL planner (jsonpath wildcard error); INSERT_NEW_ENTITIES + DME deletes entity 1 in PL/pgSQL but not native; *_FOR_PORTION_OF + DMT deletes uncovered time in PL/pgSQL, not native. Decision: delete modes apply to MERGE_ENTITY_* only (schema comment: 'deletion behavior for replace modes'; README); any other mode with delete_mode <> NONE is refused (22023) by both planner entry points. Entity deletion scans the whole target for every MERGE_ENTITY_* mode; the rule lives once per language (DeleteMode/MergeMode methods in Rust).
## History
- 2026-11-24T14:08 todo -> doing/active (take)
