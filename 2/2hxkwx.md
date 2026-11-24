---
id: 2hxkwx
title: "MERGE_ENTITY_UPSERT ignores DELETE_MISSING_ENTITIES"
state: todo
type: bug
priority: "high"
assignee: []
labels: []
group: ""
rank: "zzzzzq"
created: 2026-11-24
updated: 2026-11-24
parent: "0t4v31"
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
- [ ] A regression test with hand-written expected output fails on 1.14 for UPSERT with DELETE_MISSING_ENTITIES and DELETE_MISSING_TIMELINE_AND_ENTITIES, and passes after the fix
- [ ] The combination of every mode with every delete mode is either applied as documented or refused with a clear error, covered by tests
- [ ] make test fast passes

## Notes
