---
id: v6s4xq
title: "Draft temporal_merge semantics and SQL syntax proposal"
state: todo
type: spike
priority: ""
assignee: []
labels: []
group: ""
rank: "zzzzy"
created: 2026-11-24
updated: 2026-11-24
parent: "0t4v31"
---

## Goal
A written proposal of exact rules and SQL syntax for every temporal_merge case, ready to discuss upstream.

## Purpose
If the PostgreSQL temporal authors are interested, a precise proposal (rules plus syntax plus examples) is what they can react to; the owner likes having a syntax to show.

## Principle
Waits for the owner's decision after the first conversation; drafting earlier is allowed only if the owner asks. Build on SQL's MERGE and SQL:2011 temporal terms rather than inventing new statements. Every rule is backed by an existing sql_saga regression test.

## Details
Sketch to start from: MERGE INTO t USING s ON ... FOR PORTION OF t.valid USING s.valid, with WHEN MATCHED UPDATE (upsert or patch via COALESCE), WHEN NOT MATCHED INSERT, WHEN NOT MATCHED BY SOURCE DELETE (delete-missing modes); a table property for columns ignored when merging adjacent equal pieces (ephemeral columns); REFERENCES ... AT ANY TIME for regular-to-temporal keys. Must answer the concurrency question from the revert study. Builds on 9fxqkc's mapping.

## Depends on

- axdept
## Acceptance
- [ ] Each temporal_merge merge mode and delete mode has its syntax, its rules and an example traced to a regression test
- [ ] The proposal states its behaviour under concurrent writes at READ COMMITTED

## Notes

2026-11-25 owner: do the syntax first, then the first message. Goal is consistency and clarity, not fewest reserved words; reuse sql_saga's vocabulary (entity, timeline, part of the timeline, whole timeline). Wants the design-space matrix, principles and concrete examples, covering INSERT, UPDATE, DELETE and MERGE.
