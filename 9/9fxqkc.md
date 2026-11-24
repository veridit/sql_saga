---
id: 9fxqkc
title: "Explore upstream PostgreSQL support for temporal_merge, regular-to-temporal FKs"
state: "doing"
type: spike
priority: ""
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzzzh"
created: 2026-11-24
updated: 2026-11-24
parent: "0t4v31"
doing_since: "2026-11-24T14:03"
stage: "active"
model: "claude-opus-5-5"
branch: "master"
worktree: "/Users/jhf/ssb/sql_saga"
---

## Goal
A written case, for the owner to decide on, for what PostgreSQL itself could support of sql_saga's temporal model, and how to propose it.

## Purpose
temporal_merge, the PL/pgSQL and native planner, carries logic (Allen relations, coalescing, DELETE-UPDATE-INSERT ordering for deferred temporal foreign keys) that PostgreSQL's own temporal features (WITHOUT OVERLAPS, PERIOD foreign keys, FOR PORTION OF) partly cover; native support would remove code sql_saga maintains and make it faster.

## Principle
Research and a proposal draft only; nothing is sent to the PostgreSQL community without the owner's decision. Build on what sql_saga has proven in tests and benchmarks; cite current PostgreSQL work, do not assume it.

## Details
Starting points: doc/design/, src/2k_temporal_merge_plan.sql, src/2l_temporal_merge_execute.sql, native/ (Rust planner), AGENTS.md section on PostgreSQL 18 temporal constraints, benchmark results in expected/performance/. Check the current state of SQL:2011 temporal work in PostgreSQL (commitfest, pgsql-hackers) before drafting. A bigger task for a separate agent (owner, 2026-11-24).
Owner's three areas (2026-11-24):
1. Foreign keys from a regular (non-temporal) table to a temporal table, by existence: the referenced key must exist at some time. PostgreSQL cannot express this today; sql_saga does it with add_regular_foreign_key (constraint trigger, src/2a_add_foreign_key.sql; tests 137-143, 147-148).
2. FOR PORTION OF is weak: it covers UPDATE/DELETE of one time slice. The owner sees the need for more, covering the whole logical problem space temporal_merge does.
3. temporal_merge's problem space as a whole: the merge modes (MERGE_ENTITY_UPSERT/PATCH/REPLACE, *_FOR_PORTION_OF, INSERT_NEW_ENTITIES), the delete modes (DELETE_MISSING_TIMELINE/ENTITIES), coalescing of adjacent equal pieces, ephemeral columns, and the DELETE-UPDATE-INSERT order that lets deferred PERIOD foreign keys tolerate temporary gaps. Map each to SQL:2011 and to what PostgreSQL has, to show what a fuller feature would contain.

## Depends on

## Acceptance
- [x] A document lists what PostgreSQL already has, what is in progress upstream (with links), and which parts of temporal_merge are not covered
- [x] A recommendation names the smallest proposal with the best chance upstream, with its cost to prepare, for the owner to decide
- [x] The three areas each have a section: the problem with a concrete example, what sql_saga does, what PostgreSQL offers and is working on

## Notes

Mapping modes surfaced two more bugs: rtwrp6 (DELETE_MISSING_TIMELINE with interior source edges) and 2hxkwx (UPSERT ignores DELETE_MISSING_ENTITIES); repros in doc/upstream/repro/.


## History
- 2026-11-24T14:03 todo -> doing/active (take)
