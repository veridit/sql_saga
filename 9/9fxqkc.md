---
id: 9fxqkc
title: "Explore upstream PostgreSQL support for temporal_merge"
state: todo
type: spike
priority: ""
assignee: []
labels: []
group: ""
rank: "zzzzh"
created: 2026-11-24
updated: 2026-11-24
---

## Goal
A written case, for the owner to decide on, for what of temporal_merge PostgreSQL itself could support, and how to propose it.

## Purpose
temporal_merge, the PL/pgSQL and native planner, carries logic (Allen relations, coalescing, DELETE-UPDATE-INSERT ordering for deferred temporal foreign keys) that PostgreSQL's own temporal features (WITHOUT OVERLAPS, PERIOD foreign keys, FOR PORTION OF) partly cover; native support would remove code sql_saga maintains and make it faster.

## Principle
Research and a proposal draft only; nothing is sent to the PostgreSQL community without the owner's decision. Build on what sql_saga has proven in tests and benchmarks; cite current PostgreSQL work, do not assume it.

## Details
Starting points: doc/design/, src/2k_temporal_merge_plan.sql, src/2l_temporal_merge_execute.sql, native/ (Rust planner), AGENTS.md section on PostgreSQL 18 temporal constraints, benchmark results in expected/performance/. Check the current state of SQL:2011 temporal work in PostgreSQL (commitfest, pgsql-hackers) before drafting. A bigger task for a separate agent (owner, 2026-11-24).

## Depends on

## Acceptance
- [ ] A document lists what PostgreSQL already has, what is in progress upstream (with links), and which parts of temporal_merge are not covered
- [ ] A recommendation names the smallest proposal with the best chance upstream, with its cost to prepare, for the owner to decide

## Notes
