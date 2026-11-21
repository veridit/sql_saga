---
id: s4bvm1
title: "Build provenance columns (release 1.9)"
state: "doing"
type: feature
priority: ""
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzq"
created: 2026-11-21
updated: 2026-11-21
doing_since: "2026-11-21T20:15"
stage: "active"
model: "claude"
branch: "feature/provenance-columns"
worktree: "/Users/jhf/ssb/sql_saga"
---

## Goal
sql_saga fills an era's provenance columns from their DEFAULT on changed pieces and keeps them on untouched ones

## Purpose
StatBus needs edit_by_user_id and edit_at to name who last changed each piece of history; today they are wrong in most write paths

## Principle
Smallest subset in doc/design/provenance.md; writes run as the caller (D0/D1); no deliberate-forgery protection in this subset

## Relevant details
Branch feature/provenance-columns. Catalog: sql_saga.era.provenance_columns name[], catalog_format 1.9. API: set_era_provenance_columns(table, era, columns) refuses non-ephemeral, no-DEFAULT; set_era_ephemeral_columns refuses dropping a provenance column. Planner (PL/pgSQL and native/src/sweep.rs): changed/new pieces take no provenance value, untouched keep target's, no coalescing across provenance. Executor: untouched-remainder INSERTs in their own statement under a transaction-local marker. Trigger <era>_provenance: DEFAULT on INSERT and data-changing UPDATE, keep OLD on period-only UPDATE, NULL-DEFAULT refusal. check_integrity: disabled trigger. Rename follows, drop refused. Release 1.9 cycle as 1.8.

## Depends on

## Acceptance
- [ ] make test TESTS='001_install 161_provenance_columns' passes with the hand-written expected output
- [ ] Full fast suite, upgrade matrix 1.0..1.8 -> 1.9 and restore round trips green
- [ ] Benchmarks 100 and 101 compared before and after, numbers recorded

## Notes


## History
- 2026-11-21T20:15 todo -> doing/active (take)
