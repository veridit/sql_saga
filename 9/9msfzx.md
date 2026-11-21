---
id: 9msfzx
title: "Release 1.10: provenance fixes"
state: todo
type: bug
priority: ""
assignee: []
labels: []
group: ""
rank: "zzx"
created: 2026-11-22
updated: 2026-11-22
---

## Goal
Ship cbf60ed and 760e6c1 (provenance trigger fixes found after the 1.9 tag) as release 1.10

## Purpose
1.9 stores a jsonb provenance DEFAULT as a JSON string, stores NULL silently when a provenance DEFAULT is dropped, and accepts a generated column that then breaks every write

## Principle
Released artifacts are immutable: a new version, never a re-tag; only the routines these commits change; same release discipline as 1.9

## Relevant details
Situation: found 2026-11-21 by probing the 1.9 trigger (test 161 scenarios 15-17, both planners); fixed on feature/provenance-columns (cbf60ed, 760e6c1), branch CI 44959359503 and 44959402701 green; master and tag v1.9 untouched. Changed routines: sql_saga.provenance_trigger(), sql_saga.__internal_set_era_provenance_columns(regclass,name,name[]) and the check_integrity worker in src/2h_health_checks.sql. No catalog change. Change: freeze the 1.9 artifacts, generator 1_10 with that manifest, preflight for the 1.9 shape, upgrade_test v1.9 baseline, old_dump_19, docs, CI, tag. Why: StatBus plans edit_at and edit_by_user_id as provenance columns; a jsonb provenance column is stored wrong on 1.9 and a dropped DEFAULT records NULL silently. Cost: a routine-only release, as 1.5 to 1.7 were. Version: 1.10, following the minor-only series (v1.0 to v1.9, no patch tags); the harness version comparisons must be checked with a two-digit minor.

## Depends on

## Acceptance
- [ ] Upgrade paths 1.0..1.9 -> new version GREEN locally and in CI
- [ ] Test 161 scenarios 15-17 pass on a fresh install and after upgrade from 1.9
- [ ] Annotated tag on a CI-verified master commit

## Notes
