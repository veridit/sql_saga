---
id: 9msfzx
title: "Release 1.10: provenance fixes"
state: "done"
type: bug
priority: ""
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzx"
created: 2026-11-22
updated: 2026-11-22
model: "claude-opus-5-5"
branch: "feature/provenance-columns"
worktree: "/Users/jhf/ssb/sql_saga"
done_at: "2026-11-22T03:03"
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
- [x] Upgrade paths 1.0..1.9 -> new version GREEN locally and in CI
- [x] Test 161 scenarios 15-17 pass on a fresh install and after upgrade from 1.9
- [x] Annotated tag on a CI-verified master commit

## Notes


Shipped: annotated tag v1.10 ('sql_saga 1.10') on 39da88a; branch CI 44967440348 and master CI 44971023328 green on that commit. Local: fast 146/146; generator suite 1.4-1.10 + wiring; upgrade paths 1.0..1.9 -> 1.10 GREEN, 0 FAIL (the 1.9 path shows the jsonb defect before and the fix after); restore 12 cases + old dumps 1.0/1.1/1.2/1.7/1.8/1.9; test 161 scenarios 15-17 on fresh install. Benchmarks v1.9 vs 1.10 in todo.md (2ebd...: see 'todo: v1.10 tagged'). Version chosen 1.10 (minor-only series).
Reflection (open, 2026-11-22, sabertooth): One gap: nothing in doc/upgrade.md's release checklist says a harness check written for release N may name N literally; the 1.8 audit section asserted catalog_format = target version and the 1.9 hint check spelled '1.9', so the first 1.10 matrix failed 15 checks that were test bugs. Also: editing upgrade_test.sh while a matrix run was executing it corrupted that run (bash reads scripts lazily); I stopped it and reran.
Correction to the previous note: the benchmark numbers are in todo.md, section 'Version 1.10' (commit 4967659); there is no commit 2ebd.
## History
- 2026-11-22T00:42 todo -> doing/active (take)
- 2026-11-22T03:03 doing/active -> done (done); was doing_since: 2026-11-22T00:42
