---
id: s4bvm1
title: "Build provenance columns (release 1.9)"
state: "done"
type: feature
priority: ""
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzq"
created: 2026-11-21
updated: 2026-11-21
model: "claude"
branch: "feature/provenance-columns"
worktree: "/Users/jhf/ssb/sql_saga"
done_at: "2026-11-21T23:47"
---

## Goal
sql_saga fills an era's provenance columns from their DEFAULT on changed pieces and keeps them on untouched ones

## Purpose
StatBus needs edit_by_user_id and edit_at to name who last changed each piece of history; today they are wrong in most write paths

## Principle
Smallest subset in doc/design/provenance.md; writes run as the caller (D0/D1); no deliberate-forgery protection in this subset

## Relevant details
Design: doc/design/provenance.md. Branch feature/provenance-columns. Failing test: sql/161_provenance_columns.sql (hand-written expected). Catalog: sql_saga.era.provenance_columns name[], catalog_format 1.9. API: add_era(..., provenance_columns => ...) and set_era_provenance_columns(table, era, columns) to add/change/remove (NULL removes it and the trigger); shared validation: column must be ephemeral and have a DEFAULT; set_era_ephemeral_columns refuses removing a provenance column. Trigger <era>_provenance: DEFAULT on INSERT and data-changing UPDATE, OLD values on period-only UPDATE, NULL-DEFAULT refusal naming column and role. Planner (PL/pgSQL and native/src/sweep.rs): changed/new pieces take no provenance value, untouched keep target's, no coalescing across provenance. Executor: untouched-remainder INSERTs in their own statement under a transaction-local marker. check_integrity: disabled trigger. Rename follows, drop refused. Release 1.9: freeze 1.8, generator 1_9 with schema fragment and a declared add_era signature replacement (refuse customised privileges, as 1.2 did), preflight, v1.8 upgrade baseline, old_dump_18, docs, CI, tag.

## Depends on

## Acceptance
- [x] make test TESTS='001_install 161_provenance_columns' passes with the hand-written expected output
- [x] Full fast suite, upgrade matrix 1.0..1.8 -> 1.9 and restore round trips green
- [x] Benchmarks 100 and 101 compared before and after, numbers recorded

## Notes


Owner 2026-11-21: provenance_columns is a named argument of add_era (principled, at creation) plus set_era_provenance_columns to add/change/remove later, mirroring ephemeral_columns. Requires replacing add_era's signature in the 1.9 update (generator must support a declared signature change; refuse if add_era privileges were customised).
Build complete on feature/provenance-columns (a4fd156..e7ecc23): catalog + add_era arg + setter, managed trigger, native and PL/pgSQL planners, executor mark, check_integrity, drop protection, rename following, README. Test 161 passes under both planners; fast suite 146/146 on the installed build. Next: release 1.9 packaging.
Shipped: tag v1.9 (annotated 'sql_saga 1.9') on fb9b351; branch CI 44950393810 and master CI 44953800430 green on that commit. Local: fast 146/146, generator suite 1.4-1.9 + make wiring, upgrade paths 1.0..1.8 -> 1.9 GREEN (0 FAIL), restore 12 cases + old dumps 1.0/1.1/1.2/1.7/1.8. Benchmarks v1.8 vs 1.9 two runs each: temporal_merge PATCH parent 263-291 vs 266 ms, child 274-277 vs 277 ms, seeds equal, DML within 1-3 ms. Record in todo.md (9acbd30). Scratch cleaned.
Reflection (open, 2026-11-21, sabertooth): Mostly well guided. One gap: AGENTS.md 'Cutting the next release' step 4 says to extend upgrade_test.sh with the new baseline, but nothing warns that a replaced signature adds a second privilege refusal to the 1.1 add_system_versioning cases, nor that v1.0's Makefile does not build its native library (fixtures silently load the candidate's). I added both to the harness; the release checklist in doc/upgrade.md now mentions replaced_routines and commented_routines.
## History
- 2026-11-21T20:15 todo -> doing/active (take)
- 2026-11-21T23:47 doing/active -> done (done); was doing_since: 2026-11-21T20:15
