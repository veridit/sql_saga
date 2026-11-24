---
id: 7rzgv5
title: "temporal_merge silently loses concurrent changes under READ COMMITTED"
state: todo
type: bug
priority: "high"
assignee: []
labels: []
group: ""
rank: "zzzzz"
created: 2026-11-24
updated: 2026-11-24
parent: "0t4v31"
---

## Goal
temporal_merge gives the serial result, or fails loudly, when another transaction changes the same entity concurrently.

## Purpose
Found in the FOR PORTION OF revert study (vhhp80): under READ COMMITTED, the default, three races silently corrupt data. A plain UPDATE of another column committed while temporal_merge waits is overwritten on every piece (lost update). A DELETE of the entity committed while temporal_merge waits is undone: the entity comes back for the untouched part of its history. The same lost update happens through the for_portion_of view, which calls temporal_merge. This is the same class of bug that got FOR PORTION OF reverted from PostgreSQL 19, and sql_saga must not have it before we discuss the topic upstream.

## Principle
Serial-equivalent results at READ COMMITTED without asking callers to retry; never a silent partial write. Lock what the plan reads, before planning, in a fixed order so two batches cannot deadlock. Disjoint entities must not block each other (concurrent batch imports stay parallel). Failing isolation tests first.

## Details
Cause: the planner reads the target with one snapshot; the executor then matches rows by (key, old range) and writes the plan's values. PostgreSQL re-checks only that WHERE clause after waiting, so a data-only change passes and is overwritten, and a deleted row matches 0 rows while the INSERTs still run. Nothing checks the row counts. Reproducers: tmp/upstream/iso/ (gen.py; specs b_note_vs_portion_rc, c_delete_all_vs_portion_rc, e_view_note_vs_portion_rc; run with pgxs isolationtester against a *_jhf db). Loud-only cases (exclusion error or 40001): a, d, f, and all RR runs. Prototype that gives serial results in all six RC specs: before the CALL, pg_advisory_xact_lock per entity plus SELECT ... FOR UPDATE of the entity's rows, so the planner's later statement snapshot sees committed state. Proposed fix: temporal_merge takes these locks itself, sorted by entity key, before planning (an advisory lock also covers new entities with no rows yet); the executor also checks that each UPDATE/DELETE changed exactly the planned row count and raises otherwise. Needs: isolation tests in the suite (make target using pgxs isolationtester), benchmark before and after, doc of the concurrency contract, release.

## Depends on

## Acceptance
- [ ] The isolation specs b, c and e fail on 1.14 and give the serial result after the fix
- [ ] Specs a, d and f give the serial result instead of an exclusion error
- [ ] Two concurrent temporal_merge calls on disjoint entities do not wait for each other (isolation spec)
- [ ] The executor raises when an UPDATE or DELETE changes a different number of rows than planned (test)
- [ ] Benchmark rows/s before and after are recorded

## Notes
