---
id: bjd8v7
title: "XOR natural key accepts a row that sets more than one exclusive column"
state: "doing"
type: feature
priority: ""
assignee: ["fish"]
labels: []
group: ""
rank: "zzzzzzy"
created: 2026-11-25
updated: 2026-11-25
doing_since: "2026-11-25T02:20"
stage: "active"
model: "claude-opus"
branch: "master"
worktree: "/Users/jhf/ssb/sql_saga"
---

## Goal
A row giving two columns of a mutually exclusive key set is refused by the schema and gets per-row ERROR feedback from temporal_merge.

## Purpose
Such a row is accepted as a new entity holding two other entities' identifiers; with a user CHECK the whole batch aborts instead.

## Principle
mutually_exclusive_columns means at most one non-NULL per row; sql_saga enforces what it declares.

## Details
Second 1.15 review N2 (tmp/review_1_15b.md, repros tmp/review_115b/c6b.sql, c6c.sql). Pre-existing; both planners agree. Proposed: add_unique_key adds CHECK (num_nonnulls(cols) <= 1) for the set; the update step adds it to existing XOR keys and refuses violating rows with DETAIL and a hint, preflight lists them; both planners refuse such a source row per row. StatBus already has CHECK (num_nonnulls(establishment_id, legal_unit_id) = 1) on its XOR tables, so nothing changes there except per-row feedback instead of an aborted batch. Cost: one constraint per XOR key, a preflight query, two planner checks, tests.

## Depends on

## Acceptance
- [x] add_unique_key with mutually_exclusive_columns creates the CHECK; check_integrity reports it missing
- [x] update step adds it, refuses violating rows; preflight lists them; upgrade gate red on 1.15
- [ ] both planners give per-row ERROR for a row with two exclusive columns; batch continues

## Notes


check 1: 7011af1: test 167 shows the CHECK, NOTICE, catalog, drop, rename, drop protection, and check_integrity's report with its HINT
check 2: update step adds the CHECK and refuses violating rows (DETAIL+HINT); preflight lists them; gate RED on the pre-change candidate from v1.14, upgrade_test GREEN for 15 baselines
## History
- 2026-11-25T02:20 todo -> doing/active (take)
