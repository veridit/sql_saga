---
id: mnntde
title: "A natural key is always bound to one primary key, also composite"
state: "doing"
type: "bug"
priority: "medium"
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzzv"
created: 2026-11-23
updated: 2026-11-23
parent: "2q5nvf"
doing_since: "2026-11-23T13:51"
stage: "depends"
model: "claude-opus-5-5"
branch: "master"
worktree: "/Users/jhf/ssb/sql_saga"
---

## Goal
add_unique_key always ties a natural key value to one primary key value, also for a composite primary key; no opt-out.

## Purpose
A natural key that can move between entities identifies nothing, and StatBus import treats such a mismatch as dangerous. Today the result depends on call order: test 016 4a adds the key unguarded, 4b refuses.

## Principle
Owner decided 2026-11-23: no opt-out, no deprecated parameter; enforce_consistency_with_primary_key is removed. Existing unguarded keys get the guard on update; conflicting rows refuse the update and are listed, never changed.

## Details
Guard for a composite stable primary key: one EXCLUDE (natural key WITH =, pk_col WITH <>) per stable PK column, which together bind the natural key to the whole PK (proved in a scratch database: same nk on (1,2) or (2,1) refused, on (1,1) later allowed). The 'PK with no stable column' branch is dead: PostgreSQL refuses WITHOUT OVERLAPS on one column. Opt-out users today: tests 016 (scenario 3), 039, 116, 117; none relies on a drifting natural key. StatBus: every natural key is on a table whose primary key is (id), unaffected. Signature change of add_unique_key goes through replaced_routines (EXECUTE privilege rule). Needs a release.

## Depends on

- qnzn33
## Acceptance
- [x] The owner's decision is recorded here, and 016 4a's comment and expected output match the chosen behaviour
- [x] 016 4a and 4b both succeed and both tables reject the same natural key on another primary key; expected output hand-written first
- [x] add_unique_key has no enforce_consistency_with_primary_key parameter; doc/api.md, README and tests updated
- [ ] The 1.14 update adds the guard to every natural key without one, and refuses with the conflicting rows listed when data breaks it
- [ ] check_integrity reports a natural key without its guard (missing pk_consistency constraint, or NULL/empty state while a primary key exists); test hand-written first

## Notes

Owner decided (a), 2026-11-23: refuse in both orders. Reason: a natural key that can drift between primary keys is dangerous and confusing; StatBus import checks for exactly this. Order must not matter. Checked: every StatBus table with a natural key has a single-column primary key (id), so StatBus is unaffected. Explicit enforce_consistency_with_primary_key => false stays the documented opt-out and is unaffected.

check 1: Owner decided 2026-11-23: no opt-out at all; recorded in notes, goal and principle. 016 4a is covered by check 2.


check 2: 016 4a (PK then NK) and 4b (NK then composite PK) both succeed; both refuse NK1 on another unit (and 4a on another org) with the per-column guard; 9d5f52d
check 3: parameter removed from src/1j, 3h, 3g; doc/api.md regenerated (line removed); README never documented it; tests 016/039/116/117 updated; 016 scenario 3 shows 'function ... does not exist'
## History
- 2026-11-23T13:51 todo -> doing/active (take)
