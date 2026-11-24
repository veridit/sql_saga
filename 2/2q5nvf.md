---
id: 2q5nvf
title: "Release 1.14: clean up what 1.9 to 1.13 deferred"
state: "done"
type: chore
priority: "high"
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzzx"
created: 2026-11-23
updated: 2026-11-23
model: "claude-opus"
branch: "master"
worktree: "/Users/jhf/ssb/sql_saga"
done_at: "2026-11-24T05:00"
---

## Goal
1.14 ships every fix deferred during the 1.9 to 1.13 releases, with nothing new left for a later release.

## Purpose
The owner runs a clean code ship: work found during a release is done in it, not filed for later. 1.13 left a disabled-trigger gap, an inconsistent natural-key rule and a 1.9 data defect for sites to repair.

## Principle
Add-ons found while doing these belong in this sweep. Anything uncertain is asked of the owner at once, never filed as a later task. Released through the same gates as 1.13.

## Details
Children: a5sdk3 (check_integrity reports a disabled sync trigger), mnntde (natural key always bound to its primary key), and the 1.9 jsonb provenance repair. Agreed with the owner 2026-11-23.

## Depends on

## Acceptance
- [x] Every child is done
- [x] v1.14 tagged on a CI-verified master commit; todo.md records it

## Notes


check 1: qnzn33, mnntde, a5sdk3, k5zndr done
check 2: v1.14 annotated at 87085d0, CI 45314858890 green; todo.md a35a10a
Reflection (unanswered, 2026-11-24, sabertooth)
## History
- 2026-11-24T05:00 todo -> doing/active (take)
- 2026-11-24T05:00 doing/active -> done (done); was doing_since: 2026-11-24T05:00
