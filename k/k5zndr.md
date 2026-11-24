---
id: k5zndr
title: "Repair jsonb provenance values that 1.9 stored as JSON strings"
state: "doing"
type: bug
priority: "high"
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzzy"
created: 2026-11-23
updated: 2026-11-23
parent: "2q5nvf"
doing_since: "2026-11-24T05:00"
stage: "active"
model: "claude-opus"
branch: "master"
worktree: "/Users/jhf/ssb/sql_saga"
---

## Goal
The 1.14 update turns jsonb provenance values that 1.9 wrote as a JSON string back into the JSON object they encode.

## Purpose
1.9 evaluated each provenance DEFAULT as text, so a jsonb column got a JSON string instead of an object. doc/upgrade.md (1.10) left the repair to each site; it is mechanical and unambiguous, so the update does it.

## Principle
Repair only what can only be the 1.9 defect: a column in era.provenance_columns of type jsonb (or a domain over it) whose value is a JSON string that parses to an object. Report the count in a NOTICE; touch nothing else.

## Details
Provenance values come only from the column DEFAULT via sql_saga.provenance_trigger, so a jsonb provenance value that is a string encoding an object has no other source. The repair UPDATE must not fire the provenance trigger (it would overwrite the value) nor write history a second time; check system versioning on the same table. Remove the manual repair text from doc/upgrade.md once the update does it.

## Depends on

- qnzn33
## Acceptance
- [x] A 1.9 database with such rows, updated to 1.14, has objects in those columns and a NOTICE with the count; a jsonb provenance value that is a plain string not encoding an object is untouched
- [x] The upgrade matrix gate fails on the v1.13 binaries and passes on 1.14

## Notes

check 1: Upgrade gate from 1.9 and 1.13 (fixture writes the 1.9 strings): base and history converted to objects, plain-string value untouched, history count and REPLICA trigger mode unchanged; NOTICE 'Converted the jsonb provenance values of N rows' (c03b7b9). Manual probe on real 1.9 binaries: 2 rows + history converted

check 2: RED on the 1.13 step (strings kept), GREEN on 1.14 from 1.9 and 1.13


## History
- 2026-11-24T05:00 todo -> doing/active (take)
