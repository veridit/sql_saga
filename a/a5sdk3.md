---
id: a5sdk3
title: "check_integrity reports a disabled era synchronization trigger"
state: todo
type: bug
priority: "medium"
assignee: []
labels: []
group: ""
rank: "zzzq"
created: 2026-11-22
updated: 2026-11-22
parent: "2q5nvf"
---

## Goal
check_integrity fails when an era's synchronization trigger is disabled, as it already does for a disabled provenance trigger.

## Purpose
With ALTER TABLE ... DISABLE TRIGGER on the sync trigger, valid_from/valid_until silently stop following the range column, and check_integrity says nothing.

## Principle
Report only; DISABLE TRIGGER stays allowed (drop protection covers drops, doc/design/drop-protection.md). Name the repair: ALTER TABLE ... ENABLE TRIGGER.

## Details
Found in the 1.13 review of p60d2m (probe on 1.12/1.13: ALTER TABLE dp DISABLE TRIGGER dp_p_sync_temporal_trg then check_integrity returns without error). The provenance check in src/2h_health_checks.sql already tests t.tgenabled = 'D'; the era check (violation 5) tests only existence and shape. Needs a release (installed SQL).

## Depends on

- qnzn33
## Acceptance
- [x] A test disables the sync trigger and check_integrity raises a named error with the ENABLE TRIGGER repair; expected output hand-written first
- [ ] An enabled (ALWAYS or REPLICA) sync trigger still passes; make test fast passes

## Notes

check 1: 116 case 'disabled era synchronization trigger': expected written first, showed 'unexpected success' on 1.13 code; named error with ALTER TABLE ... ENABLE TRIGGER repair (5f7c71e)
