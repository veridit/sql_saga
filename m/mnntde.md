---
id: mnntde
title: "Refuse a natural key whose consistency guard cannot be built (composite PK)"
state: todo
type: spike
priority: "medium"
assignee: []
labels: []
group: ""
rank: "zzzv"
created: 2026-11-23
updated: 2026-11-23
---

## Goal
Decide whether add_unique_key refuses a natural key on a table whose primary key is composite, or keeps opting out silently.

## Purpose
Test 016 scenario 4a says '-- fails' but has never failed since c71c01f (2025-09-29): the key is added without PK consistency enforcement, while the mirror case 4b (composite PK after a natural key) is refused.

## Principle
Tests document intended behaviour. Either the code or the test comment is wrong; decide which, then make them agree.

## Details
Question for the owner: when the consistency check cannot be enforced because the stable PK part is composite, should add_unique_key with the default enforce_consistency_with_primary_key (NULL) (a) refuse, as 4b does, or (b) add the key without enforcement and say so in a NOTICE? Today: src/1j_add_unique_key.sql line ~480 opts out silently unless enforcement was explicitly requested; line ~618 (4b path) always refuses. (a) changes installed behaviour and needs a release, and may refuse existing StatBus-like calls; (b) only needs a NOTICE plus the test comment fixed. Found by auditing every '-- fails' comment in expected/*.out against its output: 33 such statements, 7 without a visible ERROR, 6 of those caught inside DO blocks (correct), 1 real (this one).

## Depends on

## Acceptance
- [ ] The owner's decision is recorded here, and 016 4a's comment and expected output match the chosen behaviour

## Notes

Owner decided (a), 2026-11-23: refuse in both orders. Reason: a natural key that can drift between primary keys is dangerous and confusing; StatBus import checks for exactly this. Order must not matter. Checked: every StatBus table with a natural key has a single-column primary key (id), so StatBus is unaffected. Explicit enforce_consistency_with_primary_key => false stays the documented opt-out and is unaffected.
