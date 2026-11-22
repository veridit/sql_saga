---
id: 7yz6xb
title: "Document the complex and expected-failure test cases"
state: "done"
type: docs
priority: "low"
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zh"
created: 2026-11-16
updated: 2026-11-16
model: "claude-opus-5-5"
branch: "feature/provenance-columns"
worktree: "/Users/jhf/ssb/sql_saga"
done_at: "2026-11-22T21:42"
---

## Goal
Tests with intentional failures or intricate setups explain what they prove at the top of the file

## Purpose
Faster review; fewer accidental lock-ins of wrong output

## Principle
Comments in the test files, no behaviour change

## Relevant details
Candidates: tests using SAVEPOINT expect_error patterns and the adversarial tests 117, 139, 146

## Depends on

## Acceptance
- [x] Each listed test starts with a short statement of what it proves

## Notes


Done at e107dad: headers on 051, 052, 088, 094, 117 (139, 146, 061, 066 already had them; 010, 016 state purpose in first comments). Comment-only expected diffs; make test fast 147/147. Writing 094's header found s9 has never failed since v1.0: era check constraints and sync trigger are not drop-protected, filed as p60d2m.
Reflection (open, 2026-11-22, sabertooth): Writing a header that claims what a test proves forced checking each claim against the expected output, which exposed a '-- fails' comment that was never true.
## History
- 2026-11-22T21:35 todo -> doing/active (take)
- 2026-11-22T21:42 doing/active -> done (done); was doing_since: 2026-11-22T21:35
