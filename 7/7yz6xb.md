---
id: 7yz6xb
title: "Document the complex and expected-failure test cases"
state: "doing"
type: docs
priority: "low"
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zh"
created: 2026-11-16
updated: 2026-11-16
doing_since: "2026-11-22T21:35"
stage: "active"
model: "claude-opus-5-5"
branch: "feature/provenance-columns"
worktree: "/Users/jhf/ssb/sql_saga"
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


## History
- 2026-11-22T21:35 todo -> doing/active (take)
