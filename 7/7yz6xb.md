---
id: 7yz6xb
title: "Document the complex and expected-failure test cases"
state: todo
type: docs
priority: "low"
assignee: []
labels: []
group: ""
rank: "zh"
created: 2026-11-16
updated: 2026-11-16
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
- [ ] Each listed test starts with a short statement of what it proves

## Notes
