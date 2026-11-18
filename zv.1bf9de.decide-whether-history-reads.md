---
id: 1bf9de
title: "Decide whether history reads fail closed during a parallel restore"
state: todo
type: spike
priority: "low"
assignee: []
labels: []
created: 2026-11-16
updated: 2026-11-16
---

## Goal
The owner confirms or drops failing closed on direct history-table reads between a parallel restore and its final sync_all_history_policies()

## Purpose
Close a short window in which history policies are not yet synchronized

## Principle
Owner judged it likely unnecessary; do not build without a decision

## Depends on

## Acceptance
- [ ] Decision recorded in a note

## Notes
