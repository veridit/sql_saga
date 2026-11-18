---
id: 1bf9de
title: "Decide whether history reads fail closed during a parallel restore"
state: todo
type: spike
priority: "low"
assignee: []
labels: []
group: ""
rank: "zv"
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

## Requests
- 2026-11-18T20:18 request 1 by jorgen:
  I don't understand what this gives us. You're missing the relevant details. I don't understand how you would possibly do this. It's like, I would do something. Yes. What? Why? How?

## Notes


## History
- 2026-11-18T20:18 request 1 by jorgen
- 2026-11-18T20:18 request 1 not delivered: no coordinator set (hug task coordinator NAME)
