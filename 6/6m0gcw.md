---
id: 6m0gcw
title: "Design native regular-to-temporal foreign keys for PostgreSQL"
state: todo
type: spike
priority: "low"
assignee: []
labels: ["upstream"]
group: ""
rank: "y"
created: 2026-11-16
updated: 2026-11-16
---

## Goal
A design for native PostgreSQL foreign keys from a plain table to a key in a temporal parent's history

## Purpose
Long-term replacement for sql_saga's C-trigger regular foreign keys

## Principle
Separate project, later. Owner agreed 2026-10-26: practical narrow helpers in sql_saga now, upstream changes another time

## Relevant details
Scope: a child without a time dimension referencing a key that exists anywhere in the temporal parent's history, including the last-row delete check and CASCADE.

## Depends on

## Acceptance
- [ ] Design document reviewed by the owner

## Notes
