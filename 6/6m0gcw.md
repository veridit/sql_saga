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
A design for core PostgreSQL support of a child without a time dimension referencing a key that exists in a temporal parent's history, including last-row delete and CASCADE

## Purpose
Long-term replacement for sql_saga's C-trigger regular foreign keys

## Principle
Separate project, later. Owner agreed 2026-10-26: practical narrow helpers in sql_saga now, upstream changes another time

## Relevant details
Semantic baseline: the 1.4 C-trigger behaviour and the parity tests (149, 137-148)

## Depends on

## Acceptance
- [ ] Design document reviewed by the owner

## Notes
