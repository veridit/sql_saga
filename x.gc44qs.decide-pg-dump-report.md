---
id: gc44qs
title: "Decide on a pg_dump report for CHECK constraints that read other tables"
status: todo
type: spike
priority: "low"
assignee: []
labels: ["upstream"]
created: 2026-11-16
updated: 2026-11-16
---

## Goal
The owner decides whether to send the separate report on CHECK constraints calling functions that read other tables

## Purpose
A real PostgreSQL ordering problem, but it no longer affects sql_saga since 1.3 (constraint triggers)

## Principle
Optional; owner decides

## Relevant details
Draft: tmp/upstream_pg_dump_check_order.md, not sent

## Depends on

## Acceptance
- [ ] Owner decision recorded in a note (send or drop)

## Notes
