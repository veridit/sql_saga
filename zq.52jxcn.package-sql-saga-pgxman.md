---
id: 52jxcn
title: "Package sql_saga with pgxman"
state: todo
type: chore
priority: "low"
assignee: []
labels: []
created: 2026-11-16
updated: 2026-11-16
---

## Goal
sql_saga installs with pgxman, including the C and internal native libraries

## Purpose
Easier distribution than building from source

## Principle
The native library stays internal: no cargo pgrx install, no separate extension

## Relevant details
make install already stages with DESTDIR

## Depends on

## Acceptance
- [ ] pgxman install of sql_saga works on a clean PostgreSQL 18

## Notes
