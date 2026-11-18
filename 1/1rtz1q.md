---
id: 1rtz1q
title: "Support parallel pg_restore once PostgreSQL fixes the load order"
state: todo
type: feature
priority: "low"
assignee: []
labels: ["upstream"]
group: ""
rank: "v"
created: 2026-11-16
updated: 2026-11-16
---

## Goal
When a fixed PostgreSQL release exists, parallel pg_restore -j is a supported case in the restore harness and README

## Purpose
Remove the known-failure carve-out and the split-restore instructions for fixed versions

## Principle
Only after a released PostgreSQL contains the fix

## Relevant details
devops/restore_roundtrip.sh keeps the parallel case as a known failure (7/20 in the 1.5 CI run)

## Depends on
- hsa1hp

## Acceptance
- [ ] Restore harness treats parallel restore as a required pass on the fixed version
- [ ] README.md restore section updated

## Notes
