---
id: hsa1hp
title: "Submit the parallel-restore bug report to pgsql-bugs"
state: todo
type: chore
priority: "high"
assignee: []
labels: ["upstream"]
group: ""
rank: "h"
created: 2026-11-16
updated: 2026-11-16
---

## Goal
The approved report about pg_dump losing the load order of extension configuration tables is on pgsql-bugs

## Purpose
Parallel pg_restore -j of any sql_saga database fails intermittently; only an upstream fix removes the split-restore workaround

## Principle
The owner signs in; the agent fills the form from the approved text, shows it, and submits only after the owner confirms. Never in a client's browser window

## Relevant details
Text: tmp/upstream_repro_20261113/pgsql_bugs_report.txt (approved 2026-11-13), reproducer beside it. Re-run on PostgreSQL 18.3: serial restore works, pg_restore -j 2 failed 5/5. Same code in REL_18_STABLE (18.7) and master. Form: https://www.postgresql.org/account/submitbug/, mails pgsql-bugs@lists.postgresql.org

## Depends on

## Acceptance
- [ ] Owner confirmed the filled form before submission
- [ ] Bug number or list message link recorded in a note

## Notes
