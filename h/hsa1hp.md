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
Files (in the repository): doc/upstream/pg-dump-config-fk-order/README.md (index), pgsql_bugs_report.txt (the approved text, 2026-11-13), config_fk_order.control and config_fk_order--1.0.sql (reproducer), parallel_restore_failure.log. Re-run on PostgreSQL 18.3: serial restore works, pg_restore -j 2 failed 5/5. Same code in REL_18_STABLE (18.7) and master. Form: https://www.postgresql.org/account/submitbug/, mails pgsql-bugs@lists.postgresql.org. Moved 2026-11-26 from tmp/upstream_repro_20261113/, which is gitignored.

## Depends on

## Acceptance
- [ ] Owner confirmed the filled form before submission
- [ ] Bug number or list message link recorded in a note

## Notes

file:pgsql_bugs_report.txt (3665 bytes, sha256:403fb398) the approved report text (2026-11-13), for the pgsql-bugs form

file:config_fk_order.control (100 bytes, sha256:47942e1a) reproducer: a two-table extension whose configuration tables reference each other

file:config_fk_order--1.0.sql (384 bytes, sha256:d9e6f03f) reproducer: a two-table extension whose configuration tables reference each other

file:parallel_restore_failure.log (269 bytes, sha256:20002445) pg_restore -j 2 failing on the reproducer, PostgreSQL 18.3
