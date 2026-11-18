---
id: dee3fq
title: "Remove the dead era audit columns; guard history tables' persistence"
state: "doing"
type: bug
priority: "high"
assignee: ["sabertooth"]
labels: ["clean-code"]
group: ""
rank: "zzh"
created: 2026-11-18
updated: 2026-11-18
doing_since: "2026-11-18T16:48"
stage: "active"
model: "claude"
branch: "master"
worktree: "/Users/jhf/ssb/sql_saga"
---

## Goal
sql_saga.era has no audit_schema_name/audit_table_name, and making a system-versioned table's history UNLOGGED is refused at once like its base table, with an actionable message

## Purpose
The two columns were left from the periods extension's system versioning (2023-10-31, 470376b), are never written, always NULL, and the only check reading them is dead code, so ALTER TABLE <history> SET UNLOGGED is accepted silently and only check_integrity later reports it, with a repair hint (rebuild_system_versioning_views) that does not change persistence

## Principle
Clean code: no dead catalog columns or dead checks. Fail fast with the exact fix (ALTER TABLE <h> SET LOGGED). A catalog change ships in a release, with catalog_format bump, update scripts and tests

## Relevant details
Uses: src/0b_schema.sql:87-88, src/0e_internal_functions.sql:131 (__internal_ddl_command_affects_managed_object), src/2h_health_checks.sql:133-142 (dead history persistence loop). Live probe 2026-11-18 on 1.7: both eras NULL/NULL; ALTER TABLE doc SET UNLOGGED refused ('must remain persistent because it has an era'); ALTER TABLE doc_history SET UNLOGGED accepted, relpersistence u; check_integrity then: 'history family definition is noncanonical; repair with rebuild_system_versioning_views'. 25 expected files list the columns (era catalog dumps). Dropping catalog columns changes the dump format: catalog_format 1.4 -> next, preflight, old-dump procedure; check whether it can share a release with the provenance work (f86twg)

## Depends on

## Acceptance
- [x] Failing test first: ALTER TABLE <history> SET UNLOGGED is refused with 'history table ... must remain persistent' and HINT naming SET LOGGED
- [ ] audit_schema_name and audit_table_name gone from src, fresh install and every update path; check_integrity passes after update
- [ ] Upgrade matrix and restore round trips green, including an old dump restored at its own version

## Notes


## History
- 2026-11-18T16:48 todo -> doing/active (take)
