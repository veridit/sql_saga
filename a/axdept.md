---
id: axdept
title: "Explore candidate SQL syntaxes for temporal merge and foreign keys"
state: "doing"
type: spike
priority: ""
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzzzl"
created: 2026-11-24
updated: 2026-11-24
parent: "0t4v31"
doing_since: "2026-11-24T14:03"
stage: "active"
model: "claude-opus-5-5"
branch: "master"
worktree: "/Users/jhf/ssb/sql_saga"
---

## Goal
Two or three candidate SQL syntaxes for the temporal_merge cases and regular-to-temporal keys, compared, to show upstream.

## Purpose
The owner wants a concrete syntax to talk about with the PostgreSQL temporal authors; a few compared candidates make the first conversation concrete without committing to one design. The full proposal (v6s4xq) comes after their buy-in.

## Principle
Exploration, not a spec: each candidate shows every temporal_merge mode as a statement, with its trade-offs. Prefer extending SQL's MERGE and SQL:2011 temporal terms; note where other databases (Db2, Oracle, MariaDB, SQL Server, Teradata) already have syntax. No grammar patch.

## Details
Inputs: the revert study (vhhp80, concurrency must be answerable in the syntax) and the three-area map (9fxqkc). Candidates to consider: MERGE ... FOR PORTION OF per source row; a TEMPORAL MERGE statement with mode keywords; table-level declarations (coalescing, ignored columns) plus plain DML. Write to doc/design/upstream-syntax-candidates.md.

## Depends on

- vhhp80
- 9fxqkc
## Acceptance
- [x] Each candidate expresses every temporal_merge merge mode and delete mode, and the regular-to-temporal key, as an example statement
- [x] A comparison table gives each candidate's fit with the standard, effort to implement and open questions

## Notes


## History
- 2026-11-24T14:03 todo -> doing/active (take)
