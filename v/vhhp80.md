---
id: vhhp80
title: "Study the FOR PORTION OF revert and test temporal_merge for it"
state: todo
type: spike
priority: ""
assignee: []
labels: []
group: ""
rank: "zzzzv"
created: 2026-11-24
updated: 2026-11-24
parent: "0t4v31"
---

## Goal
A report on why FOR PORTION OF was reverted from PostgreSQL 19, and whether sql_saga has the same lost-update problem.

## Purpose
The owner's first step toward upstream work is following up on the mailing-list issues. Knowing them, and having a tested answer, is what makes a conversation with Paul Jungwirth and Peter Eisentraut worth their time. If sql_saga has the same concurrency bug, that is an sql_saga bug to fix regardless.

## Principle
Cite primary sources (pgsql-hackers thread, revert commit, docs), not secondhand summaries. Test concurrency with real concurrent sessions (isolation tester or parallel psql), not by reasoning. A found sql_saga bug gets its own hug task.

## Details
Revert commit a9d2f728240 (Peter Eisentraut, 2026-09-15, 23 commits; its message links the discussion thread). Reported cause: lost updates under READ COMMITTED when concurrent temporal updates and deletes touch the same rows; a FOR UPDATE workaround did not cover DELETE. Feature commit 8e72d914. sql_saga side: src/2k_temporal_merge_plan.sql, src/2l_temporal_merge_execute.sql, for_portion_of view triggers. Write the report to doc/design/upstream-for-portion-of-revert.md.

## Depends on

## Acceptance
- [ ] The report lists each issue raised in the revert discussion, with links to the messages
- [ ] Each issue has a concurrent-session test against temporal_merge and the for_portion_of view, with its observed result
- [ ] Any sql_saga failure found has its own hug task with the reproducer

## Notes
