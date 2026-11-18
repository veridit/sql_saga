---
id: 1bf9de
title: "Decide whether history reads fail closed during a parallel restore"
state: todo
type: spike
priority: "low"
assignee: []
labels: []
group: ""
rank: "zv"
created: 2026-11-16
updated: 2026-11-16
---

## Goal
The owner confirms or drops failing closed on direct history-table reads between a parallel restore and its final sync_all_history_policies()

## Purpose
Close a short window in which history policies are not yet synchronized

## Principle
Owner judged it likely unnecessary; do not build without a decision

## Depends on

## Acceptance
- [ ] Decision recorded in a note

## Requests
- 2026-11-18T20:18 request 1 by jorgen:
  I don't understand what this gives us. You're missing the relevant details. I don't understand how you would possibly do this. It's like, I would do something. Yes. What? Why? How?

## Notes


What this is about, in full. Only the parallel restore in README (Backup and Restore) is affected: pg_restore -j 4 with sql_saga.parallel_restore=on. In that mode sql_saga does not copy row-level security policies from a base table to its history table while the restore runs, because two workers restoring the base and the history of one family would deadlock. The last line of the procedure, SELECT sql_saga.sync_all_history_policies(), does the copying. Between the -j command and that line, what does a reader get? (a) Through sql_saga's history views, users under row-level security get an error: already fail closed. (b) Reading the history table directly is filtered by the policies the dump itself restored. Those are the source database's own policy copies under the source's token, so they hide what the source hid. Option on the table: also make (b) fail with an error until the sync runs, e.g. a policy that refuses everyone until sync_all_history_policies() replaces it. Why not worth it, in my view: (b) already filters correctly, the window lasts until one command that the documented procedure runs next, and the procedure says not to use the database before it. The cost: one more restore-only mechanism on every history table, which a restore must also clean up. Recommendation: drop it (close as won't do). It needs your yes or no.
## History
- 2026-11-18T20:18 request 1 by jorgen
- 2026-11-18T20:18 request 1 not delivered: no coordinator set (hug task coordinator NAME)
