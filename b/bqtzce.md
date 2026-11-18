---
id: bqtzce
title: "Repoint tmp/ design citations inside released function bodies"
state: "doing"
type: chore
priority: "low"
assignee: ["sabertooth"]
labels: []
group: ""
rank: "e"
created: 2026-11-16
updated: 2026-11-16
doing_since: "2026-11-18T22:14"
stage: "active"
model: "claude"
branch: "master"
worktree: "/Users/jhf/ssb/sql_saga"
---

## Goal
The five comments inside released function bodies that still name tmp/ working notes point to doc/design/

## Purpose
Fresh clones can follow every citation; doc/design/README.md maps the old names until then

## Principle
Only together with the next real change to those functions (it changes routine bodies, so it ships in a release); never a release for comments alone

## Relevant details
src/0e_internal_functions.sql:85 (__internal_ddl_command_affects_managed_object), src/1i_add_system_versioning.sql:553 (__api_add_system_versioning), src/2e_drop_protection.sql:583 and :735 (__internal_drop_protection), src/2h_health_checks.sql:206 (__internal_health_checks)

## Depends on

## Acceptance
- [x] git grep 'tmp/design' src returns nothing
- [x] the old-names table in doc/design/README.md is removed

## Requests
- 2026-11-18T20:18 request 1 by jorgen:
  I think we should try to get this into the next release so that we do the cleanup and we don't leave garbage lying around
  resolved 2026-11-18T20:24 by sabertooth:
    Folded into 1.8 (not yet tagged): the last three tmp/ citations (__api_add_system_versioning, __internal_drop_protection x2) now name doc/design/; the other two went with 1.8's earlier changes. git grep 'tmp/design' src is empty. The old-names table became one note for the frozen versions/ scripts, which cannot change.

## Notes


## History
- 2026-11-18T20:18 request 1 by jorgen
- 2026-11-18T20:18 request 1 not delivered: no coordinator set (hug task coordinator NAME)
- 2026-11-18T20:24 resolved request 1 by sabertooth
- 2026-11-18T22:14 todo -> doing/active (take)
