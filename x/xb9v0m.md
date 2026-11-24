---
id: xb9v0m
title: "make test-native aborts on macOS before running any test"
state: "doing"
type: bug
priority: "low"
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzzzzx"
created: 2026-11-24
updated: 2026-11-24
parent: "pj849j"
doing_since: "2026-11-24T16:25"
stage: "active"
model: "claude-opus-5-5"
branch: "master"
worktree: "/Users/jhf/ssb/sql_saga"
---

## Goal
make test-native runs the native planner's unit tests on macOS as it does on Linux.

## Purpose
Found while fixing 2hxkwx: on macOS the test executable aborts at load ('symbol not found in flat namespace _CurrentMemoryContext'); the Makefile only sets the lazy-binding link flags for Linux. Unit tests of the sweep planner (the default planner) can then only run in CI, so a developer on macOS gets no fast feedback.

## Principle
Same tests on both platforms; no skipped tests. Keep the installed library free of test link flags (own target dir, as on Linux).

## Details
Makefile lines ~113-121 (NATIVE_TEST_ENV only for PORTNAME linux). macOS candidates: -C link-arg=-undefined -C link-arg=dynamic_lookup, or -Wl,-flat_namespace with lazy binding; the data symbols the test module defines may need the same on macOS. Reproduced on 1.14 code (not caused by 1.15 changes). CI (ubuntu) runs it: 7 passed.

## Depends on

## Acceptance
- [x] make test-native passes on macOS (Postgres.app 18) and in CI

## Notes

landed d04b7b4 on master 2026-11-24: Native planner: read the target per key set, combined by ctid (aqqsgb); make…


check 1: make test-native 7 passed on macOS (Postgres.app 18); CI ubuntu passed 7 before; d04b7b42
## History
- 2026-11-24T16:25 todo -> doing/active (take)
