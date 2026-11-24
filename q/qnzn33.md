---
id: qnzn33
title: "Updates as a chain of single-release steps instead of direct scripts"
state: "doing"
type: feature
priority: "high"
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzzz"
created: 2026-11-23
updated: 2026-11-23
parent: "2q5nvf"
doing_since: "2026-11-24T02:12"
stage: "active"
model: "claude-opus"
branch: "master"
worktree: "/Users/jhf/ssb/sql_saga"
---

## Goal
Each release ships one step script; PostgreSQL chains them from any released version in one transaction.

## Purpose
Direct scripts copy every earlier script each release (1.13: 14 files, 19 MB) and need hand-written routine lists; the 1.14 signature change needed a crutch flag.

## Principle
Owner approved 2026-11-23: PostgreSQL's own chain, each step with its own release's definitions. The released single steps change only as doc/design/update-chain.md lists (devops/chain_steps.py, checked against the tags). No hand-written routine lists; version only in sql_saga.control. The assembly variant was tried and rejected.

## Details
Design: doc/design/update-chain.md. Committed ed7d9c8 (chain, 142 MB -> 17 MB in versions/), cd70456. The 1.14 signature changes (add_unique_key, its worker, the guard generator) are found by devops/update_step.py with no entry (9d5f52d).

## Depends on

## Acceptance
- [ ] ~~doc/design/update-assembly.md reviewed by an independent agent; every blocking finding resolved in the document~~ (not relevant: it named update-assembly.md, the rejected design; replaced by check 4 (update-chain.md), sabertooth, 2026-11-24)
- [ ] make builds every sql_saga--<source>--1.14.sql from the template, with no per-release generator or signature list
- [ ] The upgrade matrix passes for every released source; the 1.14 add_unique_key signature change needs no hand-written entry
- [ ] doc/design/update-chain.md reviewed by an independent agent; every blocking finding resolved in the document
- [ ] make builds sql_saga--1.13--1.14.sql with devops/update_step.py; versions/ holds the released steps exactly as devops/chain_steps.py derives them from the tags
- [ ] The upgrade matrix passes for every released source through the chain, locally and in CI

## Notes


## History
- 2026-11-24T02:12 todo -> doing/active (take)
