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
- [ ] ~~make builds every sql_saga--<source>--1.14.sql from the template, with no per-release generator or signature list~~ (not relevant: direct scripts replaced by the chain; replaced by check 5, sabertooth, 2026-11-24)
- [ ] ~~The upgrade matrix passes for every released source; the 1.14 add_unique_key signature change needs no hand-written entry~~ (not relevant: restated for the chain as check 6, sabertooth, 2026-11-24)
- [x] doc/design/update-chain.md reviewed by an independent agent; every blocking finding resolved in the document
- [x] make builds sql_saga--1.13--1.14.sql with devops/update_step.py; versions/ holds the released steps exactly as devops/chain_steps.py derives them from the tags
- [x] The upgrade matrix passes for every released source through the chain, locally and in CI

## Notes


check 5: make builds sql_saga--1.13--1.14.sql via devops/update_step.py (13 unit tests OK); chain_steps.py --check versions exit 0; versions/ 142 MB -> 17 MB; 34/34 C symbols (ed7d9c8)
check 4: Independent agent (cat) reviewed design + code: tmp/review_update_chain.md. Blocking B1 (DROP ROUTINE) and B2 (dependency refusal) fixed with tests, N1/N4/N5 addressed, design doc updated (commit 69fcfe2).
check 6: Local upgrade matrix 14/14 GREEN (5408 checks) at 87085d0; CI 45314858890 green on every step incl. the matrix; tag v1.14
## History
- 2026-11-24T02:12 todo -> doing/active (take)
