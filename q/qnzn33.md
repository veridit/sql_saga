---
id: qnzn33
title: "Updates as a chain of single-release steps instead of direct scripts"
state: todo
type: feature
priority: "high"
assignee: []
labels: []
group: ""
rank: "zzzz"
created: 2026-11-23
updated: 2026-11-23
parent: "2q5nvf"
---

## Goal
Each release ships one step script; PostgreSQL chains them from any released version in one transaction.

## Purpose
Direct scripts copy every earlier script each release (1.13: 14 files, 19 MB) and need hand-written routine lists; the 1.14 signature change needed a crutch flag.

## Principle
Owner approved 2026-11-23: PostgreSQL's own chain, each step with its own release's definitions. The released single steps change only in their native check (this release or newer). No hand-written routine lists; version only in sql_saga.control. The assembly variant was tried and rejected.

## Details
Design: doc/design/update-chain.md. Prototype tmp/chain.py; empty DBs 1.0..1.12 -> 1.13 identical to fresh. Full data matrix running.

## Depends on

## Acceptance
- [ ] doc/design/update-assembly.md reviewed by an independent agent; every blocking finding resolved in the document
- [ ] make builds every sql_saga--<source>--1.14.sql from the template, with no per-release generator or signature list
- [ ] The upgrade matrix passes for every released source; the 1.14 add_unique_key signature change needs no hand-written entry

## Notes
