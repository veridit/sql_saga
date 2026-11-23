---
id: qnzn33
title: "Assemble update scripts from pieces instead of per-release generators"
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
Update scripts are assembled at build time from a macro template, all target routines, and per-release step files.

## Purpose
Each release copies every older update script into a new one (1.0 to 1.13 is 69,213 lines, versions/ is 142 MB) and needs a hand-written generator with signature lists; 1.14's signature change needed a crutch flag.

## Principle
Same guarantees as today: one transaction, one native check, one integrity gate, site privileges never lost silently, released files never change. A simple line-macro assembler, no template language. Reviewed by another agent; the owner does not review.

## Details
Design: doc/design/update-assembly.md (owner approved direction 2026-11-23, review delegated). Floor at 1.8: older sources embed their frozen bridge to 1.8. Replaces devops/generate_update_scripts*.py, direct_update.py and the per-release spec tests; version only in sql_saga.control. First used by 1.14.

## Depends on

## Acceptance
- [ ] doc/design/update-assembly.md reviewed by an independent agent; every blocking finding resolved in the document
- [ ] make builds every sql_saga--<source>--1.14.sql from the template, with no per-release generator or signature list
- [ ] The upgrade matrix passes for every released source; the 1.14 add_unique_key signature change needs no hand-written entry

## Notes
