---
id: y7hbe9
title: "Planners disagree on REPLACE when the source lacks a target column"
state: todo
type: feature
priority: ""
assignee: []
labels: []
group: ""
rank: "zzzzzzz"
created: 2026-11-25
updated: 2026-11-25
---

## Goal
MERGE_ENTITY_REPLACE and REPLACE_FOR_PORTION_OF give the same result on both planners for a target column the source does not have.

## Purpose
REPLACE must mean one thing; today the default planner behaves as UPSERT and the PL/pgSQL planner clears the column, even a natural key column.

## Principle
Decide the rule first (the syntax design says REPLACE states every column the statement names, and a column the source does not have is not named, so it keeps its value), then make both planners follow it, test first.

## Details
Found while drafting doc/upstream/temporal-dml.md. Repro: doc/upstream/repro/replace_absent_column_planners.sql. Native: region kept per piece (North/South), new time NULL. PL/pgSQL: one piece 2023-2027 with region NULL and code NULL (natural key cleared). Native REPLACE is then identical to UPSERT for every case I ran; where should they differ? Proposed rule: REPLACE sets every column the source has, NULL included, and ignores columns it does not have; UPSERT is the same except it keeps target pieces separate... to be decided with the owner in the syntax review.

## Depends on

## Acceptance
- [ ] a rule for absent columns under REPLACE is written in doc/upstream/temporal-dml.md and README
- [ ] both planners follow it: test red before, green after

## Notes

StatBus uses MERGE_ENTITY_REPLACE/REPLACE_FOR_PORTION_OF in import procedures (legal_unit, location, contact, statistical variables, legal relationship), on the native planner: so the native behaviour (absent column kept) is what StatBus relies on. Changing native would change StatBus imports; check their source tables carry every column before deciding.
