---
id: rtwrp6
title: "DELETE_MISSING_TIMELINE keeps target time beside interior source edges"
state: todo
type: bug
priority: "high"
assignee: []
labels: []
group: ""
rank: "zzzzzh"
created: 2026-11-24
updated: 2026-11-24
parent: "pj849j"
---

## Goal
DELETE_MISSING_TIMELINE deletes all of a source entity's target time the source does not cover, wherever source edges fall.

## Purpose
Found while checking mode semantics for the upstream map (9fxqkc). README and doc/api.md promise source-as-truth: target time not covered by the source is deleted. When a source row starts and ends inside target slices, the uncovered head and tail of those slices are kept instead. An import using it to replace a timeline then silently keeps stale history.

## Principle
Hand-written expected output first, failing on 1.14; fix the planner, not the test. Cover every Allen relation of source edges against target slices, with MERGE_ENTITY_REPLACE, _UPSERT and _PATCH, and with DELETE_MISSING_TIMELINE_AND_ENTITIES.

## Details
Repro: doc/upstream/repro/delete_missing_timeline_interior.sql (psql against a *_jhf db with sql_saga). Target entity 1: a [2020-01-01,2021-01-01), b [2022-01-01,2023-01-01). Source X [2020-06-01,2022-06-01), MERGE_ENTITY_REPLACE + DELETE_MISSING_TIMELINE. Expected: only X [2020-06-01,2022-06-01). Actual: a [2020-01,2020-06), X, b [2022-06,2023-01); the plan SHRINKs the two slices instead of deleting the uncovered parts. Works when the source starts on a target boundary (variant D) and in test 069 scenario 1. Planner: src/2k_temporal_merge_plan.sql around the delete_mode resolver (v_final_data_payload_expr := 's_data_payload' for DMT). Existing coverage: sql/069, sql/129 (lines 237-259) only use boundary-aligned sources.

## Depends on

## Acceptance
- [x] A regression test with hand-written expected output fails on 1.14 for interior source edges and passes after the fix
- [x] The test covers source edges inside, on and outside target slices, for REPLACE, UPSERT and PATCH, with both timeline delete modes
- [x] make test fast passes

## Notes

Native planner only: the PL/pgSQL planner (use_plpgsql_planner) deletes the uncovered head and tail correctly. Cause: native/src/sweep.rs resolve_payloads keeps target-only segments of an entity that has sources; the uncovered part of a partly covered target row coalesces with its ancestor and becomes SHRINK/INSERT. PL/pgSQL drops them (v_final_data_payload_expr := 's_data_payload' under DMT). Fix: under deletes_timeline, skip target-only segments when the entity has active sources; fully uncovered rows still become DELETE in compute_diff.
