---
id: 4h7png
title: "PL/pgSQL planner reports one entity as two when its natural key starts late"
state: todo
type: feature
priority: ""
assignee: []
labels: []
group: ""
rank: "zzzzzzx"
created: 2026-11-25
updated: 2026-11-25
---

## Goal
Both planners give the same target_entity_keys for an entity whose natural key is NULL on its early pieces.

## Purpose
Feedback names the entity a row touched; two names for one entity misleads callers counting entities.

## Principle
entity_keys carry the entity's natural key values, as the native planner does.

## Details
Review 1.15 S1. Fixed in 98c3a82 via entity_nk_json per grouping key in diff_ranked.

## Depends on

## Acceptance
- [ ] test 166 whole-entity scenario: red before, both planners one key after

## Notes
