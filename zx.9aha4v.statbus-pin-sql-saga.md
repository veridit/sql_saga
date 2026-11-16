---
id: 9aha4v
title: "StatBus: pin sql_saga 1.7 and use the view delete protocol"
status: todo
type: chore
priority: "medium"
assignee: []
labels: ["statbus"]
created: 2026-11-16
updated: 2026-11-16
---

## Goal
StatBus pins sql_saga 1.7, builds Remove on the for_portion_of view delete protocol (STATBUS-611), and its six regular-to-temporal addendum references use ON DELETE CASCADE

## Purpose
Reference for the StatBus side; the work happens in the StatBus repository

## Principle
Tracked here for visibility only; done by StatBus agents in their repository

## Relevant details
References: external_ident, tag_for_unit, unit_notes x legal_unit_id, establishment_id. sql_saga test 142 asserts the cascade

## Depends on

## Acceptance
- [ ] StatBus repository pins 1.7

## Notes
