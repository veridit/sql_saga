---
id: 9aha4v
title: "StatBus: pin sql_saga 1.7 and use the view delete protocol"
state: "dropped"
type: chore
priority: "medium"
assignee: []
labels: ["statbus"]
group: ""
rank: "zx"
created: 2026-11-16
updated: 2026-11-16
dropped_reason: "Work in another system (StatBus), done there long ago. sql_saga tracks only its own work."
dropped_at: "2026-11-18T21:46"
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

## Requests
- 2026-11-18T20:17 request 1 by jorgen:
  This makes no sense to have here. It is long since done in StatBus. We should never make tickets in our system for things in another system
  resolved 2026-11-18T21:46 by sabertooth:
    Dropped. No more tasks here for work that belongs to StatBus.

## Notes


## History
- 2026-11-18T20:17 request 1 by jorgen
- 2026-11-18T20:17 request 1 not delivered: no coordinator set (hug task coordinator NAME)
- 2026-11-18T21:46 todo -> dropped (drop)
- 2026-11-18T21:46 resolved request 1 by sabertooth
