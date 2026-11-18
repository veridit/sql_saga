---
id: f86twg
title: "Provenance columns: sql_saga stamps changed pieces, keeps carried ones"
state: todo
type: spike
priority: "medium"
assignee: []
labels: ["statbus"]
group: ""
rank: "zz"
created: 2026-11-18
updated: 2026-11-18
---

## Goal
The owner can decide on a sql_saga feature where an era names its writer and time columns and their fill functions, so temporal_merge stamps the pieces whose data changed and carries the writer of untouched pieces, without elevated rights and without mixing writers when it coalesces

## Purpose
StatBus febdra: a regular user's partial edit of a row another user wrote is refused by WITH CHECK (edit_by_user_id = auth.uid()) on the carried pieces; StatBus restores WITH CHECK (true) now and wants the writer to stay meaningful

## Principle
D0/D1 (decision-log): writes run as the caller, PostgreSQL authorizes; no SECURITY DEFINER writer role (StatBus idea 1) without an explicit owner exception. No value-equality heuristic in a trigger (idea 2)

## Relevant details
Probe on 1.7, 2026-11-18, era with ephemeral edit_by/edit_at: (1) a split keeps the admin as writer of both carried pieces and stamps the user on the changed piece; (2) a re-save with unchanged data writes nothing; (3) coalescing breaks provenance: when the user's new value equals a neighbouring carried piece, the two merge into one row whose writer is the later segment's: user sets 2025 to admin's 2024 value -> [2024,2026) credited to the user; user sets late 2024 to admin's 2025+ value -> [2024-06,inf) credited to the admin. Ephemeral columns are excluded from the coalescing comparison by design. A trigger cannot tell a carried INSERT from a new one (no OLD), which is why idea 2 needs a lookup. Open: compare the writer column during coalescing (but not the time column?), or never coalesce across writers; how direct base-table DML gets stamped

## Depends on

## Acceptance
- [ ] A written proposal with the probe cases as tests, reviewed by the owner
- [ ] Owner decision recorded in a note

## Notes

Shape discussed with owner 2026-11-18: explicit registration per era, not discovery by name. E.g. sql_saga.set_era_provenance(table, era, writer_column => 'edit_by_user_id', writer_function => 'auth.uid()', time_column => 'edit_at', time_function => 'statement_timestamp()'), stored in sql_saga.era (catalog change, so a release with catalog_format bump), function bound by signature and validated at registration like add_current_view's current_func_name, called once per statement as the caller. Alternative: no function, use each column's DEFAULT (StatBus already has DEFAULT auth.uid()). Open: an explicitly supplied writer in the source (import on behalf of someone) wins or is overwritten.

Owner 2026-11-18 01:53: must be declarative, no detection. Simplest form proposed: one era property listing provenance columns, values from each column's own DEFAULT, e.g. add_era(..., provenance_columns => ARRAY['edit_by_user_id','edit_at']) / set_era_provenance_columns(table, era, columns). Owner thinks it over until morning.

Owner 2026-11-18 15:24: name to settle before any prototype; prefers 'provenance' over 'stamp'. Requirement sharpened: ENFORCE, not just default: sql_saga sets writer/time on every write, including direct INSERT/UPDATE on the base table; today it is only a DEFAULT because enforcing it breaks on sql_saga's carried pieces. RLS is not the goal; tracking who did what is. Naming: era has unused audit_schema_name/audit_table_name (from periods), so 'audit_*' collides; edit_comment stays ephemeral (user-written why), only writer and time are provenance. Design: a managed trigger sets the columns on every write; carried pieces must be recognised unforgeably.
