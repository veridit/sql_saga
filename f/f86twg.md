---
id: f86twg
title: "Provenance columns: sql_saga stamps changed pieces, keeps carried ones"
state: "doing"
type: spike
priority: "medium"
assignee: ["sabertooth"]
labels: ["statbus"]
group: ""
rank: "c"
created: 2026-11-18
updated: 2026-11-18
doing_since: "2026-11-18T22:14"
stage: "active"
model: "claude"
branch: "master"
worktree: "/Users/jhf/ssb/sql_saga"
---

## Goal
The owner decides on provenance and annotation columns for an era, and sql_saga then fills them on every write path

## Purpose
StatBus febdra: a regular user's partial edit of a row another user wrote is refused by WITH CHECK (edit_by_user_id = auth.uid()) on the carried pieces; StatBus restores WITH CHECK (true) now and wants the writer to stay meaningful

## Principle
D0/D1 (decision-log): writes run as the caller, PostgreSQL authorizes; no SECURITY DEFINER writer role (StatBus idea 1) without an explicit owner exception. No value-equality heuristic in a trigger (idea 2)

## Relevant details
Design: doc/design/provenance.md (decisions 1-6 of 2026-11-21 recorded there). Failing test: sql/161_provenance_columns.sql. Branch: feature/provenance-columns. Open: a view change whose comment equals the old one stores NULL (proposal) or is refused.

## Depends on

## Acceptance
- [ ] A written proposal with the probe cases as tests, reviewed by the owner
- [ ] Owner decision recorded in a note

## Notes

Shape discussed with owner 2026-11-18: explicit registration per era, not discovery by name. E.g. sql_saga.set_era_provenance(table, era, writer_column => 'edit_by_user_id', writer_function => 'auth.uid()', time_column => 'edit_at', time_function => 'statement_timestamp()'), stored in sql_saga.era (catalog change, so a release with catalog_format bump), function bound by signature and validated at registration like add_current_view's current_func_name, called once per statement as the caller. Alternative: no function, use each column's DEFAULT (StatBus already has DEFAULT auth.uid()). Open: an explicitly supplied writer in the source (import on behalf of someone) wins or is overwritten.

Owner 2026-11-18 01:53: must be declarative, no detection. Simplest form proposed: one era property listing provenance columns, values from each column's own DEFAULT, e.g. add_era(..., provenance_columns => ARRAY['edit_by_user_id','edit_at']) / set_era_provenance_columns(table, era, columns). Owner thinks it over until morning.

Owner 2026-11-18 15:24: name to settle before any prototype; prefers 'provenance' over 'stamp'. Requirement sharpened: ENFORCE, not just default: sql_saga sets writer/time on every write, including direct INSERT/UPDATE on the base table; today it is only a DEFAULT because enforcing it breaks on sql_saga's carried pieces. RLS is not the goal; tracking who did what is. Naming: era has unused audit_schema_name/audit_table_name (from periods), so 'audit_*' collides; edit_comment stays ephemeral (user-written why), only writer and time are provenance. Design: a managed trigger sets the columns on every write; carried pieces must be recognised unforgeably.

Probe on 1.8 (2026-11-18, tmp/prov_probe.sql), table lu with edit_by name DEFAULT current_user, edit_at DEFAULT clock_timestamp(), both ephemeral; admin wrote both rows. Case 4: direct base UPDATE by user with SET edit_by='admin' -> forged value kept. Case 5: direct base UPDATE by user without touching edit_by -> row still credited to admin (DEFAULT only applies to INSERT). Case 6: user splits via for_portion_of view (SET name='mid' for 2024-04..07) -> carried pieces correctly admin, but the CHANGED piece 'mid' is also credited to admin: the view row starts from OLD, so the old writer travels into the source. So today the writer column is wrong after every UPDATE that does not set it, through the view and directly. Code: native/src/sweep.rs computes data_hash without ephemeral columns (coalescing ignores writers), merges ephemeral payload target-then-source, and diff skips pieces whose data is unchanged (that is how carried pieces are recognised: they are not written, or written with their target ephemeral).

Probe 2 (tmp/prov_probe2.sql): user runs temporal_merge PATCH with a source that has no edit_by -> the NEW piece 'late' [2024-10,2025) is credited to admin (planner merges target ephemeral, then source ephemeral on top). The executor wrote two rows: UPDATE of the carried piece A shrinking to [2024-07,2024-10), INSERT of the new piece. In the split case the carried remainder is an INSERT. So carried writes are both UPDATE and INSERT, and a row trigger alone cannot tell a carried INSERT from a user INSERT.

New probe facts (test 161 on 1.8): in the view split the carried remainder [2024-07,2025) is INSERTed with a NEW edit_at but the old edit_by (DEFAULT fires for edit_at because the planner did not supply it, but did supply edit_by): today's carried pieces already lose their time. Case 8: a source-supplied edit_by='admin' for a NEW entity is kept, edit_at=0 replaced by DEFAULT (0 is not... actually new; check).

Correction of the previous note. Test 161 (hand-written expected = correct behaviour) fails on 1.8 exactly where the proposal says: registration missing; split: changed piece 'mid' credited to admin, and the carried remainder A [2024-07,2025) keeps writer admin but gets a NEW edit_at (the planner supplies edit_by from the target but the INSERT takes edit_at from its DEFAULT: today carried pieces already lose their time); case 3 user's piece credited to admin; 4 forged writer kept; 5 writer not updated; 7 and 8 new piece credited to admin. Case 2 (re-save) already right.


2026-11-21 owner with StatBus: 1-5 yes (2: always overwrite with DEFAULT; StatBus makes DEFAULT name the writer on every path before adopting 1.9, sql_saga's rule must not depend on how). Added: edit_comment as annotation_columns (carried keeps own, changed never inherits); clear refusal when a NOT NULL provenance DEFAULT gives NULL; backfill procedure with trigger valid_provenance. One open question: view change with an unchanged comment -> NULL or refuse.
## History
- 2026-11-18T22:14 todo -> doing/active (take)
