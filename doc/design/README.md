# Design documents

Reviewed designs behind the code. Code comments cite them; user-facing behaviour is in README.md, upgrades in
`doc/upgrade.md`, and short decision records in `doc/decision-log.md`. Each document states its status, the code
that implements it and the tests that hold it.

| Document | Decision | Status |
|---|---|---|
| [history-access.md](history-access.md) | Access to history is checked at read time against the base table: fixed grants, a gated view, policy copies with an installation token, FORCE mirrored, no user policies on history | 1.1, current |
| [manual-schema-changes.md](manual-schema-changes.md) | Column changes of a versioned table are made by hand on both tables; columns match by name; `rebuild_system_versioning_views`; the writer fails closed with the fix | 1.1, current |
| [drop-protection.md](drop-protection.md) | Event triggers act only on the current statement; family sanction; column and function rename attribution; administrative `check_integrity()` | 1.2, current |
| [restore.md](restore.md) | `pg_restore --clean` teardown recognition; restoring dumps of older releases at their own version; the `catalog_format` safeguard | 1.2 to 1.4, current |
| [catalog-column-order.md](catalog-column-order.md) | Key and foreign key columns follow the native constraint order; updatable views follow their base table's columns (1.3 audit) | 1.3, current |

The planning briefs, review rounds and probe logs these were distilled from are archived outside the repository
(`sql_saga_archive/sql_saga_tmp_2026-11-15.tar.zst`).

## Old names

Comments inside the bodies of functions released in 1.7 still name the working notes. Changing a released body
needs a release, so they are updated with the next change to those functions. Until then:

| Cited as | Now |
|---|---|
| `tmp/design_final.md` (any section or decision) | [history-access.md](history-access.md) |
| `tmp/design_restore_clean.md` | [restore.md](restore.md) |
| `tmp/design_manual_schema_changes.md` | [manual-schema-changes.md](manual-schema-changes.md) |
| `tmp/fable_audit_v13.md`, `tmp/STATBUS_REPORT_xor_key_order.md` | [catalog-column-order.md](catalog-column-order.md) |
| `tmp/design_drop_protection_v12_r4.md` | [drop-protection.md](drop-protection.md) |
