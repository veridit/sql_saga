# pg_dump loses the load order of extension configuration tables

Upstream bug report, not yet sent (hug task hsa1hp). Approved by the owner on 2026-11-13.

| File | Content |
|---|---|
| [pgsql_bugs_report.txt](pgsql_bugs_report.txt) | The approved report text for the pgsql-bugs form |
| [config_fk_order.control](config_fk_order.control), [config_fk_order--1.0.sql](config_fk_order--1.0.sql) | The reproducer: a two-table extension whose configuration tables reference each other |
| [parallel_restore_failure.log](parallel_restore_failure.log) | `pg_restore -j 2` failing on it (PostgreSQL 18.3) |

Submit at <https://www.postgresql.org/account/submitbug/> (a postgresql.org account; it mails pgsql-bugs). The owner
signs in and confirms the filled form before it is sent.
