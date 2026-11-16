# History access: privileges and row-level security

Status: implemented in 1.1, current in 1.7. Owner-approved 2026-10-13 to 2026-10-15 (decisions 1 to 7 below).
Code: `src/0k_history_access.sql`, `src/1i_add_system_versioning.sql`, `src/2h_health_checks.sql`,
`src/2e_drop_protection.sql`. Tests: 018, 024, 028, 123, 136. User documentation: README.md, "Privileges and
history policies".

## Principle

- Derived objects never hold a copy of who may access them. PostgreSQL checks the base table's ACL and its
  row-level security when the data is read.
- Every rule the health check enforces looks at one object, or at one family's fixed structure, so it holds after
  every statement of a `pg_restore` in any order: serial, `--single-transaction` or `-j N`.
- `GRANT` and `REVOKE` need no reaction from sql_saga.

## Objects of a system-versioned family and their fixed state

For a base table `B` owned by `O`:

| object | owner | ACL (set at creation, never derived from B) | options |
|---|---|---|---|
| `B__for_portion_of_<era>` view | O | SELECT, INSERT, UPDATE, DELETE to PUBLIC | `security_invoker` |
| `B__current_<era>` view | O | SELECT, INSERT, UPDATE, DELETE to PUBLIC | `security_invoker`, `security_barrier` |
| `B_with_history` view | O | SELECT to PUBLIC | `security_invoker`; three branches, see below |
| `B_history_gated` view | O | SELECT to PUBLIC | owner rights, with the access gate |
| as-of functions (`B__as_of`, `__between`, `__between_symmetric`, `__from_to`) | O | EXECUTE to PUBLIC, granted explicitly (hardened databases revoke the default) | SECURITY INVOKER SQL over `B_with_history` |
| `B_history` table | O | SELECT to PUBLIC (only reachable through its policies) | RLS enabled; FORCE exactly when B is forced |

The access gate of `B_history_gated` is
`CASE WHEN has_table_privilege(B, 'SELECT') THEN true ELSE sql_saga.__internal_deny_history(B) END`.
The helper is STABLE, so the planner evaluates it once as a one-time filter (0.07 ms point lookup on one million
rows, index scan kept). It raises 42501 with PostgreSQL's own text, `permission denied for table B`, so a caller
sees one consistent error whichever branch fails first.

## Row-level security on history

Which history rows a caller sees through `B_with_history` and the as-of functions is decided by `B` at access
time, with no setting or opt-in:

| B for this caller | history rows visible |
|---|---|
| RLS not active (disabled, owner, BYPASSRLS) | all rows, subject only to the SELECT gate |
| RLS active (`row_security_active(B)`) | the rows B's SELECT policies would admit |

PostgreSQL cannot apply another table's policies inside an owner-rights view, so `B_with_history` reads history
through two branches:

```sql
SELECT cols FROM B
UNION ALL SELECT cols FROM B_history_gated                          -- caller not subject to B's RLS
UNION ALL SELECT cols FROM B_history WHERE row_security_active(B)   -- caller subject to B's RLS
```

The gated view also requires `NOT row_security_active(B)`, so a row is never returned twice. The third branch
reads `B_history` as the caller, and PostgreSQL filters it with copies of B's policies.

### Policies on the history table

sql_saga owns every policy on `B_history`:

- one copy of each `SELECT` or `ALL` policy of `B`: `FOR SELECT`, same roles, same permissive or restrictive kind,
  same USING expression. WITH CHECK and INSERT/UPDATE/DELETE-only policies are not copied: history is read-only,
  and the history writer bypasses RLS;
- `sql_saga_history_reader_<token>`, PERMISSIVE: SELECT on `B` and `B`'s RLS not active for the caller (defence in
  depth for direct reads; not what the gated view relies on);
- `sql_saga_history_base_select_<token>`, RESTRICTIVE: SELECT on `B` (decision 2).

### Decisions

1. **Installation token and hashed copy names** (2026-10-14). `CREATE EXTENSION` creates `sql_saga.installation`
   with one random token. The table is not an extension configuration table, so it is never dumped and every
   installation, including a restore target, mints its own token. A copy is named
   `sql_saga_<left(md5(base policy name || token), 16)>`, always 25 characters. Copies carry no comment; the base
   policy of a copy is found by recomputing the name. The synchronization is idempotent: it drops every managed
   policy that is not exactly a desired copy (dumped copies with another token, stale and orphaned copies) and
   creates the missing ones. Event triggers run it on CREATE/ALTER/DROP POLICY of either table, so a restore that
   replays dumped copies ends with exactly one copy per base policy. `sync_history_policies(regclass)` and
   `sync_all_history_policies()` remain as repair tools.
2. **Restrictive base-privilege policy** (2026-10-14). Because `B_history` is granted SELECT to PUBLIC, every history
   table also gets `sql_saga_history_base_select_<token>`, RESTRICTIVE, `USING (has_table_privilege(B, 'SELECT'))`.
   It closes a direct-read leak through a copied `TO PUBLIC` policy (a role without the base privilege read 500,000
   rows; now 0). Accepted cost: only when B has RLS, all of B's SELECT policies are `USING (true)`, the caller is
   subject to RLS and the lookup is by range, a GiST as-of lookup becomes a full scan (0.09 ms to about 60 ms on one
   million rows), because range operators are not leakproof. Key lookups (`id = ...`) keep their index.
3. **Copies match columns by name** (2026-10-14). The copy's USING text is `pg_get_expr(polqual, B)` executed as
   `CREATE POLICY ... ON B_history`, so PostgreSQL resolves the column names in `B_history` whatever their positions.
   Before creating a copy the synchronization checks that every referenced column exists in `B_history` with the
   same type, or raises `policy <p> on <B> references column <c>, which <H> lacks`.
4. **No "synchronized" marker** (2026-10-14). The copy set is a pure function of B's policies and the token. The
   read-time check is the only guard: when B's RLS is active for the caller and copies are missing, stale or
   unmanaged, the history branch fails closed with
   `sql_saga: history of <B> is not synchronized (<N> policy copies missing, <M> stale[, <K> not managed by sql_saga])`
   and a HINT naming the repair. It costs about 9 microseconds and runs only when B's RLS is active.
5. **FORCE is mirrored** (2026-10-15). With FORCE on B but not on its history, B's owner would read other tenants'
   history through the third branch. The history table's `relforcerowsecurity` therefore always equals B's; the
   history writer runs as the extension owner and is unaffected.
6. **Tokenized fixed policies** (2026-10-15). The two fixed policies carry the token as well, for the same restore
   reason as decision 1: every synchronization drops fixed policies and copies of any other token. Object
   references inside the views and policies are written as `'schema.table'::regclass::oid`, so a dump stores names,
   not OIDs that mean nothing after a restore.
7. **No user policies on history** (2026-10-15). An owner-added permissive policy on `B_history` would widen what
   RLS-subject callers see. `CREATE POLICY` on a history table is refused unless the name is a managed name (restore
   replays only those): `policies on history table "<H>" are managed by sql_saga`, HINT: create it on B, which is
   copied. One created while event triggers were off counts as drift in the read-time check, and synchronization
   refuses to proceed until the user drops it; sql_saga never drops a user's policy.

## Health check and protection

| event | rule |
|---|---|
| ALTER on a managed view | `security_invoker` kept; the gated view's body unchanged |
| ALTER TABLE on a history table | RLS enabled, FORCE equal to the base |
| policy DDL on B or B_history | synchronization for that family; direct DROP/ALTER of a managed policy refused |
| owner change of B | every derived object follows |
| GRANT / REVOKE | nothing |

## Accepted limitation

A role with SELECT on only some columns of B gets `permission denied` for every read of `B_with_history`, because
the view selects all of B's columns as the caller. This fails closed, like `SELECT *` on B (README.md).
