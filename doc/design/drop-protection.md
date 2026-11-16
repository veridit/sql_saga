# Drop and rename protection, and `check_integrity()`

Status: implemented in 1.2, current in 1.7 (revision 4 of the 1.2 design, 2026-10-15). Code:
`src/2e_drop_protection.sql`, `src/2f_rename_following.sql`, `src/2h_health_checks.sql`,
`src/2j_event_triggers.sql`. Tests: 037-039, 094, 116, 117, 127. User documentation: README.md.

## Safety model

**Event triggers act only on the current command.** `drop_protection()`, `health_checks()` and
`rename_following()` refuse or change metadata only for objects attributable to the statement that fired them: at
`sql_drop` only the rows of this invocation of `pg_event_trigger_dropped_objects()`, at `ddl_command_end` only those
of `pg_event_trigger_ddl_commands()`. An unrelated statement never discovers, repairs or is blamed for damage
elsewhere. Global validation belongs only in `check_integrity()`.

**Family sanction.** Only dropping a managed base table sanctions dropping the rest of its family. The sanctioned
base OIDs come from the current payload, before metadata cleanup. Dropping a history table or another member alone
is refused; a base and its members in one statement are allowed; a sanction for family A never covers a member of
family B; schema and owner cascades judge each family on its own.

**Narrow bypasses.** The metadata-first drop APIs, the rebuild guard of one family
(`sql_saga.rebuilding_system_versioning_views`), the policy-synchronization guard, the clean-restore teardown
(`restore.md`) and old-token policy cleanup each keep their single purpose. Tests prove that setting a guard by
hand cannot remove an unrelated managed object.

## Statement-scoped drop protection

At entry, the current dropped-object payload is read once, with its order. Every check joins a catalog row with a
dropped object of this statement (constraint, trigger, column, view, policy, function, index), never a global scan
for missing objects. Refusal candidates have a fixed priority and are ordered by `(priority, position in the
payload)`, so a cascade containing many protected objects always reports the same first error.
`DISABLE TRIGGER` is not a drop and is outside this promise.

## Column renames of a system-versioned table

`pg_event_trigger_ddl_commands()` names the altered relation but not a renamed column's old name. So
`system_time_era.table_columns_snapshot name[]` keeps the live user columns in attnum order: portable, readable,
dumped with the configuration. At `ddl_command_end` of an ALTER TABLE on a versioned base, the stored and live
names are compared: one removed plus one added is a rename (refused if the old name is an excluded column,
otherwise followed); other differences are processed as ADD/DROP. The snapshot is refreshed only after a valid
shape is established: creation, supported ADD/DROP/RENAME, `rebuild_system_versioning_views`, updates and restores.
It is never refreshed because of unrelated DDL, which would bless damage. The attribution is exact only if the
family was intact before the statement; after event triggers were bypassed or a restore was interrupted, run
`check_integrity()` first.

## Renames of the generated as-of functions

The four functions stay plain string-body `LANGUAGE sql`, STABLE, SECURITY INVOKER, with no `SET search_path`:
`BEGIN ATOMIC` bodies were measured to lose inlining and predicate pushdown. Each function returns
`SETOF B_with_history`, which gives PostgreSQL a dependency from the function to the view's row type;
`check_integrity()` uses it to confirm that the function under each stored name belongs to the right family.

Renames are attributed by comparing identities before and after the command, never by guessing from what is
missing afterwards: a `ddl_command_start` trigger on ALTER FUNCTION and ALTER ROUTINE records which stored
identities resolve (`to_regprocedure`) in the transaction-local setting `sql_saga.alter_function_resolved`, and the
end trigger clears it and refuses only an identity that resolved before and no longer does. Identities that were
already broken are ignored, so pre-existing damage is never blamed on an unrelated rename. OWNER, COST, volatility
and similar changes keep the identity and are not refused.

## `check_integrity()`

```sql
sql_saga.check_integrity() RETURNS void   -- SECURITY DEFINER, EXECUTE revoked from PUBLIC
```

The extension owner runs it; a site may grant it to a trusted diagnostic role. It never repairs, synchronizes,
grants or rebuilds. It raises the first violation in a fixed order (base metadata, eras, SYSTEM_TIME, unique keys,
foreign keys, updatable views, system-versioning families, as-of functions, rename-following metadata), so it can
serve as the final gate of every update script. Errors name managed objects, the invariant and the sanctioned repair
API, and never disclose policy expressions, ACLs, role memberships, data or function bodies. The expected function
bodies come from the same generator the rebuild uses, not from a second copy. A row-returning report API was
deliberately not added.
