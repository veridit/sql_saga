# STATBUS-611 portion-delete candidate validation

Date: 2026-11-12. Review branch: `feature/for-portion-delete`.
Base: released master / peeled `v1.4`, `8759b7b88ca2ede7ace54b44f6e1d9e63ca23170`.
Candidate package: **1.5**, not tagged or released. The shelved STATBUS-591
experiment is not an input to this branch. StatBus source is unchanged.

## Observable behavior

Released 1.4 was reproduced returning `UPDATE 1` but retaining the complete
period after `SET id = NULL`. The candidate uses OLD's complete identity and the
existing `DELETE_FOR_PORTION_OF` planner/executor mode to remove exactly the
intersection of the requested and matched period. Surviving periods keep their
original payload. Ordinary payload NULL assignments remain updates.

Regression `157_view_for_portion_of_delete` exercises all eight incoming cases:
middle splitting, a period spanning stored rows, exact alignment, complete
matched history without bounds, range and inclusive `valid_to` forms, ambiguous
payload refusal, temporal-FK atomic refusal, and invoker access with RLS. It also
checks composite identities, empty-intersection `UPDATE 0`, generated-payload
ambiguity, missing logical DELETE privilege, physical INSERT-policy refusal,
RLS-invisible rows, and regular-FK CASCADE retention/last-segment behavior.
The released implementation failed the new specification before the candidate
passed it. A clean private local fast suite passed **142/142**.

The trigger remains SECURITY INVOKER. A logical removal requires base-table
DELETE privilege even when the physical split is UPDATE plus INSERT. Physical
SELECT/UPDATE/INSERT/DELETE permissions and RLS still apply according to the
plan. Granting only DELETE, or writing only a DELETE policy, is not sufficient
for every split. See the README protocol and permission examples.

## Upgrade and packaging

- Frozen released 1.4 installation and upgrade files are authenticated by SHA-256.
- Direct update scripts support released 1.0, 1.1, 1.2, 1.3 and 1.4 to 1.5.
- Catalog format remains 1.4. The native library and extension version are 1.5.
- The 1.4-to-1.5 routine delta uses CREATE OR REPLACE without a user-data rewrite.
- A local proof actually began with released 1.4 libraries and an existing view.
  Rollback rehearsal and committed upgrade preserved the trigger routine OID,
  a named site EXECUTE grant and data. Exact removal and integrity passed, and
  rollback restored the old definition, version and data.
- The permanent 1.4 upgrade gate additionally checks the existing view OID and
  uses deletion through that preexisting view, not a newly created replacement.
- Root validation passed **42 unit tests** (7 migration-safety, 7 current-generator,
  28 historical-generator), parallel Makefile wiring, and scans of all five real
  generated direct migrations. Mutations containing cascading DDL are rejected,
  and scanner errors cannot report PASS. Comments and quoted fragments retained
  in executable DO bodies are tested explicitly.

The first full Linux CI, **43313888202** at `dae4206`, concluded **success**:
native units, 142 fast regressions, restore, all five genuine released-binary
upgrades, attribution isolation and regular-FK concurrency passed. Review also
found its inherited GNU-incompatible grep scanner logging errors while reporting
PASS. That harness defect was corrected in `37e9aff` and `3c464fb`. No runtime
code changed after the first green run. Final CI **43320411030** on
`3c464fbe51e58c2e3123625d1937f68d92a772da` is pending and must be observed before
this branch is considered ready for handoff.

## Ordinary UPDATE performance

This isolates the changed trigger body on otherwise released 1.4 runtime, not a
whole-package or application performance benchmark. Each trial uses a fresh
backend and one transaction, restores the chosen definition, runs an ordinary
30-entity payload correction that splits periods, checks 90 resulting segments
and 30 exact updated payload slices, then rolls everything back. A separate
query confirms all 30 original rows are restored. EXPLAIN (ANALYZE, BUFFERS,
FORMAT JSON) supplies execution timings. Definition replacement and rollback
are not included in the EXPLAIN execution time.

After one warmup pair, eight measured pairs alternate execution order:

| Pair | Released trigger ms | Candidate trigger ms |
|---|---:|---:|
| 1 | 215.391 | 247.864 |
| 2 | 222.530 | 214.578 |
| 3 | 296.945 | 210.710 |
| 4 | 214.642 | 221.167 |
| 5 | 211.951 | 221.834 |
| 6 | 211.940 | 218.239 |
| 7 | 234.274 | 224.257 |
| 8 | 272.685 | 241.292 |
| Median | **218.9605** | **221.5005** |

The candidate median is **2.5400 ms higher per 30-entity statement, +1.16%**.
Load average was about 21, with substantial individual-pair variation. An earlier
sample at load roughly 80 to 95 had a lower candidate median but is inconclusive,
not a claimed speedup. These results bound a small observed ordinary-update
cost in this fixture, not zero overhead, application throughput, concurrent
scaling or performance on an idle machine. A fresh full SQL benchmark suite was
not claimed as an acceptance result for this branch.

## Remaining release and operational boundaries

- Final exact-code CI must pass before review handoff. Review, master merge and a
  `v1.5` release are separate future actions, not performed here.
- Follow `doc/upgrade.md`: stop writers, deploy matching SQL/control/C/native
  package, restart, rehearse on a verified restored backup, upgrade and check
  integrity before reopening writers. There is no downgrade script.
- Supported documented split-restore sequencing is tested. The intentionally
  unsupported parallel restore variant is not counted as green acceptance.
- This work does not add set-based bulk view deletion, change FK timing, resume
  the deferred-check prototype, or modify StatBus.
