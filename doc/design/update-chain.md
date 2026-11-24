# Updates as a chain of single-release steps

Status: approved by the owner 2026-11-23, implemented for 1.14 (hug qnzn33). Replaces the direct update scripts built
per release by `devops/generate_update_scripts*.py` (in the tags up to v1.13). Code: `devops/chain_steps.py` (the
released steps), `devops/update_step.py` and `devops/update_step.sql` (the new step), the Makefile. Tests: `make
test-generator` (no database), `devops/upgrade_test.sh` (every released version, with data, through the chain),
`devops/restore_roundtrip.sh`.

## Problem

Until 1.13 every release shipped one direct update script per older release, each containing every earlier script.
1.13 shipped 14 of them (19 MB; `1.0--1.13` alone is 69,213 lines), and each release needed a generator with
hand-written lists of changed and replaced routines and the version number in about ten files.

The reason for the copying: every update script starts with a check that the installed native library is exactly its
own version (`native_library_version() <> '1.12'`). Only the newest library is installed, so PostgreSQL's normal way,
running single-release steps in sequence, failed at the first step.

## Decision

PostgreSQL's own mechanism: one step script per release, `sql_saga--<previous>--<release>.sql`. `ALTER EXTENSION
sql_saga UPDATE` finds the path from the installed version and runs every step in order, in one transaction, each step
with the definitions of its own release, exactly as that release was tested.

- The released single steps 1.0--1.1 to 1.12--1.13 are shipped once more as chain steps. Each is its frozen file
  (read from the tag that shipped it, authenticated by SHA-256) with exactly these changes, made by
  `devops/chain_steps.py` and checked by `make test-generator` and the upgrade test:
  1. The native check reads "the library is this release **or newer**" (versions compared numerically).
  2. The integrity gate at the end runs only in the step that reaches the installed library's version, the last step
     a plain update runs. An earlier step's gate would run that release's `check_integrity` against a catalog the
     later steps still change. The released direct scripts likewise had one gate, at the end.
  3. A hint that names an update command says `ALTER EXTENSION sql_saga UPDATE` without a version: the update goes to
     the installed package's version, not the release that wrote the hint.
  4. The 1.0--1.1 and 1.1--1.2 steps carry the corrections the released direct scripts made to them, so a 1.0 or 1.1
     database gets the same update it got from the 1.0--1.13 and 1.1--1.13 scripts:
     - the 1.3 preamble (refusals and repairs of 1.0 and 1.1 damage, mispaired foreign keys, XOR key order) runs
       first, with its source value. Its native check asks for the step's own release. It pauses
       `sql_saga_rename_following`, and the step puts the recorded state back at its end, as the 1.2--1.3 step does,
       so every step leaves the trigger as it found it;
     - 1.0--1.1: the shared regular-FK helper validator reads each helper once (released in 1.0--1.2);
     - 1.0--1.1: the ACL guard runs before the historical grants (released in 1.0--1.3).
- Because a step accepts a newer library, `ALTER EXTENSION sql_saga UPDATE TO '1.5'` with the 1.14 package stops at
  1.5, and a later plain update continues from there.
- A new release adds exactly one step, built by `devops/update_step.py`. Its routine changes are found by comparing
  the new install script with the previous one; nothing is listed by hand. A routine whose signature changes (or that
  CREATE OR REPLACE cannot change) is dropped and created after a check that the site did not change its privileges.
  Every other change (catalog tables, types, data) is written by hand in `devops/update_step.sql`, which names each
  install-script difference it handles; an unnamed difference fails the build. The version lives only in
  `sql_saga.control`; the previous release is the newest install script in `versions/`.
- `make install` removes every `sql_saga--*.sql` from the extension directory before installing, so direct scripts
  left by an older package cannot shortcut the chain.
- The multi-release direct scripts are no longer shipped. With a newer library installed none of them could run anyway
  (their native check).
- Rule for the native library: a C symbol that any released script references is never removed (an old step creates
  functions pointing to it). A retired function stays as a stub that raises an error. The build checks the list (34
  symbols today, all present).

## Evidence (2026-11-23)

- Empty databases of every release 1.0 to 1.12, updated through the chain to 1.13: all succeed, PostgreSQL picks the
  chain path itself, and the extension objects are identical to a fresh 1.13 install. 1.0 takes 0.45 s.
- The full upgrade matrix (data, foreign keys, views, system versioning, StatBus shapes, privilege cases) through the
  chain: see the release record.
- The 13 single steps total 69,266 lines, the same as today's single direct `1.0--1.13` script: the direct script is the
  chain glued into one file.

## Trade-offs

| | Direct scripts (until 1.13) | Chain |
|---|---|---|
| New files per release | one per older release (14 for 1.13) | one |
| Repository growth per release | about 19 MB | the step's size (about 1,000 lines) |
| Hand-written per release | generator, hashes, routine lists, spec test, version in ten files | none beyond the step's catalog and data changes |
| What an old database runs | one file with every old step, guards retargeted | the same steps, each as released |
| Integrity checks during an update | one, at the end | one, at the end |

Rejected: assembling every update from all current routines plus replayed catalog steps (tried 2026-11-23). It ran old
steps against today's routines and failed for 1.0 and 1.1 with data, the releases StatBus shipped longest.
