#!/usr/bin/env python3
"""Generate direct 1.0..1.6 to 1.7 updates from authenticated v1.6 artifacts. Standard library only.

Specified by devops/test_generate_update_scripts_1_7.py. Each script X--1.7 is:
  header, 1.7 native-library guard and upgrade lock,
  for X in 1.0..1.5: the frozen, SHA-256 authenticated X--1.6 update without header, native guard, lock and final
      integrity gate, with only its native guards, actionable update hints and upgrade lock retargeted to 1.7
      (catalog markers and GUCs keep their earlier values),
  the 1.7 delta, the same for every source:
      every routine of the 1.7 manifest as CREATE OR REPLACE (new routines are created),
      the privilege statements on new routines,
      devops/upgrade_1_7_repairs.sql (apply triggers on existing current views),
  exactly one final integrity gate.
Every other difference between src/ and the frozen 1.6 installation fails the build.
"""
import argparse
import os
import sys
from pathlib import Path
import generate_update_scripts_1_4 as release

g = release.g13
GeneratorError = release.GeneratorError
TARGET = '1.7'
PREVIOUS = '1.6'
SOURCES = ('1.0', '1.1', '1.2', '1.3', '1.4', '1.5', '1.6')
# Produced only from git archive 252c0f7 (v1.6), using that archive's generator; two builds were identical.
FROZEN_SHA256 = {
    'sql_saga--1.6.sql': 'e701a15531d3e679b794db83c272d7d9776dc9999f4e7ac5af6cbb57cf125a53',
    'sql_saga--1.0--1.6.sql': '9478c619b1c98c8eec42738a50c84ef07b4df5c7a72644be655d42aff8823574',
    'sql_saga--1.1--1.6.sql': '162412ee3563fc61d40cf6adeb2d0d8e022badc80702b5e77c270ba9dae32124',
    'sql_saga--1.2--1.6.sql': 'f61bfbdc686ee922a596c9b47d91a795611a829d33b44749a1f30b1ed65ccebd',
    'sql_saga--1.3--1.6.sql': 'ec39665b4008ab1af674e51267daa1b164054437a0f3e1faa7ae6f287955155b',
    'sql_saga--1.4--1.6.sql': '895467881031b8fd5a9d591f3d0b2fd80c7f89468857f11e10662d2e224b887b',
    'sql_saga--1.5--1.6.sql': '9938d0c177816d421cf242c2c7441700e8e8222b5ec33e0a2a14c324453fc568',
}
# Routines changed or new since 1.6, in installation order of their dependencies.
ROUTINE_MANIFEST = (
    'sql_saga.__internal_view_apply_trigger_name(sql_saga.updatable_view_type, name)',
    'sql_saga.current_view_trigger()',
    'sql_saga.current_view_apply_trigger()',
    'sql_saga.__api_add_current_view(regclass, name, name, text)',
    'sql_saga.__internal_rename_following()',
    'sql_saga.check_integrity()',
    'sql_saga.__internal_updatable_view_follow_shape(oid[])',
)
NEW_ROUTINES = (
    'sql_saga.current_view_apply_trigger()',
)
# The privilege statements on the new routines, exactly as src/ has them; nothing else may differ.
NEW_PRIVILEGES = (
    'GRANT EXECUTE ON FUNCTION sql_saga.current_view_apply_trigger() TO PUBLIC;',
)
REPAIRS = 'upgrade_1_7_repairs.sql'
GATE = release.GATE
NATIVE_GUARD_ANCHORS = (
    "native_library_version() <> '1.6'", 'extension update version 1.6', 'sql_saga 1.6 native library',
    "UPDATE TO ''1.6''", "'sql_saga 1.6 upgrade'")


def routine_key(s):
    if not g._ROUTINE_RE.match(g._core(s)):
        return None
    r = g._parse_routine(s)
    return g._key(g._identity(r['name'], r['args']))


def delta(src, frozen):
    old = g.split_sql(frozen)
    current = g.split_sql(''.join(p.read_text() for p in sorted(Path(src).glob('[0-9][a-z]_*.sql'))))
    manifest = ROUTINE_MANIFEST

    def remainder(statements):
        return [g._core(s).strip() for s in statements if routine_key(s) not in manifest]
    expected = remainder(old)
    actual = [s for s in remainder(current) if s not in NEW_PRIVILEGES]
    if expected != actual:
        raise GeneratorError('source changes outside the 1.7 routine manifest')
    privileges = [s for s in remainder(current) if s in NEW_PRIVILEGES]
    if sorted(privileges) != sorted(NEW_PRIVILEGES):
        raise GeneratorError('expected exactly the 1.7 privilege statements on new routines')

    chunks = []
    for key in manifest:
        before = [s for s in old if routine_key(s) == key]
        after = [s for s in current if routine_key(s) == key]
        if len(after) != 1:
            raise GeneratorError('manifest routine must exist exactly once: ' + key)
        if key in NEW_ROUTINES:
            if before:
                raise GeneratorError('new routine already exists in 1.6: ' + key)
        else:
            if len(before) != 1:
                raise GeneratorError('manifest routine must exist exactly once: ' + key)
            a, b = g._parse_routine(before[0]), g._parse_routine(after[0])
            if a['args'] != b['args'] or a['kind'] != b['kind'] or g.routine_signature(before[0])[2] != g.routine_signature(after[0])[2]:
                raise GeneratorError('manifest routine signature changed: ' + key)
            if g._core(before[0]).strip() == g._core(after[0]).strip():
                raise GeneratorError('manifest routine unchanged since 1.6: ' + key)
        chunks.append(g.to_create_or_replace(after[0]))
    for statement in NEW_PRIVILEGES:
        chunks.append(statement + '\n')
    return ''.join(chunks)


def _guard_prefix(version):
    return """CREATE OR REPLACE FUNCTION sql_saga.native_library_version() RETURNS text LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT AS 'sql_saga_native', 'sql_saga_native_version_wrapper';
DO $native_guard_%(tag)s$
BEGIN
    IF sql_saga.native_library_version() <> '%(v)s' THEN
        RAISE EXCEPTION 'sql_saga: native library version %% does not match extension update version %(v)s', sql_saga.native_library_version()
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Install the sql_saga %(v)s native library and restart PostgreSQL before retrying ALTER EXTENSION.';
    END IF;
END;
$native_guard_%(tag)s$;
SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('sql_saga %(v)s upgrade'));
""" % {'v': version, 'tag': version.replace('.', '_')}


def embedded(text, require_anchors=True):
    """The frozen X--1.6 body, retargeting only native guards, actionable hints and the upgrade lock."""
    header, _, body = text.partition('\n')
    prefix = _guard_prefix(PREVIOUS)
    if header != release.header(PREVIOUS) or not body.startswith(prefix) or not body.endswith(GATE + '\n'):
        raise GeneratorError('unexpected frozen 1.6 update framing')
    body = body[len(prefix):-len(GATE + '\n')]
    lines = []
    changed = 0
    for line in body.splitlines(keepends=True):
        if PREVIOUS in line and any(anchor in line for anchor in NATIVE_GUARD_ANCHORS):
            line = line.replace(PREVIOUS, TARGET)
            changed += 1
        lines.append(line)
    if require_anchors and not changed:
        raise GeneratorError('no native guard anchors found')
    return ''.join(lines)


def build(src, versions, devops, extversion):
    if extversion != TARGET:
        raise GeneratorError('this generator builds 1.7 only')
    frozen = {n: release.authenticate(os.path.join(versions, n), h) for n, h in FROZEN_SHA256.items()}
    change = delta(src, frozen['sql_saga--1.6.sql'])
    repairs = Path(devops, REPAIRS).read_text()
    if GATE in repairs:
        raise GeneratorError('the repairs fragment must not contain the integrity gate')
    outputs = {}
    for source in SOURCES:
        prior = '' if source == PREVIOUS else embedded(
            frozen['sql_saga--%s--1.6.sql' % source], source not in ('1.4', '1.5'))
        text = release.header(TARGET) + '\n' + _guard_prefix(TARGET) + prior + change + repairs + GATE + '\n'
        if text.splitlines().count(GATE) != 1:
            raise GeneratorError('expected exactly one integrity gate')
        outputs['sql_saga--%s--1.7.sql' % source] = text
    return outputs


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('src', 'versions', 'devops', 'outdir', 'extversion'):
        parser.add_argument('--' + name, required=True)
    a = parser.parse_args()
    g.write_atomically(a.outdir, build(a.src, a.versions, a.devops, a.extversion))


if __name__ == '__main__':
    try:
        main()
    except GeneratorError as error:
        sys.exit('generate_update_scripts_1_7: ' + str(error))
