#!/usr/bin/env python3
"""Generate direct 1.0..1.5 to 1.6 updates from authenticated v1.5 artifacts. Standard library only.

Specified by devops/test_generate_update_scripts_1_6.py. Each script X--1.6 is:
  header, 1.6 native-library guard and upgrade lock,
  for X in 1.0..1.4: the frozen, SHA-256 authenticated X--1.5 update without header, native guard, lock and final
      integrity gate, with only its native guards, actionable update hints and upgrade lock retargeted to 1.6
      (catalog markers and GUCs keep their 1.4 values),
  the 1.6 delta, the same for every source:
      every routine of the 1.6 manifest as CREATE OR REPLACE (new routines are created),
      the privilege statements on new routines,
      devops/upgrade_1_6_repairs.sql (apply triggers on existing for_portion_of views),
  exactly one final integrity gate.
Every other difference between src/ and the frozen 1.5 installation fails the build.
"""
import argparse
import os
import sys
from pathlib import Path
import generate_update_scripts_1_4 as release

g = release.g13
GeneratorError = release.GeneratorError
TARGET = '1.6'
PREVIOUS = '1.5'
SOURCES = ('1.0', '1.1', '1.2', '1.3', '1.4', '1.5')
# Produced only from git archive 545839a (v1.5), using that archive's generator.
FROZEN_SHA256 = {
    'sql_saga--1.5.sql': '37dbcdbb3572dfd01fe5d1090ad098cbb91527ecbc2db728e65926669949d8a7',
    'sql_saga--1.0--1.5.sql': '5cb7ce6c3854fd7ae314f602e459df1663d3fec46376dd1d31c5729237ef92f8',
    'sql_saga--1.1--1.5.sql': '2070fea4c1fd602285f2f349ba26d991d79015cede64525be45beee5d4a1d7c8',
    'sql_saga--1.2--1.5.sql': 'd32ad1a720015d0ac1bae40e1d9da442ea18d6e89e31aec4f32bd1194ae01688',
    'sql_saga--1.3--1.5.sql': '399788ef7517c1a0189eadbe921ab7fe43b2af52e2695c7746e7940d3c4ad601',
    'sql_saga--1.4--1.5.sql': 'e8dc9f74f6c0fd87ac9cca4964e04d1059da86f5973e5928ff2fd564ba2d8b3d',
}
# Routines changed or new since 1.5, in installation order of their dependencies.
ROUTINE_MANIFEST = (
    'sql_saga.__internal_view_apply_trigger_name(sql_saga.updatable_view_type, name)',
    'sql_saga.for_portion_of_trigger()',
    'sql_saga.for_portion_of_apply_trigger()',
    'sql_saga.__api_add_for_portion_of_view(regclass, name, name[])',
    'sql_saga.__internal_drop_protection()',
    'sql_saga.__internal_rename_following()',
    'sql_saga.check_integrity()',
    'sql_saga.__internal_updatable_view_follow_shape(oid[])',
)
NEW_ROUTINES = (
    'sql_saga.__internal_view_apply_trigger_name(sql_saga.updatable_view_type, name)',
    'sql_saga.for_portion_of_apply_trigger()',
)
# The privilege statements on the new routines, exactly as src/ has them; nothing else may differ.
NEW_PRIVILEGES = (
    'GRANT EXECUTE ON FUNCTION sql_saga.for_portion_of_apply_trigger() TO PUBLIC;',
    'REVOKE ALL ON FUNCTION sql_saga.__internal_view_apply_trigger_name(sql_saga.updatable_view_type, name) FROM PUBLIC;',
)
REPAIRS = 'upgrade_1_6_repairs.sql'
GATE = release.GATE
NATIVE_GUARD_ANCHORS = (
    "native_library_version() <> '1.5'", 'extension update version 1.5', 'sql_saga 1.5 native library',
    "UPDATE TO ''1.5''", "'sql_saga 1.5 upgrade'")


def routine_key(s):
    if not g._ROUTINE_RE.match(g._core(s)):
        return None
    r = g._parse_routine(s)
    return g._key(g._identity(r['name'], r['args']))


def _manifest_keys():
    return ROUTINE_MANIFEST


def delta(src, frozen):
    old = g.split_sql(frozen)
    current = g.split_sql(''.join(p.read_text() for p in sorted(Path(src).glob('[0-9][a-z]_*.sql'))))
    manifest = _manifest_keys()
    new_keys = NEW_ROUTINES

    def remainder(statements):
        return [g._core(s).strip() for s in statements if routine_key(s) not in manifest]
    expected = remainder(old)
    actual = [s for s in remainder(current) if s not in NEW_PRIVILEGES]
    if expected != actual:
        raise GeneratorError('source changes outside the 1.6 routine manifest')
    privileges = [s for s in remainder(current) if s in NEW_PRIVILEGES]
    if sorted(privileges) != sorted(NEW_PRIVILEGES):
        raise GeneratorError('expected exactly the 1.6 privilege statements on new routines')

    chunks = []
    for key in manifest:
        before = [s for s in old if routine_key(s) == key]
        after = [s for s in current if routine_key(s) == key]
        if len(after) != 1:
            raise GeneratorError('manifest routine must exist exactly once: ' + key)
        if key in new_keys:
            if before:
                raise GeneratorError('new routine already exists in 1.5: ' + key)
        else:
            if len(before) != 1:
                raise GeneratorError('manifest routine must exist exactly once: ' + key)
            a, b = g._parse_routine(before[0]), g._parse_routine(after[0])
            if a['args'] != b['args'] or a['kind'] != b['kind'] or g.routine_signature(before[0])[2] != g.routine_signature(after[0])[2]:
                raise GeneratorError('manifest routine signature changed: ' + key)
            if g._core(before[0]).strip() == g._core(after[0]).strip():
                raise GeneratorError('manifest routine unchanged since 1.5: ' + key)
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
    """The frozen X--1.5 body, retargeting only native guards, actionable hints and the upgrade lock."""
    header, _, body = text.partition('\n')
    prefix = _guard_prefix(PREVIOUS)
    if header != release.header(PREVIOUS) or not body.startswith(prefix) or not body.endswith(GATE + '\n'):
        raise GeneratorError('unexpected frozen 1.5 update framing')
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
        raise GeneratorError('this generator builds 1.6 only')
    frozen = {n: release.authenticate(os.path.join(versions, n), h) for n, h in FROZEN_SHA256.items()}
    change = delta(src, frozen['sql_saga--1.5.sql'])
    repairs = Path(devops, REPAIRS).read_text()
    if GATE in repairs:
        raise GeneratorError('the repairs fragment must not contain the integrity gate')
    outputs = {}
    for source in SOURCES:
        prior = '' if source == PREVIOUS else embedded(frozen['sql_saga--%s--1.5.sql' % source], source != '1.4')
        text = release.header(TARGET) + '\n' + _guard_prefix(TARGET) + prior + change + repairs + GATE + '\n'
        if text.splitlines().count(GATE) != 1:
            raise GeneratorError('expected exactly one integrity gate')
        outputs['sql_saga--%s--1.6.sql' % source] = text
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
        sys.exit('generate_update_scripts_1_6: ' + str(error))
