#!/usr/bin/env python3
"""Generate direct 1.0..1.4 to 1.5 updates from authenticated v1.4 artifacts."""
import argparse
import os
import sys
from pathlib import Path
import generate_update_scripts_1_4 as release

g = release.g13
GeneratorError = release.GeneratorError
TARGET = '1.5'
SOURCES = ('1.0', '1.1', '1.2', '1.3', '1.4')
# Produced only from git archive 8759b7b (v1.4), using that archive's generator.
FROZEN_SHA256 = {
    'sql_saga--1.4.sql': 'cbb4682b601d42223661149416120bcbf8f2b2cb4d2c44f33f64c8786ca7fda7',
    'sql_saga--1.0--1.4.sql': 'c1c431664d886cd99d7e65759fbf61a1f9b3adbf7fb2ea584a74cc8d5671c914',
    'sql_saga--1.1--1.4.sql': '8b4830d99c451801dcb411c720c9deade62a2fd7beb3744e4ab1690c559fd3e8',
    'sql_saga--1.2--1.4.sql': 'dfb6ba8966155279c9121285615b028d3ba765138214f01cfb54886affe67715',
    'sql_saga--1.3--1.4.sql': '20e21225e0fcac0c3fb4ebaf35b89a96d97d3f51d2a77e81f97b863285eebd84',
}
ROUTINE_MANIFEST = ('sql_saga.for_portion_of_trigger()',)
GATE = release.GATE


def routine_key(s):
    if not g._ROUTINE_RE.match(g._core(s)):
        return None
    r = g._parse_routine(s)
    return g._key(g._identity(r['name'], r['args']))


def delta(src, frozen):
    old = g.split_sql(frozen)
    current = g.split_sql(''.join(p.read_text() for p in sorted(Path(src).glob('[0-9][a-z]_*.sql'))))
    # Compare every unmanifested statement, including catalog DDL, privileges and comments.
    remainder = lambda statements: [g._core(s).strip() for s in statements if routine_key(s) not in ROUTINE_MANIFEST]
    if remainder(old) != remainder(current):
        raise GeneratorError('source changes outside the 1.5 routine manifest')
    chunks = []
    for key in ROUTINE_MANIFEST:
        before = [s for s in old if routine_key(s) == key]
        after = [s for s in current if routine_key(s) == key]
        if len(before) != 1 or len(after) != 1:
            raise GeneratorError('manifest routine must exist exactly once: ' + key)
        a, b = g._parse_routine(before[0]), g._parse_routine(after[0])
        if a['args'] != b['args'] or a['kind'] != b['kind'] or g.routine_signature(before[0])[2] != g.routine_signature(after[0])[2]:
            raise GeneratorError('manifest routine signature changed: ' + key)
        chunks.append(g.to_create_or_replace(after[0]))
    return ''.join(chunks)


def embedded(text):
    """Retarget only native guards, actionable hints and upgrade locks, never catalog markers/GUCs."""
    header, _, body = text.partition('\n')
    if header != release.header('1.4') or not body.endswith(GATE + '\n'):
        raise GeneratorError('unexpected frozen 1.4 update framing')
    body = body[:-len(GATE + '\n')]
    lines = []
    changed = 0
    for line in body.splitlines(keepends=True):
        if '1.4' in line and any(anchor in line for anchor in (
                "native_library_version() <> '1.4'", 'extension update version 1.4',
                'sql_saga 1.4 native library', "UPDATE TO ''1.4''", "'sql_saga 1.4 upgrade'")):
            line = line.replace('1.4', TARGET)
            changed += 1
        lines.append(line)
    if not changed:
        raise GeneratorError('no native guard anchors found')
    return ''.join(lines)


def build(src, versions, devops, extversion):
    if extversion != TARGET:
        raise GeneratorError('this generator builds 1.5 only')
    frozen = {n: release.authenticate(os.path.join(versions, n), h) for n, h in FROZEN_SHA256.items()}
    change = delta(src, frozen['sql_saga--1.4.sql'])
    guard = """CREATE OR REPLACE FUNCTION sql_saga.native_library_version() RETURNS text LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT AS 'sql_saga_native', 'sql_saga_native_version_wrapper';
DO $native_guard_1_5$
BEGIN
    IF sql_saga.native_library_version() <> '1.5' THEN
        RAISE EXCEPTION 'sql_saga: native library version % does not match extension update version 1.5', sql_saga.native_library_version()
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Install the sql_saga 1.5 native library and restart PostgreSQL before retrying ALTER EXTENSION.';
    END IF;
END;
$native_guard_1_5$;
SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('sql_saga 1.5 upgrade'));
"""
    outputs = {}
    for source in SOURCES:
        prior = '' if source == '1.4' else embedded(frozen['sql_saga--%s--1.4.sql' % source])
        text = release.header(TARGET) + '\n' + guard + prior + change + '\n' + GATE + '\n'
        if text.splitlines().count(GATE) != 1:
            raise GeneratorError('expected exactly one integrity gate')
        outputs['sql_saga--%s--1.5.sql' % source] = text
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
        sys.exit('generate_update_scripts_1_5: ' + str(error))
