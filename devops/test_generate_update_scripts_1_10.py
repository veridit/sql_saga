#!/usr/bin/env python3
"""No-database 1.10 packaging contract tests (devops/generate_update_scripts_1_10.py)."""
import re
import shutil
import tempfile
import unittest
from pathlib import Path
import generate_update_scripts_1_10 as gen
import direct_update

R = gen.RELEASE

ROOT = Path(__file__).resolve().parent.parent
# The 1.10 generator is specified against the released v1.10 source tree (later releases change src/).
import released_tree  # noqa: E402
SRC = Path(released_tree.released_src('v1.10'))
DEVOPS = Path(released_tree.released_devops('v1.10'))


class Packaging(unittest.TestCase):
    def build(self, src=None, versions=None, devops=None):
        return gen.build(str(src or SRC), str(versions or ROOT / 'versions'),
                         str(devops or DEVOPS), '1.10')

    def copy(self, d, name):
        dst = Path(d) / name
        shutil.copytree({'src': SRC, 'devops': DEVOPS}.get(name, ROOT / name), dst)
        return dst

    def test_deterministic_framing_and_native_guards(self):
        outputs = self.build()
        self.assertEqual(outputs, self.build())
        self.assertEqual(set(outputs), {'sql_saga--%s--1.10.sql' % s for s in R.sources})
        for name, text in outputs.items():
            self.assertTrue(text.startswith('\\echo Use "ALTER EXTENSION sql_saga UPDATE TO \'1.10\'"'), name)
            self.assertEqual(text.splitlines().count(direct_update.GATE), 1, name)
            self.assertTrue(text.endswith(direct_update.GATE + '\n'), name)
            # Two-digit minor: every guard names 1.10 exactly, none an older release or a mangled one.
            self.assertEqual(set(re.findall(r"native_library_version\(\) <> '([^']+)'", text)), {'1.10'}, name)
            self.assertEqual(set(re.findall(r"'sql_saga ([0-9.]+) upgrade'", text)), {'1.10'}, name)
            self.assertNotRegex(text, r"1\.10[0-9]", name)

    def test_only_the_three_routines_change(self):
        for name, text in self.build().items():
            # The 1.10 delta follows the embedded <source>--1.9 body; it starts at its last provenance_trigger.
            tail = text[text.rindex('CREATE OR REPLACE FUNCTION sql_saga.provenance_trigger()'):]
            if name != 'sql_saga--1.9--1.10.sql':
                self.assertEqual(text.count('CREATE OR REPLACE FUNCTION sql_saga.provenance_trigger()'), 2, name)
            heads = re.findall(r'CREATE OR REPLACE (?:FUNCTION|PROCEDURE) (sql_saga\.[A-Za-z_0-9]+)\(', tail)
            self.assertEqual(heads, ['sql_saga.provenance_trigger', 'sql_saga.__internal_set_era_provenance_columns',
                                     'sql_saga.check_integrity'], name)
            for fixed in ("EXECUTE pg_catalog.format('SELECT pg_catalog.to_jsonb(%s)', v_default)",
                          'is a generated column', 'provenance column % has no DEFAULT'):
                self.assertIn(fixed, tail, name)
            self.assertNotIn('ALTER TABLE sql_saga.', tail, name)
            self.assertNotIn('DROP FUNCTION', tail, name)

    def test_embedded_only_documented_retargeting(self):
        for source in R.sources[:-1]:
            original = (ROOT / 'versions' / ('sql_saga--%s--1.9.sql' % source)).read_text()
            transformed = R.embedded(original, source not in R.sources_without_inner_guards)
            body = original.split('\n', 1)[1]
            before = body[len(direct_update.guard_prefix('1.9')):-len(direct_update.GATE + '\n')].splitlines()
            after = transformed.splitlines()
            self.assertEqual(len(before), len(after))
            for old, new in zip(before, after):
                if old != new:
                    self.assertEqual(new.replace('1.10', '1.9'), old)
                    self.assertTrue(any(a in old for a in R.native_guard_anchors), old)

    def test_frozen_tamper_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            versions = self.copy(d, 'versions')
            p = versions / 'sql_saga--1.9.sql'
            p.write_text(p.read_text() + '\n')
            with self.assertRaisesRegex(direct_update.GeneratorError, 'SHA-256'):
                self.build(versions=versions)

    def test_change_outside_manifest_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            (src / '9z_omitted.sql').write_text('SELECT 1010;\n')
            with self.assertRaisesRegex(direct_update.GeneratorError, 'outside.*manifest'):
                self.build(src=src)

    def test_manifest_routine_unchanged_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            frozen = (ROOT / 'versions' / 'sql_saga--1.9.sql').read_text()
            old = re.search(r'CREATE FUNCTION sql_saga\.provenance_trigger\(\).*?\$provenance_trigger\$;', frozen, re.S).group(0)
            p = src / '2c_set_era_provenance_columns.sql'
            text = p.read_text()
            new = re.search(r'CREATE FUNCTION sql_saga\.provenance_trigger\(\).*?\$provenance_trigger\$;', text, re.S).group(0)
            p.write_text(text.replace(new, old, 1))
            with self.assertRaisesRegex(direct_update.GeneratorError, 'unchanged since 1.9'):
                self.build(src=src)

    def test_repairs_with_gate_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            devops = self.copy(d, 'devops')
            p = devops / R.repairs
            p.write_text(p.read_text() + direct_update.GATE + '\n')
            with self.assertRaisesRegex(direct_update.GeneratorError, 'integrity gate'):
                self.build(devops=devops)

    def test_wrong_target_refused(self):
        with self.assertRaises(direct_update.GeneratorError):
            gen.build('src', 'versions', 'devops', '1.9')


if __name__ == '__main__':
    (ROOT / 'tmp').mkdir(exist_ok=True)
    unittest.main()
