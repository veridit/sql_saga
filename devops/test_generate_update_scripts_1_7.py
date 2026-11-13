#!/usr/bin/env python3
"""No-database 1.7 packaging contract tests (devops/generate_update_scripts_1_7.py)."""
import re
import shutil
import tempfile
import unittest
from pathlib import Path
import generate_update_scripts_1_7 as gen

ROOT = Path(__file__).resolve().parent.parent


class Packaging(unittest.TestCase):
    def build(self, src=None, versions=None, devops=None):
        return gen.build(str(src or ROOT / 'src'), str(versions or ROOT / 'versions'),
                         str(devops or ROOT / 'devops'), '1.7')

    def copy(self, d, name):
        dst = Path(d) / name
        shutil.copytree(ROOT / name, dst)
        return dst

    def test_deterministic_framing_and_native_guards(self):
        outputs = self.build()
        self.assertEqual(outputs, self.build())
        self.assertEqual(set(outputs), {'sql_saga--%s--1.7.sql' % s for s in gen.SOURCES})
        for name, text in outputs.items():
            self.assertTrue(text.startswith('\\echo Use "ALTER EXTENSION sql_saga UPDATE TO \'1.7\'"'), name)
            self.assertEqual(text.splitlines().count(gen.GATE), 1, name)
            self.assertTrue(text.endswith(gen.GATE + '\n'), name)
            guards = re.findall(r"native_library_version\(\) <> '([^']+)'", text)
            self.assertTrue(guards, name)
            self.assertEqual(set(guards), {'1.7'}, name)
            for old in ('1.5', '1.6'):
                self.assertNotIn("'sql_saga %s upgrade'" % old, text, name)
            self.assertIn("'sql_saga 1.7 upgrade'", text, name)
            for routine in ('current_view_trigger()', 'current_view_apply_trigger()', 'check_integrity()'):
                self.assertIn('CREATE OR REPLACE FUNCTION sql_saga.' + routine, text, name)
            for statement in gen.NEW_PRIVILEGES:
                self.assertEqual(text.count(statement), 1, name)
            self.assertEqual(text.count('$create_current_view_apply_triggers$'), 2, name)
            # The 1.7 delta follows every embedded older update, and the repairs follow the delta.
            self.assertLess(text.rindex('CREATE OR REPLACE FUNCTION sql_saga.current_view_apply_trigger()'),
                            text.index('$create_current_view_apply_triggers$'))
            if name != 'sql_saga--1.6--1.7.sql':
                # Every older source also passes through the 1.6 update, which adds the for_portion_of apply triggers.
                self.assertEqual(text.count('$create_for_portion_of_apply_triggers$'), 2, name)
                self.assertLess(text.index('$create_for_portion_of_apply_triggers$'),
                                text.index('CREATE OR REPLACE FUNCTION sql_saga.current_view_apply_trigger()'))
            else:
                self.assertNotIn('$create_for_portion_of_apply_triggers$', text, name)

    def test_embedded_updates_keep_catalog_markers(self):
        outputs = self.build()
        for source in gen.SOURCES[:4]:
            text = outputs['sql_saga--%s--1.7.sql' % source]
            self.assertIn('sql_saga.upgrade_from_1_4', text)
            self.assertIn("catalog_format = '1.4'", text)
        for source in ('1.4', '1.5', '1.6'):
            self.assertNotIn('sql_saga.upgrade_from_1_4', outputs['sql_saga--%s--1.7.sql' % source])

    def test_embedded_only_documented_retargeting(self):
        for source in gen.SOURCES[:-1]:
            original = (ROOT / 'versions' / ('sql_saga--%s--1.6.sql' % source)).read_text()
            transformed = gen.embedded(original, source not in ('1.4', '1.5'))
            body = original.split('\n', 1)[1]
            before = body[len(gen._guard_prefix('1.6')):-len(gen.GATE + '\n')].splitlines()
            after = transformed.splitlines()
            self.assertEqual(len(before), len(after))
            for old, new in zip(before, after):
                if old != new:
                    self.assertEqual(new.replace('1.7', '1.6'), old)
                    self.assertTrue(any(a in old for a in gen.NATIVE_GUARD_ANCHORS), old)

    def test_frozen_tamper_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            versions = self.copy(d, 'versions')
            p = versions / 'sql_saga--1.6.sql'
            p.write_text(p.read_text() + '\n')
            with self.assertRaisesRegex(gen.GeneratorError, 'SHA-256'):
                self.build(versions=versions)

    def test_change_outside_manifest_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            (src / '9z_omitted.sql').write_text('SELECT 617;\n')
            with self.assertRaisesRegex(gen.GeneratorError, 'outside.*manifest'):
                self.build(src=src)

    def test_unlisted_routine_change_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            p = src / '0g_for_portion_of_trigger.sql'
            text = p.read_text()
            self.assertIn('BEGIN', text)
            p.write_text(text.replace('BEGIN', 'BEGIN\n    NULL;', 1))
            with self.assertRaisesRegex(gen.GeneratorError, 'outside.*manifest'):
                self.build(src=src)

    def test_missing_new_privilege_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            p = src / '3f_public_execute.sql'
            p.write_text(p.read_text().replace(gen.NEW_PRIVILEGES[0] + '\n', '', 1))
            with self.assertRaisesRegex(gen.GeneratorError, 'privilege statements'):
                self.build(src=src)

    def test_manifest_routine_missing_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            (src / '3c_current_view_trigger.sql').unlink()
            with self.assertRaises(gen.GeneratorError):
                self.build(src=src)

    def test_manifest_signature_change_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            p = src / '3c_current_view_trigger.sql'
            text = p.read_text()
            marker = 'CREATE FUNCTION sql_saga.current_view_trigger()\n RETURNS trigger'
            self.assertIn(marker, text)
            p.write_text(text.replace(marker, 'CREATE FUNCTION sql_saga.current_view_trigger()\n RETURNS text', 1))
            with self.assertRaisesRegex(gen.GeneratorError, 'signature changed'):
                self.build(src=src)

    def test_repairs_with_gate_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            devops = self.copy(d, 'devops')
            p = devops / gen.REPAIRS
            p.write_text(p.read_text() + gen.GATE + '\n')
            with self.assertRaisesRegex(gen.GeneratorError, 'integrity gate'):
                self.build(devops=devops)

    def test_wrong_target_refused(self):
        with self.assertRaises(gen.GeneratorError):
            gen.build('src', 'versions', 'devops', '1.6')


if __name__ == '__main__':
    (ROOT / 'tmp').mkdir(exist_ok=True)
    unittest.main()
