#!/usr/bin/env python3
"""No-database 1.13 packaging contract tests (devops/generate_update_scripts_1_13.py)."""
import re
import shutil
import tempfile
import unittest
from pathlib import Path
import generate_update_scripts_1_13 as gen
import direct_update
import released_tree

R = gen.RELEASE

ROOT = Path(__file__).resolve().parent.parent


class Packaging(unittest.TestCase):
    def build(self, src=None, versions=None, devops=None):
        return gen.build(str(src or ROOT / 'src'), str(versions or ROOT / 'versions'),
                         str(devops or ROOT / 'devops'), '1.13')

    def copy(self, d, name):
        dst = Path(d) / name
        shutil.copytree(ROOT / name, dst)
        return dst

    def test_deterministic_framing_and_native_guards(self):
        outputs = self.build()
        self.assertEqual(outputs, self.build())
        self.assertEqual(set(outputs), {'sql_saga--%s--1.13.sql' % s for s in R.sources})
        for name, text in outputs.items():
            self.assertTrue(text.startswith('\\echo Use "ALTER EXTENSION sql_saga UPDATE TO \'1.13\'"'), name)
            self.assertEqual(text.splitlines().count(direct_update.GATE), 1, name)
            self.assertTrue(text.endswith(direct_update.GATE + '\n'), name)
            # Every guard names 1.13 exactly: not 1.1 or a mangled one.
            self.assertEqual(set(re.findall(r"native_library_version\(\) <> '([^']+)'", text)), {'1.13'}, name)
            self.assertEqual(set(re.findall(r"'sql_saga ([0-9.]+) upgrade'", text)), {'1.13'}, name)
            self.assertNotRegex(text, r"1\.13[0-9]", name)

    def test_only_drop_protection_and_the_sync_trigger_drop_change(self):
        head = 'CREATE OR REPLACE FUNCTION sql_saga.__internal_drop_protection('
        for name, text in self.build().items():
            # The 1.13 delta follows the embedded <source>--1.12 body; it starts at its last __internal_drop_protection.
            tail = text[text.rindex(head):]
            heads = re.findall(r'CREATE OR REPLACE (?:FUNCTION|PROCEDURE) (sql_saga\.[A-Za-z_0-9]+)\(', tail)
            self.assertEqual(heads, ['sql_saga.__internal_drop_protection',
                                     'sql_saga.__api_drop_synchronize_temporal_columns_trigger'], name)
            # The era's checks and synchronization trigger are refused with their own messages.
            self.assertIn('because it keeps empty ranges out of period', tail, name)
            self.assertIn('because it keeps the start of period', tail, name)
            self.assertIn('because it synchronizes the columns of period', tail, name)
            # Metadata first: the trigger name is cleared before the DROP TRIGGER.
            drop = tail[tail.index('sql_saga.__api_drop_synchronize_temporal_columns_trigger'):]
            self.assertLess(drop.index('SET sync_temporal_trg_name = NULL'), drop.index('$$DROP TRIGGER %1$I ON %2$s$$'), name)
            self.assertNotIn('ALTER TABLE sql_saga.', tail, name)
            self.assertNotIn('DROP FUNCTION sql_saga.__', tail, name)

    def test_embedded_only_documented_retargeting(self):
        for source in R.sources[:-1]:
            original = (ROOT / 'versions' / ('sql_saga--%s--1.12.sql' % source)).read_text()
            transformed = R.embedded(original, source not in R.sources_without_inner_guards)
            body = original.split('\n', 1)[1]
            before = body[len(direct_update.guard_prefix('1.12')):-len(direct_update.GATE + '\n')].splitlines()
            after = transformed.splitlines()
            self.assertEqual(len(before), len(after))
            for old, new in zip(before, after):
                if old != new:
                    self.assertEqual(new.replace('1.13', '1.12'), old)
                    self.assertTrue(any(a in old for a in R.native_guard_anchors), old)

    def test_frozen_tamper_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            versions = self.copy(d, 'versions')
            p = versions / 'sql_saga--1.12.sql'
            p.write_text(p.read_text() + '\n')
            with self.assertRaisesRegex(direct_update.GeneratorError, 'SHA-256'):
                self.build(versions=versions)

    def test_change_outside_manifest_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            (src / '9z_omitted.sql').write_text('SELECT 1013;\n')
            with self.assertRaisesRegex(direct_update.GeneratorError, 'outside.*manifest'):
                self.build(src=src)

    def test_manifest_routine_unchanged_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            p = src / '2e_drop_protection.sql'
            p.write_text(Path(released_tree.released_src('v1.12'), '2e_drop_protection.sql').read_text())
            with self.assertRaisesRegex(direct_update.GeneratorError, 'unchanged since 1.12'):
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
            gen.build('src', 'versions', 'devops', '1.12')


if __name__ == '__main__':
    (ROOT / 'tmp').mkdir(exist_ok=True)
    unittest.main()
