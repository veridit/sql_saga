#!/usr/bin/env python3
"""No-database 1.11 packaging contract tests (devops/generate_update_scripts_1_11.py)."""
import re
import shutil
import tempfile
import unittest
from pathlib import Path
import generate_update_scripts_1_11 as gen
import direct_update
import released_tree

R = gen.RELEASE

ROOT = Path(__file__).resolve().parent.parent


class Packaging(unittest.TestCase):
    def build(self, src=None, versions=None, devops=None):
        return gen.build(str(src or ROOT / 'src'), str(versions or ROOT / 'versions'),
                         str(devops or ROOT / 'devops'), '1.11')

    def copy(self, d, name):
        dst = Path(d) / name
        shutil.copytree(ROOT / name, dst)
        return dst

    def test_deterministic_framing_and_native_guards(self):
        outputs = self.build()
        self.assertEqual(outputs, self.build())
        self.assertEqual(set(outputs), {'sql_saga--%s--1.11.sql' % s for s in R.sources})
        for name, text in outputs.items():
            self.assertTrue(text.startswith('\\echo Use "ALTER EXTENSION sql_saga UPDATE TO \'1.11\'"'), name)
            self.assertEqual(text.splitlines().count(direct_update.GATE), 1, name)
            self.assertTrue(text.endswith(direct_update.GATE + '\n'), name)
            # Every guard names 1.11 exactly: not 1.1, 1.10 or a mangled one.
            self.assertEqual(set(re.findall(r"native_library_version\(\) <> '([^']+)'", text)), {'1.11'}, name)
            self.assertEqual(set(re.findall(r"'sql_saga ([0-9.]+) upgrade'", text)), {'1.11'}, name)
            self.assertNotRegex(text, r"1\.11[0-9]", name)

    def test_only_the_executor_changes(self):
        head = 'CREATE OR REPLACE PROCEDURE sql_saga.temporal_merge_execute('
        for name, text in self.build().items():
            # The 1.11 delta follows the embedded <source>--1.10 body; it starts at its last temporal_merge_execute.
            tail = text[text.rindex(head):]
            heads = re.findall(r'CREATE OR REPLACE (?:FUNCTION|PROCEDURE) (sql_saga\.[A-Za-z_0-9]+)\(', tail)
            self.assertEqual(heads, ['sql_saga.temporal_merge_execute'], name)
            self.assertIn('pg_catalog.jsonb_populate_record(NULL::%', tail, name)
            self.assertNotIn("p.data->>%1$L", tail, name)
            self.assertNotIn("s.full_data->>%1$L", tail, name)
            self.assertNotIn('ALTER TABLE sql_saga.', tail, name)
            self.assertNotIn('DROP FUNCTION', tail, name)

    def test_embedded_only_documented_retargeting(self):
        for source in R.sources[:-1]:
            original = (ROOT / 'versions' / ('sql_saga--%s--1.10.sql' % source)).read_text()
            transformed = R.embedded(original, source not in R.sources_without_inner_guards)
            body = original.split('\n', 1)[1]
            before = body[len(direct_update.guard_prefix('1.10')):-len(direct_update.GATE + '\n')].splitlines()
            after = transformed.splitlines()
            self.assertEqual(len(before), len(after))
            for old, new in zip(before, after):
                if old != new:
                    self.assertEqual(new.replace('1.11', '1.10'), old)
                    self.assertTrue(any(a in old for a in R.native_guard_anchors), old)

    def test_frozen_tamper_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            versions = self.copy(d, 'versions')
            p = versions / 'sql_saga--1.10.sql'
            p.write_text(p.read_text() + '\n')
            with self.assertRaisesRegex(direct_update.GeneratorError, 'SHA-256'):
                self.build(versions=versions)

    def test_change_outside_manifest_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            (src / '9z_omitted.sql').write_text('SELECT 1011;\n')
            with self.assertRaisesRegex(direct_update.GeneratorError, 'outside.*manifest'):
                self.build(src=src)

    def test_manifest_routine_unchanged_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            p = src / '2l_temporal_merge_execute.sql'
            p.write_text(Path(released_tree.released_src('v1.10'), '2l_temporal_merge_execute.sql').read_text())
            with self.assertRaisesRegex(direct_update.GeneratorError, 'unchanged since 1.10'):
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
            gen.build('src', 'versions', 'devops', '1.10')


if __name__ == '__main__':
    (ROOT / 'tmp').mkdir(exist_ok=True)
    unittest.main()
