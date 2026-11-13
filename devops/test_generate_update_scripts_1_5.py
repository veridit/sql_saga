#!/usr/bin/env python3
"""No-database 1.5 packaging contract tests."""
import re
import shutil
import tempfile
import unittest
from pathlib import Path
import generate_update_scripts_1_5 as gen

ROOT = Path(__file__).resolve().parent.parent
# The 1.5 generator is specified against the released v1.5 source tree (later releases change src/).
import released_tree  # noqa: E402
SRC = Path(released_tree.released_src('v1.5'))


class Packaging(unittest.TestCase):
    def build(self, src=None, versions=None):
        return gen.build(str(src or SRC), str(versions or ROOT / 'versions'), str(ROOT / 'devops'), '1.5')

    def test_deterministic_and_native_guards(self):
        outputs = self.build()
        self.assertEqual(outputs, self.build())
        self.assertEqual(len(outputs), 5)
        for name, text in outputs.items():
            self.assertEqual(text.splitlines().count(gen.GATE), 1)
            self.assertTrue(text.endswith(gen.GATE + '\n'))
            guards = re.findall(r"native_library_version\(\) <> '([^']+)'", text)
            self.assertTrue(guards, name)
            self.assertEqual(set(guards), {'1.5'}, name)
            self.assertIn('CREATE OR REPLACE FUNCTION sql_saga.for_portion_of_trigger()', text)
            if '1.4--' not in name:
                self.assertIn("sql_saga.upgrade_from_1_4", text)
                self.assertIn("catalog_format", text)

    def test_frozen_tamper_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            dst = Path(d) / 'versions'
            shutil.copytree(ROOT / 'versions', dst)
            p = dst / 'sql_saga--1.4.sql'
            p.write_text(p.read_text() + '\n')
            with self.assertRaisesRegex(gen.GeneratorError, 'SHA-256'):
                self.build(versions=dst)

    def test_manifest_omission_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = Path(d) / 'src'
            shutil.copytree(SRC, src)
            (src / '9z_omitted.sql').write_text('SELECT 611;\n')
            with self.assertRaisesRegex(gen.GeneratorError, 'outside.*manifest'):
                self.build(src=src)

    def test_manifest_missing_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = Path(d) / 'src'
            shutil.copytree(SRC, src)
            (src / '0g_for_portion_of_trigger.sql').unlink()
            with self.assertRaisesRegex(gen.GeneratorError, 'exactly once'):
                self.build(src=src)

    def test_manifest_signature_change_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = Path(d) / 'src'
            shutil.copytree(SRC, src)
            p = src / '0g_for_portion_of_trigger.sql'
            p.write_text(p.read_text().replace(' RETURNS trigger', ' RETURNS text', 1))
            with self.assertRaisesRegex(gen.GeneratorError, 'signature changed'):
                self.build(src=src)

    def test_embedded_only_documented_retargeting(self):
        for source in gen.SOURCES[:-1]:
            original = (ROOT / 'versions' / ('sql_saga--%s--1.4.sql' % source)).read_text()
            transformed = gen.embedded(original)
            before = original.split('\n', 1)[1][:-len(gen.GATE + '\n')].splitlines()
            after = transformed.splitlines()
            self.assertEqual(len(before), len(after))
            for old, new in zip(before, after):
                if old != new:
                    self.assertEqual(new.replace('1.5', '1.4'), old)
                    self.assertNotIn('catalog_format', old)
                    self.assertNotIn('upgrade_from_1_4', old)
            self.assertEqual(original.count("'1.4'::text"), transformed.count("'1.4'::text"))

    def test_wrong_target_refused(self):
        with self.assertRaises(gen.GeneratorError):
            gen.build('src', 'versions', 'devops', '1.4')


if __name__ == '__main__':
    (ROOT / 'tmp').mkdir(exist_ok=True)
    unittest.main()
