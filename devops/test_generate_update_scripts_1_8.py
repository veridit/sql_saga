#!/usr/bin/env python3
"""No-database 1.8 packaging contract tests (devops/generate_update_scripts_1_8.py)."""
import re
import shutil
import tempfile
import unittest
from pathlib import Path
import generate_update_scripts_1_8 as gen
import direct_update

R = gen.RELEASE

ROOT = Path(__file__).resolve().parent.parent
SCHEMA_MARK = '$era_audit_columns_1_8$'
FORMAT_MARK = '$catalog_format_1_8$'


class Packaging(unittest.TestCase):
    def build(self, src=None, versions=None, devops=None):
        return gen.build(str(src or ROOT / 'src'), str(versions or ROOT / 'versions'),
                         str(devops or ROOT / 'devops'), '1.8')

    def copy(self, d, name):
        dst = Path(d) / name
        shutil.copytree(ROOT / name, dst)
        return dst

    def test_deterministic_framing_and_native_guards(self):
        outputs = self.build()
        self.assertEqual(outputs, self.build())
        self.assertEqual(set(outputs), {'sql_saga--%s--1.8.sql' % s for s in R.sources})
        for name, text in outputs.items():
            self.assertTrue(text.startswith('\\echo Use "ALTER EXTENSION sql_saga UPDATE TO \'1.8\'"'), name)
            self.assertEqual(text.splitlines().count(direct_update.GATE), 1, name)
            self.assertTrue(text.endswith(direct_update.GATE + '\n'), name)
            guards = re.findall(r"native_library_version\(\) <> '([^']+)'", text)
            self.assertEqual(set(guards), {'1.8'}, name)
            for old in ('1.5', '1.6', '1.7'):
                self.assertNotIn("'sql_saga %s upgrade'" % old, text, name)
            self.assertIn("'sql_saga 1.8 upgrade'", text, name)
            for routine in ('__internal_health_checks()', 'check_integrity()', '__internal_ddl_command_affects_managed_object()'):
                self.assertIn('CREATE OR REPLACE FUNCTION sql_saga.' + routine, text, name)

    def test_schema_fragment_once_before_the_routines(self):
        for name, text in self.build().items():
            self.assertEqual(text.count(SCHEMA_MARK), 2, name)
            self.assertEqual(text.count(FORMAT_MARK), 2, name)
            # After every embedded older update (which may itself change sql_saga.era), before the 1.8 routines.
            first_routine = text.index('CREATE OR REPLACE FUNCTION sql_saga.__internal_ddl_command_affects_managed_object()',
                                       text.rindex(SCHEMA_MARK))
            self.assertLess(text.rindex(FORMAT_MARK), first_routine, name)
            if name not in ('sql_saga--1.7--1.8.sql',):
                self.assertLess(text.rindex("'sql_saga 1.8 upgrade'"), text.index(SCHEMA_MARK), name)

    def test_embedded_updates_keep_catalog_markers(self):
        outputs = self.build()
        for source in R.sources[:4]:
            text = outputs['sql_saga--%s--1.8.sql' % source]
            self.assertIn('sql_saga.upgrade_from_1_4', text)
            # The embedded 1.4 step sets catalog_format 1.4; the 1.8 schema fragment then moves it to 1.8.
            self.assertLess(text.index("catalog_format = '1.4'"), text.index(FORMAT_MARK))
        for source in ('1.4', '1.5', '1.6', '1.7'):
            self.assertNotIn('sql_saga.upgrade_from_1_4', outputs['sql_saga--%s--1.8.sql' % source])

    def test_embedded_only_documented_retargeting(self):
        for source in R.sources[:-1]:
            original = (ROOT / 'versions' / ('sql_saga--%s--1.7.sql' % source)).read_text()
            transformed = R.embedded(original, source not in R.sources_without_inner_guards)
            body = original.split('\n', 1)[1]
            before = body[len(direct_update.guard_prefix('1.7')):-len(direct_update.GATE + '\n')].splitlines()
            after = transformed.splitlines()
            self.assertEqual(len(before), len(after))
            for old, new in zip(before, after):
                if old != new:
                    self.assertEqual(new.replace('1.8', '1.7'), old)
                    self.assertTrue(any(a in old for a in R.native_guard_anchors), old)

    def test_frozen_tamper_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            versions = self.copy(d, 'versions')
            p = versions / 'sql_saga--1.7.sql'
            p.write_text(p.read_text() + '\n')
            with self.assertRaisesRegex(direct_update.GeneratorError, 'SHA-256'):
                self.build(versions=versions)

    def test_change_outside_manifest_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            (src / '9z_omitted.sql').write_text('SELECT 618;\n')
            with self.assertRaisesRegex(direct_update.GeneratorError, 'outside.*manifest'):
                self.build(src=src)

    def test_change_to_an_undeclared_table_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            p = src / '0b_schema.sql'
            text = p.read_text()
            marker = 'CREATE TABLE sql_saga.system_time_era (\n'
            self.assertIn(marker, text)
            p.write_text(text.replace(marker, marker + '    sneaky_column text,\n', 1))
            with self.assertRaisesRegex(direct_update.GeneratorError, 'outside.*manifest'):
                self.build(src=src)

    def test_declared_table_unchanged_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            frozen = (ROOT / 'versions' / 'sql_saga--1.7.sql').read_text()
            old_era = re.search(r'CREATE TABLE sql_saga\.era \(.*?\n\);', frozen, re.S).group(0)
            p = src / '0b_schema.sql'
            text = p.read_text()
            new_era = re.search(r'CREATE TABLE sql_saga\.era \(.*?\n\);', text, re.S).group(0)
            p.write_text(text.replace(new_era, old_era, 1))
            with self.assertRaisesRegex(direct_update.GeneratorError, 'unchanged since 1.7: sql_saga.era'):
                self.build(src=src)

    def test_unlisted_routine_change_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            p = src / '0g_for_portion_of_trigger.sql'
            text = p.read_text()
            p.write_text(text.replace('BEGIN', 'BEGIN\n    NULL;', 1))
            with self.assertRaisesRegex(direct_update.GeneratorError, 'outside.*manifest'):
                self.build(src=src)

    def test_schema_or_repairs_with_gate_refused(self):
        for fragment in (R.schema, R.repairs):
            with self.subTest(fragment=fragment), tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
                devops = self.copy(d, 'devops')
                p = devops / fragment
                p.write_text(p.read_text() + direct_update.GATE + '\n')
                with self.assertRaisesRegex(direct_update.GeneratorError, 'integrity gate'):
                    self.build(devops=devops)

    def test_wrong_target_refused(self):
        with self.assertRaises(direct_update.GeneratorError):
            gen.build('src', 'versions', 'devops', '1.7')


if __name__ == '__main__':
    (ROOT / 'tmp').mkdir(exist_ok=True)
    unittest.main()
