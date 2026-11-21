#!/usr/bin/env python3
"""No-database 1.9 packaging contract tests (devops/generate_update_scripts_1_9.py)."""
import re
import shutil
import tempfile
import unittest
from pathlib import Path
import generate_update_scripts_1_9 as gen
import direct_update

R = gen.RELEASE

ROOT = Path(__file__).resolve().parent.parent
SCHEMA_MARK = '$era_provenance_columns_1_9$'
FORMAT_MARK = '$catalog_format_1_9$'
OLD_ADD_ERA = 'sql_saga.add_era(%s)' % gen.OLD_ADD_ERA_ARGS
OLD_API_ADD_ERA = 'sql_saga.__api_add_era(%s)' % gen.OLD_ADD_ERA_ARGS


class Packaging(unittest.TestCase):
    def build(self, src=None, versions=None, devops=None):
        return gen.build(str(src or ROOT / 'src'), str(versions or ROOT / 'versions'),
                         str(devops or ROOT / 'devops'), '1.9')

    def copy(self, d, name):
        dst = Path(d) / name
        shutil.copytree(ROOT / name, dst)
        return dst

    def test_deterministic_framing_and_native_guards(self):
        outputs = self.build()
        self.assertEqual(outputs, self.build())
        self.assertEqual(set(outputs), {'sql_saga--%s--1.9.sql' % s for s in R.sources})
        for name, text in outputs.items():
            self.assertTrue(text.startswith('\\echo Use "ALTER EXTENSION sql_saga UPDATE TO \'1.9\'"'), name)
            self.assertEqual(text.splitlines().count(direct_update.GATE), 1, name)
            self.assertTrue(text.endswith(direct_update.GATE + '\n'), name)
            self.assertEqual(set(re.findall(r"native_library_version\(\) <> '([^']+)'", text)), {'1.9'}, name)
            for old in ('1.5', '1.6', '1.7', '1.8'):
                self.assertNotIn("'sql_saga %s upgrade'" % old, text, name)
            self.assertIn("'sql_saga 1.9 upgrade'", text, name)

    def test_add_era_signature_replaced_before_it_is_created(self):
        for name, text in self.build().items():
            # The old signatures are dropped by guarded blocks (refusing customised privileges), once each.
            self.assertEqual(text.count("old_proc CONSTANT text := '%s';" % OLD_ADD_ERA), 1, name)
            self.assertEqual(text.count("old_proc CONSTANT text := '%s';" % OLD_API_ADD_ERA), 1, name)
            drop = text.index("old_proc CONSTANT text := '%s';" % OLD_API_ADD_ERA)
            create = text.rindex('CREATE OR REPLACE FUNCTION sql_saga.__api_add_era(')
            self.assertLess(drop, create, name)
            self.assertIn('would lose its EXECUTE privileges', text, name)
            # The public function is checked for privileges beyond PUBLIC; the private worker for any privilege.
            public_block = text[text.index("old_proc CONSTANT text := '%s';" % OLD_ADD_ERA):]
            self.assertIn('AND NOT a.is_grantable AND true)', public_block[:2000], name)
            worker_block = text[drop:]
            self.assertIn('AND NOT a.is_grantable AND false)', worker_block[:2000], name)

    def test_new_routines_privileges_and_comments(self):
        for name, text in self.build().items():
            for routine in ('provenance_trigger()', 'set_era_provenance_columns(', '__api_set_era_provenance_columns(',
                            '__internal_set_era_provenance_columns('):
                self.assertIn('CREATE OR REPLACE FUNCTION sql_saga.' + routine, text, name)
            for statement in R.new_privileges:
                self.assertEqual(text.count(statement), 1, name)
            self.assertEqual(text.count('COMMENT ON FUNCTION sql_saga.provenance_trigger() IS'), 1, name)
            self.assertEqual(text.count('COMMENT ON FUNCTION sql_saga.__api_set_era_provenance_columns IS'), 1, name)

    def test_schema_fragment_once_after_the_routines(self):
        for name, text in self.build().items():
            self.assertEqual(text.count(SCHEMA_MARK), 2, name)
            self.assertEqual(text.count(FORMAT_MARK), 2, name)
            # After the 1.9 routines: the ALTER TABLE fires sql_saga's event triggers.
            for routine in R.routine_manifest:
                head = re.compile(r'CREATE OR REPLACE (?:FUNCTION|PROCEDURE) ' + re.escape(routine.split('(')[0]) + r'\(')
                starts = [m.start() for m in head.finditer(text)]
                self.assertTrue(starts, (name, routine))
                self.assertLess(starts[-1], text.index(SCHEMA_MARK), (name, routine))
            self.assertLess(text.rindex(FORMAT_MARK), text.index(direct_update.GATE), name)
            if name != 'sql_saga--1.8--1.9.sql':
                self.assertLess(text.rindex("'sql_saga 1.9 upgrade'"), text.index(SCHEMA_MARK), name)

    def test_embedded_only_documented_retargeting(self):
        for source in R.sources[:-1]:
            original = (ROOT / 'versions' / ('sql_saga--%s--1.8.sql' % source)).read_text()
            transformed = R.embedded(original, source not in R.sources_without_inner_guards)
            body = original.split('\n', 1)[1]
            before = body[len(direct_update.guard_prefix('1.8')):-len(direct_update.GATE + '\n')].splitlines()
            after = transformed.splitlines()
            self.assertEqual(len(before), len(after))
            for old, new in zip(before, after):
                if old != new:
                    self.assertEqual(new.replace('1.9', '1.8'), old)
                    self.assertTrue(any(a in old for a in R.native_guard_anchors), old)

    def test_frozen_tamper_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            versions = self.copy(d, 'versions')
            p = versions / 'sql_saga--1.8.sql'
            p.write_text(p.read_text() + '\n')
            with self.assertRaisesRegex(direct_update.GeneratorError, 'SHA-256'):
                self.build(versions=versions)

    def test_change_outside_manifest_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            (src / '9z_omitted.sql').write_text('SELECT 619;\n')
            with self.assertRaisesRegex(direct_update.GeneratorError, 'outside.*manifest'):
                self.build(src=src)

    def test_undeclared_comment_change_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            p = src / '2c_set_era_ephemeral_columns.sql'
            text = p.read_text()
            marker = "COMMENT ON FUNCTION sql_saga.__api_set_era_ephemeral_columns IS\n'Sets"
            self.assertIn(marker, text)
            p.write_text(text.replace(marker, marker + ' (changed)', 1))
            with self.assertRaisesRegex(direct_update.GeneratorError, 'outside.*manifest'):
                self.build(src=src)

    def test_replaced_routine_still_in_source_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            (src / '9z_old_add_era.sql').write_text(
                'CREATE FUNCTION sql_saga.add_era(%s) RETURNS boolean LANGUAGE sql AS $$ SELECT true $$;\n'
                % ', '.join('a%d %s' % (i, t) for i, t in enumerate(gen.OLD_ADD_ERA_ARGS.split(', '))))
            with self.assertRaises(direct_update.GeneratorError):
                self.build(src=src)

    def test_declared_table_unchanged_refused(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            src = self.copy(d, 'src')
            frozen = (ROOT / 'versions' / 'sql_saga--1.8.sql').read_text()
            old_era = re.search(r'CREATE TABLE sql_saga\.era \(.*?\n\);', frozen, re.S).group(0)
            p = src / '0b_schema.sql'
            text = p.read_text()
            new_era = re.search(r'CREATE TABLE sql_saga\.era \(.*?\n\);', text, re.S).group(0)
            p.write_text(text.replace(new_era, old_era, 1))
            with self.assertRaisesRegex(direct_update.GeneratorError, 'unchanged since 1.8: sql_saga.era'):
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
            gen.build('src', 'versions', 'devops', '1.8')


if __name__ == '__main__':
    (ROOT / 'tmp').mkdir(exist_ok=True)
    unittest.main()
