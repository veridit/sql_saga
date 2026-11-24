#!/usr/bin/env python3
"""Specification of devops/update_step.py (no database). Run: python3 devops/test_update_step.py"""
import os
import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import update_step as u  # noqa: E402
import sql_statements as s  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent

BASE = '''\\echo Use "CREATE EXTENSION sql_saga" to load this file. \\quit
CREATE SCHEMA sql_saga;
CREATE TABLE sql_saga.era (table_name name, era_name name);
CREATE FUNCTION sql_saga.add_era(table_oid regclass, era_name name DEFAULT 'valid') RETURNS boolean LANGUAGE plpgsql AS $add_era$ BEGIN RETURN true; END; $add_era$;
COMMENT ON FUNCTION sql_saga.add_era(regclass, name) IS 'Adds an era.';
CREATE FUNCTION sql_saga.__internal_helper(x integer) RETURNS integer LANGUAGE sql AS $h$ SELECT x $h$;
REVOKE ALL ON FUNCTION sql_saga.__internal_helper(integer) FROM PUBLIC;
CREATE FUNCTION sql_saga.old_thing() RETURNS void LANGUAGE sql AS $o$ SELECT $o$;
'''


def step(new, fragment=''):
    return u.build(BASE, new, fragment, '1.13', '1.14')


def routines(text):
    return [u.key_text(u.routine_ident(x)) for x in s.split_sql(text.split('\n', 1)[1]) if u.routine_ident(x)]


class Step(unittest.TestCase):
    def test_unchanged_source_is_check_lock_and_gate(self):
        out = step(BASE)
        self.assertTrue(out.startswith('\\echo Use "ALTER EXTENSION sql_saga UPDATE TO \'1.14\'" to load this file. \\quit\n'))
        self.assertEqual(routines(out), ['sql_saga.native_library_version()'])
        self.assertIn("string_to_array('1.14', '.')", out)
        self.assertIn("hashtext('sql_saga 1.14 upgrade')", out)
        self.assertTrue(out.endswith(u.LAST_STEP_GATE))

    def test_changed_body_is_create_or_replace_and_keeps_privileges(self):
        out = step(BASE.replace('RETURN true;', 'RETURN false;'))
        self.assertIn('CREATE OR REPLACE FUNCTION sql_saga.add_era(', out)
        self.assertNotIn('DROP FUNCTION', out)
        self.assertNotIn('COMMENT ON', out)

    def test_new_routine_gets_its_privileges(self):
        new = BASE + ('CREATE FUNCTION sql_saga.__internal_new(y integer) RETURNS integer LANGUAGE sql AS $n$ SELECT y $n$;\n'
                      'REVOKE ALL ON FUNCTION sql_saga.__internal_new(integer) FROM PUBLIC;\n')
        out = step(new)
        self.assertIn('CREATE OR REPLACE FUNCTION sql_saga.__internal_new(', out)
        self.assertIn('REVOKE ALL ON FUNCTION sql_saga.__internal_new(integer) FROM PUBLIC;', out)
        self.assertNotIn('$replace_', out)

    def test_signature_change_checks_privileges_drops_and_recreates_with_acl_and_comment(self):
        new = BASE.replace("era_name name DEFAULT 'valid')", "era_name name DEFAULT 'valid', strict boolean DEFAULT true)") \
                  .replace("COMMENT ON FUNCTION sql_saga.add_era(regclass, name)", "COMMENT ON FUNCTION sql_saga.add_era(regclass, name, boolean)")
        out = step(new)
        self.assertIn("old_proc CONSTANT text := 'sql_saga.add_era(regclass, name)';", out)
        self.assertIn("new_proc CONSTANT text := 'sql_saga.add_era(regclass, name, boolean)';", out)
        self.assertIn("'ALTER EXTENSION sql_saga UPDATE;'", out)
        self.assertIn('COMMENT ON FUNCTION sql_saga.add_era(regclass, name, boolean) IS', out)
        self.assertLess(out.index('$replace_add_era$'), out.index('CREATE OR REPLACE FUNCTION sql_saga.add_era('))

    def test_same_signature_change_create_or_replace_cannot_make_is_replaced(self):
        new = BASE.replace('RETURNS integer LANGUAGE sql AS $h$', 'RETURNS bigint LANGUAGE sql AS $h$')
        out = step(new)
        self.assertIn("old_proc CONSTANT text := 'sql_saga.__internal_helper(integer)';", out)
        self.assertIn('REVOKE ALL ON FUNCTION sql_saga.__internal_helper(integer) FROM PUBLIC;', out)

    def test_removed_routine_is_dropped(self):
        out = step(BASE.replace('CREATE FUNCTION sql_saga.old_thing() RETURNS void LANGUAGE sql AS $o$ SELECT $o$;\n', ''))
        self.assertIn('DROP FUNCTION sql_saga.old_thing();', out)

    def test_changed_comment_on_existing_routine_is_emitted(self):
        out = step(BASE.replace("'Adds an era.'", "'Adds an era to a table.'"))
        self.assertIn("COMMENT ON FUNCTION sql_saga.add_era(regclass, name) IS 'Adds an era to a table.';", out)
        self.assertNotIn('CREATE OR REPLACE FUNCTION sql_saga.add_era', out)

    def test_catalog_change_must_be_handled_by_the_fragment(self):
        new = BASE.replace('era_name name);', 'era_name name, extra text);')
        with self.assertRaisesRegex(u.GeneratorError, r'-- handles: <key>": CREATE TABLE sql_saga\.era$'):
            step(new)
        fragment = '-- handles: CREATE TABLE sql_saga.era\nALTER TABLE sql_saga.era ADD COLUMN extra text;\n'
        out = step(new, fragment)
        self.assertIn('ALTER TABLE sql_saga.era ADD COLUMN extra text;', out)
        self.assertLess(out.index('ALTER TABLE sql_saga.era'), out.index('$last_step_gate$'))

    def test_fragment_may_not_name_changes_src_does_not_make(self):
        with self.assertRaisesRegex(u.GeneratorError, 'names changes that src/ does not make: CREATE TABLE sql_saga.era'):
            step(BASE, '-- handles: CREATE TABLE sql_saga.era\n')

    def test_fragment_may_not_run_the_gate(self):
        with self.assertRaisesRegex(u.GeneratorError, 'must not run the integrity gate'):
            step(BASE, 'SELECT sql_saga.check_integrity();\n')

    def test_ambiguous_signature_change_fails(self):
        new = BASE.replace('CREATE FUNCTION sql_saga.add_era(table_oid regclass, era_name name DEFAULT \'valid\')',
                           'CREATE FUNCTION sql_saga.add_era(table_oid regclass, era_name name, a int)') + \
            "CREATE FUNCTION sql_saga.add_era(table_oid regclass, era_name name, b text) RETURNS boolean LANGUAGE sql AS $x$ SELECT true $x$;\n"
        new = new.replace('COMMENT ON FUNCTION sql_saga.add_era(regclass, name)', 'COMMENT ON FUNCTION sql_saga.add_era(regclass, name, integer)')
        with self.assertRaisesRegex(u.GeneratorError, 'more than one overload'):
            step(new)


class History(unittest.TestCase):
    """The released steps 1.9--1.10 to 1.12--1.13 change only routines: the generator finds exactly their routines."""

    def test_released_routine_sets(self):
        for a, b in (('1.9', '1.10'), ('1.10', '1.11'), ('1.11', '1.12')):
            old = (ROOT / 'versions' / ('sql_saga--%s.sql' % a)).read_text()
            new = (ROOT / 'versions' / ('sql_saga--%s.sql' % b)).read_text()
            released = (ROOT / 'versions' / ('sql_saga--%s--%s.sql' % (a, b))).read_text()
            self.assertEqual(sorted(routines(u.build(old, new, '', a, b))), sorted(routines(released)), '%s--%s' % (a, b))


class Package(unittest.TestCase):
    def test_version_is_read_from_the_control_file_and_previous_from_versions(self):
        target = u.control_version(ROOT)
        self.assertRegex(target, r'^[0-9]+\.[0-9]+$')
        prev = u.previous_version(ROOT, target)
        self.assertTrue((ROOT / 'versions' / ('sql_saga--%s.sql' % prev)).is_file())
        self.assertLess(u.version_key(prev), u.version_key(target))


if __name__ == '__main__':
    unittest.main()
