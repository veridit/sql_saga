#!/usr/bin/env python3
"""Mutation tests of the portable migration-only cascading-DDL gate."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
HARNESS = (ROOT / 'devops/upgrade_test.sh').read_text()
# Exercise the real, unchanged extraction without running database setup.
EXTRACTOR = HARNESS.split('executable_migration_sql() {', 1)[1].split('\n}\n', 1)[0]
EXTRACTOR = 'executable_migration_sql() {' + EXTRACTOR + '\n}\n'


class MigrationSafety(unittest.TestCase):
    def scan(self, sql, interpreter=None):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            path = Path(d) / 'mutation.sql'
            path.write_text(sql)
            script = EXTRACTOR + '''
. "$REPO/devops/upgrade_migration_safety.sh"
ok() { printf 'PASS: %s\\n' "$*"; }
fail() { printf 'REJECT: %s\\n' "$*"; exit 1; }
die() { printf 'ERROR: %s\\n' "$*"; exit 2; }
body=$(executable_migration_sql "$1") || die 'extraction failed'
assert_no_cascading_ddl "$body"
'''
            env = dict(os.environ, REPO=str(ROOT))
            if interpreter:
                env['SQL_SAGA_PYTHON'] = interpreter
            return subprocess.run(['bash', '-c', script, 'mutation', str(path)], env=env,
                                  text=True, capture_output=True)

    def test_dangerous_statements_rejected(self):
        for sql in ('DROP TABLE x CASCADE;', 'DROP\nTABLE x\nCASCADE;',
                    'truncate x\n cascade;', 'DO $$ BEGIN DROP TABLE x\nCASCADE; END $$;',
                    'SELECT 1; TRUNCATE ONLY x, y RESTART IDENTITY CASCADE;',
                    'DROP TABLE "cascade; quoted" CASCADE;'):
            with self.subTest(sql=sql):
                result = self.scan(sql)
                self.assertEqual(result.returncode, 1, result)
                self.assertIn('REJECT:', result.stdout)
                self.assertNotIn('PASS:', result.stdout)

    def test_literals_identifiers_and_statement_boundaries_safe(self):
        for sql in ("INSERT INTO catalog VALUES ('DROP TABLE x CASCADE', 'CASCADE');",
                    'DROP TABLE "CASCADE";', 'DROP TABLE "quoted""CASCADE";',
                    "DO $$ BEGIN DROP TABLE x RESTRICT; INSERT INTO catalog VALUES ('CASCADE'); END $$;",
                    "DO $$ BEGIN RAISE NOTICE 'DROP TABLE x CASCADE'; END $$;",
                    'DROP TABLE cascade_backup;', 'SELECT drop_count, truncate_count, cascade;',
                    'DROP TABLE x; SELECT cascade;', 'DROP TABLE x RESTRICT; -- CASCADE\n',
                    '/* DROP TABLE x CASCADE; */ SELECT 1;',
                    'CREATE FUNCTION x() RETURNS void LANGUAGE plpgsql AS $$ BEGIN DROP TABLE x CASCADE; END $$;'):
            with self.subTest(sql=sql):
                result = self.scan(sql)
                self.assertEqual(result.returncode, 0, result)
                self.assertIn('PASS:', result.stdout)

    def test_scanner_failure_never_passes(self):
        with tempfile.TemporaryDirectory(dir=ROOT / 'tmp') as d:
            scanner = Path(d) / 'broken-python'
            scanner.write_text('#!/bin/sh\nexit 2\n')
            scanner.chmod(0o755)
            result = self.scan('SELECT 1;', str(scanner))
            self.assertEqual(result.returncode, 2, result)
            self.assertIn('ERROR:', result.stdout)
            self.assertNotIn('PASS:', result.stdout)
        result = self.scan('SELECT 1;', '/nonexistent/sql-saga-python')
        self.assertEqual(result.returncode, 2, result)
        self.assertNotIn('PASS:', result.stdout)

    def test_both_harness_paths_use_checked_dispatch(self):
        self.assertEqual(HARNESS.count('assert_no_cascading_ddl "$body"'), 2)
        self.assertNotIn('[[:<:]]', HARNESS)


if __name__ == '__main__':
    (ROOT / 'tmp').mkdir(exist_ok=True)
    unittest.main()
