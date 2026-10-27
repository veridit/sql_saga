#!/usr/bin/env python3
"""Specification tests for devops/generate_update_scripts_1_4.py.

Standard library only. Run: python3 devops/test_generate_update_scripts_1_4.py -v
Authentic fixtures are the SHA-256 authenticated files in versions/. SYNTHETIC_* text and temporary copies are
explicitly test-only material, never product migration SQL.
"""
import hashlib
import importlib.util
import os
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
VERSIONS = os.path.join(ROOT, "versions")
SRC = os.path.join(ROOT, "src")
DEVOPS = os.path.join(ROOT, "devops")
MODULE_PATH = os.path.join(HERE, "generate_update_scripts_1_4.py")
GATE = "SELECT sql_saga.check_integrity();"
EXPECTED_FROZEN = {
    "sql_saga--1.3.sql": "c59b9e3f501217dc8364fe8f57da44cfa39e0825a7571bff12082bd5624cfd52",
    "sql_saga--1.0--1.3.sql": "9646114bc85feabc9a03314da1a8298ab8c3607deb5bdc43eb27843ae1f002f1",
    "sql_saga--1.1--1.3.sql": "3b28f9bb314a75f3bae0cf363026e8e23fb1f5184e2aa8811dacb35fd67cd7d6",
    "sql_saga--1.2--1.3.sql": "d670f8e21f8a9b68afb7fbd3fb378eeb998419fef375268f7797f1cd7aae6aee",
}

SYNTHETIC_NESTED = """-- leading comment; with semicolon
CREATE FUNCTION sql_saga.outer_fn() RETURNS void LANGUAGE plpgsql AS $outer_fn$
BEGIN
    EXECUTE $ddl$
CREATE FUNCTION sql_saga.__FUNC_NAME__() RETURNS void LANGUAGE sql AS $b$ SELECT 1; $b$;
$ddl$;
    RAISE NOTICE 'it''s; fine';
END;
$outer_fn$;
/* block ; comment /* nested ; */ still comment */
COMMENT ON FUNCTION sql_saga.outer_fn() IS 'semi; colon "q" $x$';
CREATE TABLE sql_saga."weird;name" (a int);
SELECT 'trailing';
"""


def load():
    if not os.path.exists(MODULE_PATH):
        raise AssertionError("devops/generate_update_scripts_1_4.py does not exist")
    spec = importlib.util.spec_from_file_location("generate_update_scripts_1_4", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def read(name):
    with open(os.path.join(VERSIONS, name), encoding="utf-8") as f:
        return f.read()


def sha(path):
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


class Red(unittest.TestCase):
    def test_module_exists(self):
        self.assertTrue(os.path.exists(MODULE_PATH))


class Authentication(unittest.TestCase):
    def setUp(self):
        self.g = load()

    def test_frozen_sha_table(self):
        self.assertEqual(self.g.FROZEN_SHA256, EXPECTED_FROZEN)
        self.assertEqual(self.g.SOURCES, ("1.0", "1.1", "1.2", "1.3"))

    def test_authentic_inputs_pass(self):
        for name, expected in EXPECTED_FROZEN.items():
            path = os.path.join(VERSIONS, name)
            self.assertEqual(sha(path), expected)
            self.assertEqual(self.g.authenticate(path, expected), read(name))

    def test_tampered_input_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "tampered.sql")
            with open(path, "w", encoding="utf-8") as f:
                f.write(read("sql_saga--1.3.sql") + "\n")
            with self.assertRaises(self.g.GeneratorError):
                self.g.authenticate(path, EXPECTED_FROZEN["sql_saga--1.3.sql"])


class Splitter(unittest.TestCase):
    def setUp(self):
        self.g = load().g13

    def test_synthetic_split_respects_quotes_comments_dollar(self):
        stmts = self.g.split_sql(SYNTHETIC_NESTED)
        self.assertEqual(len(stmts), 4)
        self.assertIn("__FUNC_NAME__", stmts[0].text)
        self.assertTrue(stmts[0].text.rstrip().endswith("$outer_fn$;"))
        self.assertIn("'semi; colon", stmts[1].text)
        self.assertIn('"weird;name"', stmts[2].text)
        self.assertEqual("".join(s.text for s in stmts).strip(), SYNTHETIC_NESTED.strip())

    def test_unterminated_rejected(self):
        for bad in ["SELECT $a$ x;", "SELECT 'x;", "SELECT 1 /* x;", 'SELECT "x;', "SELECT 1"]:
            with self.assertRaises(self.g.GeneratorError, msg=bad):
                self.g.split_sql(bad)

    def test_authentic_install_splits_losslessly(self):
        text = read("sql_saga--1.3.sql")
        stmts = self.g.split_sql(text.split("\n", 1)[1])
        self.assertGreater(len(stmts), 100)
        self.assertEqual("".join(s.text for s in stmts).strip(), text.split("\n", 1)[1].strip())


class Statements(unittest.TestCase):
    def setUp(self):
        self.g = load()

    def statements(self, code):
        return self.g.g13.split_sql(self.g._read(self.g._src_file(SRC, code)))

    def test_classify(self):
        s = self.g.g13.split_sql(SYNTHETIC_NESTED)
        self.assertEqual([self.g.g13.classify(x) for x in s], ["routine", "comment", "create_table", "select"])
        for text, kind in [("GRANT EXECUTE ON FUNCTION sql_saga.f() TO PUBLIC;", "grant"),
                           ("REVOKE ALL ON FUNCTION sql_saga.f() FROM PUBLIC;", "revoke"),
                           ("DO $d$ BEGIN END; $d$;", "do"),
                           ("CREATE OR REPLACE PROCEDURE sql_saga.p() LANGUAGE sql AS $$ $$;", "routine"),
                           ("DROP FUNCTION sql_saga.f(int);", "drop_routine")]:
            self.assertEqual(self.g.g13.classify(self.g.g13.split_sql(text)[0]), kind)

    def test_real_signature_shapes(self):
        cases = [
            ("0c", "FUNCTION sql_saga.generated_always_as_row_start_end()", ((), "trigger")),
            ("2h", "FUNCTION sql_saga.__internal_health_checks()", ((), "event_trigger")),
            ("2o", "FUNCTION sql_saga.__internal_updatable_view_restore(",
             (("sql_saga.__internal_updatable_view_pending",), "void")),
        ]
        for code, marker, expected in cases:
            for stmt in self.statements(code):
                if marker in stmt.text:
                    name, args, result = self.g.g13.routine_signature(stmt)
                    self.assertEqual((args, result), expected, (code, name))
                    break
            else:
                self.fail((code, marker))
        for stmt in self.statements("0c"):
            if "FUNCTION sql_saga.__internal_regular_fk_initial_check(" in stmt.text:
                _, args, result = self.g.g13.routine_signature(stmt)
                self.assertEqual(len(args), 7)
                self.assertEqual(args[:3], ("text", "text", "text"))
                self.assertEqual(result, "boolean")
                break
        else:
            self.fail("regular FK initial-check signature missing")

    def test_create_or_replace_top_level_only(self):
        stmt = self.g.g13.split_sql(SYNTHETIC_NESTED)[0]
        out = self.g.g13.to_create_or_replace(stmt)
        self.assertEqual(out.count("CREATE OR REPLACE"), 1)
        self.assertIn("\nCREATE FUNCTION sql_saga.__FUNC_NAME__()", out)


class Historical(unittest.TestCase):
    def setUp(self):
        self.g = load()

    def test_transform_round_trip_and_retarget(self):
        for source in ("1.0", "1.1", "1.2"):
            original = read("sql_saga--%s--1.3.sql" % source)
            out = self.g.transform_embedded(original, source)
            self.assertEqual(self.g.reverse_embedded(out, source), original)
            self.assertIn("<> '1.4'", out)
            self.assertIn("sql_saga 1.4 native library", out)
            self.assertNotIn("sql_saga 1.3 native library", out)
            self.assertNotIn("\n" + GATE + "\n", "\n" + out)

    def test_wrong_header_and_extra_gate_rejected(self):
        original = read("sql_saga--1.1--1.3.sql")
        with self.assertRaises(self.g.GeneratorError):
            self.g.transform_embedded(original.replace("UPDATE TO '1.3'", "UPDATE TO '1.2'", 1), "1.1")
        with self.assertRaises(self.g.GeneratorError):
            self.g.transform_embedded(original + GATE + "\n", "1.1")

    def test_anchor_count_enforced(self):
        for source in ("1.0", "1.1", "1.2"):
            original = read("sql_saga--%s--1.3.sql" % source)
            anchor = self.g._ANCHORS[source][0][0]
            for mutated in (original.replace(anchor, "", 1), original.replace(anchor, anchor + anchor, 1)):
                with self.assertRaises(self.g.GeneratorError, msg=source):
                    self.g.transform_embedded(mutated, source)


class DeltaFailures(unittest.TestCase):
    def setUp(self):
        self.g = load()
        self.tmp = tempfile.TemporaryDirectory()
        self.src = os.path.join(self.tmp.name, "src")
        self.devops = os.path.join(self.tmp.name, "devops")
        shutil.copytree(SRC, self.src)
        shutil.copytree(DEVOPS, self.devops)
        self.frozen = read("sql_saga--1.3.sql")

    def tearDown(self):
        self.tmp.cleanup()

    def delta(self):
        return self.g.delta(self.src, self.devops, self.frozen)

    def test_changed_return_rejected(self):
        path = self.g._src_file(self.src, "0c")
        text = self.g._read(path)
        old = "FUNCTION sql_saga.generated_always_as_row_start_end()\nRETURNS trigger"
        self.assertIn(old, text)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text.replace(old, old.replace("trigger", "event_trigger"), 1))
        with self.assertRaisesRegex(self.g.GeneratorError, "result type"):
            self.delta()

    def test_changed_default_rejected(self):
        path = self.g._src_file(self.src, "3h")
        text = self.g._read(path)
        old = "synchronize_columns boolean DEFAULT true"
        self.assertIn(old, text)
        with open(path, "w", encoding="utf-8") as f:
            f.write(text.replace(old, "synchronize_columns boolean", 1))
        with self.assertRaisesRegex(self.g.GeneratorError, "argument names or defaults"):
            self.delta()

    def test_new_routine_without_acl_rejected(self):
        path = self.g._src_file(self.src, "0e")
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n-- SYNTHETIC no-ACL routine, test only\nCREATE FUNCTION sql_saga.__synthetic_no_acl() RETURNS boolean LANGUAGE sql AS $$ SELECT true $$;\n")
        with self.assertRaisesRegex(self.g.GeneratorError, "without a privilege decision"):
            self.delta()

    def test_new_table_rejected(self):
        path = self.g._src_file(self.src, "2o")
        with open(path, "a", encoding="utf-8") as f:
            f.write("\n-- SYNTHETIC table in a routine manifest, test only\nCREATE TABLE sql_saga.__synthetic_table (x int);\n")
        with self.assertRaisesRegex(self.g.GeneratorError, "schema fragment"):
            self.delta()

    def test_unknown_acl_target_rejected(self):
        path = self.g._src_file(self.src, "3g")
        with open(path, "a", encoding="utf-8") as f:
            f.write("\nREVOKE ALL ON FUNCTION sql_saga.__synthetic_missing() FROM PUBLIC;\n")
        with self.assertRaisesRegex(self.g.GeneratorError, "unknown routine"):
            self.delta()

    def test_fragment_gate_and_meta_command_rejected(self):
        for fragment in self.g.FRAGMENTS:
            path = os.path.join(self.devops, "upgrade_1_4_%s.sql" % fragment)
            with open(path, "a", encoding="utf-8") as f:
                f.write("\n" + GATE + "\n")
            with self.assertRaisesRegex(self.g.GeneratorError, "own integrity gate"):
                self.delta()
        path = os.path.join(self.devops, "upgrade_1_4_preamble.sql")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\\echo synthetic meta-command\n")
        with self.assertRaisesRegex(self.g.GeneratorError, "psql meta-command"):
            self.delta()

    def test_wrong_target_rejected(self):
        with self.assertRaises(self.g.GeneratorError):
            self.g.build(self.src, VERSIONS, self.devops, "1.3")


class RealBuild(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.g = load()
        cls.frozen = read("sql_saga--1.3.sql")
        cls.delta = cls.g.delta(SRC, DEVOPS, cls.frozen)
        cls.out = cls.g.build(SRC, VERSIONS, DEVOPS, "1.4")

    def statements(self, code):
        return self.g.g13.split_sql(self.g._read(self.g._src_file(SRC, code)))

    def test_manifest_includes_view_follow_and_event_workers(self):
        for code in ("0c", "2e", "2f", "2h", "2o", "3h"):
            self.assertIn(code, self.g.ROUTINE_MANIFEST + (self.g.TRAMPOLINES,))

    def test_four_scripts_single_gate_and_source(self):
        self.assertEqual(set(self.out), {"sql_saga--%s--1.4.sql" % s for s in self.g.SOURCES})
        for name, text in sorted(self.out.items()):
            source = name.split("--")[1]
            self.assertEqual(text.splitlines()[0], self.g.header("1.4"))
            self.assertEqual(text.splitlines()[1], "SELECT pg_catalog.set_config('sql_saga.upgrade_from_1_4', '%s', true);" % source)
            self.assertEqual(sum(line == GATE for line in text.split("\n")), 1)
            self.assertTrue(text.endswith(GATE + "\n"))
            if source != "1.3":
                self.assertIn(self.g.transform_embedded(read("sql_saga--%s--1.3.sql" % source), source), text)

    def test_real_routines_included_verbatim_as_create_or_replace(self):
        for code in self.g.ROUTINE_MANIFEST + (self.g.TRAMPOLINES,):
            for stmt in self.statements(code):
                if self.g.g13._ROUTINE_RE.match(self.g.g13._core(stmt)):
                    self.assertIn(self.g.g13.to_create_or_replace(stmt), self.delta, (code, stmt.line))

    def test_existing_acl_is_not_replayed(self):
        existing = self.g._frozen_identities(self.frozen)
        for code in self.g.ACL_SOURCES:
            for stmt in self.statements(code):
                kind = self.g.g13.classify(stmt)
                if kind not in ("grant", "revoke"):
                    continue
                targets = self.g._acl_targets(stmt)
                deliberate = kind == "revoke" and all(self.g.g13._key(t) in self.g.DELIBERATE_REVOKES_14 for t in targets)
                if targets and all(t in existing for t in targets) and not deliberate:
                    self.assertNotIn(stmt.text, self.delta, (code, targets))

    def test_default_only_public_entrypoint_grants(self):
        self.assertIn("$grant_public_event_entrypoints$", self.delta)
        self.assertIn("p.proacl = pg_catalog.acldefault('f', p.proowner)", self.delta)
        self.assertIn("FROM pg_catalog.pg_default_acl AS d", self.delta)
        self.assertIn("d.defaclnamespace IN (0, ns.oid)", self.delta)
        self.assertIn("d.defaclacl = p.proacl", self.delta)
        for routine in ("drop_protection()", "health_checks()", "rename_following()", "sync_history_policies(regclass)"):
            self.assertIn("sql_saga.%s" % routine, self.delta)

    def test_deliberate_revokes_are_emitted(self):
        found = set()
        for code in self.g.ACL_SOURCES:
            for stmt in self.statements(code):
                if self.g.g13.classify(stmt) != "revoke":
                    continue
                targets = self.g._acl_targets(stmt)
                if all(self.g.g13._key(t) in self.g.DELIBERATE_REVOKES_14 for t in targets):
                    self.assertIn(stmt.text, self.delta)
                    found.update(self.g.g13._key(t) for t in targets)
        self.assertEqual(found, self.g.DELIBERATE_REVOKES_14)


class EndToEnd(unittest.TestCase):
    def setUp(self):
        self.g = load()

    def test_main_is_deterministic_and_leaves_frozen_untouched(self):
        before = {name: sha(os.path.join(VERSIONS, name)) for name in EXPECTED_FROZEN}
        with tempfile.TemporaryDirectory() as out:
            args = ["--src", SRC, "--versions", VERSIONS, "--devops", DEVOPS, "--outdir", out, "--extversion", "1.4"]
            self.g.main(args)
            first = {name: sha(os.path.join(out, name)) for name in os.listdir(out)}
            self.g.main(args)
            second = {name: sha(os.path.join(out, name)) for name in os.listdir(out)}
            self.assertEqual(first, second)
            self.assertEqual(set(first), {"sql_saga--%s--1.4.sql" % s for s in self.g.SOURCES})
        after = {name: sha(os.path.join(VERSIONS, name)) for name in EXPECTED_FROZEN}
        self.assertEqual(before, after)

    def test_late_failure_preserves_existing_outputs(self):
        with tempfile.TemporaryDirectory() as d:
            devops = os.path.join(d, "devops")
            out = os.path.join(d, "out")
            shutil.copytree(DEVOPS, devops)
            os.mkdir(out)
            sentinels = {}
            for source in self.g.SOURCES:
                name = "sql_saga--%s--1.4.sql" % source
                sentinels[name] = "sentinel %s\n" % source
                with open(os.path.join(out, name), "w", encoding="utf-8") as f:
                    f.write(sentinels[name])
            with open(os.path.join(devops, "upgrade_1_4_repairs.sql"), "a", encoding="utf-8") as f:
                f.write("\n" + GATE + "\n")
            with self.assertRaises(self.g.GeneratorError):
                self.g.main(["--src", SRC, "--versions", VERSIONS, "--devops", devops, "--outdir", out, "--extversion", "1.4"])
            for name, expected in sentinels.items():
                with open(os.path.join(out, name), encoding="utf-8") as f:
                    self.assertEqual(f.read(), expected)


if __name__ == "__main__":
    unittest.main()
