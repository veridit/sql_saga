#!/usr/bin/env python3
"""Specification tests for devops/generate_update_scripts.py (sql_saga 1.3 update generator).

Standard library only. Run: python3 devops/test_generate_update_scripts.py -v
Fixtures named SYNTHETIC_* are synthetic SQL written for these tests, not product SQL.
Tests marked "authentic" read the frozen, SHA-256 authenticated files in versions/.
"""
import hashlib
import importlib.util
import os
import re
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
VERSIONS = os.path.join(ROOT, "versions")
MODULE_PATH = os.path.join(HERE, "generate_update_scripts.py")

SHA_10_12 = "11b6174d2a428bed86e82fb5e7560ffa8a0f640affd8146c399c9e1dae49c321"
SHA_11_12 = "3a33a535b57d1990f29358b5bfa6443a53217787766bd39abc78a8243a5260c5"
SHA_12 = "448ffb0b496578b9243443aefb5ddc2f2c66de7dc6315a5da2bdbddd92a6fa83"
GATE = "SELECT sql_saga.check_integrity();"
P_1K_MARKER = "-- sql_saga 1.3 generator: insert src/1k_foreign_key_pairing.sql routines here"


def load():
    if not os.path.exists(MODULE_PATH):
        raise AssertionError("RED: devops/generate_update_scripts.py does not exist yet")
    spec = importlib.util.spec_from_file_location("generate_update_scripts", MODULE_PATH)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["generate_update_scripts"] = mod
    spec.loader.exec_module(mod)
    return mod


def read(name):
    with open(os.path.join(VERSIONS, name), encoding="utf-8") as f:
        return f.read()


# --- SYNTHETIC fixtures -------------------------------------------------------
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

SYNTHETIC_HISTORICAL_BAD = "\\echo Use \"ALTER EXTENSION sql_saga UPDATE TO '1.2'\" to load this file. \\quit\nSELECT '1.2 surprise';\n"


class Red(unittest.TestCase):
    def test_module_exists(self):
        self.assertTrue(os.path.exists(MODULE_PATH), "generator module must exist")


class Authentication(unittest.TestCase):
    def setUp(self):
        self.g = load()

    def test_frozen_sha_table(self):
        self.assertEqual(self.g.FROZEN_SHA256["sql_saga--1.0--1.2.sql"], SHA_10_12)
        self.assertEqual(self.g.FROZEN_SHA256["sql_saga--1.1--1.2.sql"], SHA_11_12)
        self.assertEqual(self.g.FROZEN_SHA256["sql_saga--1.2.sql"], SHA_12)

    def test_authentic_inputs_pass(self):  # authentic
        for name, sha in self.g.FROZEN_SHA256.items():
            self.assertEqual(self.g.authenticate(os.path.join(VERSIONS, name), sha), read(name))

    def test_tampered_input_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            p = os.path.join(d, "x.sql")
            with open(p, "w") as f:
                f.write(read("sql_saga--1.1--1.2.sql") + "\n")
            with self.assertRaises(self.g.GeneratorError):
                self.g.authenticate(p, SHA_11_12)


class Splitter(unittest.TestCase):
    def setUp(self):
        self.g = load()

    def test_synthetic_split_respects_quotes_comments_dollar(self):
        stmts = self.g.split_sql(SYNTHETIC_NESTED)
        self.assertEqual(len(stmts), 4)
        self.assertIn("__FUNC_NAME__", stmts[0].text)
        self.assertTrue(stmts[0].text.rstrip().endswith("$outer_fn$;"))
        self.assertIn("'semi; colon", stmts[1].text)
        self.assertIn('"weird;name"', stmts[2].text)
        self.assertEqual(stmts[0].line, 1)
        # concatenation is lossless
        self.assertEqual("".join(s.text for s in stmts).strip(), SYNTHETIC_NESTED.strip())

    def test_unterminated_rejected(self):
        for bad in ["SELECT $a$ x;", "SELECT 'x;", "SELECT 1 /* x;", 'SELECT "x;', "SELECT 1"]:
            with self.assertRaises(self.g.GeneratorError, msg=bad):
                self.g.split_sql(bad)

    def test_authentic_install_splits_losslessly(self):  # authentic
        text = read("sql_saga--1.2.sql")
        stmts = self.g.split_sql(text.split("\n", 1)[1])
        self.assertGreater(len(stmts), 100)
        self.assertEqual("".join(s.text for s in stmts).strip(), text.split("\n", 1)[1].strip())


class Statements(unittest.TestCase):
    def setUp(self):
        self.g = load()

    def test_classify(self):
        s = self.g.split_sql(SYNTHETIC_NESTED)
        self.assertEqual([self.g.classify(x) for x in s], ["routine", "comment", "create_table", "select"])
        for ok, kind in [("GRANT EXECUTE ON FUNCTION sql_saga.f() TO PUBLIC;", "grant"),
                         ("REVOKE ALL ON FUNCTION sql_saga.f() FROM PUBLIC;", "revoke"),
                         ("DO $d$ BEGIN END; $d$;", "do"),
                         ("CREATE OR REPLACE PROCEDURE sql_saga.p() LANGUAGE sql AS $$ $$;", "routine"),
                         ("DROP FUNCTION sql_saga.f(int);", "drop_routine")]:
            self.assertEqual(self.g.classify(self.g.split_sql(ok)[0]), kind)

    def test_unsupported_rejected_not_dropped(self):
        for bad in ["CREATE VIEW sql_saga.v AS SELECT 1;", "ALTER TABLE sql_saga.era ADD c int;",
                    "CREATE FUNCTION public.f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;",
                    "DROP TABLE sql_saga.era;"]:
            with self.assertRaises(self.g.GeneratorError, msg=bad):
                self.g.classify(self.g.split_sql(bad)[0])

    def test_create_or_replace_top_level_only(self):
        s = self.g.split_sql(SYNTHETIC_NESTED)[0]
        out = self.g.to_create_or_replace(s)
        self.assertTrue(out.lstrip("- leadingcomment;with semicolon\n").startswith("CREATE OR REPLACE FUNCTION sql_saga.outer_fn()")
                        or "\nCREATE OR REPLACE FUNCTION sql_saga.outer_fn()" in out)
        self.assertIn("\nCREATE FUNCTION sql_saga.__FUNC_NAME__()", out)  # body verbatim
        self.assertEqual(out.count("CREATE OR REPLACE"), 1)
        already = self.g.split_sql("CREATE OR REPLACE FUNCTION sql_saga.f() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;")[0]
        self.assertEqual(self.g.to_create_or_replace(already), already.text)

    def test_signature_real_shapes(self):  # authentic src statements
        def stmt(fname, startswith):
            with open(os.path.join(ROOT, "src", fname)) as f:
                for s in self.g.split_sql(f.read()):
                    if startswith in s.text:
                        return s
            self.fail(startswith)
        name, args, ret = self.g.routine_signature(stmt("1k_foreign_key_pairing.sql",
            "FUNCTION sql_saga.__internal_foreign_key_mispairing_report("))
        self.assertEqual((name, args), ("sql_saga.__internal_foreign_key_mispairing_report", ("name",)))
        self.assertEqual(ret, "record(message text, hint text)")  # implicit OUT result
        name, args, ret = self.g.routine_signature(stmt("3d_manage_temporal_triggers.sql",
            "PROCEDURE sql_saga.manage_temporal_fk_triggers("))
        self.assertEqual(args, ("sql_saga.trigger_action", "variadic regclass[]"))
        self.assertIsNone(ret)  # procedure
        name, args, ret = self.g.routine_signature(stmt("0c_c_functions.sql",
            "FUNCTION sql_saga.covers_without_gaps_transfn("))
        self.assertEqual(args, ("internal", "anyrange", "anyrange"))  # unnamed C args
        self.assertEqual(ret, "internal")
        name, args, ret = self.g.routine_signature(stmt("2o_updatable_view_column_follow.sql",
            "FUNCTION sql_saga.__internal_updatable_view_restore("))
        self.assertEqual(args, ("sql_saga.__internal_updatable_view_pending",))  # composite arg
        # SYNTHETIC valid TABLE form
        s = self.g.split_sql("CREATE FUNCTION sql_saga.t(a int) RETURNS TABLE (x int, y text) LANGUAGE sql AS $$ SELECT 1, '' $$;")[0]
        self.assertEqual(self.g.routine_signature(s)[2], "table(x int, y text)")
        name, args, ret = self.g.routine_signature(stmt("2h_health_checks.sql",
            "FUNCTION sql_saga.__internal_system_versioning_member_attribution("))  # real TABLE output
        self.assertEqual(args, ("oid",))
        self.assertTrue(ret.startswith("table(as_of_attributable boolean, between_attributable boolean"), ret)


class Historical(unittest.TestCase):
    def setUp(self):
        self.g = load()

    def check(self, name):  # authentic
        src = read(name)
        out = self.g.transform_historical(src, name.replace("sql_saga--", "").replace(".sql", ""))
        self.assertFalse(out.startswith("\\echo"))
        self.assertNotIn("\n" + GATE + "\n", "\n" + out)
        self.assertIn("-- sql_saga 1.3: the 1.2 integrity gate runs at the end of this update instead", out)
        self.assertNotIn("<> '1.2'", out)
        self.assertIn("ALTER EXTENSION sql_saga UPDATE TO ''1.3'';", out)
        self.assertIn("Install the sql_saga 1.3 native library", out)
        self.assertEqual(self.g.reverse_historical(out, name.replace("sql_saga--", "").replace(".sql", "")), src)
        # Independent check: only explicitly allowed lines differ (first line dropped; rest is line-for-line).
        if name == "sql_saga--1.0--1.2.sql":  # ACL guard insertion: line mapping in HistoricalAclGuard
            return out
        src_lines, out_lines = src.split("\n")[1:], out.split("\n")
        self.assertEqual(len(src_lines), len(out_lines))
        allowed = [GATE, "<> '1.2'", "extension update version 1.2", "sql_saga 1.2 native library",
                   "UPDATE TO ''1.2'';"]
        for a, b in zip(src_lines, out_lines):
            if a != b:
                self.assertTrue(any(x in a for x in allowed), a)
        return out

    def test_header_is_source_specific(self):  # authentic: 1.0--1.2 header targets 1.1
        self.assertIn("UPDATE TO '1.1'", read("sql_saga--1.0--1.2.sql").split("\n", 1)[0])
        self.assertIn("UPDATE TO '1.2'", read("sql_saga--1.1--1.2.sql").split("\n", 1)[0])
        src = read("sql_saga--1.1--1.2.sql")
        with self.assertRaises(self.g.GeneratorError):  # wrong header for the declared source
            self.g.transform_historical(src.replace("UPDATE TO '1.2'", "UPDATE TO '1.1'", 1), "1.1--1.2")

    def test_11_12(self):
        out = self.check("sql_saga--1.1--1.2.sql")
        self.assertEqual(out.count("<> '1.3'"), 1)

    def test_10_12(self):
        out = self.check("sql_saga--1.0--1.2.sql")
        self.assertEqual(out.count("<> '1.3'"), 2)  # T2 one-line + T3 block

    def test_unexpected_12_occurrence_rejected(self):  # authentic input, all anchors intact
        for name in ["sql_saga--1.1--1.2.sql", "sql_saga--1.0--1.2.sql"]:
            src = read(name)
            head, rest = src.split("\n", 1)
            mutated = head + "\nSELECT '1.2 surprise';\n" + rest
            with self.assertRaises(self.g.GeneratorError, msg=name):
                self.g.transform_historical(mutated, name[10:-4])

    def test_anchor_count_enforced(self):  # authentic input: delete or duplicate each anchor
        guard_block = ("    IF sql_saga.native_library_version() <> '1.2' THEN\n")
        hint = "                      'ALTER EXTENSION sql_saga UPDATE TO ''1.2'';',\n"
        guard_line = ("DO $native_guard$ BEGIN IF sql_saga.native_library_version() <> '1.2' THEN")
        cases = [("sql_saga--1.1--1.2.sql", GATE + "\n"), ("sql_saga--1.1--1.2.sql", guard_block),
                 ("sql_saga--1.1--1.2.sql", hint), ("sql_saga--1.0--1.2.sql", GATE + "\n"),
                 ("sql_saga--1.0--1.2.sql", guard_block), ("sql_saga--1.0--1.2.sql", hint),
                 ("sql_saga--1.0--1.2.sql", guard_line)]
        for name, anchor in cases:
            src = read(name)
            self.assertIn(anchor, src)
            for mutated in [src.replace(anchor, "", 1), src.replace(anchor, anchor + anchor, 1)]:
                with self.assertRaises(self.g.GeneratorError, msg=(name, anchor)):
                    self.g.transform_historical(mutated, name[10:-4])


ACL_REPLAY_GRANTS_10 = ["GRANT EXECUTE ON FUNCTION sql_saga.for_portion_of_trigger() TO PUBLIC;\n",
                        "GRANT EXECUTE ON FUNCTION sql_saga.current_view_trigger() TO PUBLIC;\n"]
# The exact 1.0 signatures of the routines the historical 1.0--1.1 grants touch (released sql_saga--1.0.sql).
ACL_GUARD_ROUTINES_10 = [
    "sql_saga.for_portion_of_trigger()",
    "sql_saga.current_view_trigger()",
    "sql_saga.temporal_merge(regclass,regclass,text[],sql_saga.temporal_merge_mode,name,name,name,boolean,text[],"
    "sql_saga.temporal_merge_delete_mode,boolean,name,name,name,name,text[],boolean)",
    "sql_saga.temporal_merge_plan(regclass,regclass,sql_saga.temporal_merge_mode,name,text[],name,name,"
    "sql_saga.temporal_merge_delete_mode,jsonb,text[],boolean,boolean)",
    "sql_saga.temporal_merge_plan_native(oid,oid,text,text,text[],text,text,text,jsonb,text[],boolean,boolean)",
    "sql_saga.temporal_merge_execute(regclass,regclass,text[],sql_saga.temporal_merge_mode,name,name,name,boolean,"
    "text[],sql_saga.temporal_merge_delete_mode,boolean,name,name,name,name,text[],boolean)",
    "sql_saga.temporal_merge_drop_temp_tables()",
    "sql_saga.temporal_merge_executor_introspect(oid,oid,text[],text[],text,text[],text,text)",
]


def acl_replay_do_10(src):  # authentic: the exact DO $grant_temporal_merge$ statement from frozen 1.0--1.2
    start = src.index("DO $grant_temporal_merge$\n")
    end = src.index("$grant_temporal_merge$;\n", start + 1) + len("$grant_temporal_merge$;\n")
    return src[start:end]


def acl_guard_10(out):
    """The single read-only guard statement the 1.0 copy inserts directly before the first historical grant."""
    first = ACL_REPLAY_GRANTS_10[0]
    i = out.index("\n" + first) + 1
    head = out[:i]
    start = head.rindex("\nDO $") + 1
    tag = head[start + 3:head.index("$", start + 4) + 1]
    return tag, head[start:]


class HistoricalAclGuard(unittest.TestCase):
    """1.0 path: released 1.0 records no pg_init_privs for the 8 routines, and a historical in-extension GRANT
    records the then-current ACL as their initial privileges, so pg_dump would drop any site grant. The 1.0
    copy therefore keeps the three historical grants and refuses, immediately before the first of them, unless
    all 8 routines carry exactly the canonical ACL (owner EXECUTE + PUBLIC EXECUTE, granted by the owner,
    without grant option). The refusal rolls the whole ALTER EXTENSION back."""
    def setUp(self):
        self.g = load()
        self.src10 = read("sql_saga--1.0--1.2.sql")
        self.src11 = read("sql_saga--1.1--1.2.sql")
        self.anchors = ACL_REPLAY_GRANTS_10 + [acl_replay_do_10(self.src10)]
        self.out = self.g.transform_historical(self.src10, "1.0--1.2")

    def test_frozen_anchors_once(self):  # authentic
        for a in self.anchors:
            self.assertEqual(self.src10.count(a), 1, a[:60])
            self.assertEqual(self.src11.count(a.split("\n")[0]), 0, a[:60])

    def test_10_keeps_all_three_historical_grants_verbatim(self):
        for a in self.anchors:
            self.assertEqual(self.out.count(a), 1, a[:60])
        self.assertEqual(self.g.reverse_historical(self.out, "1.0--1.2"), self.src10)

    def test_10_guard_directly_before_first_grant(self):
        tag, guard = acl_guard_10(self.out)
        self.assertTrue(guard.endswith(tag + ";\n"), guard[-80:])
        self.assertEqual(self.out.count("DO " + tag + "\n"), 1)
        self.assertNotIn(tag, self.src10)
        g = self.out.index(guard)
        for a in self.anchors:
            self.assertLess(g, self.out.index(a))
        self.assertEqual(self.out.index(guard) + len(guard), self.out.index(ACL_REPLAY_GRANTS_10[0]))

    def test_10_guard_checks_exact_eight_signatures(self):
        _, guard = acl_guard_10(self.out)
        flat = re.sub(r"\s+", "", guard)
        for sig in ACL_GUARD_ROUTINES_10:
            self.assertIn("'" + sig.replace(" ", "") + "'", flat, sig)
        # The ACL comparison targets exact regprocedure signatures. The only name-based selection is the
        # refusal of any extra routine the frozen DO $grant_temporal_merge$ loop would also reach.
        loop_names = re.search(r"p\.proname IN \(([^)]*)\)", acl_replay_do_10(self.src10)).group(1)
        names = sorted(re.findall(r"'(\w+)'", loop_names))
        self.assertEqual(len(names), 6)
        m = re.search(r"proname\s+IN\s*\(([^)]*)\)", guard)
        self.assertIsNotNone(m, "guard must refuse extra overloads of the historical loop names")
        self.assertEqual(sorted(re.findall(r"'(\w+)'", m.group(1))), names)
        self.assertEqual(len(re.findall(r"\bproname\b", guard)), 1)
        self.assertGreaterEqual(guard.count("RAISE EXCEPTION"), 2)  # non-canonical ACL and extra overload

    def test_10_guard_compares_full_normalized_acl(self):
        _, guard = acl_guard_10(self.out)
        self.assertIn("aclexplode", guard)
        self.assertIn("acldefault", guard)  # NULL proacl normalizes to the owner's default
        for col in ["grantor", "grantee", "privilege_type", "is_grantable"]:
            self.assertIn(col, guard, col)
        self.assertNotIn("has_function_privilege", guard)  # PUBLIC is not a role; read the ACL itself

    def test_10_guard_is_read_only_and_raises(self):
        _, guard = acl_guard_10(self.out)
        body = guard.upper()
        for forbidden in ["GRANT ", "REVOKE ", "CASCADE", "SET_CONFIG", "CURRENT_SETTING", "ALTER ", "CREATE ",
                          "INSERT ", "UPDATE ", "DELETE ", "PG_INIT_PRIVS"]:
            self.assertNotIn(forbidden, re.sub(r"'(?:[^']|'')*'", "''", body), forbidden)
        self.assertIn("RAISE EXCEPTION", body)
        self.assertIn("ERRCODE", body)
        self.assertIn("HINT", body)
        self.assertEqual(len(self.g.split_sql(guard)), 1)

    def test_10_guard_message_names_routine_and_owner_recipe(self):
        _, guard = acl_guard_10(self.out)
        self.assertRegex(guard, r"RAISE EXCEPTION '[^']*%")  # names the exact routine (and its ACL)
        hints = re.findall(r"HINT = '((?:[^']|'')*)'", guard)
        self.assertGreaterEqual(len(hints), 2)
        for h in hints:
            self.assertIn("ALTER EXTENSION sql_saga UPDATE TO ''1.3''", h)
            self.assertIn("doc/upgrade.md", h)
            self.assertNotIn("listed routines", h)  # each error reports one routine
        acl_hints = [h for h in hints if "routine owner and PUBLIC" in h]
        self.assertEqual(len(acl_hints), 1)  # the owner's own grant stays; nothing generic like CASCADE
        hint = acl_hints[0]
        self.assertIn("pg_dump", hint)  # explains why the site ACL cannot be carried through the update
        self.assertIn("outside the extension", hint)
        overload_hints = [h for h in hints if h is not hint]
        self.assertTrue(any("rename" in h for h in overload_hints))  # owner's choice, never automatic

    def test_10_only_guard_and_existing_retargets_differ(self):
        _, guard = acl_guard_10(self.out)
        out_wo_guard = self.out.replace(guard, "", 1)
        src_lines, out_lines = self.src10.split("\n")[1:], out_wo_guard.split("\n")
        self.assertEqual(len(src_lines), len(out_lines))
        allowed = [GATE, "<> '1.2'", "extension update version 1.2", "sql_saga 1.2 native library",
                   "UPDATE TO ''1.2'';"]
        for a, b in zip(src_lines, out_lines):
            if a != b:
                self.assertTrue(any(x in a for x in allowed), a)

    def test_10_retains_new_helper_acl(self):
        for line in self.src10.split("\n"):
            if re.match(r"^(GRANT|REVOKE) ", line):
                self.assertEqual(self.out.count("\n" + line + "\n"), self.src10.count("\n" + line + "\n"), line)

    def test_11_unchanged_by_acl_rule(self):
        out = self.g.transform_historical(self.src11, "1.1--1.2")
        self.assertEqual(len(out.split("\n")), len(self.src11.split("\n")) - 1)
        self.assertEqual(self.g.reverse_historical(out, "1.1--1.2"), self.src11)
        for sig in ACL_GUARD_ROUTINES_10[2:]:
            self.assertNotIn(sig, out)

    def test_acl_anchor_missing_or_duplicated_fails_closed(self):  # authentic input mutated
        for a in self.anchors:
            for mutated in [self.src10.replace(a, "", 1), self.src10.replace(a, a + a, 1)]:
                with self.assertRaises(self.g.GeneratorError, msg=a[:60]):
                    self.g.transform_historical(mutated, "1.0--1.2")

    def test_guard_text_already_present_fails_closed(self):  # authentic input mutated
        tag, guard = acl_guard_10(self.out)
        first = ACL_REPLAY_GRANTS_10[0]
        mutated = self.src10.replace(first, guard + first, 1)
        with self.assertRaises(self.g.GeneratorError):
            self.g.transform_historical(mutated, "1.0--1.2")


class AclAndSignatures(unittest.TestCase):
    def setUp(self):
        self.g = load()

    def test_manifest(self):
        self.assertEqual(sorted(self.g.ROUTINE_MANIFEST), sorted(
            "0c 0g 0i 0j 1d 1g 1h 1k 2a 2e 2f 2h 2l 2o 3c 3d 3e".split()))
        self.assertIn("0b", self.g.MANUAL)
        self.assertNotIn("3f", self.g.ROUTINE_MANIFEST)
        self.assertNotIn("3g", self.g.ROUTINE_MANIFEST)

    def frozen(self):
        return read("sql_saga--1.2.sql")

    def src(self, fname):
        with open(os.path.join(ROOT, "src", fname)) as f:
            return f.read()

    def test_acl_only_for_new_routines(self):
        acl = ("REVOKE ALL ON FUNCTION sql_saga.check_integrity() FROM PUBLIC;\n"
               "REVOKE ALL ON FUNCTION sql_saga.__internal_brand_new(name) FROM PUBLIC;\n"
               "GRANT EXECUTE ON FUNCTION sql_saga.for_portion_of_trigger() TO PUBLIC;\n"
               "GRANT EXECUTE ON FUNCTION sql_saga.__internal_brand_public() TO PUBLIC;\n")  # SYNTHETIC
        out = self.g.routine_acl_statements(self.g.split_sql(acl), self.frozen())
        self.assertEqual(len(out), 2)
        self.assertIn("__internal_brand_new", out[0])
        self.assertTrue(out[1].lstrip().startswith("GRANT EXECUTE ON FUNCTION sql_saga.__internal_brand_public()"))

    def test_real_3f_3g(self):  # authentic 3f/3g against frozen 1.2
        stmts = self.g.split_sql(self.src("3f_public_execute.sql") + self.src("3g_private_function_acl.sql"))
        out = self.g.routine_acl_statements(stmts, self.frozen())
        # every 3f/3g target already exists in 1.2, so no blanket replay at all
        self.assertEqual(out, [])

    def test_3f_do_block_targets_must_exist(self):
        # The 3f DO block is skipped only because every routine it names exists in frozen 1.2.
        stmts = self.g.split_sql(self.src("3f_public_execute.sql"))
        self.assertEqual(self.g.routine_acl_statements(stmts, self.frozen()), [])
        bad = self.src("3f_public_execute.sql").replace("'temporal_merge_executor_introspect'",
                                                       "'temporal_merge_executor_introspect', 'brand_new_public'")
        with self.assertRaises(self.g.GeneratorError):
            self.g.routine_acl_statements(self.g.split_sql(bad), self.frozen())
        mixed = "REVOKE ALL ON FUNCTION sql_saga.check_integrity(), sql_saga.__internal_brand_new(name) FROM PUBLIC;"
        with self.assertRaises(self.g.GeneratorError):  # mixed existing/new targets fail closed
            self.g.routine_acl_statements(self.g.split_sql(mixed), self.frozen())
        unknown = "DO $other$ BEGIN EXECUTE 'GRANT ALL ON FUNCTION sql_saga.f() TO PUBLIC'; END; $other$;"
        with self.assertRaises(self.g.GeneratorError):
            self.g.routine_acl_statements(self.g.split_sql(unknown), self.frozen())

    def test_new_routine_without_acl_decision_rejected(self):
        routines = self.g.split_sql("CREATE FUNCTION sql_saga.__internal_brand_new(n name) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;")
        with self.assertRaises(self.g.GeneratorError):
            self.g.new_routine_acl(routines, [], self.frozen())

    def test_new_routine_gets_explicit_acl(self):
        routines = self.g.split_sql("CREATE FUNCTION sql_saga.__internal_brand_new(n name) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;")
        acl = self.g.split_sql("REVOKE ALL ON FUNCTION sql_saga.__internal_brand_new(name) FROM PUBLIC;")
        self.assertEqual(len(self.g.new_routine_acl(routines, acl, self.frozen())), 1)

    def test_real_new_routines_have_acl_policy(self):
        """Policy: every routine new in 1.3 needs an explicit GRANT/REVOKE in src, or must be listed in
        DEFAULT_ACL_NEW_ROUTINES (then no ACL is emitted and creation-time defaults apply, like a fresh install).
        Source GRANT/REVOKE for new routines are emitted verbatim; nothing is invented (no blind PUBLIC grant).
        Real src: regular_fk_check_c() carries an explicit, authorized GRANT EXECUTE TO PUBLIC (restore needs it)."""
        routines, acl = [], []
        for code in self.g.ROUTINE_MANIFEST:
            for fname in os.listdir(os.path.join(ROOT, "src")):
                if fname.startswith(code + "_") and fname.endswith(".sql"):
                    for s in self.g.split_sql(self.src(fname)):
                        k = self.g.classify(s)
                        (routines if k == "routine" else acl if k in ("grant", "revoke") else []).append(s)
        out = self.g.new_routine_acl(routines, acl, self.frozen())
        joined = "".join(out)
        self.assertIn("GRANT EXECUTE ON FUNCTION sql_saga.regular_fk_check_c() TO PUBLIC;", joined)
        self.assertEqual(joined.count("TO PUBLIC"), sum(1 for s in acl if "TO PUBLIC" in s.text
                                                        and self.g.routine_acl_statements([s], self.frozen())))
        self.assertIn("REVOKE ALL ON FUNCTION sql_saga.__internal_foreign_key_mispairing_report(name) FROM PUBLIC", joined)
        # without its source GRANT and not listed, a new routine is refused
        no_grant = [s for s in acl if "regular_fk_check_c" not in s.text]
        with self.assertRaises(self.g.GeneratorError):
            self.g.new_routine_acl(routines, no_grant, self.frozen(), default_acl=set())

    def test_changed_return_fails_fast(self):
        changed = self.g.split_sql("CREATE FUNCTION sql_saga.check_integrity() RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;")
        with self.assertRaises(self.g.GeneratorError):
            self.g.check_signatures(changed, self.frozen(), approved_drops=[])
        drop = self.g.split_sql("DROP FUNCTION sql_saga.check_integrity();")
        self.g.check_signatures(changed, self.frozen(), approved_drops=drop)

    def test_changed_argument_list_fails_fast(self):
        # same name as an existing routine, different args: would leave a stale overload behind
        drift = self.g.split_sql("CREATE PROCEDURE sql_saga.manage_temporal_fk_triggers(p_action text, VARIADIC p_table_oids regclass[]) LANGUAGE sql AS $$ $$;")
        with self.assertRaises(self.g.GeneratorError):
            self.g.check_signatures(drift, self.frozen(), approved_drops=[])
        renamed = self.g.split_sql("CREATE PROCEDURE sql_saga.manage_temporal_fk_triggers(action sql_saga.trigger_action, VARIADIC p_table_oids regclass[]) LANGUAGE sql AS $$ $$;")
        with self.assertRaises(self.g.GeneratorError):  # input argument rename is refused by CREATE OR REPLACE
            self.g.check_signatures(renamed, self.frozen(), approved_drops=[])
        # approving an unrelated DROP does not help
        other = self.g.split_sql("DROP PROCEDURE sql_saga.disable_temporal_triggers(VARIADIC regclass[]);")
        with self.assertRaises(self.g.GeneratorError):
            self.g.check_signatures(drift, self.frozen(), approved_drops=other)
        exact = self.g.split_sql("DROP PROCEDURE sql_saga.manage_temporal_fk_triggers(sql_saga.trigger_action, VARIADIC regclass[]);")
        self.g.check_signatures(drift, self.frozen(), approved_drops=exact)

    def test_unchanged_existing_signature_accepted(self):  # control
        same = self.g.split_sql("CREATE OR REPLACE PROCEDURE sql_saga.manage_temporal_fk_triggers(p_action sql_saga.trigger_action, VARIADIC p_table_oids regclass[]) LANGUAGE sql AS $$ $$;")
        self.g.check_signatures(same, self.frozen(), approved_drops=[])

    def test_removed_default_or_out_column_change_fails(self):
        # frozen 1.2: __internal_make_name(resizable text[], fixed text DEFAULT NULL, separator text DEFAULT '_', extra integer DEFAULT 2)
        nodefault = self.g.split_sql("CREATE FUNCTION sql_saga.__internal_make_name(resizable text[], fixed text, separator text, extra integer) RETURNS name LANGUAGE sql AS $$ SELECT NULL::name $$;")
        with self.assertRaises(self.g.GeneratorError):
            self.g.check_signatures(nodefault, self.frozen(), approved_drops=[])
        with open(os.path.join(ROOT, "src", "2h_health_checks.sql")) as f:
            real = [s for s in self.g.split_sql(f.read())
                    if "FUNCTION sql_saga.__internal_system_versioning_member_attribution(" in s.text][0]
        renamed_col = self.g.split_sql(real.text.replace("as_of_attributable boolean", "as_of_ok boolean", 1))
        with self.assertRaises(self.g.GeneratorError):
            self.g.check_signatures(renamed_col, self.frozen(), approved_drops=[])

    def test_real_manifest_signatures_pass(self):  # authentic: current src vs frozen 1.2
        routines = []
        for code in self.g.ROUTINE_MANIFEST:
            for fname in sorted(os.listdir(os.path.join(ROOT, "src"))):
                if fname.startswith(code + "_") and fname.endswith(".sql"):
                    routines += [s for s in self.g.split_sql(self.src(fname)) if self.g.classify(s) == "routine"]
        self.g.check_signatures(routines, self.frozen(), approved_drops=[])

    def test_only_allowed_new_table_emitted(self):
        self.assertEqual(set(self.g.NEW_TABLES), {"sql_saga.__internal_updatable_view_pending"})
        with self.assertRaises(self.g.GeneratorError):
            self.g.new_tables(self.g.split_sql("CREATE TABLE sql_saga.era (x int);"), self.frozen())
        with self.assertRaises(self.g.GeneratorError):  # new but unrelated
            self.g.new_tables(self.g.split_sql("CREATE TABLE sql_saga.__unrelated (x int);"), self.frozen())
        stmts = [s for s in self.g.split_sql(self.src("2o_updatable_view_column_follow.sql"))
                 if "__internal_updatable_view_pending" in s.text and self.g.classify(s) in ("create_table", "revoke", "comment")
                 and "FUNCTION" not in s.text.split("(")[0]]
        new = self.g.new_tables(stmts, self.frozen())  # authentic CREATE TABLE + REVOKE ON TABLE + COMMENT ON TABLE
        self.assertEqual(len(new), 3)
        self.assertTrue(new[0].lstrip().startswith("CREATE TABLE") or "\nCREATE TABLE" in new[0])


class Assembly(unittest.TestCase):
    def setUp(self):
        self.g = load()
        self.kw = dict(preamble="-- P\n" + P_1K_MARKER + "\n", schema="-- S\n", pending_table="-- T\n",
                       routines="-- R\n", repairs="-- X\n", bootstrap_1k="-- K\n")

    def test_order_and_single_gate(self):
        for frm in ["1.0", "1.1", "1.2"]:
            hist = None if frm == "1.2" else "-- H" + frm + "\n"
            out = self.g.assemble(frm, "1.3", historical=hist, **self.kw)
            self.assertTrue(out.startswith("\\echo Use \"ALTER EXTENSION sql_saga UPDATE TO '1.3'\""))
            idx = [out.index(m) for m in ["-- P", "-- K", "-- S", "-- T", "-- R", "-- X"]]
            if hist:
                idx.insert(2, out.index("-- H" + frm))
            self.assertEqual(idx, sorted(idx))
            self.assertNotIn(P_1K_MARKER, out)
            self.assertEqual(out.count(GATE), 1)
            self.assertTrue(out.rstrip().endswith(GATE))

    def test_trusted_source_version_set_before_preamble(self):
        # PG18 sets extversion to the TARGET before running the script, so fragments cannot read the source
        # from the catalog. The generator knows it and emits it as a transaction-local literal right after H.
        for frm in ["1.0", "1.1", "1.2"]:
            hist = None if frm == "1.2" else "-- H\n"
            out = self.g.assemble(frm, "1.3", historical=hist, **self.kw)
            line = "SELECT pg_catalog.set_config('sql_saga.upgrade_from', '%s', true);" % frm
            self.assertEqual(out.split("\n")[1], line)
            self.assertEqual(out.count("sql_saga.upgrade_from"), 1)
            self.assertLess(out.index(line), out.index("-- P"))
        with self.assertRaises(self.g.GeneratorError):
            self.g.assemble("0.9", "1.3", historical="-- H\n", **self.kw)

    def test_marker_required_exactly_once(self):
        for p in ["-- P\n", "-- P\n" + P_1K_MARKER + "\n" + P_1K_MARKER + "\n"]:
            with self.assertRaises(self.g.GeneratorError):
                self.g.assemble("1.2", "1.3", historical=None, **dict(self.kw, preamble=p))

    def test_fragment_gate_rejected_in_every_slot(self):
        for slot in ["preamble", "bootstrap_1k", "schema", "pending_table", "routines", "repairs"]:
            kw = dict(self.kw)
            kw[slot] = kw[slot] + GATE + "\n"
            with self.assertRaises(self.g.GeneratorError, msg=slot):
                self.g.assemble("1.2", "1.3", historical=None, **kw)
        with self.assertRaises(self.g.GeneratorError):
            self.g.assemble("1.1", "1.3", historical="-- H\n" + GATE + "\n", **self.kw)


class EndToEnd(unittest.TestCase):
    def setUp(self):
        self.g = load()

    def test_current_view_trigger_replaced_on_every_direct_path(self):
        # Binding correctness is shared by rebuilt view rules and current-view DML.
        # Every direct update must install the actual source trigger, including 1.2
        # where there is no historical update block to supply a replacement.
        with open(os.path.join(ROOT, "src", "3c_current_view_trigger.sql")) as f:
            source = f.read()
        routines = [s for s in self.g.split_sql(source)
                    if self.g.classify(s) == "routine"
                    and self.g.routine_signature(s) == ("sql_saga.current_view_trigger", (), "trigger")]
        self.assertEqual(len(routines), 1)
        replacement = self.g.to_create_or_replace(routines[0])
        scripts = self.g.build(os.path.join(ROOT, "src"), VERSIONS,
                               os.path.join(ROOT, "devops"), "1.3", self.g.ROUTINE_MANIFEST)
        for name, script in scripts.items():
            with self.subTest(script=name):
                self.assertTrue(replacement in script,
                                name + " does not install the source current_view_trigger()")

    def make_tree(self, d, preamble=None):
        src = os.path.join(d, "src")
        dev = os.path.join(d, "devops")
        os.makedirs(src)
        os.makedirs(dev)
        with open(os.path.join(src, "1k_foreign_key_pairing.sql"), "w") as f:  # SYNTHETIC
            f.write("CREATE FUNCTION sql_saga.__internal_brand_new(n name) RETURNS int LANGUAGE sql AS $$ SELECT 1 $$;\n"
                    "REVOKE ALL ON FUNCTION sql_saga.__internal_brand_new(name) FROM PUBLIC;\n")
        with open(os.path.join(src, "2o_updatable_view_column_follow.sql"), "w") as f:
            f.write("CREATE TABLE sql_saga.__internal_updatable_view_pending (x int);\n")
        for n, body in [("preamble", preamble or P_1K_MARKER + "\n"), ("schema", "-- S\n"), ("repairs", "-- X\n")]:
            with open(os.path.join(dev, "upgrade_1_3_%s.sql" % n), "w") as f:
                f.write(body)
        return src, dev

    def test_main_writes_three_scripts_atomically(self):
        with tempfile.TemporaryDirectory() as d:
            src, dev = self.make_tree(d)
            out = os.path.join(d, "out")
            os.makedirs(out)
            self.g.main(["--src", src, "--versions", VERSIONS, "--devops", dev, "--outdir", out,
                         "--extversion", "1.3", "--manifest", "1k,2o"])
            names = sorted(os.listdir(out))
            self.assertEqual(names, ["sql_saga--1.0--1.3.sql", "sql_saga--1.1--1.3.sql", "sql_saga--1.2--1.3.sql"])
            for n in names:
                with open(os.path.join(out, n)) as f:
                    self.assertEqual(f.read().count(GATE), 1)

    def test_main_failure_writes_nothing(self):
        with tempfile.TemporaryDirectory() as d:
            src, dev = self.make_tree(d, preamble="-- no marker\n")
            out = os.path.join(d, "out")
            os.makedirs(out)
            with self.assertRaises(self.g.GeneratorError):
                self.g.main(["--src", src, "--versions", VERSIONS, "--devops", dev, "--outdir", out,
                             "--extversion", "1.3", "--manifest", "1k,2o"])
            self.assertEqual(os.listdir(out), [])

    def test_late_failure_preserves_existing_outputs(self):
        # Failure discovered while building the LAST script (1.2--1.3 schema gate) must not publish any script.
        with tempfile.TemporaryDirectory() as d:
            src, dev = self.make_tree(d)
            with open(os.path.join(dev, "upgrade_1_3_repairs.sql"), "w") as f:
                f.write("-- X\n" + GATE + "\n")
            out = os.path.join(d, "out")
            os.makedirs(out)
            names = ["sql_saga--1.0--1.3.sql", "sql_saga--1.1--1.3.sql", "sql_saga--1.2--1.3.sql"]
            for n in names:
                with open(os.path.join(out, n), "w") as f:
                    f.write("SENTINEL " + n)
            with self.assertRaises(self.g.GeneratorError):
                self.g.main(["--src", src, "--versions", VERSIONS, "--devops", dev, "--outdir", out,
                             "--extversion", "1.3", "--manifest", "1k,2o"])
            self.assertEqual(sorted(os.listdir(out)), names)  # no temp leftovers
            for n in names:
                with open(os.path.join(out, n)) as f:
                    self.assertEqual(f.read(), "SENTINEL " + n)

    def test_frozen_files_untouched(self):
        for name, sha in [("sql_saga--1.0--1.2.sql", SHA_10_12), ("sql_saga--1.1--1.2.sql", SHA_11_12),
                          ("sql_saga--1.2.sql", SHA_12)]:
            with open(os.path.join(VERSIONS, name), "rb") as f:
                self.assertEqual(hashlib.sha256(f.read()).hexdigest(), sha)




class RealManifest(unittest.TestCase):
    """Real src manifest + SYNTHETIC fox-compatible fragments."""
    def setUp(self):
        self.g = load()
        self.tmp = tempfile.TemporaryDirectory()
        dev = os.path.join(self.tmp.name, "devops")
        os.makedirs(dev)
        frags = {"preamble": "-- SYNTHETIC P\n" + P_1K_MARKER + "\n",
                 "schema": "-- SYNTHETIC S\n", "repairs": "-- SYNTHETIC X\n"}
        for n, body in frags.items():
            with open(os.path.join(dev, "upgrade_1_3_%s.sql" % n), "w") as f:
                f.write(body)
        self.out = os.path.join(self.tmp.name, "out")
        os.makedirs(self.out)
        self.g.main(["--src", os.path.join(ROOT, "src"), "--versions", VERSIONS, "--devops", dev,
                     "--outdir", self.out, "--extversion", "1.3"])
        with open(os.path.join(self.out, "sql_saga--1.2--1.3.sql")) as f:
            self.s12 = f.read()

    def tearDown(self):
        self.tmp.cleanup()

    def test_three_scripts_single_gate(self):
        self.assertEqual(sorted(os.listdir(self.out)),
                         ["sql_saga--1.0--1.3.sql", "sql_saga--1.1--1.3.sql", "sql_saga--1.2--1.3.sql"])
        for n in os.listdir(self.out):
            with open(os.path.join(self.out, n)) as f:
                self.assertEqual(f.read().count("\n" + GATE), 1, n)

    def test_real_routines_included_bodies_verbatim(self):
        for code in self.g.ROUTINE_MANIFEST:
            for fname in os.listdir(os.path.join(ROOT, "src")):
                if fname.startswith(code + "_") and fname.endswith(".sql"):
                    with open(os.path.join(ROOT, "src", fname)) as f:
                        for s in self.g.split_sql(f.read()):
                            if self.g.classify(s) == "routine":
                                self.assertIn(self.g.to_create_or_replace(s), self.s12, (fname, s.line))
        self.assertIn("CREATE OR REPLACE FUNCTION sql_saga.regular_fk_check_c()", self.s12)
        self.assertIn("VARIADIC p_table_oids regclass[]", self.s12)
        # nested template inside 3e body preserved byte for byte, not rewritten
        self.assertIn("specialized_trigger_template := $template$\nCREATE OR REPLACE FUNCTION sql_saga.__FUNC_NAME__()\n", self.s12)

    def test_no_unrewritten_top_level_create_function(self):
        for s in self.g.split_sql(self.s12.split("\n", 1)[1]):
            self.assertNotRegex(s.text.lstrip(), r"^(--[^\n]*\n\s*|/\*.*?\*/\s*)*CREATE (FUNCTION|PROCEDURE)")

    def test_pending_table_before_composite_routine(self):
        t = self.s12.index("CREATE TABLE sql_saga.__internal_updatable_view_pending")
        r = self.s12.index("FUNCTION sql_saga.__internal_updatable_view_restore(pending sql_saga.__internal_updatable_view_pending)")
        self.assertLess(self.s12.index("-- SYNTHETIC S"), t)
        self.assertLess(t, r)
        self.assertEqual(self.s12.count("CREATE TABLE sql_saga.__internal_updatable_view_pending"), 1)
        self.assertIn("REVOKE ALL ON TABLE sql_saga.__internal_updatable_view_pending FROM PUBLIC;", self.s12)
        self.assertIn("COMMENT ON TABLE sql_saga.__internal_updatable_view_pending IS", self.s12)

    def test_1k_bootstrapped_in_preamble(self):
        p = self.s12.index("-- SYNTHETIC P")
        k = self.s12.index("CREATE OR REPLACE FUNCTION sql_saga.__internal_foreign_key_mispairing_report(")
        self.assertLess(p, k)
        self.assertLess(k, self.s12.index("-- SYNTHETIC S"))
        # routine section keeps 1k too: identical CREATE OR REPLACE text, emitted twice
        s = self.s12.index("-- SYNTHETIC S")
        k2 = self.s12.index("CREATE OR REPLACE FUNCTION sql_saga.__internal_foreign_key_mispairing_report(", s)
        self.assertLess(k2, self.s12.index("-- SYNTHETIC X"))
        with open(os.path.join(ROOT, "src", "1k_foreign_key_pairing.sql")) as f:
            for st in self.g.split_sql(f.read()):
                if self.g.classify(st) == "routine":
                    self.assertEqual(self.s12.count(self.g.to_create_or_replace(st)), 2, st.line)
        for psql in ["\n\\echo", "\n\\quit", "\n\\i "]:
            self.assertNotIn(psql, self.s12.split("\n", 1)[1])

    def test_no_blanket_acl_replay(self):
        self.assertNotIn("$grant_temporal_merge$", self.s12)
        self.assertNotIn("GRANT EXECUTE ON FUNCTION sql_saga.for_portion_of_trigger() TO PUBLIC", self.s12)
        self.assertNotIn("REVOKE ALL ON FUNCTION sql_saga.check_integrity() FROM PUBLIC", self.s12)


if __name__ == "__main__":
    unittest.main()
