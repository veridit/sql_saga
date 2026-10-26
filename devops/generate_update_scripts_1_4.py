#!/usr/bin/env python3
"""Generate the sql_saga 1.4 extension update scripts (1.0/1.1/1.2/1.3 -> 1.4). Standard library only.

Specified by devops/test_generate_update_scripts_1_4.py. Reuses the statement splitter, classifier and signature
parser of devops/generate_update_scripts.py (the 1.3 generator).

Each script X--1.4 is:
  header
  for X in 1.0, 1.1, 1.2: the frozen, SHA-256 authenticated X--1.3 update without its header and without its final
      integrity gate, with its 1.3 native-library guard, advisory lock, and update hints retargeted to 1.4. Every
      line of it that mentions 1.3 is either such an anchor or listed as cosmetic; anything else fails the build.
      The embedded update is exactly reversible to the frozen file (checked).
  the 1.4 delta, the same for every source:
      the 1.4 preamble (source check on the 1.3 catalog layout, 1.4 native guard, lock),
      the 1.4 schema fragment,
      every routine of the 1.4 manifest as CREATE OR REPLACE (new routines are created, existing ones replaced;
      a public management function that became a C entry point keeps its OID, privileges and dependants),
      the privilege statements for routines new since 1.3 and the REVOKEs on existing routines of 3g,
      the 1.4 repairs fragment,
  exactly one final integrity gate.
Frozen inputs are never written. Anything not understood fails the build.
"""
import argparse
import hashlib
import importlib.util
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("generate_update_scripts", os.path.join(HERE, "generate_update_scripts.py"))
g13 = importlib.util.module_from_spec(_spec)
sys.modules.setdefault("generate_update_scripts", g13)
_spec.loader.exec_module(g13)
GeneratorError = g13.GeneratorError

TARGET = "1.4"
SOURCES = ("1.0", "1.1", "1.2", "1.3")
FROZEN_SHA256 = {
    "sql_saga--1.3.sql": "c59b9e3f501217dc8364fe8f57da44cfa39e0825a7571bff12082bd5624cfd52",
    "sql_saga--1.0--1.3.sql": "9646114bc85feabc9a03314da1a8298ab8c3607deb5bdc43eb27843ae1f002f1",
    "sql_saga--1.1--1.3.sql": "3b28f9bb314a75f3bae0cf363026e8e23fb1f5184e2aa8811dacb35fd67cd7d6",
    "sql_saga--1.2--1.3.sql": "d670f8e21f8a9b68afb7fbd3fb378eeb998419fef375268f7797f1cd7aae6aee",
}
# src files with routines changed or added since v1.3 (`git diff --name-only v1.3 -- src`), in src order.
ROUTINE_MANIFEST = ("0c", "0e", "0h", "0i", "0j", "0k", "1a", "1b", "1c", "1d", "1e", "1f", "1g", "1h", "1i", "1j",
                    "2a", "2b", "2c", "2d", "2e", "2f", "2h", "2i", "2l", "2o", "3a", "3b", "3d", "3e")
# Files whose top-level statements are privilege statements (3f, 3g) or the trampoline file (3h: routines, COMMENT
# and one DO block that copies the workers' comments).
ACL_SOURCES = ("3f", "3g")
TRAMPOLINES = "3h"
# Routines new since 1.3 that keep creation-time default privileges (no GRANT/REVOKE in src). Key: name(types).
# The public management entry points are not new (same signature as in 1.3): CREATE OR REPLACE keeps their ACL.
DEFAULT_ACL_NEW_ROUTINES = frozenset()
# Routines that existed in 1.3 and lose PUBLIC EXECUTE in 1.4 on every installation (review round 2, R1: SECURITY
# DEFINER internals that acted for any caller). Key: name(types) as g13._key prints it.
DELIBERATE_REVOKES_14 = frozenset({
    "sql_saga.__internal_add_system_time_era(regclass, name, name, name, name, name, name[])",
    "sql_saga.__internal_ddl_command_affects_managed_object()",
    "sql_saga.__internal_reconcile_system_versioning_family(regclass, name[])",
    "sql_saga.__internal_sync_history_policies(oid, name[])",
})
GATE = "SELECT sql_saga.check_integrity();"
FRAGMENTS = ("preamble", "schema", "repairs")

_HEADER_13 = '\\echo Use "ALTER EXTENSION sql_saga UPDATE TO \'1.3\'" to load this file. \\quit'


def header(version):
    return '\\echo Use "ALTER EXTENSION sql_saga UPDATE TO \'%s\'" to load this file. \\quit' % version


def authenticate(path, expected_sha256):
    with open(path, "rb") as f:
        data = f.read()
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected_sha256:
        raise GeneratorError("frozen artifact %s failed SHA-256 authentication: expected %s, got %s"
                             % (os.path.basename(path), expected_sha256, actual))
    return data.decode("utf-8")


# ----------------------------------------------------------------------------------- embedded X--1.3 updates
# Whole-line anchors retargeted from 1.3 to 1.4; each must occur exactly as often as listed per source.
_GUARD_BLOCK_13 = "\n".join([
    "    IF sql_saga.native_library_version() <> '1.3' THEN",
    "        RAISE EXCEPTION 'sql_saga: native library version % does not match extension update version 1.3', sql_saga.native_library_version()",
    "            USING ERRCODE = 'object_not_in_prerequisite_state',",
    "                  HINT = 'Install the sql_saga 1.3 native library and restart PostgreSQL before retrying ALTER EXTENSION.';",
])
_GUARD_LINE_13 = ("DO $native_guard$ BEGIN IF sql_saga.native_library_version() <> '1.3' THEN RAISE EXCEPTION 'sql_saga: "
                  "native library version % does not match extension update version 1.3', sql_saga.native_library_version() "
                  "USING ERRCODE = 'object_not_in_prerequisite_state', HINT = 'Install the sql_saga 1.3 native library and "
                  "restart PostgreSQL before retrying ALTER EXTENSION.'; END IF; END; $native_guard$;")
_HINT_13 = "                      'ALTER EXTENSION sql_saga UPDATE TO ''1.3'';',"
_ACL_HINT_A_13 = ("                  HINT = 'The update grants EXECUTE to PUBLIC on every routine in schema sql_saga with "
                  "this name and records it as the extension''s initial privileges. If you choose to, rename or move this "
                  "routine, run ALTER EXTENSION sql_saga UPDATE TO ''1.3'', then rename or move it back. doc/upgrade.md has "
                  "examples.';")
_ACL_HINT_B_13 = ("                  HINT = 'The update would record the current privileges as the extension''s initial "
                  "privileges, so pg_dump would no longer dump them. Check each of the eight routines listed in doc/upgrade.md. "
                  "In one transaction, as the routine owner: note the exact current grants, change EXECUTE so that only the "
                  "routine owner and PUBLIC hold it (without grant option), run ALTER EXTENSION sql_saga UPDATE TO ''1.3'', "
                  "then reapply the noted site changes outside the extension in the same transaction. doc/upgrade.md has "
                  "examples.';")
_LOCK_13 = "SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('sql_saga 1.3 upgrade'));"


def _retarget(s):
    return s.replace("'1.3'", "'1.4'").replace("version 1.3'", "version 1.4'").replace("version 1.3',", "version 1.4',") \
            .replace("sql_saga 1.3 native", "sql_saga 1.4 native").replace("''1.3''", "''1.4''") \
            .replace("'sql_saga 1.3 upgrade'", "'sql_saga 1.4 upgrade'")


# (old, count) per source. The embedded 1.3 update keeps its own 1.3 catalog content (catalog_format '1.3',
# routines of 1.3); the 1.4 delta that follows brings everything to 1.4.
_ANCHORS = {
    "1.0": [(_GUARD_BLOCK_13, 2), (_GUARD_LINE_13, 1), (_HINT_13, 1), (_ACL_HINT_A_13, 1), (_ACL_HINT_B_13, 1),
            (_LOCK_13, 1)],
    "1.1": [(_GUARD_BLOCK_13, 2), (_HINT_13, 1), (_LOCK_13, 1)],
    "1.2": [(_GUARD_BLOCK_13, 1), (_LOCK_13, 1)],
}
_HAS_13 = re.compile(r"(?<![0-9.])1\.3(?![0-9])")


def _count_lines(body, block):
    return len(re.findall("(?=\n" + re.escape(block) + "\n)", "\n" + body + "\n"))


def _replace_lines(body, old, new):
    return ("\n" + body + "\n").replace("\n" + old + "\n", "\n" + new + "\n")[1:-1]


def transform_embedded(text, source):
    """The frozen X--1.3 update, retargeted to 1.4, without header and without its final gate."""
    if source not in _ANCHORS:
        raise GeneratorError("unknown embedded source %r" % source)
    first, sep, body = text.partition("\n")
    if first != _HEADER_13:
        raise GeneratorError("unexpected header in %s--1.3: %r" % (source, first))
    if not body.endswith(GATE + "\n"):
        raise GeneratorError("%s--1.3 does not end with exactly the integrity gate" % source)
    body = body[:-len(GATE + "\n")]
    if _count_lines(body, GATE) != 0:
        raise GeneratorError("%s--1.3 has more than one integrity gate" % source)
    anchor_lines = set()
    for old, n in _ANCHORS[source]:
        c = _count_lines(body, old)
        if c != n:
            raise GeneratorError("expected %d anchor(s) in %s--1.3, found %d: %s" % (n, source, c, old.split("\n")[0][:80]))
        anchor_lines.update(old.split("\n"))
    out = body
    for old, n in _ANCHORS[source]:
        out = _replace_lines(out, old, _retarget(old))
    for no, line in enumerate(out.split("\n"), 2):
        if line in anchor_lines and _HAS_13.search(line):
            raise GeneratorError("anchor left unretargeted in %s--1.3 line %d" % (source, no))
    if reverse_embedded(out, source) != text:
        raise GeneratorError("embedded transformation of %s--1.3 is not exactly reversible" % source)
    return out


def reverse_embedded(out, source):
    body = out
    for old, n in _ANCHORS[source]:
        new = _retarget(old)
        if _count_lines(body, new) != n:
            raise GeneratorError("expected %d transformed anchor(s) in %s" % (n, source))
        body = _replace_lines(body, new, old)
    return _HEADER_13 + "\n" + body + GATE + "\n"


# ----------------------------------------------------------------------------------- the 1.4 delta from src
def _src_file(src_dir, code):
    names = sorted(f for f in os.listdir(src_dir) if f.startswith(code + "_") and f.endswith(".sql"))
    if len(names) != 1:
        raise GeneratorError("expected exactly one src file for %s, found %s" % (code, names))
    return os.path.join(src_dir, names[0])


def _read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def _frozen_routine_stmts(frozen13):
    """The routine definitions of the frozen 1.3 install script (its header is a psql line, not SQL)."""
    body = frozen13.split("\n", 1)[1] if frozen13.startswith("\\echo") else frozen13
    return [s for s in g13.split_sql(body) if g13._ROUTINE_RE.match(g13._core(s))]


def _frozen_identities(frozen13):
    return set(g13._identity(r["name"], r["args"]) for r in (g13._parse_routine(s) for s in _frozen_routine_stmts(frozen13)))


def _acl_targets(stmt):
    m = g13._ACL_RE.match(g13._core(stmt))
    if not m:
        raise GeneratorError("unsupported privilege statement at line %d" % stmt.line)
    return g13._parse_target_list(m.group(3))


def delta(src_dir, devops_dir, frozen13):
    """The 1.4 delta: routines (manifest + trampolines), privileges, with the schema and repairs fragments."""
    existing = _frozen_identities(frozen13)
    frozen_signatures = {}
    for s in _frozen_routine_stmts(frozen13):
        name, args, result = g13.routine_signature(s)
        r = g13._parse_routine(s)
        frozen_signatures[g13._identity(r["name"], r["args"])] = (g13._canon_result(result), r)

    # Statements of a manifest file that are not routines, comments or privileges must exist in the frozen 1.3
    # install script byte for byte (tables, indexes, rows, triggers that 1.3 already created): they are skipped.
    # Any other non-routine statement fails the build; catalog changes belong in the schema fragment.
    frozen_body = frozen13.split("\n", 1)[1] if frozen13.startswith("\\echo") else frozen13
    frozen_stmts = set(g13._core(s) for s in g13.split_sql(frozen_body))

    routine_chunks, new_idents, all_idents = [], set(), set()
    for code in ROUTINE_MANIFEST + (TRAMPOLINES,):
        chunk = []
        for s in g13.split_sql(_read(_src_file(src_dir, code))):
            try:
                k = g13.classify(s)
            except GeneratorError:
                k = "other"
            if k in ("create_table", "other", "select", "comment", "grant", "revoke") and g13._core(s) in frozen_stmts \
                    and not (k in ("grant", "revoke") and g13._is_routine_acl(s)):
                continue  # a table, index, row, trigger, table grant or table comment exactly as in 1.3
            if k in ("grant", "revoke") and not g13._is_routine_acl(s):
                raise GeneratorError("privilege statement on a non-routine at line %d of src %s differs from 1.3" % (s.line, code))
            if k == "routine":
                r = g13._parse_routine(s)
                ident = g13._identity(r["name"], r["args"])
                if ident in all_idents:
                    raise GeneratorError("routine %s is defined twice in the manifest" % g13._key(ident))
                all_idents.add(ident)
                if ident in existing:
                    old_result, old_r = frozen_signatures[ident]
                    if old_r["kind"] != r["kind"]:
                        raise GeneratorError("%s changes from %s to %s; CREATE OR REPLACE cannot" % (g13._key(ident), old_r["kind"], r["kind"]))
                    new_result = g13._canon_result(g13.routine_signature(s)[2])
                    if old_result != new_result:
                        raise GeneratorError("%s changes its result type; CREATE OR REPLACE cannot" % g13._key(ident))
                    old_defaults = [a[3] for a in old_r["args"] if a[0] != "out"]
                    new_defaults = [a[3] for a in r["args"] if a[0] != "out"]
                    if old_defaults != new_defaults or [a[1] for a in old_r["args"]] != [a[1] for a in r["args"]]:
                        raise GeneratorError("%s changes argument names or defaults; CREATE OR REPLACE cannot" % g13._key(ident))
                else:
                    new_idents.add(ident)
                chunk.append(g13.to_create_or_replace(s))
            elif k == "comment":
                chunk.append(s.text)
            elif k in ("grant", "revoke"):
                targets = _acl_targets(s)
                for t in targets:
                    if t not in existing and t not in all_idents:
                        raise GeneratorError("privilege statement at line %d before or without its routine" % s.line)
                # Replayed only for routines new since 1.3 (CREATE OR REPLACE keeps the ACL of the others, site
                # changes included). A statement mixing both kinds is refused.
                old_targets = [t for t in targets if t in existing]
                if not old_targets:
                    chunk.append(s.text)
                elif len(old_targets) != len(targets):
                    raise GeneratorError("privilege statement at line %d of src %s mixes 1.3 and new routines" % (s.line, code))
            elif k == "do" and code == TRAMPOLINES and "$copy_api_comments$" in s.text:
                chunk.append(s.text)
            elif k == "create_table":
                raise GeneratorError("new table in src %s at line %d: catalog changes belong in the schema fragment" % (code, s.line))
            else:
                raise GeneratorError("unsupported %s statement in src %s at line %d" % (k, code, s.line))
        text = "".join(chunk)
        routine_chunks.append(text if text.endswith("\n") else text + "\n")

    # Privilege statements of 3f/3g. CREATE OR REPLACE keeps the privileges of a routine that exists in 1.3, also a
    # site's own GRANT or REVOKE, so a statement about such a routine is NOT replayed. The exceptions are the
    # deliberate 1.4 REVOKEs from PUBLIC on routines that existed in 1.3 (DELIBERATE_REVOKES_14, review round 2,
    # R1): those must change every installation. Statements about routines new since 1.3 are emitted.
    acl_chunks, covered = [], set()
    for code in ACL_SOURCES:
        for s in g13.split_sql(_read(_src_file(src_dir, code))):
            k = g13.classify(s)
            if k in ("grant", "revoke"):
                targets = _acl_targets(s)
                if any(t not in existing and t not in all_idents for t in targets):
                    raise GeneratorError("privilege statement at line %d of %s names an unknown routine" % (s.line, code))
                covered.update(targets)
                old_targets = [t for t in targets if t in existing]
                if not old_targets:
                    acl_chunks.append(s.text)
                elif k == "revoke" and all(g13._key(t) in DELIBERATE_REVOKES_14 for t in targets) \
                        and re.search(r"\bFROM\s+PUBLIC\s*;\s*$", g13._core(s), re.I):
                    acl_chunks.append(s.text)
                elif len(old_targets) != len(targets):
                    raise GeneratorError("privilege statement at line %d of %s mixes 1.3 and new routines" % (s.line, code))
            elif k == "do" and "$grant_temporal_merge$" in s.text:
                continue  # every routine it names exists in 1.3 with these grants: CREATE OR REPLACE keeps them
            else:
                raise GeneratorError("unsupported statement in privilege source %s at line %d" % (code, s.line))
    missing_revokes = sorted(DELIBERATE_REVOKES_14 - set(g13._key(t) for t in covered))
    if missing_revokes:
        raise GeneratorError("deliberate 1.4 REVOKEs not found in 3f/3g: %s" % ", ".join(missing_revokes))
    for s in sum((g13.split_sql(_read(_src_file(src_dir, c))) for c in ROUTINE_MANIFEST + (TRAMPOLINES,)), []):
        if g13._ACL_RE.match(g13._core(s)):
            covered.update(_acl_targets(s))
    missing = sorted(g13._key(i) for i in new_idents if i not in covered and g13._key(i) not in DEFAULT_ACL_NEW_ROUTINES)
    if missing:
        raise GeneratorError("routines new since 1.3 without a privilege decision in src: %s" % ", ".join(missing))

    frags = dict((n, _read(os.path.join(devops_dir, "upgrade_1_4_%s.sql" % n))) for n in FRAGMENTS)
    for n, t in frags.items():
        for line in t.split("\n"):
            if line.strip() == GATE:
                raise GeneratorError("fragment %s contains its own integrity gate" % n)
            if line.startswith("\\"):
                raise GeneratorError("fragment %s contains a psql meta-command" % n)
    acl_text = "".join(t if t.endswith("\n") else t + "\n" for t in acl_chunks)
    return "".join([frags["preamble"] if frags["preamble"].endswith("\n") else frags["preamble"] + "\n",
                    frags["schema"] if frags["schema"].endswith("\n") else frags["schema"] + "\n",
                    "".join(routine_chunks), acl_text,
                    frags["repairs"] if frags["repairs"].endswith("\n") else frags["repairs"] + "\n"])


def build(src_dir, versions_dir, devops_dir, extversion):
    if extversion != TARGET:
        raise GeneratorError("this generator builds %s update scripts only, not %r" % (TARGET, extversion))
    frozen = dict((n, authenticate(os.path.join(versions_dir, n), sha)) for n, sha in FROZEN_SHA256.items())
    d = delta(src_dir, devops_dir, frozen["sql_saga--1.3.sql"])
    out = {}
    for src in SOURCES:
        parts = [header(TARGET), "\n", "SELECT pg_catalog.set_config('sql_saga.upgrade_from_1_4', '%s', true);\n" % src]
        if src != "1.3":
            parts.append(transform_embedded(frozen["sql_saga--%s--1.3.sql" % src], src))
        parts += [d, GATE, "\n"]
        text = "".join(parts)
        if sum(1 for line in text.split("\n") if line == GATE) != 1:
            raise GeneratorError("%s--1.4 must end with exactly one integrity gate" % src)
        out["sql_saga--%s--%s.sql" % (src, TARGET)] = text
    return out


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--src", required=True)
    ap.add_argument("--versions", required=True)
    ap.add_argument("--devops", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--extversion", required=True)
    a = ap.parse_args(argv)
    g13.write_atomically(a.outdir, build(a.src, a.versions, a.devops, a.extversion))


if __name__ == "__main__":
    try:
        main()
    except GeneratorError as e:
        sys.stderr.write("generate_update_scripts_1_4: %s\n" % e)
        sys.exit(1)
