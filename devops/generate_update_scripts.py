#!/usr/bin/env python3
"""Generate the sql_saga 1.3 extension update scripts (1.0/1.1/1.2 -> 1.3). Standard library only.

Specified by devops/test_generate_update_scripts.py; design notes in the module docstrings below.

Each script is: header, trusted source-version literal, preamble fragment (its marker replaced by the src/1k
routines), the transformed frozen historical update (1.0 and 1.1 only), schema fragment, the new pending table
from src/2o, the current routines from the manifest, repairs fragment, and exactly one final integrity gate.
Frozen inputs are authenticated by SHA-256 and never written. Anything not understood fails the build.
"""
import argparse
import hashlib
import os
import re
import sys
import tempfile


class GeneratorError(Exception):
    pass


FROZEN_SHA256 = {
    "sql_saga--1.0--1.2.sql": "11b6174d2a428bed86e82fb5e7560ffa8a0f640affd8146c399c9e1dae49c321",
    "sql_saga--1.1--1.2.sql": "3a33a535b57d1990f29358b5bfa6443a53217787766bd39abc78a8243a5260c5",
    "sql_saga--1.2.sql": "448ffb0b496578b9243443aefb5ddc2f2c66de7dc6315a5da2bdbddd92a6fa83",
}

# src files with routines changed since v1.2 (coordinator confirmed against `git diff --name-only v1.2 -- src`).
ROUTINE_MANIFEST = ("0c", "0g", "0i", "0j", "1d", "1g", "1h", "1k", "2a", "2e", "2f", "2h", "2l", "2o", "3c", "3d", "3e")
# Changed, but handled by hand (schema fragment) or compiled, never emitted from src.
MANUAL = ("0b", "sql_saga.c", "sql_saga.h", "covers_without_gaps.c", "covers_without_gaps.h")
# ACL source files: only statements for routines new in 1.3 are emitted from them.
ACL_SOURCES = ("3f", "3g")
# The only table new in 1.3, emitted from src/2o.
NEW_TABLES = frozenset({"sql_saga.__internal_updatable_view_pending"})
# New routines that deliberately keep creation-time default privileges (no GRANT/REVOKE in src). Key: name(types).
DEFAULT_ACL_NEW_ROUTINES = frozenset()

SOURCES = ("1.0", "1.1", "1.2")
TARGET = "1.3"
GATE = "SELECT sql_saga.check_integrity();"
GATE_REPLACEMENT = "-- sql_saga 1.3: the 1.2 integrity gate runs at the end of this update instead"
P_1K_MARKER = "-- sql_saga 1.3 generator: insert src/1k_foreign_key_pairing.sql routines here"


def header(version):
    return "\\echo Use \"ALTER EXTENSION sql_saga UPDATE TO '%s'\" to load this file. \\quit" % version


# --------------------------------------------------------------------------------------------- authentication
def authenticate(path, expected_sha256):
    with open(path, "rb") as f:
        data = f.read()
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected_sha256:
        raise GeneratorError("frozen artifact %s failed SHA-256 authentication: expected %s, got %s"
                             % (path, expected_sha256, actual))
    return data.decode("utf-8")


# --------------------------------------------------------------------------------------------- splitting
class Statement(object):
    __slots__ = ("text", "line")

    def __init__(self, text, line):
        self.text = text
        self.line = line

    def __repr__(self):
        return "Statement(line=%d, %r)" % (self.line, self.text[:60])


_SPECIAL = re.compile(r"[;'\"$]|--|/\*")
_DOLLAR = re.compile(r"\$([A-Za-z_\x80-\uffff][A-Za-z0-9_\x80-\uffff]*)?\$")
_IDENT_CHAR = re.compile(r"[A-Za-z0-9_$\x80-\uffff]")


def _skip_block_comment(text, i):
    """i points at '/*'; return index after the matching (nested) '*/'."""
    depth = 0
    n = len(text)
    while i < n:
        if text.startswith("/*", i):
            depth += 1
            i += 2
        elif text.startswith("*/", i):
            depth -= 1
            i += 2
            if depth == 0:
                return i
        else:
            i += 1
    raise GeneratorError("unterminated block comment")


def _core_offset(text):
    """Offset of the first character that is neither whitespace nor comment."""
    i, n = 0, len(text)
    while i < n:
        if text[i].isspace():
            i += 1
        elif text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j + 1
        elif text.startswith("/*", i):
            i = _skip_block_comment(text, i)
        else:
            return i
    return n


def split_sql(text):
    """Split SQL into top-level statements. Lossless: the statement texts concatenate to the input, except that
    whitespace or comments after the last statement are attached to it."""
    stmts = []
    start = 0
    i = 0
    n = len(text)
    while True:
        m = _SPECIAL.search(text, i)
        if not m:
            break
        tok = m.group(0)
        p = m.start()
        if tok == ";":
            chunk = text[start:p + 1]
            if _core_offset(chunk) >= len(chunk) - 1:
                raise GeneratorError("empty statement at line %d" % (text.count("\n", 0, p) + 1))
            stmts.append(Statement(chunk, text.count("\n", 0, start) + 1 + (1 if start and text[start] == "\n" else 0)))
            start = i = p + 1
        elif tok == "--":
            j = text.find("\n", p)
            i = n if j < 0 else j + 1
        elif tok == "/*":
            i = _skip_block_comment(text, p)
        elif tok == "'":
            escape = p > 0 and text[p - 1] in "eE" and (p < 2 or not _IDENT_CHAR.match(text[p - 2]))
            j = p + 1
            while True:
                if j >= n:
                    raise GeneratorError("unterminated string literal at line %d" % (text.count("\n", 0, p) + 1))
                c = text[j]
                if escape and c == "\\":
                    j += 2
                elif c == "'":
                    if text.startswith("''", j):
                        j += 2
                    else:
                        break
                else:
                    j += 1
            i = j + 1
        elif tok == '"':
            j = p + 1
            while True:
                j = text.find('"', j)
                if j < 0:
                    raise GeneratorError("unterminated quoted identifier at line %d" % (text.count("\n", 0, p) + 1))
                if text.startswith('""', j):
                    j += 2
                else:
                    break
            i = j + 1
        else:  # "$"
            dm = _DOLLAR.match(text, p)
            if dm and not (p > 0 and _IDENT_CHAR.match(text[p - 1])):
                tag = dm.group(0)
                j = text.find(tag, dm.end())
                if j < 0:
                    raise GeneratorError("unterminated dollar quote %s at line %d" % (tag, text.count("\n", 0, p) + 1))
                i = j + len(tag)
            else:
                i = p + 1
    rest = text[start:]
    if _core_offset(rest) < len(rest):
        raise GeneratorError("statement without terminating semicolon at line %d"
                             % (text.count("\n", 0, start + _core_offset(rest)) + 1))
    if rest and stmts:
        stmts[-1] = Statement(stmts[-1].text + rest, stmts[-1].line)
    return stmts


def _core(stmt):
    text = stmt.text if isinstance(stmt, Statement) else stmt
    return text[_core_offset(text):]


# --------------------------------------------------------------------------------------------- classification
_ROUTINE_RE = re.compile(r"CREATE\s+(OR\s+REPLACE\s+)?(FUNCTION|PROCEDURE)\s+([^\s(]+)\s*\(", re.I)
_KINDS = [
    ("comment", re.compile(r"COMMENT\s+ON\s+[A-Z ]+?\s+sql_saga\.", re.I)),
    ("grant", re.compile(r"GRANT\s", re.I)),
    ("revoke", re.compile(r"REVOKE\s", re.I)),
    ("create_table", re.compile(r"CREATE\s+TABLE\s+sql_saga\.", re.I)),
    ("do", re.compile(r"DO\s+\$", re.I)),
    ("select", re.compile(r"SELECT\s", re.I)),
    ("drop_routine", re.compile(r"DROP\s+(FUNCTION|PROCEDURE)\s+(IF\s+EXISTS\s+)?sql_saga\.", re.I)),
]


def classify(stmt):
    core = _core(stmt)
    m = _ROUTINE_RE.match(core)
    if m:
        if not m.group(3).lower().startswith("sql_saga."):
            raise GeneratorError("routine outside schema sql_saga at line %d: %s" % (stmt.line, m.group(3)))
        return "routine"
    for kind, rx in _KINDS:
        if rx.match(core):
            return kind
    raise GeneratorError("unsupported top-level statement at line %d: %s" % (stmt.line, core.split("\n", 1)[0][:100]))


def to_create_or_replace(stmt):
    off = _core_offset(stmt.text)
    m = _ROUTINE_RE.match(stmt.text, off)
    if not m:
        raise GeneratorError("not a routine definition at line %d" % stmt.line)
    if m.group(1):
        return stmt.text
    return stmt.text[:off] + stmt.text[off:off + 6] + " OR REPLACE" + stmt.text[off + 6:]


# --------------------------------------------------------------------------------------------- signatures
_TYPE_ALIASES = {
    "int": "integer", "int4": "integer", "int8": "bigint", "int2": "smallint", "bool": "boolean",
    "float8": "double precision", "float4": "real", "varchar": "character varying", "decimal": "numeric",
    "timestamptz": "timestamp with time zone", "timetz": "time with time zone",
}
_MULTIWORD_START = {"double", "character", "char", "timestamp", "time", "bit", "national", "interval"}
_MULTIWORD_NEXT = {"precision", "varying", "with", "without", "zone", "time"}


def _split_top(text, sep=","):
    parts, depth, cur, i, n = [], 0, [], 0, len(text)
    while i < n:
        c = text[i]
        if c == "'":
            j = i + 1
            while True:
                j = text.find("'", j)
                if j < 0:
                    raise GeneratorError("unterminated literal in %r" % text)
                if text.startswith("''", j):
                    j += 2
                else:
                    break
            cur.append(text[i:j + 1])
            i = j + 1
            continue
        if c == '"':
            j = text.find('"', i + 1)
            cur.append(text[i:j + 1])
            i = j + 1
            continue
        if c in "([":
            depth += 1
        elif c in ")]":
            depth -= 1
        if c == sep and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(c)
        i += 1
    if "".join(cur).strip():
        parts.append("".join(cur))
    return [p.strip() for p in parts]


def _norm_type(t):
    t = re.sub(r"\s+", " ", t.strip().lower())
    t = re.sub(r"\s+\[", "[", t)
    return t


def _canon_type(t):
    t = _norm_type(t)
    t = re.sub(r"^pg_catalog\.", "", t)
    suffix = ""
    m = re.match(r"^(.*?)((\[\])*)$", t)
    t, suffix = m.group(1), m.group(2)
    return _TYPE_ALIASES.get(t, t) + suffix


def _parse_arg(arg):
    """Return (mode, name, type, has_default) for one parameter declaration."""
    m = re.search(r"\s+(DEFAULT\s|=)", arg, re.I)
    has_default = bool(m)
    if m:
        arg = arg[:m.start()]
    tokens = arg.split()
    mode = "in"
    if tokens and tokens[0].upper() in ("IN", "OUT", "INOUT", "VARIADIC"):
        mode = tokens[0].lower()
        tokens = tokens[1:]
    if not tokens:
        raise GeneratorError("cannot parse parameter %r" % arg)
    if len(tokens) == 1:
        name, typ = "", tokens[0]
    elif tokens[0].lower() in _MULTIWORD_START and tokens[1].lower() in _MULTIWORD_NEXT:
        name, typ = "", " ".join(tokens)
    else:
        name, typ = tokens[0].lower(), " ".join(tokens[1:])
    return mode, name, _norm_type(typ), has_default


def _matching_paren(text, open_index):
    depth = 0
    i = open_index
    while i < len(text):
        c = text[i]
        if c == "'":
            j = i + 1
            while True:
                j = text.find("'", j)
                if text.startswith("''", j):
                    j += 2
                else:
                    break
            i = j + 1
            continue
        if c == "(":
            depth += 1
        elif c == ")":
            depth -= 1
            if depth == 0:
                return i
        i += 1
    raise GeneratorError("unbalanced parentheses")


_RETURNS_END = re.compile(r"\s(LANGUAGE|AS|STABLE|IMMUTABLE|VOLATILE|SET|SECURITY|STRICT|CALLED|PARALLEL|COST|ROWS|"
                          r"LEAKPROOF|NOT|WINDOW|SUPPORT|TRANSFORM|EXTERNAL|BEGIN|RETURN)\b", re.I)


def _parse_routine(stmt):
    core = _core(stmt)
    m = _ROUTINE_RE.match(core)
    if not m:
        raise GeneratorError("not a routine definition at line %d" % stmt.line)
    kind = m.group(2).lower()
    name = m.group(3).lower().replace('"', "")
    close = _matching_paren(core, m.end() - 1)
    args = [_parse_arg(a) for a in _split_top(core[m.end():close])]
    result = None
    if kind == "function":
        after = core[close + 1:]
        rm = re.match(r"\s*RETURNS\s+", after, re.I)
        outs = [(a[1], a[2]) for a in args if a[0] in ("out", "inout")]
        if rm:
            body = after[rm.end():]
            tm = re.match(r"TABLE\s*\(", body, re.I)
            if tm:
                tclose = _matching_paren(body, tm.end() - 1)
                cols = []
                for c in _split_top(body[tm.end():tclose]):
                    parts = c.split(None, 1)
                    cols.append("%s %s" % (parts[0].lower(), _norm_type(parts[1])))
                result = "table(%s)" % ", ".join(cols)
            else:
                em = _RETURNS_END.search(" " + body)
                result = _norm_type(body[:em.start() - 1] if em else body)
        if outs and (result is None or result in ("record", "setof record")):
            prefix = "setof " if result == "setof record" else ""
            if len(outs) == 1:
                result = prefix + outs[0][1]
            else:
                result = prefix + "record(%s)" % ", ".join("%s %s" % o for o in outs)
        if result is None:
            raise GeneratorError("function without result type at line %d" % stmt.line)
    return {"kind": kind, "name": name, "args": args, "result": result}


def routine_signature(stmt):
    r = _parse_routine(stmt)
    args = tuple(("variadic " + t) if mode == "variadic" else t for mode, _, t, _ in r["args"] if mode != "out")
    return r["name"], args, r["result"]


def _identity(name, args):
    return name, tuple(_canon_type(t) for mode, _, t, _ in args if mode != "out")


def _canon_result(result):
    if result is None:
        return None
    m = re.match(r"^(setof )?(record|table)\((.*)\)$", result)
    if m:
        cols = []
        for c in _split_top(m.group(3)):
            n, t = c.split(None, 1)
            cols.append((n, _canon_type(t)))
        return (m.group(1) or "", m.group(2), tuple(cols))
    m = re.match(r"^(setof )?(.*)$", result)
    return (m.group(1) or "", _canon_type(m.group(2)))


_FROZEN_CACHE = {}


def _frozen_routines(frozen_text):
    key = hashlib.sha256(frozen_text.encode("utf-8")).hexdigest()
    if key not in _FROZEN_CACHE:
        body = frozen_text.split("\n", 1)[1] if frozen_text.startswith("\\echo") else frozen_text
        by_identity, by_name = {}, {}
        for s in split_sql(body):
            if _ROUTINE_RE.match(_core(s)):
                r = _parse_routine(s)
                ident = _identity(r["name"], r["args"])
                by_identity[ident] = r
                by_name.setdefault(r["name"], set()).add(ident)
        tables = set(m.lower() for m in re.findall(r"(?im)^\s*CREATE\s+(?:UNLOGGED\s+)?TABLE\s+(sql_saga\.\w+)", body))
        _FROZEN_CACHE[key] = (by_identity, by_name, tables)
    return _FROZEN_CACHE[key]


def _parse_target_list(text):
    """'sql_saga.f(int), sql_saga.g()' -> [(name, identity args)]"""
    out = []
    for target in _split_top(text):
        p = target.find("(")
        if p < 0:
            raise GeneratorError("routine target without argument list: %r" % target)
        close = _matching_paren(target, p)
        name = target[:p].strip().lower().replace('"', "")
        args = [_parse_arg(a) for a in _split_top(target[p + 1:close])]
        out.append(_identity(name, args))
    return out


def _drop_identities(drops):
    idents = set()
    for d in drops:
        core = _core(d)
        m = re.match(r"DROP\s+(FUNCTION|PROCEDURE)\s+(IF\s+EXISTS\s+)?(.*?)\s*(CASCADE|RESTRICT)?\s*;\s*$", core, re.I | re.S)
        if not m:
            raise GeneratorError("cannot parse approved drop at line %d" % d.line)
        idents.update(_parse_target_list(m.group(3)))
    return idents


def check_signatures(routines, frozen_text, approved_drops):
    """Refuse changes CREATE OR REPLACE cannot make (or that leave a stale overload) unless the old signature has
    an approved DROP."""
    by_identity, by_name, _ = _frozen_routines(frozen_text)
    drops = _drop_identities(approved_drops)
    parsed = [_parse_routine(s) for s in routines]
    new_idents = set(_identity(r["name"], r["args"]) for r in parsed)
    for s, r in zip(routines, parsed):
        ident = _identity(r["name"], r["args"])
        old = by_identity.get(ident)
        if old is not None:
            problems = []
            if old["kind"] != r["kind"]:
                problems.append("routine kind")
            o_in = [a for a in old["args"] if a[0] != "out"]
            n_in = [a for a in r["args"] if a[0] != "out"]
            for (om, on, _, _), (nm, nn, _, _) in zip(o_in, n_in):
                if on and on != nn:
                    problems.append("input parameter name %s -> %s" % (on, nn or "(none)"))
                if (om == "variadic") != (nm == "variadic"):
                    problems.append("VARIADIC")
            if sum(a[3] for a in n_in) < sum(a[3] for a in o_in):
                problems.append("removed parameter defaults")
            if _canon_result(old["result"]) != _canon_result(r["result"]):
                problems.append("result %s -> %s" % (old["result"], r["result"]))
            if problems and ident not in drops:
                raise GeneratorError("%s(%s) at line %d changes %s, which CREATE OR REPLACE cannot do; add an approved "
                                     "DROP of the old signature to the schema fragment"
                                     % (ident[0], ", ".join(ident[1]), s.line, "; ".join(problems)))
        for old_ident in by_name.get(r["name"], ()):
            if old_ident not in new_idents and old_ident not in drops:
                raise GeneratorError("%s(%s) replaces 1.2 signature %s(%s), which would stay behind as a stale overload; "
                                     "add an approved DROP of the old signature"
                                     % (ident[0], ", ".join(ident[1]), old_ident[0], ", ".join(old_ident[1])))


# --------------------------------------------------------------------------------------------- privileges
_ACL_RE = re.compile(r"(GRANT|REVOKE)\s+.*?\s+ON\s+(FUNCTION|PROCEDURE|ROUTINE)\s+(.*?)\s+(TO|FROM)\s", re.I | re.S)
_ACL_OBJ_RE = re.compile(r"(GRANT|REVOKE)\s+.*?\s+ON\s+(\w+)\s", re.I | re.S)


def _is_routine_acl(stmt):
    m = _ACL_OBJ_RE.match(_core(stmt))
    return bool(m) and m.group(2).upper() in ("FUNCTION", "PROCEDURE", "ROUTINE")


def _acl_targets(stmt):
    m = _ACL_RE.match(_core(stmt))
    if not m:
        raise GeneratorError("unsupported privilege statement at line %d" % stmt.line)
    return _parse_target_list(m.group(3))


def _key(ident):
    return "%s(%s)" % (ident[0], ", ".join(ident[1]))


def routine_acl_statements(stmts, frozen_text):
    """Keep GRANT/REVOKE only for routines new in 1.3; CREATE OR REPLACE preserves existing ACLs and owners."""
    by_identity, by_name, _ = _frozen_routines(frozen_text)
    kept = []
    for s in stmts:
        kind = classify(s)
        if kind in ("grant", "revoke"):
            if not _is_routine_acl(s):
                raise GeneratorError("privilege statement on a non-routine at line %d" % s.line)
            targets = _acl_targets(s)
            existing = [t in by_identity for t in targets]
            if all(existing):
                continue
            if any(existing):
                raise GeneratorError("privilege statement at line %d mixes 1.2 and new routines" % s.line)
            kept.append(s.text)
        elif kind == "do" and "$grant_temporal_merge$" in s.text:
            m = re.search(r"proname\s+IN\s*\((.*?)\)", s.text, re.S | re.I)
            names = re.findall(r"'([^']+)'", m.group(1)) if m else []
            if not names or any("sql_saga." + n not in by_name for n in names):
                raise GeneratorError("$grant_temporal_merge$ at line %d names routines absent from 1.2" % s.line)
        else:
            raise GeneratorError("unsupported statement in privilege source at line %d" % s.line)
    return kept


def new_routine_acl(routines, acl_stmts, frozen_text, default_acl=DEFAULT_ACL_NEW_ROUTINES):
    by_identity, _, _ = _frozen_routines(frozen_text)
    routine_acl = [s for s in acl_stmts if _is_routine_acl(s)]
    kept_text = routine_acl_statements(routine_acl, frozen_text)
    covered = set()
    for s in routine_acl:
        if s.text in kept_text:
            covered.update(_acl_targets(s))
    for s in routines:
        r = _parse_routine(s)
        ident = _identity(r["name"], r["args"])
        if ident not in by_identity and ident not in covered and _key(ident) not in default_acl:
            raise GeneratorError("new routine %s (line %d) has no GRANT/REVOKE in src and is not listed in "
                                 "DEFAULT_ACL_NEW_ROUTINES" % (_key(ident), s.line))
    return kept_text


def new_tables(stmts, frozen_text):
    _, _, tables = _frozen_routines(frozen_text)
    out = []
    for s in stmts:
        kind = classify(s)
        core = _core(s)
        if kind == "create_table":
            name = re.match(r"CREATE\s+TABLE\s+(sql_saga\.\w+)", core, re.I).group(1).lower()
            if name in tables:
                raise GeneratorError("table %s already exists in 1.2; schema changes belong in the schema fragment" % name)
        elif kind in ("grant", "revoke"):
            m = re.match(r"(GRANT|REVOKE)\s+.*?\s+ON\s+TABLE\s+(sql_saga\.\w+)\s", core, re.I | re.S)
            name = m.group(2).lower() if m else None
        elif kind == "comment":
            m = re.match(r"COMMENT\s+ON\s+TABLE\s+(sql_saga\.\w+)\s", core, re.I)
            name = m.group(1).lower() if m else None
        else:
            name = None
        if name not in NEW_TABLES:
            raise GeneratorError("statement at line %d is not about an approved new 1.3 table" % s.line)
        out.append(s.text)
    return out


# --------------------------------------------------------------------------------------------- historical
_GUARD_BLOCK_12 = (
    "DO $native_guard$\n"
    "BEGIN\n"
    "    IF sql_saga.native_library_version() <> '1.2' THEN\n"
    "        RAISE EXCEPTION 'sql_saga: native library version % does not match extension update version 1.2', "
    "sql_saga.native_library_version()\n"
    "            USING ERRCODE = 'object_not_in_prerequisite_state',\n"
    "                  HINT = 'Install the sql_saga 1.2 native library and restart PostgreSQL before retrying "
    "ALTER EXTENSION.';\n"
    "    END IF;\n"
    "END;\n"
    "$native_guard$;"
)
_GUARD_LINE_12 = (
    "DO $native_guard$ BEGIN IF sql_saga.native_library_version() <> '1.2' THEN RAISE EXCEPTION 'sql_saga: native "
    "library version % does not match extension update version 1.2', sql_saga.native_library_version() USING ERRCODE = "
    "'object_not_in_prerequisite_state', HINT = 'Install the sql_saga 1.2 native library and restart PostgreSQL before "
    "retrying ALTER EXTENSION.'; END IF; END; $native_guard$;"
)
_HINT_12 = "                      'ALTER EXTENSION sql_saga UPDATE TO ''1.2'';',"

# The 1.0--1.1 part of frozen 1.0--1.2 grants EXECUTE TO PUBLIC on routines that already exist in 1.0. Released 1.0
# records no pg_init_privs for them, so that in-extension GRANT records the then-current ACL as their initial
# privileges, and pg_dump stops dumping any site grant. The 1.0 copy keeps the three historical grants verbatim and
# refuses, immediately before the first of them, unless the eight routines carry exactly the canonical ACL and the
# name-based grant loop reaches no other routine. The refusal rolls the whole ALTER EXTENSION back.
_ACL_FPO_10 = "GRANT EXECUTE ON FUNCTION sql_saga.for_portion_of_trigger() TO PUBLIC;"
_ACL_CV_10 = "GRANT EXECUTE ON FUNCTION sql_saga.current_view_trigger() TO PUBLIC;"
_ACL_TM_10 = "\n".join([
    "DO $grant_temporal_merge$",
    "DECLARE",
    "    r record;",
    "BEGIN",
    "    FOR r IN",
    "        SELECT p.oid::regprocedure AS routine, p.prokind",
    "        FROM pg_catalog.pg_proc AS p",
    "        WHERE p.pronamespace = 'sql_saga'::regnamespace",
    "          AND p.proname IN ('temporal_merge', 'temporal_merge_plan', 'temporal_merge_plan_native',",
    "                            'temporal_merge_execute', 'temporal_merge_drop_temp_tables',",
    "                            'temporal_merge_executor_introspect')",
    "    LOOP",
    "        EXECUTE format('GRANT EXECUTE ON %s %s TO PUBLIC',",
    "            CASE WHEN r.prokind = 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END, r.routine);",
    "    END LOOP;",
    "END;",
    "$grant_temporal_merge$;",
])
_ACL_GUARD_10 = """DO $acl_guard_1_0$
DECLARE
    r record;
BEGIN
    -- The historical DO block that follows selects routines by name. Refuse any routine it would reach beyond the
    -- six released ones: it would get PUBLIC EXECUTE recorded as its initial privileges.
    FOR r IN
        SELECT p.oid::pg_catalog.regprocedure AS routine
        FROM pg_catalog.pg_proc AS p
        WHERE p.pronamespace = 'sql_saga'::pg_catalog.regnamespace
          AND p.proname IN ('temporal_merge', 'temporal_merge_plan', 'temporal_merge_plan_native',
                            'temporal_merge_execute', 'temporal_merge_drop_temp_tables',
                            'temporal_merge_executor_introspect')
          AND p.oid <> ALL (ARRAY[
            pg_catalog.to_regprocedure('sql_saga.temporal_merge(regclass,regclass,text[],sql_saga.temporal_merge_mode,name,name,name,boolean,text[],sql_saga.temporal_merge_delete_mode,boolean,name,name,name,name,text[],boolean)'),
            pg_catalog.to_regprocedure('sql_saga.temporal_merge_plan(regclass,regclass,sql_saga.temporal_merge_mode,name,text[],name,name,sql_saga.temporal_merge_delete_mode,jsonb,text[],boolean,boolean)'),
            pg_catalog.to_regprocedure('sql_saga.temporal_merge_plan_native(oid,oid,text,text,text[],text,text,text,jsonb,text[],boolean,boolean)'),
            pg_catalog.to_regprocedure('sql_saga.temporal_merge_execute(regclass,regclass,text[],sql_saga.temporal_merge_mode,name,name,name,boolean,text[],sql_saga.temporal_merge_delete_mode,boolean,name,name,name,name,text[],boolean)'),
            pg_catalog.to_regprocedure('sql_saga.temporal_merge_drop_temp_tables()'),
            pg_catalog.to_regprocedure('sql_saga.temporal_merge_executor_introspect(oid,oid,text[],text[],text,text[],text,text)')]::pg_catalog.oid[])
        ORDER BY 1
        LIMIT 1
    LOOP
        RAISE EXCEPTION 'sql_saga: routine % is not part of sql_saga 1.0, but the update from 1.0 would grant EXECUTE on it to PUBLIC', r.routine
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'The update grants EXECUTE to PUBLIC on every routine in schema sql_saga with this name and records it as the extension''s initial privileges. If you choose to, rename or move this routine, run ALTER EXTENSION sql_saga UPDATE TO ''1.3'', then rename or move it back. doc/upgrade.md has examples.';
    END LOOP;
    FOR r IN
        SELECT s.routine, p.proacl
        FROM pg_catalog.unnest(ARRAY[
            'sql_saga.for_portion_of_trigger()',
            'sql_saga.current_view_trigger()',
            'sql_saga.temporal_merge(regclass,regclass,text[],sql_saga.temporal_merge_mode,name,name,name,boolean,text[],sql_saga.temporal_merge_delete_mode,boolean,name,name,name,name,text[],boolean)',
            'sql_saga.temporal_merge_plan(regclass,regclass,sql_saga.temporal_merge_mode,name,text[],name,name,sql_saga.temporal_merge_delete_mode,jsonb,text[],boolean,boolean)',
            'sql_saga.temporal_merge_plan_native(oid,oid,text,text,text[],text,text,text,jsonb,text[],boolean,boolean)',
            'sql_saga.temporal_merge_execute(regclass,regclass,text[],sql_saga.temporal_merge_mode,name,name,name,boolean,text[],sql_saga.temporal_merge_delete_mode,boolean,name,name,name,name,text[],boolean)',
            'sql_saga.temporal_merge_drop_temp_tables()',
            'sql_saga.temporal_merge_executor_introspect(oid,oid,text[],text[],text,text[],text,text)'
        ]) WITH ORDINALITY AS s(routine, ord)
        LEFT JOIN pg_catalog.pg_proc AS p ON p.oid = pg_catalog.to_regprocedure(s.routine)
        WHERE p.oid IS NULL
           OR (SELECT pg_catalog.array_agg(ROW(a.grantor = p.proowner, a.grantee, a.privilege_type, a.is_grantable)::text
                                           ORDER BY a.grantee, a.privilege_type)
               FROM pg_catalog.aclexplode(COALESCE(p.proacl, pg_catalog.acldefault('f', p.proowner))) AS a)
              IS DISTINCT FROM
              (SELECT pg_catalog.array_agg(ROW(true, g, 'EXECUTE', false)::text ORDER BY g)
               FROM pg_catalog.unnest(ARRAY[0::pg_catalog.oid, p.proowner]) AS g)
        ORDER BY s.ord
        LIMIT 1
    LOOP
        RAISE EXCEPTION 'sql_saga: routine % has privileges % that differ from the standard sql_saga privileges (EXECUTE for its owner and PUBLIC); the update from 1.0 cannot carry them over',
            r.routine, COALESCE(r.proacl::text, '<missing>')
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'The update would record the current privileges as the extension''s initial privileges, so pg_dump would no longer dump them. Check each of the eight routines listed in doc/upgrade.md. In one transaction, as the routine owner: note the exact current grants, change EXECUTE so that only the routine owner and PUBLIC hold it (without grant option), run ALTER EXTENSION sql_saga UPDATE TO ''1.3'', then reapply the noted site changes outside the extension in the same transaction. doc/upgrade.md has examples.';
    END LOOP;
END;
$acl_guard_1_0$;"""
# Historical anchors kept verbatim on the 1.0 path; each must occur exactly once (count-checked, not replaced).
_VERBATIM = {"1.0--1.2": [_ACL_CV_10, _ACL_TM_10], "1.1--1.2": []}


def _retarget(s):
    return s.replace("'1.2'", "'1.3'").replace("version 1.2'", "version 1.3'") \
            .replace("sql_saga 1.2 native", "sql_saga 1.3 native").replace("''1.2''", "''1.3''")


# (old, new) whole-line(s) anchors per source; each must occur exactly once.
_ANCHORS = {
    "1.1--1.2": [(_GUARD_BLOCK_12, _retarget(_GUARD_BLOCK_12)), (_HINT_12, _retarget(_HINT_12)),
                 (GATE, GATE_REPLACEMENT)],
    "1.0--1.2": [(_GUARD_LINE_12, _retarget(_GUARD_LINE_12)), (_GUARD_BLOCK_12, _retarget(_GUARD_BLOCK_12)),
                 (_HINT_12, _retarget(_HINT_12)), (GATE, GATE_REPLACEMENT),
                 (_ACL_FPO_10, _ACL_GUARD_10 + "\n" + _ACL_FPO_10)],
}
_HEADERS = {"1.1--1.2": header("1.2"), "1.0--1.2": header("1.1")}
# Lines that mention 1.2 and stay unchanged (comments, message text, names, lock text).
_COSMETIC_12 = frozenset([
    " * 1.2 adds a parameter to add_system_versioning, so this script replaces the 1.1 function. Its",
    "        RAISE EXCEPTION 'sql_saga: function % already exists; the 1.2 update needs that name for a temporary probe', probe",
    "        RAISE EXCEPTION 'sql_saga: the privileges on % differ from those its 1.2 replacement gets; the update replaces "
    "this function, and cannot carry changed privileges over', old_proc",
    "SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('sql_saga 1.1 to 1.2 upgrade'));",
    "-- 1.2 adds a trailing history_table_must_exist parameter (DEFAULT false), so every 1.1 call form",
    "-- still resolves. A changed signature needs DROP and CREATE. The new function gets what a fresh 1.2",
    "        /* Deprecated catalog rows have no canonical 1.2 CHECK object. */",
    "    -- 1.2: Introspect the primary key (must happen after v_temporal_cols is defined).",
    "                            -- Step 1.2: Back-fill the captured, generated IDs into the plan",
])
_HAS_12 = re.compile(r"(?<![0-9.])1\.2(?![0-9])")


def _count_lines(body, block):
    return len(re.findall("(?=\n" + re.escape(block) + "\n)", "\n" + body + "\n"))


def _replace_lines(body, old, new):
    return ("\n" + body + "\n").replace("\n" + old + "\n", "\n" + new + "\n")[1:-1]


def transform_historical(text, source):
    if source not in _ANCHORS:
        raise GeneratorError("unknown historical source %r" % source)
    first, sep, body = text.partition("\n")
    if first != _HEADERS[source]:
        raise GeneratorError("unexpected header in %s: %r" % (source, first))
    anchor_lines = set()
    for old, new in _ANCHORS[source]:
        c = _count_lines(body, old)
        if c != 1:
            raise GeneratorError("expected exactly one anchor in %s, found %d: %s" % (source, c, old.split("\n")[0]))
        if _count_lines(body, new) != 0:
            raise GeneratorError("replacement text already present in %s: %s" % (source, new.split("\n")[0]))
        anchor_lines.update(old.split("\n"))
    for verbatim in _VERBATIM[source]:
        c = _count_lines(body, verbatim)
        if c != 1:
            raise GeneratorError("expected exactly one verbatim anchor in %s, found %d: %s" % (source, c, verbatim.split("\n")[0]))
    for no, line in enumerate(body.split("\n"), 2):
        if _HAS_12.search(line) and line not in _COSMETIC_12 and line not in anchor_lines:
            raise GeneratorError("unclassified 1.2 occurrence in %s line %d: %s" % (source, no, line[:120]))
    out = body
    for old, new in _ANCHORS[source]:
        out = _replace_lines(out, old, new)
    if reverse_historical(out, source) != text:
        raise GeneratorError("historical transformation of %s is not exactly reversible" % source)
    return out


def reverse_historical(out, source):
    body = out
    for old, new in _ANCHORS[source]:
        if _count_lines(body, new) != 1:
            raise GeneratorError("expected exactly one transformed anchor in %s" % source)
        body = _replace_lines(body, new, old)
    return _HEADERS[source] + "\n" + body


# --------------------------------------------------------------------------------------------- assembly
def _check_fragment(name, text):
    for line in text.split("\n"):
        if line.strip() == GATE:
            raise GeneratorError("%s contains its own final integrity gate; the generator emits exactly one" % name)
        if line.startswith("\\"):
            raise GeneratorError("%s contains a psql meta-command: %s" % (name, line))
    return text if text.endswith("\n") or not text else text + "\n"


def assemble(version_from, version_to, historical, preamble, bootstrap_1k, schema, pending_table, routines, repairs):
    if version_from not in SOURCES:
        raise GeneratorError("unsupported source version %r" % version_from)
    if version_to != TARGET:
        raise GeneratorError("unsupported target version %r" % version_to)
    if (historical is None) != (version_from == "1.2"):
        raise GeneratorError("historical part is required exactly for sources 1.0 and 1.1")
    parts = {"preamble": preamble, "bootstrap_1k": bootstrap_1k, "schema": schema, "pending_table": pending_table,
             "routines": routines, "repairs": repairs, "historical": historical or ""}
    parts = dict((k, _check_fragment(k, v)) for k, v in parts.items())
    lines = parts["preamble"].split("\n")
    if lines.count(P_1K_MARKER) != 1:
        raise GeneratorError("preamble must contain the 1k marker line exactly once")
    i = lines.index(P_1K_MARKER)
    pre = "\n".join(lines[:i]) + ("\n" if i else "")
    post = "\n".join(lines[i + 1:])
    return "".join([
        header(version_to), "\n",
        "SELECT pg_catalog.set_config('sql_saga.upgrade_from', '%s', true);\n" % version_from,
        pre, parts["bootstrap_1k"], post,
        parts["historical"], parts["schema"], parts["pending_table"], parts["routines"], parts["repairs"],
        GATE, "\n",
    ])


# --------------------------------------------------------------------------------------------- main
def _src_file(src_dir, code):
    names = sorted(f for f in os.listdir(src_dir) if f.startswith(code + "_") and f.endswith(".sql"))
    if len(names) != 1:
        raise GeneratorError("expected exactly one src file for %s, found %s" % (code, names))
    return os.path.join(src_dir, names[0])


def _read(path):
    with open(path, encoding="utf-8") as f:
        return f.read()


def build(src_dir, versions_dir, devops_dir, extversion, manifest):
    if extversion != TARGET:
        raise GeneratorError("this generator builds %s update scripts only, not %r" % (TARGET, extversion))
    frozen = dict((n, authenticate(os.path.join(versions_dir, n), sha)) for n, sha in FROZEN_SHA256.items())
    frozen12 = frozen["sql_saga--1.2.sql"]
    frags = dict((n, _read(os.path.join(devops_dir, "upgrade_1_3_%s.sql" % n))) for n in ("preamble", "schema", "repairs"))
    if "1k" not in manifest:
        raise GeneratorError("manifest must include 1k (bootstrapped in the preamble)")

    per_file, all_routines, all_acl, pending = [], [], [], []
    for code in manifest:
        stmts = split_sql(_read(_src_file(src_dir, code)))
        kinds = [classify(s) for s in stmts]
        per_file.append((code, stmts, kinds))
        for s, k in zip(stmts, kinds):
            if k == "routine":
                all_routines.append(s)
            elif k in ("grant", "revoke") and _is_routine_acl(s):
                all_acl.append(s)
            elif k == "create_table" or k in ("grant", "revoke") or (k == "comment" and re.match(r"COMMENT\s+ON\s+TABLE\s", _core(s), re.I)):
                pending.append(s)
            elif k != "comment":
                raise GeneratorError("unsupported %s statement in src %s at line %d" % (k, code, s.line))
    acl_files = []
    for code in ACL_SOURCES:
        names = [f for f in os.listdir(src_dir) if f.startswith(code + "_") and f.endswith(".sql")]
        if names:
            acl_files.extend(split_sql(_read(_src_file(src_dir, code))))
    acl_extra = routine_acl_statements(acl_files, frozen12)
    kept = set(new_routine_acl(all_routines, all_acl + acl_files, frozen12))

    drops = []
    for s in split_sql(frags["schema"]):
        try:
            if classify(s) == "drop_routine":
                drops.append(s)
        except GeneratorError:
            pass
    check_signatures(all_routines, frozen12, approved_drops=drops)
    pending_text = "".join(new_tables(pending, frozen12))

    routine_text, bootstrap = [], []
    for code, stmts, kinds in per_file:
        chunk = []
        for s, k in zip(stmts, kinds):
            if k == "routine":
                chunk.append(to_create_or_replace(s))
            elif k == "comment" and s not in pending:
                chunk.append(s.text)
            elif k in ("grant", "revoke") and s.text in kept:
                chunk.append(s.text)
        text = "".join(chunk)
        text = text if text.endswith("\n") else text + "\n"
        routine_text.append(text)
        if code == "1k":
            bootstrap.append(text)
    routine_text.append("".join(t if t.endswith("\n") else t + "\n" for t in acl_extra))

    historical = {
        "1.0": transform_historical(frozen["sql_saga--1.0--1.2.sql"], "1.0--1.2"),
        "1.1": transform_historical(frozen["sql_saga--1.1--1.2.sql"], "1.1--1.2"),
        "1.2": None,
    }
    out = {}
    for frm in SOURCES:
        out["sql_saga--%s--%s.sql" % (frm, TARGET)] = assemble(
            frm, TARGET, historical=historical[frm], preamble=frags["preamble"], bootstrap_1k="".join(bootstrap),
            schema=frags["schema"], pending_table=pending_text, routines="".join(routine_text), repairs=frags["repairs"])
    return out


def write_atomically(outdir, files):
    temps = {}
    try:
        for name, text in sorted(files.items()):
            fd, tmp = tempfile.mkstemp(prefix="." + name + ".", suffix=".tmp", dir=outdir)
            temps[name] = tmp
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(text)
            os.chmod(tmp, 0o644)
        for name, tmp in sorted(temps.items()):
            os.replace(tmp, os.path.join(outdir, name))
    finally:
        for tmp in temps.values():
            if os.path.exists(tmp):
                os.unlink(tmp)


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--src", required=True)
    ap.add_argument("--versions", required=True)
    ap.add_argument("--devops", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--extversion", required=True)
    ap.add_argument("--manifest", help="comma-separated src codes (default: ROUTINE_MANIFEST)")
    a = ap.parse_args(argv)
    manifest = tuple(a.manifest.split(",")) if a.manifest else ROUTINE_MANIFEST
    write_atomically(a.outdir, build(a.src, a.versions, a.devops, a.extversion, manifest))


if __name__ == "__main__":
    try:
        main()
    except GeneratorError as e:
        sys.stderr.write("generate_update_scripts: %s\n" % e)
        sys.exit(1)
