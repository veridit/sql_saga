#!/usr/bin/env python3
"""Top-level SQL statements and routine signatures of sql_saga's scripts (devops/update_step.py). Standard library only.

Taken unchanged from the 1.3 update-script generator, which specified it in its tests (git show v1.13:devops/).
"""
import re


class GeneratorError(Exception):
    pass


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


def _strip_line_comments(text):
    """Remove -- comments outside string literals and quoted identifiers (a signature may carry them)."""
    out, i, n = [], 0, len(text)
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
            out.append(text[i:j + 1])
            i = j + 1
            continue
        if c == '"':
            j = text.find('"', i + 1)
            out.append(text[i:j + 1])
            i = j + 1
            continue
        if text.startswith("--", i):
            j = text.find("\n", i)
            i = n if j < 0 else j
            continue
        out.append(c)
        i += 1
    return "".join(out)


def _parse_routine(stmt):
    core = _core(stmt)
    m = _ROUTINE_RE.match(core)
    if not m:
        raise GeneratorError("not a routine definition at line %d" % stmt.line)
    kind = m.group(2).lower()
    name = m.group(3).lower().replace('"', "")
    close = _matching_paren(core, m.end() - 1)
    args = [_parse_arg(a) for a in _split_top(_strip_line_comments(core[m.end():close]))]
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

