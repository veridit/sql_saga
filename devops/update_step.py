#!/usr/bin/env python3
"""Build the update step from the previous release to this one (doc/design/update-chain.md). Standard library only.

The step sql_saga--<previous>--<target>.sql is worked out by comparing the previous release's install script
(versions/sql_saga--<previous>.sql) with the current one (src/), statement by statement:

  header, native check (library <target> or newer) and upgrade lock,
  a privilege check and DROP for every routine whose signature changes or that CREATE OR REPLACE cannot change
      (the update refuses when the site changed its EXECUTE privileges: DROP and CREATE would lose them silently),
      a plain DROP for every routine the release removes,
  CREATE OR REPLACE for every routine that is new or changed, in src/ order,
  the privilege statements and comments src/ has for those routines, and the idempotent blocks that copy them,
  devops/update_step.sql: the release's catalog DDL and data repairs, written by hand,
  the integrity gate, which runs when this step is the last one of the update.

Any other difference between the two install scripts (a table, type, view, trigger or privilege on an existing object)
fails the build unless devops/update_step.sql names it in a line "-- handles: <statement key>"; the error message
gives the key. The upgrade test then proves that an updated installation equals a fresh one.

The target is default_version in sql_saga.control; the previous release is the newest install script in versions/.

Usage: devops/update_step.py --root DIR --outdir DIR
"""
import argparse
import os
import re
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sql_statements as s  # noqa: E402

GeneratorError = s.GeneratorError

FRAGMENT = 'update_step.sql'

LAST_STEP_GATE = """DO $last_step_gate$
BEGIN
    IF (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array(sql_saga.native_library_version(), '.')) AS x)
       = (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array(
              (SELECT e.extversion FROM pg_catalog.pg_extension AS e WHERE e.extname = 'sql_saga'), '.')) AS x) THEN
        PERFORM sql_saga.check_integrity();
    END IF;
END;
$last_step_gate$;
"""

# Idempotent blocks of src/ that apply privileges and comments to routines by name: rerun when a routine they reach is
# new or replaced.
GRANT_BLOCK = '$grant_temporal_merge$'
COMMENT_BLOCK = '$copy_api_comments$'


def version_key(v):
    return tuple(int(x) for x in v.split('.'))


def control_version(root):
    m = re.search(r"^default_version\s*=\s*'([^']+)'", Path(root, 'sql_saga.control').read_text(), re.M)
    if not m:
        raise GeneratorError('no default_version in sql_saga.control')
    return m.group(1)


def previous_version(root, target):
    installs = [m.group(1) for p in Path(root, 'versions').glob('sql_saga--*.sql')
                for m in [re.fullmatch(r'sql_saga--([0-9.]+)\.sql', p.name)] if m]
    older = sorted((v for v in installs if version_key(v) < version_key(target)), key=version_key)
    if not older:
        raise GeneratorError('no released install script older than %s in versions/' % target)
    return older[-1]


def header(version):
    return "\\echo Use \"ALTER EXTENSION sql_saga UPDATE TO '%s'\" to load this file. \\quit\n" % version


def prefix(version):
    """The native check (library <version> or newer) and the upgrade lock that open the step."""
    return """CREATE OR REPLACE FUNCTION sql_saga.native_library_version() RETURNS text LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT AS 'sql_saga_native', 'sql_saga_native_version_wrapper';
DO $native_guard_%(tag)s$
BEGIN
    IF (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array(sql_saga.native_library_version(), '.')) AS x) < (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array('%(v)s', '.')) AS x) THEN
        RAISE EXCEPTION 'sql_saga: native library version %% does not match extension update version %(v)s', sql_saga.native_library_version()
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Install the sql_saga %(v)s native library and restart PostgreSQL before retrying ALTER EXTENSION.';
    END IF;
END;
$native_guard_%(tag)s$;
SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('sql_saga %(v)s upgrade'));
""" % {'v': version, 'tag': version.replace('.', '_')}


def replace_routine_sql(old_key, new_key, target, revoked_from_public):
    """Refuse when the old routine's EXECUTE privileges differ from those its replacement gets, then drop it.

    DROP and CREATE cannot carry privileges over. The old routine's privileges as its release installed them are what a
    function created now gets (the database's default privileges, read from a throwaway probe), minus PUBLIC when that
    release's install script revoked it from PUBLIC. Any other privilege is a site decision that would be lost silently
    (a grant dropped, a revoke undone), so the update stops and prints the statements that carry it over to the new
    signature. The new routine then gets the privilege statements src/ has for it.
    """
    name = old_key.split('(')[0].split('.')[-1]
    tag = re.sub(r'[^a-z0-9]+', '_', name)
    keep_public = 'false' if revoked_from_public else 'true'
    template = """DO $replace_@TAG@$
DECLARE
    old_proc CONSTANT text := '@OLD@';
    new_proc CONSTANT text := '@NEW@';
    probe CONSTANT text := 'sql_saga.__sql_saga_@VTAG@_upgrade_acl_probe()';
    probe_acl pg_catalog.aclitem[];
    probe_owner pg_catalog.oid;
    differs boolean;
    old_grantees text;
    new_grantees text;
    old_grants text;
    new_grants text;
    cascade_word CONSTANT text := 'CAS' || 'CADE';
BEGIN
    IF pg_catalog.to_regprocedure(old_proc) IS NULL THEN
        RETURN;
    END IF;
    IF pg_catalog.to_regprocedure(probe) IS NOT NULL THEN
        RAISE EXCEPTION 'sql_saga: function % already exists; the @TARGET@ update needs that name for a temporary probe', probe
            USING ERRCODE = 'duplicate_function',
                  HINT = 'Drop or rename that function, then retry the update.';
    END IF;
    EXECUTE 'CREATE FUNCTION ' || probe || ' RETURNS void LANGUAGE sql AS ''SELECT''';
    SELECT p.proacl, p.proowner INTO probe_acl, probe_owner
    FROM pg_catalog.pg_proc AS p
    WHERE p.oid = pg_catalog.to_regprocedure(probe);
    EXECUTE 'DROP FUNCTION ' || probe;

    WITH old_set AS (
        SELECT a.grantee, pg_catalog.bool_or(a.is_grantable) AS grantable
        FROM pg_catalog.pg_proc AS p
        CROSS JOIN LATERAL pg_catalog.aclexplode(coalesce(p.proacl, pg_catalog.acldefault('f', p.proowner))) AS a
        WHERE p.oid = pg_catalog.to_regprocedure(old_proc)
          AND a.privilege_type = 'EXECUTE'
          AND a.grantee OPERATOR(pg_catalog.<>) p.proowner
        GROUP BY a.grantee
    ), new_set AS (
        SELECT a.grantee, pg_catalog.bool_or(a.is_grantable) AS grantable
        FROM pg_catalog.aclexplode(coalesce(probe_acl, pg_catalog.acldefault('f', probe_owner))) AS a
        WHERE a.privilege_type = 'EXECUTE'
          AND a.grantee OPERATOR(pg_catalog.<>) probe_owner
          AND (a.grantee OPERATOR(pg_catalog.<>) 0 OR @KEEP_PUBLIC@)
        GROUP BY a.grantee
    ), both_sets AS (
        SELECT 'old' AS side, o.grantee, o.grantable FROM old_set AS o
        UNION ALL
        SELECT 'new', n.grantee, n.grantable FROM new_set AS n
    ), named AS (
        SELECT b.side, b.grantable, b.grantee = 0 AS is_public,
               CASE WHEN b.grantee = 0 THEN 'PUBLIC' ELSE b.grantee::pg_catalog.regrole::text END AS who
        FROM both_sets AS b
    )
    SELECT
        EXISTS (SELECT FROM old_set AS o FULL JOIN new_set AS n ON n.grantee = o.grantee
                WHERE o.grantable IS DISTINCT FROM n.grantable),
        pg_catalog.string_agg(nm.who, ', ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'old'),
        pg_catalog.string_agg(nm.who, ', ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'new'),
        pg_catalog.string_agg(pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO %s%s;', new_proc, nm.who,
                                                CASE WHEN nm.grantable THEN ' WITH GRANT OPTION' ELSE '' END),
                              ' ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'old'),
        pg_catalog.string_agg(pg_catalog.format('GRANT EXECUTE ON FUNCTION %s TO %s%s;', old_proc, nm.who,
                                                CASE WHEN nm.grantable THEN ' WITH GRANT OPTION' ELSE '' END),
                              ' ' ORDER BY NOT nm.is_public, nm.who COLLATE "C") FILTER (WHERE nm.side = 'new')
    INTO differs, old_grantees, new_grantees, old_grants, new_grants
    FROM named AS nm;

    IF differs THEN
        RAISE EXCEPTION 'sql_saga: the privileges on % differ from those its @TARGET@ replacement gets; the update replaces this function, and cannot carry changed privileges over', old_proc
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = pg_catalog.concat_ws(' ',
                      'Reset them, update, and apply them to the new signature after the update. Run:',
                      CASE WHEN old_grantees IS NOT NULL THEN pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM %s %s;', old_proc, old_grantees, cascade_word) END,
                      new_grants,
                      'ALTER EXTENSION sql_saga UPDATE;',
                      CASE WHEN new_grantees IS NOT NULL THEN pg_catalog.format('REVOKE ALL ON FUNCTION %s FROM %s %s;', new_proc, new_grantees, cascade_word) END,
                      old_grants);
    END IF;
    EXECUTE 'DROP FUNCTION ' || old_proc;
END;
$replace_@TAG@$;
"""
    return (template.replace('@TAG@', tag).replace('@OLD@', old_key).replace('@NEW@', new_key)
            .replace('@KEEP_PUBLIC@', keep_public).replace('@VTAG@', target.replace('.', '_'))
            .replace('@TARGET@', target))


# --------------------------------------------------------------------------------------------- statements
def routine_ident(statement):
    if not s._ROUTINE_RE.match(s._core(statement)):
        return None
    r = s._parse_routine(statement)
    return s._identity(r['name'], r['args'])


def key_text(ident):
    return '%s(%s)' % (ident[0], ', '.join(ident[1]))


_ACL_ON_ROUTINE = re.compile(r'(GRANT|REVOKE)\s.*?\sON\s+(FUNCTION|PROCEDURE|ROUTINE)\s+(.*?)\s+(TO|FROM)\s', re.I | re.S)
_COMMENT_ON_ROUTINE = re.compile(r'COMMENT\s+ON\s+(FUNCTION|PROCEDURE)\s+(.*?)\s+IS\s', re.I | re.S)


def _targets(text):
    """'sql_saga.f(int), sql_saga.g()' -> [identity]"""
    out = []
    for target in s._split_top(text):
        p = target.find('(')
        if p < 0:
            raise GeneratorError('routine target without argument list: %r' % target)
        close = s._matching_paren(target, p)
        name = target[:p].strip().lower().replace('"', '')
        out.append(s._identity(name, [s._parse_arg(a) for a in s._split_top(target[p + 1:close])]))
    return out


def acl_targets(core):
    m = _ACL_ON_ROUTINE.match(core)
    return _targets(m.group(3)) if m else None


def comment_target(core, routines):
    """The routine a COMMENT ON FUNCTION/PROCEDURE names; without an argument list, the one routine of that name."""
    m = _COMMENT_ON_ROUTINE.match(core)
    if not m:
        return None
    if '(' in m.group(2):
        return _targets(m.group(2))[0]
    name = m.group(2).strip().lower().replace('"', '')
    found = [i for i in routines if i[0] == name]
    if len(found) != 1:
        raise GeneratorError('COMMENT ON %s names %d routines' % (name, len(found)))
    return found[0]


def statement_key(core):
    """A short name for a statement that is not a routine: the text before its definition, or the whole statement."""
    one = ' '.join(core.split())
    m = re.match(r'(.*?)(\s+IS\s|\s+AS\s|\s*\()', one)
    if m and re.match(r'(CREATE|COMMENT ON|DO)\b', one, re.I):
        return m.group(1)
    return one


def install_statements(text):
    """An install script is src/ concatenated, its first statement the \\echo guard of src/0a."""
    return s.split_sql(text)


def handled_keys(fragment):
    return {m.group(1).strip() for m in re.finditer(r'^-- handles: (.+)$', fragment, re.M)}


# --------------------------------------------------------------------------------------------- the step
def needs_drop(old, new):
    """True when CREATE OR REPLACE cannot turn the old definition of a routine into the new one."""
    a, b = s._parse_routine(old), s._parse_routine(new)
    if a['kind'] != b['kind'] or s._canon_result(a['result']) != s._canon_result(b['result']):
        return True
    a_in = [x for x in a['args'] if x[0] != 'out']
    b_in = [x for x in b['args'] if x[0] != 'out']
    if any(x[1] and x[1] != y[1] for x, y in zip(a_in, b_in)):
        return True
    if [x[0] for x in a['args']] != [x[0] for x in b['args']]:
        return True
    return sum(x[3] for x in b_in) < sum(x[3] for x in a_in)


def build(previous_install, current_install, fragment, previous, target):
    old = install_statements(previous_install)
    new = install_statements(current_install)
    old_routines = {routine_ident(x): x for x in old if routine_ident(x)}
    new_routines = {routine_ident(x): x for x in new if routine_ident(x)}

    changed = [i for i in new_routines if i not in old_routines
               or s._core(old_routines[i]).strip() != s._core(new_routines[i]).strip()]
    added = [i for i in new_routines if i not in old_routines]
    gone = [i for i in old_routines if i not in new_routines]

    # Routines DROP and CREATE: a signature change pairs the one old and one new overload of a name; a same-signature
    # change CREATE OR REPLACE cannot make replaces the routine in place.
    replaced = []
    for i in gone:
        partners = [n for n in added if n[0] == i[0]]
        olds = [o for o in gone if o[0] == i[0]]
        if len(partners) > 1 or len(olds) > 1:
            raise GeneratorError('%s: more than one overload changes signature; cannot pair old and new' % i[0])
        if partners:
            replaced.append((i, partners[0]))
    for i in changed:
        if i in old_routines and needs_drop(old_routines[i], new_routines[i]):
            replaced.append((i, i))
    removed = [i for i in gone if i not in {o for o, _ in replaced}]
    reached = {n for _, n in replaced} | set(added)

    # Everything else must be equal, generated here (privileges and comments of new or replaced routines, changed
    # routine comments), or handled by the fragment.
    handles = handled_keys(fragment)
    old_rest = {}
    for x in old:
        if not routine_ident(x):
            old_rest.setdefault(statement_key(s._core(x).strip()), []).append(s._core(x).strip())
    # A routine created by this step (new, or dropped and created again) gets its privileges and comment from src/,
    # also when their text did not change; an existing routine keeps both, and gets a comment only when its text
    # changed.
    emitted, unhandled, used, commented = [], [], set(), set()
    blocks = []
    for x in new:
        if routine_ident(x):
            continue
        core = s._core(x).strip()
        key = statement_key(core)
        unchanged = core in old_rest.get(key, [])
        if unchanged:
            old_rest[key].remove(core)
        targets = acl_targets(core)
        comment = comment_target(core, new_routines)
        if GRANT_BLOCK in core or COMMENT_BLOCK in core:
            (blocks if unchanged else emitted).append(core)  # a new or changed block runs; it is idempotent
        elif targets and all(t in reached for t in targets):
            emitted.append(core)
        elif comment and (comment in reached or not unchanged):
            emitted.append(core)
            commented.add(comment)
        elif unchanged:
            pass
        elif key in handles:
            used.add(key)
        else:
            unhandled.append(key)
    for key, rest in old_rest.items():
        for core in rest:
            comment = comment_target(core, old_routines)
            targets = acl_targets(core) or ([comment] if comment else [])
            if targets and all(t in old_routines and t not in new_routines for t in targets):
                continue  # privileges and comments of a removed routine go with it
            if comment and comment in commented:
                continue  # superseded by the routine's new comment
            if key in handles:
                used.add(key)
                continue
            unhandled.append(key)
    # The idempotent blocks reapply privileges and comments by name: rerun after any routine they reach is created or
    # any worker comment changes.
    names = {i[0].split('.')[-1] for i in reached} | {i[0].split('.')[-1] for i in commented}
    for core in blocks:
        if GRANT_BLOCK in core and any("'%s'" % n in core for n in names):
            emitted.append(core)
        if COMMENT_BLOCK in core and any(n.startswith('__api_') or ('sql_saga.__api_' + n) in {i[0] for i in new_routines}
                                         for n in names):
            emitted.append(core)
    if unhandled:
        raise GeneratorError('changes the step cannot make by itself; make them in devops/%s and name each there in a '
                             'line "-- handles: <key>": %s' % (FRAGMENT, '; '.join(sorted(set(unhandled)))))
    if handles - used:
        raise GeneratorError('devops/%s names changes that src/ does not make: %s' % (FRAGMENT, '; '.join(sorted(handles - used))))
    if 'SELECT sql_saga.check_integrity();' in fragment:
        raise GeneratorError('devops/%s must not run the integrity gate; the step ends with it' % FRAGMENT)

    chunks = [header(target), prefix(target)]
    # Dependents before what they depend on: the reverse of their order in the previous install script.
    order = {i: n for n, i in enumerate(routine_ident(x) for x in old)}
    revoked = {t for x in old for t in (acl_targets(s._core(x)) or [])
               if re.match(r'REVOKE\s.*\sFROM\s+PUBLIC\b', ' '.join(s._core(x).split()), re.I)}
    for o, n in sorted(replaced, key=lambda p: -order[p[0]]):
        chunks.append(replace_routine_sql(key_text(o), key_text(n), target, o in revoked))
    for i in sorted(removed, key=lambda i: -order[i]):
        kind = s._parse_routine(old_routines[i])['kind'].upper()
        chunks.append('DROP %s %s;\n' % (kind, key_text(i)))
    for x in new:
        i = routine_ident(x)
        if i in changed:
            text = s.to_create_or_replace(x).strip('\n')
            chunks.append(text + '\n')
    chunks.extend(c + '\n' for c in emitted)
    if fragment.strip():
        chunks.append(fragment if fragment.endswith('\n') else fragment + '\n')
    chunks.append(LAST_STEP_GATE)
    return ''.join(chunks)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--root', required=True)
    parser.add_argument('--outdir', required=True)
    a = parser.parse_args()
    try:
        target = control_version(a.root)
        previous = previous_version(a.root, target)
        current = ''.join(p.read_text() for p in sorted(Path(a.root, 'src').glob('[0-9][a-z]_*.sql')))
        frag = Path(a.root, 'devops', FRAGMENT)
        text = build(Path(a.root, 'versions', 'sql_saga--%s.sql' % previous).read_text(), current,
                     frag.read_text() if frag.exists() else '', previous, target)
    except GeneratorError as error:
        sys.exit('update_step: %s' % error)
    name = 'sql_saga--%s--%s.sql' % (previous, target)
    fd, tmp = tempfile.mkstemp(prefix='.' + name, dir=a.outdir)
    with os.fdopen(fd, 'w') as f:
        f.write(text)
    os.chmod(tmp, 0o644)
    os.replace(tmp, os.path.join(a.outdir, name))


if __name__ == '__main__':
    main()
