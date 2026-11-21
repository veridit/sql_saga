#!/usr/bin/env python3
"""Direct single-hop update scripts for the releases since 1.6. Standard library only.

A release file (devops/generate_update_scripts_<X_Y>.py) holds only data: a Release with the target and previous
version, the SHA-256 of every frozen previous artifact, the routine manifest, new routines and their privilege
statements, the repairs fragment and, when the catalog layout changes, the schema fragment and the changed tables.
Each script <source>--<target> it builds is:
  header, target native-library guard and upgrade lock,
  for an older source: the frozen, SHA-256 authenticated <source>--<previous> update without its header, native
      guard, lock and final integrity gate, with only native guards, actionable update hints and the upgrade lock
      retargeted to the target (catalog markers and GUCs keep their earlier values),
  the delta, the same for every source:
      every routine of the manifest as CREATE OR REPLACE (new routines are created),
      the privilege statements on new routines,
      devops/<schema>, the catalog DDL, after the routines: the DDL fires sql_saga's event triggers, which must run
          the new definitions (the previous ones may read what the DDL removes),
      devops/<repairs> (data repairs that need the new definitions),
  exactly one final integrity gate.
Every other difference between src/ and the frozen previous installation fails the build, except the CREATE TABLE
of the tables the release declares changed (their DDL is in the schema fragment).

The 1.4 and 1.5 generators predate this module and are kept as released.
"""
import argparse
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
import generate_update_scripts_1_4 as release

g = release.g13
GeneratorError = release.GeneratorError
GATE = release.GATE


def routine_key(statement):
    if not g._ROUTINE_RE.match(g._core(statement)):
        return None
    r = g._parse_routine(statement)
    return g._key(g._identity(r['name'], r['args']))


_TABLE_RE = re.compile(r'CREATE\s+TABLE\s+(sql_saga\.[A-Za-z_][A-Za-z0-9_]*)\s*\(', re.I)


def replace_routine_sql(old_key, new_key, target):
    """Refuse when the old routine's EXECUTE privileges differ from those its replacement gets, then drop it.

    DROP and CREATE cannot carry privileges over. The replacement gets what a function created now gets (the
    database's default privileges, read from a throwaway probe), minus PUBLIC for a private worker, whose PUBLIC
    EXECUTE the update revokes as a fresh install does. Any other difference is a site decision that would be lost
    silently (a grant dropped, a revoke undone), so the update stops and prints the statements that carry it over
    to the new signature (as the 1.2 update did for add_system_versioning).
    """
    name = old_key.split('(')[0].split('.')[-1]
    tag = re.sub(r'[^a-z0-9]+', '_', name)
    keep_public = 'false' if name.startswith('__') else 'true'
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
                      'ALTER EXTENSION sql_saga UPDATE TO ''@TARGET@'';',
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


def table_key(statement):
    m = _TABLE_RE.match(g._core(statement))
    return m.group(1) if m else None


def guard_prefix(version):
    """The native-library guard and upgrade lock that open every update script to version."""
    return """CREATE OR REPLACE FUNCTION sql_saga.native_library_version() RETURNS text LANGUAGE c IMMUTABLE PARALLEL SAFE STRICT AS 'sql_saga_native', 'sql_saga_native_version_wrapper';
DO $native_guard_%(tag)s$
BEGIN
    IF sql_saga.native_library_version() <> '%(v)s' THEN
        RAISE EXCEPTION 'sql_saga: native library version %% does not match extension update version %(v)s', sql_saga.native_library_version()
            USING ERRCODE = 'object_not_in_prerequisite_state',
                  HINT = 'Install the sql_saga %(v)s native library and restart PostgreSQL before retrying ALTER EXTENSION.';
    END IF;
END;
$native_guard_%(tag)s$;
SELECT pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('sql_saga %(v)s upgrade'));
""" % {'v': version, 'tag': version.replace('.', '_')}


@dataclass(frozen=True)
class Release:
    target: str
    previous: str
    sources: tuple
    frozen_sha256: dict           # file name in versions/ -> SHA-256 of the released artifact
    routine_manifest: tuple       # routines changed or new since previous, in dependency order
    new_routines: tuple           # the manifest routines that previous does not have
    new_privileges: tuple         # the privilege statements on new routines, exactly as src/ has them
    repairs: str                  # fragment in devops/, loaded after the definitions
    sources_without_inner_guards: tuple = ()  # embedded updates with no native guard after their opening prefix
    schema: str = None            # fragment in devops/ with the catalog DDL, loaded after the routines
    changed_tables: tuple = ()    # sql_saga tables whose CREATE TABLE the schema fragment changes
    # Routines whose signature changes: ((old key, new key), ...). The update refuses when the site changed the old
    # routine's EXECUTE privileges (dropping it would lose them silently), drops it and creates the new one; the new key
    # must also be in routine_manifest and new_routines.
    replaced_routines: tuple = ()
    # Routines whose COMMENT ON FUNCTION the release adds or changes (routine names, e.g. 'sql_saga.add_era'). The
    # statements are taken from src/ and emitted after the routines; any other COMMENT change fails the build.
    commented_routines: tuple = ()

    @property
    def native_guard_anchors(self):
        p = self.previous
        return ("native_library_version() <> '%s'" % p, 'extension update version ' + p, 'sql_saga %s native library' % p,
                "UPDATE TO ''%s''" % p, "'sql_saga %s upgrade'" % p)

    def delta(self, src, frozen):
        old = g.split_sql(frozen)
        current = g.split_sql(''.join(p.read_text() for p in sorted(Path(src).glob('[0-9][a-z]_*.sql'))))

        old_keys = {o for o, _ in self.replaced_routines}

        def remainder(statements):
            return [g._core(s).strip() for s in statements
                    if routine_key(s) not in self.routine_manifest and routine_key(s) not in old_keys
                    and table_key(s) not in self.changed_tables]

        def mentions_replaced(s):
            return any(k.split('(')[0] + '(' in s for k in old_keys)

        def comment_of(s):
            m = re.match(r'COMMENT ON (?:FUNCTION|PROCEDURE) (sql_saga\.[A-Za-z_0-9]+)\b', s)
            return m.group(1) if m and m.group(1) in self.commented_routines else None
        comments = [s for s in remainder(current) if comment_of(s)]
        if sorted({comment_of(s) for s in comments}) != sorted(self.commented_routines):
            raise GeneratorError('declared %s comments not all found in the source' % self.target)
        declared = set(self.new_privileges) | set(comments)
        old_rest = [s for s in remainder(old)
                    if not (mentions_replaced(s) and s not in remainder(current)) and not comment_of(s)]
        if old_rest != [s for s in remainder(current) if s not in declared]:
            raise GeneratorError('source changes outside the %s routine manifest' % self.target)
        if sorted(s for s in remainder(current) if s in self.new_privileges) != sorted(self.new_privileges):
            raise GeneratorError('expected exactly the %s privilege statements on new routines' % self.target)
        for table in self.changed_tables:
            before = [g._core(s).strip() for s in old if table_key(s) == table]
            after = [g._core(s).strip() for s in current if table_key(s) == table]
            if len(before) != 1 or len(after) != 1:
                raise GeneratorError('changed table must be created exactly once: ' + table)
            if before == after:
                raise GeneratorError('changed table unchanged since %s: %s' % (self.previous, table))

        chunks = []
        for old_key, new_key in self.replaced_routines:
            if new_key not in self.new_routines:
                raise GeneratorError('replacement routine must be a new routine: ' + new_key)
            if not [s for s in old if routine_key(s) == old_key]:
                raise GeneratorError('replaced routine not in %s: %s' % (self.previous, old_key))
            if [s for s in current if routine_key(s) == old_key]:
                raise GeneratorError('replaced routine still in the source: ' + old_key)
            chunks.append(replace_routine_sql(old_key, new_key, self.target))
        for key in self.routine_manifest:
            before = [s for s in old if routine_key(s) == key]
            after = [s for s in current if routine_key(s) == key]
            if len(after) != 1:
                raise GeneratorError('manifest routine must exist exactly once: ' + key)
            if key in self.new_routines:
                if before:
                    raise GeneratorError('new routine already exists in %s: %s' % (self.previous, key))
            else:
                if len(before) != 1:
                    raise GeneratorError('manifest routine must exist exactly once: ' + key)
                a, b = g._parse_routine(before[0]), g._parse_routine(after[0])
                if a['args'] != b['args'] or a['kind'] != b['kind'] or \
                        g.routine_signature(before[0])[2] != g.routine_signature(after[0])[2]:
                    raise GeneratorError('manifest routine signature changed: ' + key)
                if g._core(before[0]).strip() == g._core(after[0]).strip():
                    raise GeneratorError('manifest routine unchanged since %s: %s' % (self.previous, key))
            routine = g.to_create_or_replace(after[0])
            chunks.append(routine if routine.endswith('\n') else routine + '\n')
        for statement in self.new_privileges:
            chunks.append(statement + '\n')
        for statement in comments:
            chunks.append(statement + '\n')
        # A public function's comment is its worker's, copied by src/3h's idempotent block: rerun it when a worker's
        # comment is new or changed, or the public function keeps the old text (or none).
        if any(r.startswith('sql_saga.__api_') for r in self.commented_routines):
            copy = [s for s in current if '$copy_api_comments$' in g._core(s)]
            if len(copy) != 1:
                raise GeneratorError('the copy_api_comments block must exist exactly once')
            chunks.append(g._core(copy[0]).strip() + '\n')
        return ''.join(chunks)

    def embedded(self, text, require_anchors=True):
        """A frozen <source>--<previous> body, retargeting only native guards, actionable hints and the lock."""
        header, _, body = text.partition('\n')
        prefix = guard_prefix(self.previous)
        if header != release.header(self.previous) or not body.startswith(prefix) or not body.endswith(GATE + '\n'):
            raise GeneratorError('unexpected frozen %s update framing' % self.previous)
        body = body[len(prefix):-len(GATE + '\n')]
        lines = []
        changed = 0
        for line in body.splitlines(keepends=True):
            if self.previous in line and any(anchor in line for anchor in self.native_guard_anchors):
                line = line.replace(self.previous, self.target)
                changed += 1
            lines.append(line)
        if require_anchors and not changed:
            raise GeneratorError('no native guard anchors found')
        return ''.join(lines)

    def build(self, src, versions, devops, extversion):
        if extversion != self.target:
            raise GeneratorError('this generator builds %s only' % self.target)
        frozen = {n: release.authenticate(os.path.join(versions, n), h) for n, h in self.frozen_sha256.items()}
        change = self.delta(src, frozen['sql_saga--%s.sql' % self.previous])
        repairs = Path(devops, self.repairs).read_text()
        schema = Path(devops, self.schema).read_text() if self.schema else ''
        if self.changed_tables and not self.schema:
            raise GeneratorError('changed tables need a schema fragment')
        if GATE in repairs or GATE in schema:
            raise GeneratorError('the repairs and schema fragments must not contain the integrity gate')
        outputs = {}
        for source in self.sources:
            prior = '' if source == self.previous else self.embedded(
                frozen['sql_saga--%s--%s.sql' % (source, self.previous)], source not in self.sources_without_inner_guards)
            text = (release.header(self.target) + '\n' + guard_prefix(self.target) + prior + change + schema + repairs
                    + GATE + '\n')
            if text.splitlines().count(GATE) != 1:
                raise GeneratorError('expected exactly one integrity gate')
            outputs['sql_saga--%s--%s.sql' % (source, self.target)] = text
        return outputs

    def main(self, program):
        parser = argparse.ArgumentParser(description=__doc__)
        for name in ('src', 'versions', 'devops', 'outdir', 'extversion'):
            parser.add_argument('--' + name, required=True)
        a = parser.parse_args()
        try:
            g.write_atomically(a.outdir, self.build(a.src, a.versions, a.devops, a.extversion))
        except GeneratorError as error:
            sys.exit(program + ': ' + str(error))
