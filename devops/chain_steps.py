#!/usr/bin/env python3
"""The released single-release update steps 1.0--1.1 to 1.12--1.13 as chain steps (doc/design/update-chain.md).

Until 1.13 each release shipped direct update scripts; from 1.14 PostgreSQL runs one step per release in sequence. The
released single steps are reused as they were released, with exactly the changes listed in the design document:

  1. The native check accepts the step's release or newer (versions compared numerically).
  2. The final integrity gate runs only in the step that reaches the installed native library's version.
  3. A hint that names an update command says ALTER EXTENSION sql_saga UPDATE, without a version.
  4. The 1.0--1.1 and 1.1--1.2 steps carry the corrections the released 1.0/1.1--1.13 direct scripts made: the 1.3
     preamble first (its native check for the step's release), the rename_following state put back at the step's
     end, and in 1.0--1.1 the shared-helper validator fix and the ACL guard before the historical grants.

The released inputs are read from the git tags that shipped them and authenticated by SHA-256, so this script needs no
files in the working tree. The upgrade test checks that versions/ holds exactly its output.

Usage: devops/chain_steps.py --check DIR   (exit 1 when DIR's steps differ)
       devops/chain_steps.py --write DIR
Standard library only.
"""
import hashlib
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

RELEASES = ('1.0', '1.1', '1.2', '1.3', '1.4', '1.5', '1.6', '1.7', '1.8', '1.9', '1.10', '1.11', '1.12', '1.13')

# Released artifacts: (tag, path in the tag's tree or None for generated) -> SHA-256. 1.12--1.13 was generated at build
# time by the v1.13 generator (not committed); the v1.13 tree regenerates it byte-identically.
RELEASED = {
    'sql_saga--1.0--1.1.sql': ('v1.13', '37405fd81634b9cc7b088638334a9158c38252ce4d3ca69506d7fe1b2067c5f7'),
    'sql_saga--1.1--1.2.sql': ('v1.13', '3a33a535b57d1990f29358b5bfa6443a53217787766bd39abc78a8243a5260c5'),
    'sql_saga--1.2--1.3.sql': ('v1.13', 'd670f8e21f8a9b68afb7fbd3fb378eeb998419fef375268f7797f1cd7aae6aee'),
    'sql_saga--1.3--1.4.sql': ('v1.13', '20e21225e0fcac0c3fb4ebaf35b89a96d97d3f51d2a77e81f97b863285eebd84'),
    'sql_saga--1.4--1.5.sql': ('v1.13', 'e8dc9f74f6c0fd87ac9cca4964e04d1059da86f5973e5928ff2fd564ba2d8b3d'),
    'sql_saga--1.5--1.6.sql': ('v1.13', '9938d0c177816d421cf242c2c7441700e8e8222b5ec33e0a2a14c324453fc568'),
    'sql_saga--1.6--1.7.sql': ('v1.13', '5b835fefd6565c4772458c1ba0e445e316a400c6c11636e7851f737edd0e0676'),
    'sql_saga--1.7--1.8.sql': ('v1.13', '952e5555b1e9244de3c10e3114b393aed1888262561b0d20a0f2f68e4ca562c1'),
    'sql_saga--1.8--1.9.sql': ('v1.13', '9bda8cefd9fcb5aafd7b7152f8272295bb9292c405c5d5a7c9ced7f33b8cab1c'),
    'sql_saga--1.9--1.10.sql': ('v1.13', '384b7b1000a7b398423e23c5bc5905bfee6d93034295493e83e432355a0ecfb3'),
    'sql_saga--1.10--1.11.sql': ('v1.13', 'b5dfbf2b8f5104534b809fa7c656ba771e57262aa688175e3ab93e68129d0c65'),
    'sql_saga--1.11--1.12.sql': ('v1.13', 'a28efdfca788ffd6ad705c73589eea129581ad0e35b244993dab4e0120d057c3'),
    'sql_saga--1.12--1.13.sql': (None, '73361ed06d424b0b8e129574bf8a052fa8189104bb14930188bf06521e13fd91'),
    # The direct scripts that carry the 1.0/1.1 corrections.
    'sql_saga--1.0--1.2.sql': ('v1.13', '11b6174d2a428bed86e82fb5e7560ffa8a0f640affd8146c399c9e1dae49c321'),
    'sql_saga--1.0--1.3.sql': ('v1.13', '9646114bc85feabc9a03314da1a8298ab8c3607deb5bdc43eb27843ae1f002f1'),
    'sql_saga--1.1--1.3.sql': ('v1.13', '3b28f9bb314a75f3bae0cf363026e8e23fb1f5184e2aa8811dacb35fd67cd7d6'),
}

AT_LEAST = ("(SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array("
            "sql_saga.native_library_version(), '.')) AS x)"
            " < (SELECT pg_catalog.array_agg(x::int) FROM pg_catalog.unnest(pg_catalog.string_to_array('%s', '.')) AS x)")

# PostgreSQL sets extversion to each step's target before running it, so a step cannot see the final target. A plain
# ALTER EXTENSION sql_saga UPDATE goes to the control file's default_version, the installed native library's version;
# the step whose target is that version is the last one, and runs the gate.
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
GATE = 'SELECT sql_saga.check_integrity();\n'


class StepError(Exception):
    pass


_cache = {}


def released(name):
    """A released artifact, authenticated by SHA-256."""
    if name not in _cache:
        tag, sha = RELEASED[name]
        if tag:
            data = subprocess.run(['git', '-C', str(ROOT), 'show', '%s:versions/%s' % (tag, name)],
                                  check=True, capture_output=True).stdout
        else:
            data = _generated_1_13(name)
        actual = hashlib.sha256(data).hexdigest()
        if actual != sha:
            raise StepError('%s failed SHA-256 authentication: expected %s, got %s' % (name, sha, actual))
        _cache[name] = data.decode('utf-8')
    return _cache[name]


def _generated_1_13(name):
    """The 1.12--1.13 step as v1.13's generator built it, from a private copy of the v1.13 tree."""
    import shutil
    import tarfile
    import tempfile
    (ROOT / 'tmp').mkdir(exist_ok=True)
    tmp = Path(tempfile.mkdtemp(prefix='chain_steps_', dir=str(ROOT / 'tmp')))
    try:
        archive = subprocess.run(['git', '-C', str(ROOT), 'archive', '--format=tar', 'v1.13', 'src', 'devops', 'versions'],
                                 check=True, capture_output=True).stdout
        with tempfile.TemporaryFile() as f:
            f.write(archive)
            f.seek(0)
            with tarfile.open(fileobj=f) as tar:
                try:
                    tar.extractall(tmp, filter='data')
                except TypeError:
                    tar.extractall(tmp)
        (tmp / 'out').mkdir()
        subprocess.run([sys.executable, 'devops/generate_update_scripts_1_13.py', '--src', 'src', '--versions', 'versions',
                        '--devops', 'devops', '--outdir', 'out', '--extversion', '1.13'], cwd=tmp, check=True,
                       capture_output=True)
        return (tmp / 'out' / name).read_bytes()
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def once(text, old, new, what):
    n = text.count(old)
    if n != 1:
        raise StepError('%s: expected exactly one anchor, found %d' % (what, n))
    return text.replace(old, new)


def preamble_13(source, target):
    """The 1.3 preamble as the released <source>--1.3 direct script ran it (its lines 2 to 626), with its native check
    for <target>: it only reads the catalog and calls native_library_version, present since 1.1."""
    lines = released('sql_saga--%s--1.3.sql' % source).split('\n')
    pre = '\n'.join(lines[1:626]) + '\n'
    if not pre.startswith("SELECT pg_catalog.set_config('sql_saga.upgrade_from', '%s', true);" % source) \
            or not pre.rstrip().endswith('$refuse_view_dependents_1_0$;'):
        raise StepError('unexpected 1.3 preamble boundaries for ' + source)
    a = pre.index('DO $native_guard_1_3$')
    z = pre.index('$native_guard_1_3$;', a)
    guard = once(pre[a:z], "sql_saga.native_library_version() <> '1.3'", AT_LEAST % target, 'preamble guard ' + source)
    guard = once(guard, 'extension update version 1.3', 'extension update version ' + target, 'preamble message ' + source)
    guard = once(guard, 'the sql_saga 1.3 native library', 'the sql_saga %s native library' % target, 'preamble hint ' + source)
    return pre[:a] + guard + pre[z:]


def restore_13():
    """The released 1.2--1.3 step's restore of the rename_following state its preamble recorded."""
    m = re.findall(r'/\* 3\. sql_saga_rename_following back in the state.*?\$restore_rename_following\$;\n',
                   released('sql_saga--1.2--1.3.sql'), re.S)
    if len(m) != 1:
        raise StepError('expected one rename_following restore in the released 1.2--1.3')
    return m[0]


def chain_step(a, b):
    text = released('sql_saga--%s--%s.sql' % (a, b))
    # 1. Native check: this release or newer.
    text = once(text, "sql_saga.native_library_version() <> '%s'" % b, AT_LEAST % b, 'native check %s--%s' % (a, b))
    # 4. The 1.0/1.1 corrections.
    if (a, b) == ('1.0', '1.1'):
        ctx = '               pg_catalog.array_agg(c.name ORDER BY c.ordinality) AS stable_columns\n'
        old = ctx + '        FROM sql_saga.foreign_keys AS fk\n'
        new = ctx + '        FROM (SELECT DISTINCT f.fk_helper_function, f.unique_key_name, f.type FROM sql_saga.foreign_keys AS f) AS fk\n'
        if new not in released('sql_saga--1.0--1.2.sql'):
            raise StepError('released 1.0--1.2 does not carry the shared-helper fix')
        text = once(text, old, new, 'shared-helper fix')
        acl_guard = re.findall(r'DO \$acl_guard_1_0\$.*?\$acl_guard_1_0\$;\n', released('sql_saga--1.0--1.3.sql'), re.S)
        if len(acl_guard) != 1:
            raise StepError('expected one acl_guard_1_0 in the released 1.0--1.3')
        grant = 'GRANT EXECUTE ON FUNCTION sql_saga.for_portion_of_trigger() TO PUBLIC;\n'
        text = once(text, grant, acl_guard[0] + grant, 'acl guard')
    if a in ('1.0', '1.1'):
        header, _, body = text.partition('\n')
        text = header + '\n' + preamble_13(a, b) + body
        if GATE in text:
            text = once(text, GATE, restore_13() + GATE, 'restore %s--%s' % (a, b))
        else:
            text = text.rstrip('\n') + '\n' + restore_13()
    # 3. Hints name no version.
    text = re.sub(r"ALTER EXTENSION sql_saga UPDATE TO ''[0-9.]+''", 'ALTER EXTENSION sql_saga UPDATE', text)
    # 2. One gate, in the last step.
    if GATE in text:
        text = once(text, GATE, LAST_STEP_GATE, 'gate %s--%s' % (a, b))
    return text


def steps():
    return {'sql_saga--%s--%s.sql' % (a, b): chain_step(a, b) for a, b in zip(RELEASES, RELEASES[1:])}


def main(argv):
    if len(argv) != 3 or argv[1] not in ('--check', '--write'):
        sys.exit(__doc__)
    out = Path(argv[2])
    try:
        built = steps()
    except (StepError, subprocess.CalledProcessError) as error:
        sys.exit('chain_steps: %s' % error)
    if argv[1] == '--write':
        for name, text in built.items():
            (out / name).write_text(text)
        return
    differ = [n for n, t in built.items() if not (out / n).is_file() or (out / n).read_text() != t]
    for n in differ:
        print('chain_steps: %s differs from the documented transformation of the released step' % (out / n))
    sys.exit(1 if differ else 0)


if __name__ == '__main__':
    main(sys.argv)
