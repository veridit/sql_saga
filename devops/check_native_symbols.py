#!/usr/bin/env python3
"""Every C function any shipped script binds to must exist in the built libraries (doc/design/update-chain.md).

A released update step creates functions with LANGUAGE c AS '<library>', '<symbol>'; PostgreSQL resolves the symbol
when the step runs, with the newest library installed. So a symbol any released script references is never removed:
a retired function stays as a stub that raises an error.

Usage: devops/check_native_symbols.py <sql_saga library> <sql_saga_native library> <script>...
Standard library and nm only.
"""
import re
import subprocess
import sys

BINDING = re.compile(r"LANGUAGE\s+c\b[^;]*?\bAS\s+'([^']+)'\s*,\s*'([A-Za-z_0-9]+)'", re.I | re.S)
BINDING_REVERSED = re.compile(r"\bAS\s+'([^']+)'\s*,\s*'([A-Za-z_0-9]+)'[^;]*?LANGUAGE\s+c\b", re.I | re.S)
LIBRARIES = {'sql_saga': 0, '$libdir/sql_saga': 0, 'MODULE_PATHNAME': 0, 'sql_saga_native': 1, '$libdir/sql_saga_native': 1}


def exported(path):
    out = subprocess.run(['nm', '-g', path], check=True, capture_output=True, text=True).stdout
    names = set()
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[-2] in ('T', 't', 'D'):
            names.add(parts[-1].lstrip('_') if parts[-1].startswith('_') and sys.platform == 'darwin' else parts[-1])
    return names


def main(argv):
    if len(argv) < 4:
        sys.exit(__doc__)
    have = [exported(argv[1]), exported(argv[2])]
    wanted = {}
    for script in argv[3:]:
        text = open(script).read()
        for lib, symbol in BINDING.findall(text) + BINDING_REVERSED.findall(text):
            if lib not in LIBRARIES:
                sys.exit('check_native_symbols: %s binds to unknown library %r' % (script, lib))
            wanted.setdefault((LIBRARIES[lib], symbol), script)
    missing = sorted((('sql_saga', 'sql_saga_native')[i], s, f) for (i, s), f in wanted.items() if s not in have[i])
    for lib, symbol, script in missing:
        print('check_native_symbols: %s has no symbol %s (bound by %s)' % (lib, symbol, script))
    if missing:
        sys.exit(1)
    print('check_native_symbols: all %d C functions bound by %d scripts exist' % (len(wanted), len(argv) - 3))


if __name__ == '__main__':
    main(sys.argv)
