#!/usr/bin/env python3
"""Reject cascading DROP/TRUNCATE in already-extracted executable migration SQL.

Not a SQL lexer: upgrade_test.sh retains responsibility for removing comments,
strings and stored routine bodies. Quoted identifiers and string placeholders
are opaque tokens here (DO bodies can still contain quoted catalog values).
Exit 0 means safe, 1 prohibited DDL, and 2 a scanner failure.
"""
import re
import sys

TOKEN = re.compile(r'''"(?:[^"]|"")*"|'(?:[^']|'')*'|[^\W\d][\w$]*|;''')


def cascading_ddl(text):
    ddl = False
    for match in TOKEN.finditer(text):
        token = match.group()
        if token == ';':
            ddl = False
        elif token.upper() in ('DROP', 'TRUNCATE'):
            ddl = True
        elif ddl and token.upper() == 'CASCADE':
            return True
    return False


def main():
    try:
        return 1 if cascading_ddl(sys.stdin.read()) else 0
    except Exception as error:
        print('migration CASCADE scanner failed: %s' % error, file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
