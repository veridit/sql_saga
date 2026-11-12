#!/usr/bin/env python3
"""Reject cascading DROP/TRUNCATE in already-extracted executable migration SQL.

upgrade_test.sh removes stored routine bodies and top-level comments/strings.
Retained DO bodies still contain comments and quoted values, which are opaque
here. Exit 0 means safe, 1 prohibited DDL, and 2 a scanner failure.
"""
import re
import sys

TOKEN = re.compile(r'''--[^\n]*|/\*|\$[A-Za-z_0-9]*\$|"(?:[^"]|"")*"|'(?:[^']|'')*'|[^\W\d][\w$]*|;|["']''')
COMMENT_BOUNDARY = re.compile(r'/\*|\*/')


def cascading_ddl(text):
    ddl = False
    position = 0
    do_tag = None
    expect_do_body = False
    while (match := TOKEN.search(text, position)) is not None:
        token = match.group()
        position = match.end()
        if token.startswith('--'):
            continue
        if token == '/*':
            # PostgreSQL block comments nest. Quotes and semicolons inside them
            # must not change the surrounding statement's lexical state.
            depth = 1
            while depth:
                boundary = COMMENT_BOUNDARY.search(text, position)
                if boundary is None:
                    raise ValueError('unterminated block comment in migration SQL')
                depth += 1 if boundary.group() == '/*' else -1
                position = boundary.end()
            continue
        if token.startswith('$'):
            if expect_do_body:
                do_tag = token
                expect_do_body = False
            elif token == do_tag:
                do_tag = None
            else:
                # DO code is executable, but its nested dollar-quoted values
                # (including function-definition replacement fragments) are not.
                end = text.find(token, position)
                if end < 0:
                    raise ValueError('unterminated dollar quote in migration SQL')
                position = end + len(token)
            continue
        if token.upper() == 'DO':
            expect_do_body = True
        if token in ('"', "'"):
            raise ValueError('unterminated quote in migration SQL')
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
