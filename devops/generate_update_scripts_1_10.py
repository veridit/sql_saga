#!/usr/bin/env python3
"""Direct 1.0..1.9 to 1.10 updates from authenticated v1.9 artifacts (devops/direct_update.py builds them).

Specified by devops/test_generate_update_scripts_1_10.py. The 1.10 delta corrects the 1.9 provenance trigger
(doc/design/provenance.md): a DEFAULT keeps its column's type (a jsonb DEFAULT was stored as a JSON string), a
provenance column whose DEFAULT was dropped refuses writes instead of storing NULL, and a generated column is refused
at registration; check_integrity reports a provenance column without a DEFAULT. No catalog change.
"""
from direct_update import Release, GeneratorError  # noqa: F401  (GeneratorError: for the tests)

RELEASE = Release(
    target='1.10',
    previous='1.9',
    sources=('1.0', '1.1', '1.2', '1.3', '1.4', '1.5', '1.6', '1.7', '1.8', '1.9'),
    # Produced only from git archive fb9b351 (v1.9), using that archive's generator; two builds were identical.
    frozen_sha256={
        'sql_saga--1.9.sql': 'ba9375355b5f8354fe30682b3a683b0b474296b25d77c32dae14746eec1de921',
        'sql_saga--1.0--1.9.sql': '8ffd40dda12b1daf865ba1a57b33ecd2ff72bf89b4aacd695cf078f9e1bf14a5',
        'sql_saga--1.1--1.9.sql': 'c610dada55d6107ec5d5734d5ecadcd1a7038ea7ddaca3c9d116c4a6d37eca8e',
        'sql_saga--1.2--1.9.sql': '146362b49978ee49a8ad42ca7ba098335cd48828463ef4853ef2fda0e6a0ddaf',
        'sql_saga--1.3--1.9.sql': 'cdda2e13e4b4b4155b0e643e92f8fa68e6ca1ef9fad79ab838ebd904abf26de5',
        'sql_saga--1.4--1.9.sql': 'bf6848b826afa3383da09bbcb4866bb9c79ebee71aae0b67109c149d85d4eb32',
        'sql_saga--1.5--1.9.sql': '0d09edf00c7dee3aab0094bc216aed5347df7e66013926b3a69f941d0eae4ef1',
        'sql_saga--1.6--1.9.sql': 'a2d96d28fc013f7f0f7c974c00d81a495ac0f19099d1912161f5232c24b51c6a',
        'sql_saga--1.7--1.9.sql': 'c3add37126da415c671ff7b4e325eac85552c7f15f80098cd731939f98490947',
        'sql_saga--1.8--1.9.sql': '9bda8cefd9fcb5aafd7b7152f8272295bb9292c405c5d5a7c9ced7f33b8cab1c',
    },
    routine_manifest=(
        'sql_saga.provenance_trigger()',
        'sql_saga.__internal_set_era_provenance_columns(regclass, name, name[])',
        'sql_saga.check_integrity()',
    ),
    new_routines=(),
    new_privileges=(),
    repairs='upgrade_1_10_repairs.sql',
    # Embedded X--1.9 bodies with no native guard after their opening prefix (as for the X--1.8 bodies in 1.9).
    sources_without_inner_guards=('1.4', '1.5', '1.6', '1.7', '1.8'),
)
build = RELEASE.build

if __name__ == '__main__':
    RELEASE.main('generate_update_scripts_1_10')
