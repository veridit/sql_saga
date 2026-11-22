#!/usr/bin/env python3
"""Direct 1.0..1.10 to 1.11 updates from authenticated v1.10 artifacts (devops/direct_update.py builds them).

Specified by devops/test_generate_update_scripts_1_11.py. The 1.11 delta: temporal_merge reads each data value of a
plan with the target column's type (jsonb_populate_record) instead of a text cast. Under the PL/pgSQL planner every
array, composite or jsonb-string column failed ("malformed array literal"); under the native planner a jsonb column of
a new entity's later slice was stored as a JSON string. The native planner now carries json and jsonb values as
themselves (native library change). No catalog change.
"""
from direct_update import Release, GeneratorError  # noqa: F401  (GeneratorError: for the tests)

RELEASE = Release(
    target='1.11',
    previous='1.10',
    sources=('1.0', '1.1', '1.2', '1.3', '1.4', '1.5', '1.6', '1.7', '1.8', '1.9', '1.10'),
    # Produced only from git archive 39da88a (v1.10), using that archive's generator; two builds were identical.
    frozen_sha256={
        'sql_saga--1.10.sql': '2f4c6cdb9fa68c0aa04197d96e09d109c597ec3ab11ae53e8ccebacd371dbf5b',
        'sql_saga--1.0--1.10.sql': '72285a9e0a59ef0effd43300558424eaecbceefb5b1c81aa22a226a0a157216e',
        'sql_saga--1.1--1.10.sql': 'bd48d6af52b3b71faf04805e36caf43dfd50eabf5fa34c72488e547332c23146',
        'sql_saga--1.2--1.10.sql': 'f6f0d0935cf8d2b93dbe6ed81234d1aebbb4ba706f0d04d0d660b8052bc12dac',
        'sql_saga--1.3--1.10.sql': 'c0c2581e0a0ff39dfd5175b065555abbd3e53e8927dad30298d26cc43ce1b4bf',
        'sql_saga--1.4--1.10.sql': '87db4b5cff1ea0c24c03064736c46ea45292492cc5a061c40f1133279e49ee29',
        'sql_saga--1.5--1.10.sql': '02f2d153cfaa817e4dfabbf78ba024ddd07041ec72cbb40007718994cdff43e8',
        'sql_saga--1.6--1.10.sql': '8e8fec0532bd6fa79ac3abcc93de1c3c2def326f8c3c45b8752f7beb6a861ed8',
        'sql_saga--1.7--1.10.sql': '1faeaf339a5f4ebce79831541c366d45e69d84eca728cae0ad72b97185febb11',
        'sql_saga--1.8--1.10.sql': '370e09311d7c6a5a4fde6b1e297f2396a73e6ff6b5f5dfd6c9565bfecb98b615',
        'sql_saga--1.9--1.10.sql': '384b7b1000a7b398423e23c5bc5905bfee6d93034295493e83e432355a0ecfb3',
    },
    routine_manifest=(
        'sql_saga.temporal_merge_execute(regclass, regclass, text[], sql_saga.temporal_merge_mode, name, name, name, boolean, text[], sql_saga.temporal_merge_delete_mode, boolean, name, name, name, name, text[], boolean)',
    ),
    new_routines=(),
    new_privileges=(),
    repairs='upgrade_1_11_repairs.sql',
    # Embedded X--1.10 bodies with no native guard after their opening prefix (as for the X--1.9 bodies in 1.10).
    sources_without_inner_guards=('1.4', '1.5', '1.6', '1.7', '1.8', '1.9'),
)
build = RELEASE.build

if __name__ == '__main__':
    RELEASE.main('generate_update_scripts_1_11')
