#!/usr/bin/env python3
"""Direct 1.0..1.12 to 1.13 updates from authenticated v1.12 artifacts (devops/direct_update.py builds them).

Specified by devops/test_generate_update_scripts_1_13.py. The 1.13 delta: drop protection refuses user DDL that drops an
era's bounds check (NOT isempty), its boundary check (from < until), its synchronization trigger, or its range or
valid_to column (also through CASCADE from the column or the generated function). Until 1.12 those drops went through
and left the era unenforced until check_integrity was run. drop_synchronize_temporal_columns_trigger clears the trigger
name in the catalog before it drops the trigger (metadata first, as the other drop APIs). No catalog change, no native
library change.
"""
from direct_update import Release, GeneratorError  # noqa: F401  (GeneratorError: for the tests)

RELEASE = Release(
    target='1.13',
    previous='1.12',
    sources=('1.0', '1.1', '1.2', '1.3', '1.4', '1.5', '1.6', '1.7', '1.8', '1.9', '1.10', '1.11', '1.12'),
    # Produced only from git archive 676370d (v1.12), using that archive's generator; two builds were identical.
    frozen_sha256={
        'sql_saga--1.12.sql': 'e07fb60b6f0a543438cf1b3155cd73d4e04c3093c1fb4f12634af455f49b3eac',
        'sql_saga--1.0--1.12.sql': '8b7040cf294063d9f8be4a9e1ff976b8a37d65310ae770db3cba08e18fbe21c1',
        'sql_saga--1.1--1.12.sql': '8e5ce785ef239e6156d840514c09f66ff1643ce56d20b7d88bda82c729d4919a',
        'sql_saga--1.2--1.12.sql': 'b6432045f893685b38d6e9e7a5d40f44396cf9533f9d5aa857824dbdb373b9e0',
        'sql_saga--1.3--1.12.sql': '9cae933273f56518954125a491aede10a9f37dfba2b4077f12f9329cc564ecd2',
        'sql_saga--1.4--1.12.sql': '73f63cb95f3b38476be01ecddad5e071c158e452f43d567100990219d524822c',
        'sql_saga--1.5--1.12.sql': '2b1ecc5b08d3f39293af2d43746a03676899f2f308e6ef457c8daa587952a349',
        'sql_saga--1.6--1.12.sql': '54b0625fc0f2983c7e5bea8fed7c1b0e86a396ffb52e8aa605de4a5c175ef9d1',
        'sql_saga--1.7--1.12.sql': 'ee54ef070726c0159a621dc9f791e09896d08d0957ba967884ccb31215bc45f9',
        'sql_saga--1.8--1.12.sql': 'cd4ad318a531be3b56b35d72901d30531587d7e4e6032038a4406d6d451d8e04',
        'sql_saga--1.9--1.12.sql': 'cab96d904e39e1c6cf54a4d65b927a57b9b655a2bcbaef3ca194c6839408ed0e',
        'sql_saga--1.10--1.12.sql': '5a3c2ade6704e913d2ec27ec4e114a286cfd004c88501fa435aea6a94b6ee8a0',
        'sql_saga--1.11--1.12.sql': 'a28efdfca788ffd6ad705c73589eea129581ad0e35b244993dab4e0120d057c3',
    },
    routine_manifest=(
        'sql_saga.__internal_drop_protection()',
        'sql_saga.__api_drop_synchronize_temporal_columns_trigger(regclass, name)',
    ),
    new_routines=(),
    new_privileges=(),
    repairs='upgrade_1_13_repairs.sql',
    # Embedded X--1.12 bodies with no native guard after their opening prefix (as for the X--1.11 bodies in 1.12).
    sources_without_inner_guards=('1.4', '1.5', '1.6', '1.7', '1.8', '1.9', '1.10', '1.11'),
)
build = RELEASE.build

if __name__ == '__main__':
    RELEASE.main('generate_update_scripts_1_13')
