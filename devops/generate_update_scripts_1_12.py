#!/usr/bin/env python3
"""Direct 1.0..1.11 to 1.12 updates from authenticated v1.11 artifacts (devops/direct_update.py builds them).

Specified by devops/test_generate_update_scripts_1_12.py. The 1.12 delta: the PL/pgSQL planner removes only top-level
payload keys whose value is JSON null. jsonb_strip_nulls also removed nulls nested inside a jsonb column's value under
MERGE_ENTITY_PATCH (stored [1, {"k": null}] as [1, {}]) and merged adjacent pieces that differed only by such a null.
The native planner was already right. No catalog change, no native library change.
"""
from direct_update import Release, GeneratorError  # noqa: F401  (GeneratorError: for the tests)

RELEASE = Release(
    target='1.12',
    previous='1.11',
    sources=('1.0', '1.1', '1.2', '1.3', '1.4', '1.5', '1.6', '1.7', '1.8', '1.9', '1.10', '1.11'),
    # Produced only from git archive 61343fa (v1.11), using that archive's generator; two builds were identical.
    frozen_sha256={
        'sql_saga--1.11.sql': '106050bc588906441140ae75023b3096a596618f200147123b41c829955ee0ed',
        'sql_saga--1.0--1.11.sql': 'aac8892a24df9bc7b6e297511b1e3426da6762bc7d4922986c8967d00881be05',
        'sql_saga--1.1--1.11.sql': '829c63419952ee6184acee5b3d30998d137d1eb396c9e3f49c4d30f5031c0fc9',
        'sql_saga--1.2--1.11.sql': '66eaad12650b966512315adee9a8293f3c6bfd2706258617e4ba674b15e951c6',
        'sql_saga--1.3--1.11.sql': '7535c034e21b4289cf5173053f59c84ef3f0bf8dd62a7ac18a7b601661160d3f',
        'sql_saga--1.4--1.11.sql': '6186c5dc54ff6b6c430b7d33ca170a4065c124dffae5a9c0172a94ad6ba1b1c4',
        'sql_saga--1.5--1.11.sql': 'd0ee150ee93f865cc41dfcf09e8a5c885485c2ebe7daf3f4708183740a8f501c',
        'sql_saga--1.6--1.11.sql': '659ab7bd8fffd7e3dc97f2c87d8503ce16aef07d5a946bbf95fa88b8d7109a25',
        'sql_saga--1.7--1.11.sql': '96a54492794723ff3044d036f7835c8ddd6bc1c2579660aa1694f8de1ad0a176',
        'sql_saga--1.8--1.11.sql': 'f1f9ecf499fac4233c13265f5030e0aa183fcbcdaa2f2f5267c82ec1a42a2900',
        'sql_saga--1.9--1.11.sql': '55a1cc36dd5ed22b0b0f69a91c856ed120a5f2a678ea812f89ade469902bce17',
        'sql_saga--1.10--1.11.sql': 'b5dfbf2b8f5104534b809fa7c656ba771e57262aa688175e3ab93e68129d0c65',
    },
    routine_manifest=(
        'sql_saga.temporal_merge_plan(regclass, regclass, sql_saga.temporal_merge_mode, name, text[], name, name, sql_saga.temporal_merge_delete_mode, jsonb, text[], boolean, boolean)',
    ),
    new_routines=(),
    new_privileges=(),
    repairs='upgrade_1_12_repairs.sql',
    # Embedded X--1.11 bodies with no native guard after their opening prefix (as for the X--1.10 bodies in 1.11).
    sources_without_inner_guards=('1.4', '1.5', '1.6', '1.7', '1.8', '1.9', '1.10'),
)
build = RELEASE.build

if __name__ == '__main__':
    RELEASE.main('generate_update_scripts_1_12')
