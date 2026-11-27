---
id: frb2e9
title: "Split CI into parallel jobs; the upgrade check as a matrix of releases"
state: "done"
type: chore
priority: ""
assignee: ["sabertooth"]
labels: []
group: ""
rank: "zzzzzzzv"
created: 2026-11-27
updated: 2026-11-27
model: "claude-opus"
done_at: "2026-11-27T12:54"
---

## Goal
CI runs tests, PL/pgSQL, restore and one upgrade job per released baseline in parallel, in about 30 instead of 86 minutes.

## Purpose
The single main job took 1 h 26 min, 50 of them for 16 upgrade paths run one after another; each path is independent.

## Principle
Same checks as today, none dropped: the artifact gate runs once, each baseline runs alone (UPGRADE_ONLY_BASELINES), every job must pass. Benchmarks stay out of CI (noisy shared runners, server settings).

## Details
Measured on CI 45924407021: regular tests 5.8 min, restore 29 min, upgrade 50 min (3-5 min per baseline, 16 baselines from 02:22 to 03:12), PL/pgSQL job 7.7 min. Plan: jobs tests (build, generator, native, symbols, fast tests, isolation, function attribution and regular FK concurrency), plpgsql, restore, upgrade-artifacts (UPGRADE_ONLY=artifacts and extension scripts), upgrade (matrix over the baselines from RELEASED_BASELINES, fail-fast off). The matrix list must come from the script, not be copied by hand, so a new release cannot be forgotten. GitHub free plan: 20 concurrent jobs for public repos.

## Depends on

## Acceptance
- [x] CI shows the parallel jobs, all green, the upgrade matrix one job per baseline generated from devops/upgrade_test.sh
- [x] total wall time measured and recorded

## Notes

check 1: CI 45992276515 at 9e8f540: 21 jobs green (Tests, Tests PL/pgSQL planner, Restore current + old_dumps, Upgrade artifacts, 16 'Upgrade from <baseline>' jobs from UPGRADE_ONLY=list_baselines). Follow-up fixes: 5575aa3 Tests job fetches tags for chain_steps --check; 9e8f540 drops the runner's Microsoft apt source (403 under parallel start).

check 2: Wall time 13m52s (11:39:33 to 11:53:25), down from about 86 min for the single job. Longest jobs: Restore (current/old_dumps) about 13 min; upgrade baselines 3-5 min each; Tests 5 min.


Reflection (open, 2026-11-27, sabertooth): The handoff named the likely cause (missing fetch-depth/fetch-tags) correctly. One gap: 'hug task done' on an untaken todo task fails with 'todo -> done is not a move', and the hint to take it first only appears after the check commands, so it cost one extra round.
## History
- 2026-11-27T12:54 todo -> doing/active (take)
- 2026-11-27T12:54 doing/active -> done (done); was doing_since: 2026-11-27T12:54
