---
id: frb2e9
title: "Split CI into parallel jobs; the upgrade check as a matrix of releases"
state: todo
type: chore
priority: ""
assignee: []
labels: []
group: ""
rank: "zzzzzzzv"
created: 2026-11-27
updated: 2026-11-27
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
- [ ] CI shows the parallel jobs, all green, the upgrade matrix one job per baseline generated from devops/upgrade_test.sh
- [ ] total wall time measured and recorded

## Notes
