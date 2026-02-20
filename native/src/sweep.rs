use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::types::*;

/// Main entry point: run the sweep-line algorithm over source and target rows.
/// Returns a Vec of PlanRow ready for insertion into pg_temp.temporal_merge_plan.
pub fn sweep_line_plan(
    source_rows: Vec<SourceRow>,
    target_rows: Vec<TargetRow>,
    ctx: &PlannerContext,
) -> Vec<PlanRow> {
    // Phase 1: Entity correlation — match source rows to target entities
    let matched_sources = correlate_entities(&source_rows, &target_rows, ctx);

    // Phase 2: Detect eclipsed source rows
    let matched_sources = detect_eclipsed(matched_sources, ctx);

    // Phase 3: Group by entity
    let entity_groups = group_by_entity(matched_sources, &target_rows, ctx);

    // Phase 4: Per-entity sweep-line processing
    let mut all_plan_rows: Vec<PlanRow> = Vec::new();

    for (_key, group) in &entity_groups {
        // Skip entities where all source rows have early feedback
        let active_sources: Vec<&MatchedSourceRow> = group
            .source_rows
            .iter()
            .filter(|s| s.early_feedback.is_none() && !s.is_eclipsed)
            .collect();

        // Emit early feedback rows (errors, skips)
        for sr in &group.source_rows {
            if let Some(ref fb) = sr.early_feedback {
                all_plan_rows.push(make_feedback_plan_row(sr, fb, ctx));
            } else if sr.is_eclipsed {
                all_plan_rows.push(make_feedback_plan_row(
                    sr,
                    &EarlyFeedback {
                        action: PlanAction::SkipEclipsed,
                        message: None,
                    },
                    ctx,
                ));
            }
        }

        // Apply mode-specific filtering
        let active_sources = filter_by_mode(&active_sources, group, ctx);
        if active_sources.is_empty() && group.target_rows.is_empty() {
            continue;
        }

        // Atomic segmentation
        let segments = build_atomic_segments(group, &active_sources, ctx);

        // Payload resolution
        let resolved = resolve_payloads(segments, &active_sources, &group.target_rows, ctx);

        // Coalescing
        let coalesced = coalesce_segments(resolved, ctx);

        // Diff and classify
        let diff_rows = compute_diff(coalesced, &group.target_rows, ctx);

        // Classify operations
        let plan_rows = classify_operations(diff_rows, group, ctx);
        all_plan_rows.extend(plan_rows);
    }

    // Phase 5: Statement sequencing
    sequence_statements(&mut all_plan_rows, ctx);

    all_plan_rows
}

// ── Phase 1: Entity Correlation ──

fn correlate_entities(
    source_rows: &[SourceRow],
    target_rows: &[TargetRow],
    ctx: &PlannerContext,
) -> Vec<MatchedSourceRow> {
    // Build a lookup index: NK → target rows
    let mut target_by_nk: HashMap<String, Vec<(usize, &TargetRow)>> = HashMap::new();
    for (i, tr) in target_rows.iter().enumerate() {
        let nk_key = json_map_to_key(&tr.lookup_keys);
        target_by_nk.entry(nk_key).or_default().push((i, tr));
    }

    // Also index by identity columns
    let mut target_by_id: HashMap<String, Vec<(usize, &TargetRow)>> = HashMap::new();
    for (i, tr) in target_rows.iter().enumerate() {
        let id_key = json_map_to_key(&tr.identity_keys);
        if !id_key.is_empty() {
            target_by_id.entry(id_key).or_default().push((i, tr));
        }
    }

    let mut matched = Vec::with_capacity(source_rows.len());
    for sr in source_rows {
        let mut is_new = true;
        let mut discovered_identity = None;
        let mut canonical_nk = None;
        let mut early_feedback = None;
        // Try identity key match first
        if !sr.identity_keys.is_empty() {
            let id_key = json_map_to_key(&sr.identity_keys);
            if let Some(targets) = target_by_id.get(&id_key) {
                if !targets.is_empty() {
                    is_new = false;
                    discovered_identity = Some(sr.identity_keys.clone());
                }
            }
        }

        // Try NK match if identity didn't match
        if is_new && !sr.lookup_keys.is_empty() && !sr.lookup_cols_are_null {
            let nk_key = json_map_to_key(&sr.lookup_keys);
            if let Some(targets) = target_by_nk.get(&nk_key) {
                // Collect distinct entities matched
                let mut distinct_entities: BTreeSet<String> = BTreeSet::new();
                for (_i, tr) in targets {
                    distinct_entities.insert(json_map_to_key(&tr.identity_keys));
                }
                let match_count = distinct_entities.len();

                if match_count == 1 {
                    is_new = false;
                    discovered_identity = Some(targets[0].1.identity_keys.clone());
                    canonical_nk = Some(strip_nulls(&sr.lookup_keys));
                } else if match_count > 1 {
                    // Ambiguous match
                    early_feedback = Some(EarlyFeedback {
                        action: PlanAction::Error,
                        message: Some(format!(
                            "Ambiguous entity match: source row {} matches {} distinct target entities",
                            sr.row_id, match_count
                        )),
                    });
                }
            }
        }

        // For new entities, check identifiability
        // In founding mode, rows are always identifiable via the founding_id grouping key
        if is_new && !sr.is_identifiable && sr.lookup_cols_are_null && !ctx.is_founding_mode() && early_feedback.is_none() {
            early_feedback = Some(EarlyFeedback {
                action: PlanAction::Error,
                message: Some(format!(
                    "New entity from source row {} is not identifiable (no identity or lookup keys provided)",
                    sr.row_id
                )),
            });
        }

        // Build grouping key
        let grouping_key = build_grouping_key(sr, is_new, &discovered_identity, &canonical_nk, ctx);

        matched.push(MatchedSourceRow {
            source: sr.clone(),
            is_new_entity: is_new,
            grouping_key,
            discovered_identity,
            canonical_nk_json: canonical_nk,
            early_feedback,
            is_eclipsed: false,
        });
    }

    matched
}

fn build_grouping_key(
    sr: &SourceRow,
    is_new: bool,
    discovered_identity: &Option<serde_json::Map<String, serde_json::Value>>,
    canonical_nk: &Option<serde_json::Map<String, serde_json::Value>>,
    ctx: &PlannerContext,
) -> String {
    if !is_new {
        // Existing entity: use identity columns
        let id_map = discovered_identity.as_ref().unwrap_or(&sr.identity_keys);
        let key_parts: Vec<String> = ctx
            .identity_columns
            .iter()
            .map(|c| {
                id_map
                    .get(c)
                    .map(|v| json_value_to_str(v))
                    .unwrap_or_else(|| "_NULL_".to_string())
            })
            .collect();
        format!("existing_entity__{}", key_parts.join("__"))
    } else if ctx.is_founding_mode() {
        format!("new_entity__{}", sr.causal_id)
    } else {
        // New entity: use NK or causal_id
        let nk = canonical_nk
            .as_ref()
            .or(Some(&sr.lookup_keys))
            .filter(|m| !m.is_empty());
        match nk {
            Some(nk_map) => {
                let key_parts: Vec<String> = ctx
                    .all_lookup_cols
                    .iter()
                    .map(|c| {
                        nk_map
                            .get(c)
                            .map(|v| json_value_to_str(v))
                            .unwrap_or_else(|| "_NULL_".to_string())
                    })
                    .collect();
                format!("new_entity__{}", key_parts.join("__"))
            }
            None => format!("new_entity__{}", sr.causal_id),
        }
    }
}

// ── Phase 2: Eclipse Detection ──

fn detect_eclipsed(
    mut matched: Vec<MatchedSourceRow>,
    _ctx: &PlannerContext,
) -> Vec<MatchedSourceRow> {
    // Group by grouping_key
    let mut by_group: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, m) in matched.iter().enumerate() {
        by_group.entry(m.grouping_key.clone()).or_default().push(i);
    }

    for (_key, indices) in &by_group {
        if indices.len() <= 1 {
            continue;
        }
        // Sort by valid_from, then by row_id (later rows have priority)
        let mut sorted = indices.clone();
        sorted.sort_by(|&a, &b| {
            matched[a]
                .source
                .valid_from
                .cmp(&matched[b].source.valid_from)
                .then(matched[a].source.row_id.cmp(&matched[b].source.row_id))
        });

        // Simple eclipse check: a row is eclipsed if a later row completely covers it
        for i in 0..sorted.len() {
            let idx_i = sorted[i];
            if matched[idx_i].is_eclipsed || matched[idx_i].early_feedback.is_some() {
                continue;
            }
            for j in (i + 1)..sorted.len() {
                let idx_j = sorted[j];
                if matched[idx_j].early_feedback.is_some() {
                    continue;
                }
                // Check if row j completely covers row i
                if matched[idx_j].source.valid_from <= matched[idx_i].source.valid_from
                    && matched[idx_j].source.valid_until >= matched[idx_i].source.valid_until
                {
                    matched[idx_i].is_eclipsed = true;
                    break;
                }
            }
        }
    }

    matched
}

// ── Phase 3: Group by Entity ──

fn group_by_entity(
    matched_sources: Vec<MatchedSourceRow>,
    target_rows: &[TargetRow],
    ctx: &PlannerContext,
) -> BTreeMap<String, EntityGroup> {
    let mut groups: BTreeMap<String, EntityGroup> = BTreeMap::new();

    // Add source rows to their groups
    for ms in matched_sources {
        let group = groups.entry(ms.grouping_key.clone()).or_insert_with(|| {
            EntityGroup {
                grouping_key: ms.grouping_key.clone(),
                is_new_entity: ms.is_new_entity,
                identity_keys: ms
                    .discovered_identity
                    .clone()
                    .unwrap_or_else(|| ms.source.identity_keys.clone()),
                source_rows: Vec::new(),
                target_rows: Vec::new(),
                time_boundaries: BTreeSet::new(),
            }
        });
        group.source_rows.push(ms);
    }

    // Add target rows to matching groups
    for tr in target_rows {
        let id_key_parts: Vec<String> = ctx
            .identity_columns
            .iter()
            .map(|c| {
                tr.identity_keys
                    .get(c)
                    .map(|v| json_value_to_str(v))
                    .unwrap_or_else(|| "_NULL_".to_string())
            })
            .collect();
        let grouping_key = format!("existing_entity__{}", id_key_parts.join("__"));

        if let Some(group) = groups.get_mut(&grouping_key) {
            group.target_rows.push(tr.clone());
        } else if ctx.delete_mode.deletes_entities() {
            // Target entity not in source — may need deletion
            groups.insert(
                grouping_key.clone(),
                EntityGroup {
                    grouping_key,
                    is_new_entity: false,
                    identity_keys: tr.identity_keys.clone(),
                    source_rows: Vec::new(),
                    target_rows: vec![tr.clone()],
                    time_boundaries: BTreeSet::new(),
                },
            );
        }
    }

    groups
}

// ── Mode-specific filtering ──

fn filter_by_mode<'a>(
    active_sources: &[&'a MatchedSourceRow],
    _group: &EntityGroup,
    ctx: &PlannerContext,
) -> Vec<&'a MatchedSourceRow> {
    match ctx.mode {
        MergeMode::InsertNewEntities => {
            // Only keep new entities
            active_sources
                .iter()
                .filter(|s| s.is_new_entity)
                .copied()
                .collect()
        }
        MergeMode::UpdateForPortionOf
        | MergeMode::PatchForPortionOf
        | MergeMode::ReplaceForPortionOf
        | MergeMode::DeleteForPortionOf => {
            // Only keep existing entities
            active_sources
                .iter()
                .filter(|s| !s.is_new_entity)
                .copied()
                .collect()
        }
        _ => active_sources.to_vec(),
    }
}

// ── Phase 4a: Atomic Segmentation ──

fn build_atomic_segments(
    group: &EntityGroup,
    active_sources: &[&MatchedSourceRow],
    _ctx: &PlannerContext,
) -> Vec<AtomicSegment> {
    // Collect all time boundaries
    let mut boundaries: BTreeSet<String> = BTreeSet::new();

    for sr in active_sources {
        boundaries.insert(sr.source.valid_from.clone());
        boundaries.insert(sr.source.valid_until.clone());
    }
    for tr in &group.target_rows {
        boundaries.insert(tr.valid_from.clone());
        boundaries.insert(tr.valid_until.clone());
    }

    // Create segments between consecutive boundaries
    let points: Vec<&String> = boundaries.iter().collect();
    let mut segments = Vec::new();

    for window in points.windows(2) {
        let from = window[0];
        let until = window[1];
        if from >= until {
            continue;
        }
        segments.push(AtomicSegment {
            grouping_key: group.grouping_key.clone(),
            valid_from: from.clone(),
            valid_until: until.clone(),
            is_new_entity: group.is_new_entity,
            identity_keys: group.identity_keys.clone(),
            causal_id: active_sources.first().map(|s| s.source.causal_id.clone()),
        });
    }

    segments
}

// ── Phase 4b: Payload Resolution ──

fn resolve_payloads(
    segments: Vec<AtomicSegment>,
    active_sources: &[&MatchedSourceRow],
    target_rows: &[TargetRow],
    ctx: &PlannerContext,
) -> Vec<ResolvedSegment> {
    // Collect all source row_ids for the entity (used for target-only segments)
    let entity_row_ids: Vec<i64> = active_sources.iter().map(|s| s.source.row_id).collect();

    let mut resolved = Vec::with_capacity(segments.len());

    for seg in segments {
        // Find covering source rows (source interval contains segment)
        let mut covering_sources: Vec<&MatchedSourceRow> = active_sources
            .iter()
            .filter(|s| s.source.valid_from <= seg.valid_from && s.source.valid_until >= seg.valid_until)
            .copied()
            .collect();
        // Sort by row_id for deterministic payload resolution
        covering_sources.sort_by_key(|s| s.source.row_id);

        // Find covering target row
        let covering_target = target_rows
            .iter()
            .find(|t| t.valid_from <= seg.valid_from && t.valid_until >= seg.valid_until);

        // Resolve payload: source wins
        let (data_payload, mut row_ids) = resolve_source_payload(&covering_sources, covering_target, ctx);

        // For target-only segments, propagate entity-level source row_ids
        // (the source rows triggered this entity to be re-evaluated)
        if covering_sources.is_empty() && !entity_row_ids.is_empty() {
            row_ids = entity_row_ids.clone();
        }

        let source_from = covering_sources.first().map(|s| s.source.valid_from.clone());
        let source_until = covering_sources.last().map(|s| s.source.valid_until.clone());
        let target_from = covering_target.map(|t| t.valid_from.clone());
        let target_until = covering_target.map(|t| t.valid_until.clone());

        // Compute data hash for coalescing (excluding ephemeral columns)
        // Uses xxh3 (non-cryptographic, ~10x faster than MD5)
        let data_hash = data_payload.as_ref().map(|p| {
            let stripped = strip_nulls(p);
            let serialized = serde_json::to_string(&serde_json::Value::Object(stripped)).unwrap_or_default();
            format!("{:016x}", xxhash_rust::xxh3::xxh3_64(serialized.as_bytes()))
        });

        let ephemeral_payload = if !covering_sources.is_empty() {
            Some(covering_sources.last().unwrap().source.ephemeral_payload.clone())
        } else {
            covering_target.map(|t| t.ephemeral_payload.clone())
        };

        let target_data = covering_target.map(|t| t.data_payload.clone());

        // Skip segments with no source or target coverage
        if data_payload.is_none() && covering_target.is_none() {
            continue;
        }

        resolved.push(ResolvedSegment {
            grouping_key: seg.grouping_key,
            valid_from: seg.valid_from,
            valid_until: seg.valid_until,
            is_new_entity: seg.is_new_entity,
            identity_keys: seg.identity_keys,
            causal_id: seg.causal_id,
            row_ids,
            source_valid_from: source_from,
            source_valid_until: source_until,
            target_valid_from: target_from,
            target_valid_until: target_until,
            data_payload,
            ephemeral_payload,
            target_data_payload: target_data,
            data_hash,
        });
    }

    resolved
}

fn resolve_source_payload(
    covering_sources: &[&MatchedSourceRow],
    covering_target: Option<&TargetRow>,
    ctx: &PlannerContext,
) -> (Option<serde_json::Map<String, serde_json::Value>>, Vec<i64>) {
    let mut row_ids = Vec::new();

    if covering_sources.is_empty() {
        // Target-only segment
        return (covering_target.map(|t| t.data_payload.clone()), row_ids);
    }

    // Start with target payload (if any) as base
    let mut merged = covering_target
        .map(|t| t.data_payload.clone())
        .unwrap_or_default();

    // Apply source rows in order (later rows override)
    for sr in covering_sources {
        row_ids.push(sr.source.row_id);

        if ctx.mode.is_patch() {
            // PATCH mode: strip nulls from source before merging
            let stripped = strip_nulls(&sr.source.data_payload);
            for (k, v) in stripped {
                merged.insert(k, v);
            }
        } else {
            // UPSERT/REPLACE mode: source values override including nulls
            for (k, v) in &sr.source.data_payload {
                merged.insert(k.clone(), v.clone());
            }
        }
    }

    (Some(merged), row_ids)
}

// ── Phase 4c: Coalescing ──

fn coalesce_segments(
    resolved: Vec<ResolvedSegment>,
    _ctx: &PlannerContext,
) -> Vec<CoalescedSegment> {
    if resolved.is_empty() {
        return Vec::new();
    }

    let mut coalesced = Vec::new();
    let mut current: Option<CoalescedSegment> = None;

    for seg in &resolved {
        let can_merge = current.as_ref().map_or(false, |c| {
            // Same grouping key, adjacent in time, same data hash
            // data_hash is pre-computed in resolve_payloads and never changes during coalescing
            c.grouping_key == seg.grouping_key
                && c.valid_until == seg.valid_from
                && c.data_hash.is_some()
                && c.data_hash == seg.data_hash
        });

        if can_merge {
            let c = current.as_mut().unwrap();
            c.valid_until = seg.valid_until.clone();
            c.row_ids.extend(seg.row_ids.iter());
            // Keep the latest ephemeral payload
            if seg.ephemeral_payload.is_some() {
                c.ephemeral_payload = seg.ephemeral_payload.clone();
            }
        } else {
            if let Some(prev) = current.take() {
                coalesced.push(prev);
            }
            current = Some(CoalescedSegment {
                grouping_key: seg.grouping_key.clone(),
                valid_from: seg.valid_from.clone(),
                valid_until: seg.valid_until.clone(),
                is_new_entity: seg.is_new_entity,
                identity_keys: seg.identity_keys.clone(),
                causal_id: seg.causal_id.clone(),
                row_ids: seg.row_ids.clone(),
                data_payload: seg.data_payload.clone(),
                ephemeral_payload: seg.ephemeral_payload.clone(),
                ancestor_valid_from: seg.target_valid_from.clone(),
                data_hash: seg.data_hash.clone(),
            });
        }
    }

    if let Some(last) = current {
        coalesced.push(last);
    }

    // Deduplicate row_ids within each coalesced segment
    for seg in &mut coalesced {
        seg.row_ids.sort();
        seg.row_ids.dedup();
    }

    coalesced
}

// ── Phase 4d: Diff Computation ──

fn compute_diff(
    coalesced: Vec<CoalescedSegment>,
    target_rows: &[TargetRow],
    _ctx: &PlannerContext,
) -> Vec<DiffRow> {
    let mut diffs = Vec::new();

    // FULL OUTER JOIN between coalesced segments and target rows on valid_from.
    // This mirrors the PL/pgSQL planner's diff computation:
    //   coalesced_final_segments cfs FULL OUTER JOIN filtered_target_rows ftr
    //   ON cfs.grouping_key = ftr.grouping_key AND cfs.valid_from = ftr.valid_from
    //
    // Since we're already within one entity group, we only match on valid_from.

    // Index target rows by valid_from for O(1) lookup
    let mut target_by_from: HashMap<&str, Vec<(usize, &TargetRow)>> = HashMap::new();
    for (i, tr) in target_rows.iter().enumerate() {
        target_by_from.entry(tr.valid_from.as_str()).or_default().push((i, tr));
    }

    let mut matched_target_indices: BTreeSet<usize> = BTreeSet::new();

    for cs in &coalesced {
        // Match on valid_from (not ancestor_valid_from)
        let target_match = target_by_from
            .get(cs.valid_from.as_str())
            .and_then(|targets| {
                // Find the first unmatched target with this valid_from
                targets.iter().find(|(idx, _)| !matched_target_indices.contains(idx))
            });

        if let Some(&(idx, tr)) = target_match {
            matched_target_indices.insert(idx);

            let allen = AllenRelation::compute(
                &tr.valid_from,
                &tr.valid_until,
                &cs.valid_from,
                &cs.valid_until,
            );

            diffs.push(DiffRow {
                grouping_key: cs.grouping_key.clone(),
                is_new_entity: cs.is_new_entity,
                identity_keys: cs.identity_keys.clone(),
                causal_id: cs.causal_id.clone(),
                row_ids: cs.row_ids.clone(),
                final_valid_from: Some(cs.valid_from.clone()),
                final_valid_until: Some(cs.valid_until.clone()),
                final_payload: cs.data_payload.clone(),
                target_valid_from: Some(tr.valid_from.clone()),
                target_valid_until: Some(tr.valid_until.clone()),
                target_payload: Some(tr.data_payload.clone()),
                allen_relation: allen,
            });
        } else {
            // INSERT: no matching target with same valid_from
            diffs.push(DiffRow {
                grouping_key: cs.grouping_key.clone(),
                is_new_entity: cs.is_new_entity,
                identity_keys: cs.identity_keys.clone(),
                causal_id: cs.causal_id.clone(),
                row_ids: cs.row_ids.clone(),
                final_valid_from: Some(cs.valid_from.clone()),
                final_valid_until: Some(cs.valid_until.clone()),
                final_payload: cs.data_payload.clone(),
                target_valid_from: None,
                target_valid_until: None,
                target_payload: None,
                allen_relation: None,
            });
        }
    }

    // Unmatched target rows become DELETEs
    for (idx, tr) in target_rows.iter().enumerate() {
        if matched_target_indices.contains(&idx) {
            continue;
        }

        diffs.push(DiffRow {
            grouping_key: coalesced.first().map(|c| c.grouping_key.clone()).unwrap_or_default(),
            is_new_entity: false,
            identity_keys: tr.identity_keys.clone(),
            causal_id: None,
            row_ids: Vec::new(),
            final_valid_from: None,
            final_valid_until: None,
            final_payload: None,
            target_valid_from: Some(tr.valid_from.clone()),
            target_valid_until: Some(tr.valid_until.clone()),
            target_payload: Some(tr.data_payload.clone()),
            allen_relation: None,
        });
    }

    diffs
}

// ── Phase 4e: Operation Classification ──

fn classify_operations(
    diff_rows: Vec<DiffRow>,
    _group: &EntityGroup,
    ctx: &PlannerContext,
) -> Vec<PlanRow> {
    let mut plan_rows = Vec::new();
    let mut seq = 0i64;

    for d in diff_rows {
        seq += 1;

        let (operation, update_effect) = classify_single_diff(&d, ctx);

        let old_valid_range = match (&d.target_valid_from, &d.target_valid_until) {
            (Some(f), Some(u)) => Some(format!("[{},{})", f, u)),
            _ => None,
        };
        let new_valid_range = match (&d.final_valid_from, &d.final_valid_until) {
            (Some(f), Some(u)) => Some(format!("[{},{})", f, u)),
            _ => None,
        };

        // Build data payload (merge data + ephemeral for the final output)
        let data = d.final_payload.map(|p| serde_json::Value::Object(p));

        // Build entity_keys and identity_keys JSONB
        let entity_keys = if d.identity_keys.is_empty() {
            None
        } else {
            Some(serde_json::Value::Object(d.identity_keys.clone()))
        };

        plan_rows.push(PlanRow {
            plan_op_seq: seq,
            statement_seq: 0, // Set in sequencing phase
            row_ids: d.row_ids.clone(),
            operation,
            update_effect,
            causal_id: d.causal_id.clone(),
            is_new_entity: d.is_new_entity,
            entity_keys: entity_keys.clone(),
            identity_keys: entity_keys,
            lookup_keys: None, // TODO: populate from source
            s_t_relation: d.allen_relation,
            b_a_relation: None, // Computed from before/after
            old_valid_from: d.target_valid_from,
            old_valid_until: d.target_valid_until,
            new_valid_from: d.final_valid_from,
            new_valid_until: d.final_valid_until,
            old_valid_range,
            new_valid_range,
            data,
            feedback: None,
            trace: None,
            grouping_key: d.grouping_key,
        });
    }

    plan_rows
}

fn classify_single_diff(d: &DiffRow, _ctx: &PlannerContext) -> (PlanAction, Option<UpdateEffect>) {
    match (&d.target_valid_from, &d.final_valid_from) {
        (None, Some(_)) => {
            // No target row → INSERT
            (PlanAction::Insert, None)
        }
        (Some(_), None) => {
            // No final segment → DELETE
            (PlanAction::Delete, None)
        }
        (Some(t_from), Some(f_from)) => {
            let t_until = d.target_valid_until.as_ref().unwrap();
            let f_until = d.final_valid_until.as_ref().unwrap();

            // Check if payload changed (direct map comparison, no MD5/serialization)
            let payload_identical = d
                .final_payload
                .as_ref()
                .zip(d.target_payload.as_ref())
                .map(|(fp, tp)| maps_equal_ignoring_nulls(fp, tp))
                .unwrap_or(false);

            if f_from == t_from && f_until == t_until && payload_identical {
                // Same range, same payload → SKIP
                (PlanAction::SkipIdentical, None)
            } else {
                // UPDATE — compute effect
                let effect = compute_update_effect(t_from, t_until, f_from, f_until);
                (PlanAction::Update, Some(effect))
            }
        }
        (None, None) => {
            // Should not happen
            (PlanAction::Error, None)
        }
    }
}

fn compute_update_effect(
    old_from: &str,
    old_until: &str,
    new_from: &str,
    new_until: &str,
) -> UpdateEffect {
    if new_from == old_from && new_until == old_until {
        UpdateEffect::None
    } else if new_from >= old_from && new_until <= old_until {
        UpdateEffect::Shrink
    } else if new_from <= old_from && new_until >= old_until {
        UpdateEffect::Grow
    } else {
        UpdateEffect::Move
    }
}

// ── Phase 5: Statement Sequencing ──

fn sequence_statements(plan_rows: &mut [PlanRow], _ctx: &PlannerContext) {
    // Sort by operation priority: DELETE < UPDATE < INSERT
    // Within UPDATE: by effect (NONE < SHRINK < MOVE < GROW)
    // MOVE: descending by old_valid_from
    plan_rows.sort_by(|a, b| {
        let op_ord = |p: &PlanRow| -> i32 {
            match p.operation {
                PlanAction::Delete => 0,
                PlanAction::Update => {
                    match p.update_effect {
                        Some(UpdateEffect::None) => 10,
                        Some(UpdateEffect::Shrink) => 11,
                        Some(UpdateEffect::Move) => 20,
                        Some(UpdateEffect::Grow) => 30,
                        None => 15,
                    }
                }
                PlanAction::Insert => 40,
                _ => 50, // SKIPs and ERRORs
            }
        };
        let ord = op_ord(a).cmp(&op_ord(b));
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
        // For MOVE: descending by old_valid_from
        if a.operation == PlanAction::Update
            && a.update_effect == Some(UpdateEffect::Move)
        {
            return b
                .old_valid_from
                .as_deref()
                .cmp(&a.old_valid_from.as_deref());
        }
        // Default: ascending by new_valid_from
        a.new_valid_from
            .as_deref()
            .cmp(&b.new_valid_from.as_deref())
    });

    // Assign statement_seq
    let mut stmt_seq = 0i32;
    let mut prev_category = -1i32;
    let mut move_batch = 0i32;

    for (i, row) in plan_rows.iter_mut().enumerate() {
        let category = match row.operation {
            PlanAction::Delete => 1,
            PlanAction::Update => match row.update_effect {
                Some(UpdateEffect::None) | Some(UpdateEffect::Shrink) => 2,
                Some(UpdateEffect::Move) => 3,
                Some(UpdateEffect::Grow) => 4,
                None => 2,
            },
            PlanAction::Insert => 5,
            _ => 6, // SKIPs / ERRORs
        };

        if category != prev_category {
            stmt_seq += 1;
            prev_category = category;
            move_batch = 0;
        }

        // MOVE operations: each in its own statement
        if category == 3 {
            move_batch += 1;
            if move_batch > 1 {
                stmt_seq += 1;
            }
        }

        row.statement_seq = stmt_seq;
        row.plan_op_seq = (i + 1) as i64;
    }
}

// ── Utility functions ──

fn make_feedback_plan_row(sr: &MatchedSourceRow, fb: &EarlyFeedback, _ctx: &PlannerContext) -> PlanRow {
    let feedback_json = serde_json::json!({
        "action": fb.action.as_str(),
        "message": fb.message,
    });

    PlanRow {
        plan_op_seq: 0, // Set in sequencing
        statement_seq: 0,
        row_ids: vec![sr.source.row_id],
        operation: fb.action,
        update_effect: None,
        causal_id: Some(sr.source.causal_id.clone()),
        is_new_entity: sr.is_new_entity,
        entity_keys: Some(serde_json::Value::Object(sr.source.identity_keys.clone())),
        identity_keys: Some(serde_json::Value::Object(sr.source.identity_keys.clone())),
        lookup_keys: Some(serde_json::Value::Object(sr.source.lookup_keys.clone())),
        s_t_relation: None,
        b_a_relation: None,
        old_valid_from: None,
        old_valid_until: None,
        new_valid_from: Some(sr.source.valid_from.clone()),
        new_valid_until: Some(sr.source.valid_until.clone()),
        old_valid_range: None,
        new_valid_range: Some(format!("[{},{})", sr.source.valid_from, sr.source.valid_until)),
        data: None,
        feedback: Some(feedback_json),
        trace: None,
        grouping_key: sr.grouping_key.clone(),
    }
}

/// Convert a JSON map to a stable string key for hashing/comparison.
fn json_map_to_key(map: &serde_json::Map<String, serde_json::Value>) -> String {
    let mut parts: Vec<String> = map
        .iter()
        .filter(|(_, v)| !v.is_null())
        .map(|(k, v)| format!("{}={}", k, json_value_to_str(v)))
        .collect();
    parts.sort();
    parts.join("__")
}

fn json_value_to_str(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => "_NULL_".to_string(),
        other => other.to_string(),
    }
}

/// Compare two JSON maps for equality, treating null values as absent.
fn maps_equal_ignoring_nulls(
    a: &serde_json::Map<String, serde_json::Value>,
    b: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    // Check all non-null entries in a exist with same value in b
    for (k, v) in a {
        if v.is_null() {
            continue;
        }
        match b.get(k) {
            Some(bv) if bv == v => {}
            _ => return false,
        }
    }
    // Check all non-null entries in b exist in a
    for (k, v) in b {
        if v.is_null() {
            continue;
        }
        match a.get(k) {
            Some(av) if av == v => {}
            _ => return false,
        }
    }
    true
}

/// Strip null values from a JSON map.
pub fn strip_nulls(
    map: &serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    map.iter()
        .filter(|(_, v)| !v.is_null())
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}
