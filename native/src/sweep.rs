use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::types::*;

/// Compute valid_to = valid_until - 1 day for date strings ("YYYY-MM-DD").
/// PL/pgSQL: to_jsonb(MAX(valid_until) - 1)
fn date_minus_one(date_str: &str) -> Option<String> {
    let parts: Vec<&str> = date_str.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let y: i32 = parts[0].parse().ok()?;
    let m: u32 = parts[1].parse().ok()?;
    let d: u32 = parts[2].parse().ok()?;
    if d > 1 {
        Some(format!("{:04}-{:02}-{:02}", y, m, d - 1))
    } else {
        let (new_y, new_m) = if m > 1 { (y, m - 1) } else { (y - 1, 12) };
        let days = match new_m {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => {
                if (new_y % 4 == 0 && new_y % 100 != 0) || (new_y % 400 == 0) {
                    29
                } else {
                    28
                }
            }
            _ => return None,
        };
        Some(format!("{:04}-{:02}-{:02}", new_y, new_m, days))
    }
}

/// Main entry point: run the sweep-line algorithm over source and target rows.
/// Returns a Vec of PlanRow ready for insertion into pg_temp.temporal_merge_plan.
pub fn sweep_line_plan(
    source_rows: Vec<SourceRow>,
    target_rows: Vec<TargetRow>,
    ctx: &PlannerContext,
) -> Vec<PlanRow> {
    // Phase 1: Entity correlation — match source rows to target entities
    let matched_sources = correlate_entities(&source_rows, &target_rows, ctx);

    // Phase 1.5: Canonical NK resolution for new entities
    let matched_sources = canonicalize_new_entity_nks(matched_sources, ctx);

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
        let filtered_sources = filter_by_mode(&active_sources, group, ctx);

        // Emit SKIP feedback for sources filtered out by mode
        for sr in &active_sources {
            let was_filtered = !filtered_sources.iter().any(|f| f.source.row_id == sr.source.row_id);
            if was_filtered {
                // PL/pgSQL distinguishes:
                // - SKIP_FILTERED: existing entity filtered by INSERT_NEW_ENTITIES mode
                // - SKIP_NO_TARGET: new entity filtered by *_FOR_PORTION_OF modes
                let skip_action = if sr.is_new_entity {
                    PlanAction::SkipNoTarget
                } else {
                    PlanAction::SkipFiltered
                };
                all_plan_rows.push(make_feedback_plan_row(
                    sr,
                    &EarlyFeedback {
                        action: skip_action,
                        message: None,
                    },
                    ctx,
                ));
            }
        }

        let active_sources = filtered_sources;
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
    // Build per-key-set lookup indexes for NK matching.
    // PL/pgSQL tries each natural key set independently (OR logic).
    // E.g., lookup_key_sets = [["id"], ["legal_unit_id"]] means:
    //   match on {id} OR match on {legal_unit_id}
    let mut target_indexes_per_key_set: Vec<HashMap<String, Vec<(usize, &TargetRow)>>> =
        Vec::with_capacity(ctx.lookup_key_sets.len());
    for key_set in &ctx.lookup_key_sets {
        let mut index: HashMap<String, Vec<(usize, &TargetRow)>> = HashMap::new();
        for (i, tr) in target_rows.iter().enumerate() {
            let key = build_key_for_cols(&tr.lookup_keys, key_set);
            if !key.is_empty() {
                index.entry(key).or_default().push((i, tr));
            }
        }
        target_indexes_per_key_set.push(index);
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

        // Try NK match if identity didn't match — try each key set independently (OR).
        // Must try ALL key sets to detect cross-key-set ambiguity (PL/pgSQL behavior):
        // e.g., source with ssn='222' + employee_nr='E101' may match entity A via employee_nr
        // and entity B via ssn — that's ambiguous.
        if is_new && !sr.lookup_keys.is_empty() && !sr.lookup_cols_are_null {
            let mut all_matched_entities: BTreeSet<String> = BTreeSet::new();
            let mut all_matched_id_maps: Vec<serde_json::Map<String, serde_json::Value>> = Vec::new();
            let mut first_discovered_identity = None;

            for (ks_idx, key_set) in ctx.lookup_key_sets.iter().enumerate() {
                let nk_key = build_key_for_cols(&sr.lookup_keys, key_set);
                if nk_key.is_empty() {
                    continue; // All columns in this key set are NULL in source
                }
                if let Some(targets) = target_indexes_per_key_set[ks_idx].get(&nk_key) {
                    for (_i, tr) in targets {
                        let ek = json_map_to_key(&tr.identity_keys);
                        if all_matched_entities.insert(ek) {
                            all_matched_id_maps.push(tr.identity_keys.clone());
                        }
                    }
                    if first_discovered_identity.is_none() && !targets.is_empty() {
                        first_discovered_identity = Some(targets[0].1.identity_keys.clone());
                    }
                }
            }

            if all_matched_entities.len() > 1 {
                // Ambiguous: source row matches multiple distinct target entities.
                // PL/pgSQL: target_entity_exists = true (discovered_stable_pk_payload IS NOT NULL),
                // so is_new_entity = false, and discovered_identity comes from first match.
                is_new = false;
                discovered_identity = first_discovered_identity;
                let conflicting_ids: Vec<String> = all_matched_id_maps.iter()
                    .map(|m| json_to_pg_text(&serde_json::Value::Object(m.clone())))
                    .collect();
                early_feedback = Some(EarlyFeedback {
                    action: PlanAction::Error,
                    message: Some(format!(
                        "Source row is ambiguous. It matches multiple distinct target entities: [{}]",
                        conflicting_ids.join(", ")
                    )),
                });
            } else if all_matched_entities.len() == 1 {
                is_new = false;
                discovered_identity = first_discovered_identity;
                canonical_nk = Some(strip_nulls(&sr.lookup_keys));
            }
        }

        // For new entities, check identifiability
        // PL/pgSQL: In founding mode OR STRATEGY_IDENTITY_KEY_ONLY, rows are always identifiable.
        // STRATEGY_IDENTITY_KEY_ONLY: A serial PK with NULL identity is a valid INSERT —
        // the row is identified by its causal_id within the batch.
        if is_new && !sr.is_identifiable && sr.lookup_cols_are_null
            && !ctx.is_founding_mode()
            && ctx.strategy != crate::types::IdentityStrategy::IdentityKeyOnly
            && early_feedback.is_none()
        {
            // Format matches PL/pgSQL: {col1, col2} for identity, [[set1], [set2]] for keys
            let id_cols_str = format!("{{{}}}", ctx.identity_columns.join(", "));
            let key_sets_str = format!("[{}]",
                ctx.lookup_key_sets.iter()
                    .map(|ks| format!("[{}]", ks.join(", ")))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            early_feedback = Some(EarlyFeedback {
                action: PlanAction::Error,
                message: Some(format!(
                    "Source row is unidentifiable. It has NULL for all stable identity columns {} and all natural keys {}",
                    id_cols_str, key_sets_str
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

/// Canonical NK resolution for new entities (matches PL/pgSQL CTE 9.2).
/// Groups fragmented source rows that reference the same logical entity via different
/// natural key subsets. For example, rows with ssn='555' only and rows with employee_nr='E105'
/// only should be grouped with a row that has both ssn='555' AND employee_nr='E105'.
fn canonicalize_new_entity_nks(
    mut matched: Vec<MatchedSourceRow>,
    ctx: &PlannerContext,
) -> Vec<MatchedSourceRow> {
    if ctx.all_lookup_cols.is_empty() || ctx.lookup_key_sets.is_empty() {
        return matched;
    }

    // Collect indices of new entities without early_feedback
    let new_indices: Vec<usize> = matched.iter().enumerate()
        .filter(|(_, m)| m.is_new_entity && m.early_feedback.is_none())
        .map(|(i, _)| i)
        .collect();

    if new_indices.is_empty() {
        return matched;
    }

    // Build non-null NK maps for each new entity source row
    let nk_maps: Vec<serde_json::Map<String, serde_json::Value>> = new_indices.iter()
        .map(|&i| strip_nulls(&matched[i].source.lookup_keys))
        .collect();

    // Union-find: group rows that share any NK value
    let n = new_indices.len();
    let mut parent: Vec<usize> = (0..n).collect();

    fn find(parent: &mut Vec<usize>, i: usize) -> usize {
        if parent[i] != i {
            parent[i] = find(parent, parent[i]);
        }
        parent[i]
    }
    fn union(parent: &mut Vec<usize>, a: usize, b: usize) {
        let ra = find(parent, a);
        let rb = find(parent, b);
        if ra != rb {
            parent[rb] = ra;
        }
    }

    // For each key set, index rows by their key value and merge groups
    for key_set in &ctx.lookup_key_sets {
        let mut val_to_idx: HashMap<String, Vec<usize>> = HashMap::new();
        for (local_idx, nk_map) in nk_maps.iter().enumerate() {
            let key = build_key_for_cols(nk_map, key_set);
            if !key.is_empty() {
                val_to_idx.entry(key).or_default().push(local_idx);
            }
        }
        for indices in val_to_idx.values() {
            if indices.len() > 1 {
                for i in 1..indices.len() {
                    union(&mut parent, indices[0], indices[i]);
                }
            }
        }
    }

    // For each connected component, find the most specific NK (most non-null keys)
    let mut component_canonical: HashMap<usize, serde_json::Map<String, serde_json::Value>> = HashMap::new();
    for local_idx in 0..n {
        let root = find(&mut parent, local_idx);
        let entry = component_canonical.entry(root).or_insert_with(serde_json::Map::new);
        // Merge: the most specific NK is the union of all non-null keys in the component
        for (k, v) in &nk_maps[local_idx] {
            if !entry.contains_key(k) {
                entry.insert(k.clone(), v.clone());
            }
        }
    }

    // Apply canonical NK and rebuild grouping key for affected rows
    for (local_idx, &global_idx) in new_indices.iter().enumerate() {
        let root = find(&mut parent, local_idx);
        let canonical = &component_canonical[&root];
        if canonical.len() > nk_maps[local_idx].len() || *canonical != nk_maps[local_idx] {
            // This row's NK is less specific — update with canonical
            matched[global_idx].canonical_nk_json = Some(canonical.clone());
            matched[global_idx].grouping_key = build_grouping_key(
                &matched[global_idx].source,
                true,
                &matched[global_idx].discovered_identity,
                &matched[global_idx].canonical_nk_json,
                ctx,
            );
        }
    }

    matched
}

/// Build a lookup key string using only the specified columns from a JSON map.
/// Null values are excluded; returns empty string if all columns are null/missing.
fn build_key_for_cols(map: &serde_json::Map<String, serde_json::Value>, cols: &[String]) -> String {
    let mut parts: Vec<String> = Vec::new();
    for col in cols {
        if let Some(v) = map.get(col) {
            if !v.is_null() {
                parts.push(format!("{}={}", col, json_value_to_str(v)));
            }
        }
    }
    parts.join("__")
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
            None => {
                // No lookup keys — check if identity columns have non-null values
                let identity_all_null = ctx.identity_columns.iter().all(|c| {
                    sr.identity_keys
                        .get(c)
                        .map_or(true, |v| v.is_null())
                });
                if identity_all_null {
                    format!("new_entity__{}", sr.causal_id)
                } else {
                    let key_parts: Vec<String> = ctx
                        .identity_columns
                        .iter()
                        .map(|c| {
                            sr.identity_keys
                                .get(c)
                                .map(|v| json_value_to_str(v))
                                .unwrap_or_else(|| "_NULL_".to_string())
                        })
                        .collect();
                    format!("new_entity__{}", key_parts.join("__"))
                }
            }
        }
    }
}

// ── Phase 2: Eclipse Detection ──

fn detect_eclipsed(
    mut matched: Vec<MatchedSourceRow>,
    ctx: &PlannerContext,
) -> Vec<MatchedSourceRow> {
    let is_numeric = ctx.era.range_subtype_category == 'N';

    // PL/pgSQL has two eclipsed paths:
    // Path 1: Rows with lookup columns → PARTITION BY lookup_columns (raw source values)
    // Path 2: Rows without lookup columns → PARTITION BY causal_id
    // Rows from different partitions must not eclipse each other.
    let mut by_group: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, m) in matched.iter().enumerate() {
        // Build partition key from lookup column values.
        // A lookup column may be in identity_keys (when PK = NK), so check both maps.
        //
        // Optimization: for XOR nullable keys (common pattern), most rows have only
        // one non-null lookup column. Build the key inline without allocating a map
        // or sorting when possible.
        let partition_key = if ctx.all_lookup_cols.is_empty() {
            format!("causal_{}", m.source.causal_id)
        } else {
            // Fast path: collect non-null key=value pairs inline
            let mut parts: Vec<(&str, String)> = Vec::new();
            for col in &ctx.all_lookup_cols {
                let val = m.source.lookup_keys.get(col)
                    .or_else(|| m.source.identity_keys.get(col));
                if let Some(v) = val {
                    if !v.is_null() {
                        parts.push((col.as_str(), json_value_to_str(v)));
                    }
                }
            }
            if parts.is_empty() {
                // Path 2: no usable lookup keys → partition by causal_id
                format!("causal_{}", m.source.causal_id)
            } else if parts.len() == 1 {
                // Fast path: single non-null key — no sorting needed
                format!("{}={}", parts[0].0, parts[0].1)
            } else {
                // Multiple non-null keys — sort for stable key
                parts.sort_by_key(|(k, _)| *k);
                parts.iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join("__")
            }
        };
        by_group.entry(partition_key).or_default().push(i);
    }

    for (_key, indices) in &by_group {
        if indices.len() <= 1 {
            continue;
        }
        // PL/pgSQL: ORDER BY source_row_id DESC, then uses range_agg() over
        // preceding rows to check if combined coverage eclipses current row.
        let mut sorted = indices.clone();
        sorted.sort_by(|&a, &b| {
            // DESC by row_id (newer rows first, like PL/pgSQL)
            matched[b].source.row_id.cmp(&matched[a].source.row_id)
        });

        // Build running multirange of preceding rows (in DESC order).
        // A row is eclipsed if the combined range of all newer rows covers it.
        let mut multirange: Vec<(String, String)> = Vec::new();

        for &idx in &sorted {
            if matched[idx].early_feedback.is_some() {
                continue;
            }
            // Check if current row is covered by the multirange of newer rows
            if multirange_contains(&multirange, &matched[idx].source.valid_from, &matched[idx].source.valid_until, is_numeric) {
                matched[idx].is_eclipsed = true;
            }
            // Add current row's range to the multirange
            multirange_add(&mut multirange, matched[idx].source.valid_from.clone(), matched[idx].source.valid_until.clone(), is_numeric);
        }
    }

    matched
}

/// Add an interval to a sorted, non-overlapping multirange and merge overlaps.
fn multirange_add(mr: &mut Vec<(String, String)>, from: String, until: String, is_numeric: bool) {
    mr.push((from, until));
    mr.sort_by(|a, b| temporal_cmp(&a.0, &b.0, is_numeric));
    // Merge overlapping/adjacent intervals
    let mut merged: Vec<(String, String)> = Vec::new();
    for interval in mr.iter() {
        if let Some(last) = merged.last_mut() {
            if temporal_cmp(&interval.0, &last.1, is_numeric) != std::cmp::Ordering::Greater {
                // Overlapping or adjacent
                if temporal_cmp(&interval.1, &last.1, is_numeric) == std::cmp::Ordering::Greater {
                    last.1 = interval.1.clone();
                }
                continue;
            }
        }
        merged.push(interval.clone());
    }
    *mr = merged;
}

/// Check if a range [from, until) is fully contained by a merged multirange.
fn multirange_contains(mr: &[(String, String)], from: &str, until: &str, is_numeric: bool) -> bool {
    // After merging, each interval is a maximal contiguous block.
    // So [from, until) is contained iff some single interval covers it entirely.
    mr.iter().any(|(i_from, i_until)| {
        temporal_cmp(i_from, from, is_numeric) != std::cmp::Ordering::Greater
            && temporal_cmp(i_until, until, is_numeric) != std::cmp::Ordering::Less
    })
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
    ctx: &PlannerContext,
) -> Vec<AtomicSegment> {
    let is_numeric = ctx.era.range_subtype_category == 'N';

    // Collect all time boundaries (use Vec + sort for numeric-aware ordering)
    let mut boundaries: Vec<String> = Vec::new();

    for sr in active_sources {
        boundaries.push(sr.source.valid_from.clone());
        boundaries.push(sr.source.valid_until.clone());
    }
    for tr in &group.target_rows {
        boundaries.push(tr.valid_from.clone());
        boundaries.push(tr.valid_until.clone());
    }
    boundaries.sort_by(|a, b| temporal_cmp(a, b, is_numeric));
    boundaries.dedup();

    // Create segments between consecutive boundaries
    let mut segments = Vec::new();

    for window in boundaries.windows(2) {
        let from = &window[0];
        let until = &window[1];
        if temporal_cmp(from, until, is_numeric) != std::cmp::Ordering::Less {
            continue;
        }
        segments.push(AtomicSegment {
            grouping_key: group.grouping_key.clone(),
            valid_from: from.clone(),
            valid_until: until.clone(),
            is_new_entity: group.is_new_entity,
            identity_keys: group.identity_keys.clone(),
            // PL/pgSQL: FIRST_VALUE(causal_id) OVER (PARTITION BY grouping_key ORDER BY causal_id ASC NULLS LAST)
            // For existing entities, use the MIN source causal_id as the "founding" ID for the entire entity group.
            // For new entities, keep the per-source causal_id.
            causal_id: if group.is_new_entity {
                active_sources.first().map(|s| s.source.causal_id.clone())
            } else {
                active_sources.iter().map(|s| &s.source.causal_id).min().cloned()
            },
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
    let is_numeric = ctx.era.range_subtype_category == 'N';
    let mut resolved = Vec::with_capacity(segments.len());

    for seg in segments {
        // Find covering source rows (source interval contains segment)
        let mut covering_sources: Vec<&MatchedSourceRow> = active_sources
            .iter()
            .filter(|s| {
                temporal_cmp(&s.source.valid_from, &seg.valid_from, is_numeric) != std::cmp::Ordering::Greater
                    && temporal_cmp(&s.source.valid_until, &seg.valid_until, is_numeric) != std::cmp::Ordering::Less
            })
            .copied()
            .collect();
        // Sort by row_id for deterministic payload resolution
        covering_sources.sort_by_key(|s| s.source.row_id);

        // Find covering target row
        let covering_target = target_rows
            .iter()
            .find(|t| {
                temporal_cmp(&t.valid_from, &seg.valid_from, is_numeric) != std::cmp::Ordering::Greater
                    && temporal_cmp(&t.valid_until, &seg.valid_until, is_numeric) != std::cmp::Ordering::Less
            });

        // Resolve payload: source wins (except DELETE_FOR_PORTION_OF where source = deletion marker)
        let (data_payload, row_ids) = if ctx.mode == MergeMode::DeleteForPortionOf
            && !covering_sources.is_empty()
        {
            // DELETE_FOR_PORTION_OF: source-covered segments are deleted (no data)
            (None, covering_sources.iter().map(|s| s.source.row_id).collect())
        } else {
            resolve_source_payload(&covering_sources, covering_target, ctx)
        };

        let source_from = covering_sources.first().map(|s| s.source.valid_from.clone());
        let source_until = covering_sources.last().map(|s| s.source.valid_until.clone());
        let target_from = covering_target.map(|t| t.valid_from.clone());
        let target_until = covering_target.map(|t| t.valid_until.clone());

        // Compute per-segment s_t_relation: source row range vs covering target row range
        // Mirrors PL/pgSQL: get_allen_relation(propagated_s_valid_from, propagated_s_valid_until, t_valid_from, t_valid_until)
        let s_t_relation = match (&source_from, &source_until, &target_from, &target_until) {
            (Some(sf), Some(su), Some(tf), Some(tu)) => AllenRelation::compute(sf, su, tf, tu, is_numeric),
            _ => None,
        };

        // Compute data hash for coalescing (excluding ephemeral columns)
        // Uses xxh3 (non-cryptographic, ~10x faster than MD5)
        let data_hash = data_payload.as_ref().map(|p| {
            let stripped = strip_nulls(p);
            let serialized = serde_json::to_string(&serde_json::Value::Object(stripped)).unwrap_or_default();
            format!("{:016x}", xxhash_rust::xxh3::xxh3_64(serialized.as_bytes()))
        });

        // PL/pgSQL: CASE WHEN s_data_payload IS NULL THEN t_ephemeral_payload
        //           ELSE COALESCE(t_ephemeral, {}) || COALESCE(s_ephemeral, {}) END
        // Target ephemeral is the base; source ephemeral overlays on top.
        let ephemeral_payload = if !covering_sources.is_empty() {
            let mut merged_eph = covering_target
                .map(|t| t.ephemeral_payload.clone())
                .unwrap_or_default();
            // Source ephemeral overlays on top of target ephemeral, with NULL stripping
            for (k, v) in &covering_sources.last().unwrap().source.ephemeral_payload {
                if v.is_null() {
                    if ctx.mode.is_patch() {
                        // PATCH: strip ALL NULLs from source ephemeral
                        continue;
                    } else if ctx.exclude_if_null_columns.contains(k) {
                        // UPSERT/REPLACE: strip NULLs for NOT NULL / default columns
                        continue;
                    }
                }
                merged_eph.insert(k.clone(), v.clone());
            }
            Some(merged_eph)
        } else {
            covering_target.map(|t| t.ephemeral_payload.clone())
        };

        let target_data = covering_target.map(|t| t.data_payload.clone());

        // Skip segments with no source or target coverage
        if data_payload.is_none() && covering_target.is_none() {
            continue;
        }
        // *_FOR_PORTION_OF modes: skip source-covered segments with no target coverage.
        // PL/pgSQL: WHEN 'PATCH_FOR_PORTION_OF' THEN seg.t_data_payload IS NOT NULL
        // These modes only affect the "portion of" the target that already exists.
        // Without this, extending segments get INSERT with only source columns,
        // missing target-inherited columns (e.g., "null value in column 'name'").
        if ctx.mode.is_for_portion_of() && covering_target.is_none() && !covering_sources.is_empty() {
            continue;
        }
        // DELETE_FOR_PORTION_OF: source-covered segments have data=None (deletion markers).
        // These represent time periods to be removed from the target — skip them so the
        // remaining target-only segments form SHRINK/INSERT operations.
        // Note: already handled by is_for_portion_of() above for non-DELETE modes.
        if data_payload.is_none() && ctx.mode == MergeMode::DeleteForPortionOf && !covering_sources.is_empty() {
            continue;
        }

        // For target-only segments within an entity that has sources, propagate
        // causal source info (row_id, source valid range, s_t_relation).
        // PL/pgSQL does this via propagated_s_valid_from/until and causal_source_row_ids.
        let (row_ids, source_from, source_until, s_t_relation) = if covering_sources.is_empty()
            && !active_sources.is_empty()
        {
            // Find the source that shares a boundary with this segment
            let causal = active_sources
                .iter()
                .find(|s| {
                    s.source.valid_from == seg.valid_until
                        || s.source.valid_until == seg.valid_from
                })
                .or_else(|| active_sources.first());

            if let Some(sr) = causal {
                let sf = sr.source.valid_from.clone();
                let su = sr.source.valid_until.clone();
                // PL/pgSQL propagates s_t_relation only within the same t_valid_from partition.
                // This means: propagate only when the causal source overlaps the covering target
                // (i.e., they share the same target row). If the source just meets the target
                // (GROW scenario), they're in different partitions → no s_t_relation.
                let propagated_st = if let (Some(tf), Some(tu)) = (&target_from, &target_until) {
                    // Source overlaps target if source_from < target_until AND source_until > target_from
                    if temporal_cmp(&sf, tu, is_numeric) == std::cmp::Ordering::Less
                        && temporal_cmp(&su, tf, is_numeric) == std::cmp::Ordering::Greater {
                        AllenRelation::compute(&sf, &su, tf, tu, is_numeric)
                    } else {
                        None
                    }
                } else {
                    None
                };
                (vec![sr.source.row_id], Some(sf), Some(su), propagated_st)
            } else {
                (row_ids, source_from, source_until, s_t_relation)
            }
        } else {
            (row_ids, source_from, source_until, s_t_relation)
        };

        let has_target = covering_target.is_some();
        // PL/pgSQL unifies causal_id at the time_points_unified stage:
        // - Existing entities: ALL segments use the entity group's founding (min) causal_id
        // - New entities: each segment uses its covering source's causal_id
        let segment_causal_id = if seg.is_new_entity {
            covering_sources
                .last()
                .map(|s| Some(s.source.causal_id.clone()))
                .unwrap_or(seg.causal_id)
        } else {
            // For existing entities, always preserve the entity group's founding causal_id
            seg.causal_id
        };

        resolved.push(ResolvedSegment {
            grouping_key: seg.grouping_key,
            valid_from: seg.valid_from,
            valid_until: seg.valid_until,
            is_new_entity: seg.is_new_entity,
            identity_keys: seg.identity_keys,
            causal_id: segment_causal_id,
            row_ids,
            source_valid_from: source_from,
            source_valid_until: source_until,
            target_valid_from: target_from,
            target_valid_until: target_until,
            data_payload,
            ephemeral_payload,
            target_data_payload: target_data,
            data_hash,
            has_source_coverage: !covering_sources.is_empty(),
            has_target_coverage: has_target,
            s_t_relation,
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

    // Apply source rows in order (later rows override data).
    // Only the winning (last) source's row_id is kept per atomic segment.
    // Multiple row_ids accumulate during coalescing across segments.
    for sr in covering_sources {
        if ctx.mode.is_patch() {
            // PATCH mode: strip nulls from source before merging
            let stripped = strip_nulls(&sr.source.data_payload);
            for (k, v) in stripped {
                merged.insert(k, v);
            }
        } else {
            // UPSERT/REPLACE mode: source values override, but strip NULLs for
            // NOT NULL columns and columns with defaults (mirrors PL/pgSQL behavior)
            for (k, v) in &sr.source.data_payload {
                if v.is_null() && ctx.exclude_if_null_columns.contains(k) {
                    // Skip NULL — preserves target value for this column
                    continue;
                }
                merged.insert(k.clone(), v.clone());
            }
        }
    }

    if ctx.mode.is_last_writer_wins() {
        // REPLACE-family: only the last (highest source_row_id) source contributes
        if let Some(last_sr) = covering_sources.last() {
            row_ids.push(last_sr.source.row_id);
        }
    } else {
        // PATCH/UPSERT: accumulate ALL covering sources' row_ids
        for sr in covering_sources {
            if !row_ids.contains(&sr.source.row_id) {
                row_ids.push(sr.source.row_id);
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
            // OR source/target coverage
            c.has_source_coverage = c.has_source_coverage || seg.has_source_coverage;
            c.has_target_coverage = c.has_target_coverage || seg.has_target_coverage;
            // Keep first non-null ancestor_valid_from (matches PL/pgSQL sql_saga.first() which skips NULLs)
            if c.ancestor_valid_from.is_none() && seg.target_valid_from.is_some() {
                c.ancestor_valid_from = seg.target_valid_from.clone();
            }
            // Keep first non-null s_t_relation (like sql_saga.first() which skips NULLs)
            if c.s_t_relation.is_none() && seg.s_t_relation.is_some() {
                c.s_t_relation = seg.s_t_relation;
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
                has_source_coverage: seg.has_source_coverage,
                has_target_coverage: seg.has_target_coverage,
                s_t_relation: seg.s_t_relation,
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

    // FULL OUTER JOIN between coalesced segments and target rows.
    // Mirrors PL/pgSQL: JOIN ON grouping_key AND ancestor_valid_from = target.valid_from
    //
    // Key difference from a standard 1:1 join: multiple coalesced segments can share
    // the same ancestor_valid_from (when a target row is split into multiple segments).
    // PL/pgSQL handles this via update_rank: rank 1 → UPDATE, rank > 1 → INSERT.
    // All such segments get the same target data (old_valid_from/until, target_payload).

    // Index target rows by valid_from for O(1) lookup
    let target_by_from: HashMap<&str, &TargetRow> = target_rows
        .iter()
        .map(|tr| (tr.valid_from.as_str(), tr))
        .collect();

    // Track which target valid_from values are matched by at least one coalesced segment
    let mut matched_target_froms: BTreeSet<String> = BTreeSet::new();

    for cs in &coalesced {
        // Match on ancestor_valid_from (the original target valid_from this segment derives from).
        let target_match = cs.ancestor_valid_from.as_deref()
            .and_then(|af| target_by_from.get(af).copied());

        if let Some(tr) = target_match {
            matched_target_froms.insert(tr.valid_from.clone());

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
                ephemeral_payload: cs.ephemeral_payload.clone(),
                has_source_coverage: cs.has_source_coverage,
                s_t_relation: cs.s_t_relation,
                target_ephemeral: Some(tr.ephemeral_payload.clone()),
                target_lookup_keys: Some(tr.lookup_keys.clone()),
                target_pk_payload: Some(tr.pk_payload.clone()),
            });
        } else {
            // INSERT: no matching target (ancestor_valid_from is None or doesn't match)
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
                ephemeral_payload: cs.ephemeral_payload.clone(),
                has_source_coverage: cs.has_source_coverage,
                s_t_relation: cs.s_t_relation,
                target_ephemeral: None,
                target_lookup_keys: None,
                target_pk_payload: None,
            });
        }
    }

    // Unmatched target rows become DELETEs
    for tr in target_rows {
        if matched_target_froms.contains(&tr.valid_from) {
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
            ephemeral_payload: None,
            has_source_coverage: false,
            s_t_relation: None,
            target_ephemeral: Some(tr.ephemeral_payload.clone()),
            target_lookup_keys: Some(tr.lookup_keys.clone()),
            target_pk_payload: Some(tr.pk_payload.clone()),
        });
    }

    diffs
}

// ── Phase 4e: Operation Classification ──

fn classify_operations(
    diff_rows: Vec<DiffRow>,
    group: &EntityGroup,
    ctx: &PlannerContext,
) -> Vec<PlanRow> {
    let is_numeric = ctx.era.range_subtype_category == 'N';
    let mut plan_rows = Vec::new();
    let mut seq = 0i64;

    // Build lookup_keys from all_lookup_cols, searching both identity_keys and lookup_keys.
    // PL/pgSQL includes all natural identity column values in lookup_keys, even when
    // those columns are also primary identity columns.
    // For existing entities, use target values when source has NULLs (PATCH semantics).
    let group_lookup_keys: Option<serde_json::Value> = {
        if ctx.all_lookup_cols.is_empty() {
            // Always emit {} for lookup_keys when there are no lookup columns,
            // matching PL/pgSQL behavior for both source-matched and orphan entities
            Some(serde_json::Value::Object(serde_json::Map::new()))
        } else {
            // Find first source row with any values for lookup cols
            let first_sr = group.source_rows.first();
            let first_tr = group.target_rows.first();
            match first_sr {
                Some(sr) => {
                    let mut lk_map = serde_json::Map::new();
                    for col in &ctx.all_lookup_cols {
                        // Check identity_keys first, then lookup_keys, then data_payload
                        let val = sr.source.identity_keys.get(col)
                            .or_else(|| sr.source.lookup_keys.get(col))
                            .or_else(|| sr.source.data_payload.get(col))
                            .cloned()
                            .unwrap_or(serde_json::Value::Null);
                        // For existing entities, fall back to target values when source is NULL.
                        // PL/pgSQL resolves columns via COALESCE(source, target) so the diff row
                        // already carries target values for NULL source columns.
                        let val = if val.is_null() && !group.is_new_entity {
                            first_tr.and_then(|tr| {
                                tr.lookup_keys.get(col)
                                    .or_else(|| tr.identity_keys.get(col))
                                    .cloned()
                            }).unwrap_or(val)
                        } else {
                            val
                        };
                        lk_map.insert(col.clone(), val);
                    }
                    Some(serde_json::Value::Object(lk_map))
                }
                None => {
                    // Orphan target entity: build lookup_keys from the first target row
                    match first_tr {
                        Some(tr) => {
                            let mut lk_map = serde_json::Map::new();
                            for col in &ctx.all_lookup_cols {
                                let val = tr.lookup_keys.get(col)
                                    .or_else(|| tr.identity_keys.get(col))
                                    .cloned()
                                    .unwrap_or(serde_json::Value::Null);
                                lk_map.insert(col.clone(), val);
                            }
                            Some(serde_json::Value::Object(lk_map))
                        }
                        None => None,
                    }
                }
            }
        }
    };

    // Check if this entity has active source rows (for delete mode logic)
    let has_active_sources = group
        .source_rows
        .iter()
        .any(|sr| sr.early_feedback.is_none() && !sr.is_eclipsed);

    // Assign update_rank for rows sharing the same target (PL/pgSQL update_rank logic).
    // Among rows with the same (grouping_key, target_valid_from), rank 1 = UPDATE, rank > 1 = INSERT.
    // PL/pgSQL: ORDER BY (f_from==t_from), (f_payload==t_payload), f_from, f_until
    let mut update_ranks: HashMap<usize, usize> = HashMap::new();
    {
        let mut by_target: HashMap<String, Vec<usize>> = HashMap::new();
        for (i, d) in diff_rows.iter().enumerate() {
            if d.target_valid_from.is_some() && d.final_valid_from.is_some() {
                let key = d.target_valid_from.as_ref().unwrap().clone();
                by_target.entry(key).or_default().push(i);
            }
        }
        for (_, indices) in &by_target {
            let mut sorted = indices.clone();
            sorted.sort_by(|&a, &b| {
                let da = &diff_rows[a];
                let db = &diff_rows[b];
                // 1. Prefer f_from == t_from (starts at same point as target)
                let a_starts = da.final_valid_from == da.target_valid_from;
                let b_starts = db.final_valid_from == db.target_valid_from;
                // 2. Prefer matching payload (keeps original data → residual gets UPDATE)
                // PL/pgSQL: (f_payload - ephemeral_columns) IS NOT DISTINCT FROM (t_payload - ephemeral_columns)
                // Both final_payload and target_payload are data-only (no ephemeral), so compare directly.
                let a_same_payload = match (&da.final_payload, &da.target_payload) {
                    (Some(fp), Some(tp)) => maps_equal_ignoring_nulls(fp, tp),
                    (None, None) => true,
                    _ => false,
                };
                let b_same_payload = match (&db.final_payload, &db.target_payload) {
                    (Some(fp), Some(tp)) => maps_equal_ignoring_nulls(fp, tp),
                    (None, None) => true,
                    _ => false,
                };
                let is_numeric = ctx.era.range_subtype_category == 'N';
                b_starts.cmp(&a_starts)
                    .then(b_same_payload.cmp(&a_same_payload))
                    .then_with(|| {
                        let af = da.final_valid_from.as_deref().unwrap_or("");
                        let bf = db.final_valid_from.as_deref().unwrap_or("");
                        temporal_cmp(af, bf, is_numeric)
                    })
                    .then_with(|| {
                        let au = da.final_valid_until.as_deref().unwrap_or("");
                        let bu = db.final_valid_until.as_deref().unwrap_or("");
                        temporal_cmp(au, bu, is_numeric)
                    })
            });
            for (rank, &idx) in sorted.iter().enumerate() {
                update_ranks.insert(idx, rank + 1);
            }
        }
    }

    for (i, d) in diff_rows.iter().enumerate() {
        let (mut operation, update_effect) = classify_single_diff(d, update_ranks.get(&i).copied(), is_numeric);

        // Target-only segments: either delete (with delete mode) or suppress
        if operation == PlanAction::SkipIdentical && !d.has_source_coverage {
            let should_delete =
                (has_active_sources && ctx.delete_mode.deletes_timeline())
                || (!has_active_sources && ctx.delete_mode.deletes_entities());
            if should_delete {
                operation = PlanAction::Delete;
            } else {
                continue;
            }
        }

        seq += 1;

        // old_valid: from the specific matched target row (per diff row)
        // PL/pgSQL: d.t_from as old_valid_from, d.t_until as old_valid_until
        let old_from = d.target_valid_from.clone();
        let old_until = d.target_valid_until.clone();

        let old_valid_range = match (&old_from, &old_until) {
            (Some(f), Some(u)) => Some(format_range(f, u)),
            _ => None,
        };
        let new_valid_range = match (&d.final_valid_from, &d.final_valid_until) {
            (Some(f), Some(u)) => Some(format_range(f, u)),
            _ => None,
        };

        // s_t_relation: per-segment (propagated from resolve phase through coalescing)
        // Available for all operations that have it (including SKIP_IDENTICAL)
        let s_t_relation = d.s_t_relation;

        // b_a_relation: computed between specific target and final segment
        // PL/pgSQL: get_allen_relation(target_seg.valid_from, target_seg.valid_until, final_seg.valid_from, final_seg.valid_until)
        let b_a_relation = match (
            &old_from,
            &old_until,
            &d.final_valid_from,
            &d.final_valid_until,
        ) {
            (Some(of), Some(ou), Some(nf), Some(nu)) => {
                AllenRelation::compute(of, ou, nf, nu, is_numeric)
            }
            _ => None,
        };

        // Build data payload: data + ephemeral for the final output.
        // The executor uses entity_keys || data to get full_data, so lookup keys
        // are available via entity_keys and don't need to be duplicated in data.
        let data = d.final_payload.clone().map(|mut p| {
            // Include ephemeral columns
            if let Some(ref eph) = d.ephemeral_payload {
                for (k, v) in eph {
                    p.insert(k.clone(), v.clone());
                }
            }
            // PL/pgSQL: appends valid_to = valid_until - 1 AFTER coalescing.
            // We must also append it here (post-coalesce) so it doesn't affect data_hash.
            // The valid_until for the final segment is the new_valid_until (for INSERT/UPDATE).
            if let Some(ref vt_col) = ctx.era.valid_to_col {
                let vu = d.final_valid_until.as_deref();
                if let Some(vt) = vu.and_then(date_minus_one) {
                    p.insert(vt_col.clone(), serde_json::Value::String(vt));
                }
            }
            serde_json::Value::Object(p)
        });

        // Build entity_keys: identity + lookup + pk-only keys (matches PL/pgSQL entity_keys field)
        let entity_keys = {
            let mut ek = d.identity_keys.clone();
            // Include lookup keys in entity_keys (PL/pgSQL includes them)
            if let Some(ref lk) = group_lookup_keys {
                if let Some(obj) = lk.as_object() {
                    for (k, v) in obj {
                        if !ek.contains_key(k) {
                            ek.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
            // Include PK-only columns from target (e.g., serial employment_id)
            if let Some(ref pk) = d.target_pk_payload {
                for (k, v) in pk {
                    if !ek.contains_key(k) {
                        ek.insert(k.clone(), v.clone());
                    }
                }
            }
            if ek.is_empty() { None } else { Some(serde_json::Value::Object(ek)) }
        };
        // identity_keys: just the identity columns (separate from entity_keys)
        let identity_keys = if d.identity_keys.is_empty() {
            None
        } else {
            Some(serde_json::Value::Object(d.identity_keys.clone()))
        };

        // For DELETE operations: clear new-side fields (matches PL/pgSQL format)
        if operation == PlanAction::Delete {
            plan_rows.push(PlanRow {
                plan_op_seq: seq,
                statement_seq: 0,
                row_ids: Vec::new(),
                operation,
                update_effect: None,
                causal_id: None,
                is_new_entity: d.is_new_entity,
                entity_keys: entity_keys.clone(),
                identity_keys: identity_keys.clone(),
                lookup_keys: group_lookup_keys.clone(),
                s_t_relation: None,
                b_a_relation: None,
                old_valid_from: old_from,
                old_valid_until: old_until,
                new_valid_from: None,
                new_valid_until: None,
                old_valid_range,
                new_valid_range: None,
                data: None,
                feedback: None,
                trace: None,
                grouping_key: String::new(),
            });
        } else {
            plan_rows.push(PlanRow {
                plan_op_seq: seq,
                statement_seq: 0,
                row_ids: d.row_ids.clone(),
                operation,
                update_effect,
                causal_id: d.causal_id.clone(),
                is_new_entity: d.is_new_entity,
                entity_keys: entity_keys.clone(),
                identity_keys,
                lookup_keys: group_lookup_keys.clone(),
                s_t_relation,
                b_a_relation,
                old_valid_from: old_from,
                old_valid_until: old_until,
                new_valid_from: d.final_valid_from.clone(),
                new_valid_until: d.final_valid_until.clone(),
                old_valid_range,
                new_valid_range,
                data,
                feedback: None,
                trace: None,
                grouping_key: d.grouping_key.clone(),
            });
        }
    }

    plan_rows
}

/// Classify a single diff row into an operation.
/// update_rank: if Some, this row shares a target with other rows.
/// Rank 1 = UPDATE, rank > 1 = INSERT (split from same target).
fn classify_single_diff(d: &DiffRow, update_rank: Option<usize>, is_numeric: bool) -> (PlanAction, Option<UpdateEffect>) {
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

            // Check if payload changed (data + ephemeral vs target data + target ephemeral)
            // PL/pgSQL compares f_payload (data||ephemeral) with t_payload (data||ephemeral)
            let payload_identical = {
                let f_merged = merge_data_ephemeral(&d.final_payload, &d.ephemeral_payload);
                let t_merged = merge_data_ephemeral(&d.target_payload, &d.target_ephemeral);
                match (f_merged.as_ref(), t_merged.as_ref()) {
                    (Some(fp), Some(tp)) => maps_equal_ignoring_nulls(fp, tp),
                    (None, None) => true,
                    _ => false,
                }
            };

            if f_from == t_from && f_until == t_until && payload_identical {
                // Same range, same payload → SKIP
                (PlanAction::SkipIdentical, None)
            } else {
                // Determine if this is UPDATE (rank 1) or INSERT (rank > 1)
                match update_rank {
                    Some(1) => {
                        let effect = compute_update_effect(t_from, t_until, f_from, f_until, is_numeric);
                        (PlanAction::Update, Some(effect))
                    }
                    Some(_) => {
                        // Rank > 1: this segment split from the same target → INSERT
                        (PlanAction::Insert, None)
                    }
                    None => {
                        // Only one segment for this target
                        let effect = compute_update_effect(t_from, t_until, f_from, f_until, is_numeric);
                        (PlanAction::Update, Some(effect))
                    }
                }
            }
        }
        (None, None) => {
            // Should not happen
            (PlanAction::Error, None)
        }
    }
}

/// Merge data_payload and ephemeral_payload into a single map.
fn merge_data_ephemeral(
    data: &Option<serde_json::Map<String, serde_json::Value>>,
    ephemeral: &Option<serde_json::Map<String, serde_json::Value>>,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    match (data, ephemeral) {
        (Some(d), Some(e)) => {
            let mut merged = d.clone();
            for (k, v) in e {
                merged.insert(k.clone(), v.clone());
            }
            Some(merged)
        }
        (Some(d), None) => Some(d.clone()),
        (None, Some(e)) => Some(e.clone()),
        (None, None) => None,
    }
}

fn compute_update_effect(
    old_from: &str,
    old_until: &str,
    new_from: &str,
    new_until: &str,
    is_numeric: bool,
) -> UpdateEffect {
    use std::cmp::Ordering;
    let cmp_from = temporal_cmp(new_from, old_from, is_numeric);
    let cmp_until = temporal_cmp(new_until, old_until, is_numeric);
    if cmp_from == Ordering::Equal && cmp_until == Ordering::Equal {
        UpdateEffect::None
    } else if cmp_from != Ordering::Less && cmp_until != Ordering::Greater {
        UpdateEffect::Shrink
    } else if cmp_from != Ordering::Greater && cmp_until != Ordering::Less {
        UpdateEffect::Grow
    } else {
        UpdateEffect::Move
    }
}

// ── Phase 5: Statement Sequencing ──

fn sequence_statements(plan_rows: &mut [PlanRow], ctx: &PlannerContext) {
    let is_numeric = ctx.era.range_subtype_category == 'N';
    // Sort matching PL/pgSQL: ORDER BY grouping_key, <entity_key_cols>,
    //   CASE operation DELETE=1 UPDATE=2 INSERT=3 ELSE=4,
    //   update_effect NULLS FIRST, COALESCE(old_valid_from, new_valid_from), row_ids[1]
    // Empty grouping_key sorts LAST (PostgreSQL NULLS LAST behavior)
    plan_rows.sort_by(|a, b| {
        // grouping_key: empty sorts last
        let a_empty = a.grouping_key.is_empty();
        let b_empty = b.grouping_key.is_empty();
        a_empty
            .cmp(&b_empty) // false < true, so non-empty sorts first
            .then_with(|| a.grouping_key.cmp(&b.grouping_key))
            .then_with(|| {
                // Entity key columns (from entity_keys JSON) — sort by key values
                let a_ek = a.entity_keys.as_ref().and_then(|v| v.as_object());
                let b_ek = b.entity_keys.as_ref().and_then(|v| v.as_object());
                let a_str = a_ek.map(json_map_to_key).unwrap_or_default();
                let b_str = b_ek.map(json_map_to_key).unwrap_or_default();
                a_str.cmp(&b_str)
            })
            .then_with(|| {
                // Operation type: DELETE=1, UPDATE=2, INSERT=3, SKIP/ERROR=4
                let op_ord = |p: &PlanRow| match p.operation {
                    PlanAction::Delete => 1,
                    PlanAction::Update => 2,
                    PlanAction::Insert => 3,
                    _ => 4,
                };
                op_ord(a).cmp(&op_ord(b))
            })
            .then_with(|| {
                // update_effect: NULLS FIRST
                let eff_ord = |p: &PlanRow| match p.update_effect {
                    None => 0,
                    Some(_) => 1,
                };
                eff_ord(a).cmp(&eff_ord(b))
            })
            .then_with(|| {
                // PL/pgSQL sorts MOVEs by old_valid_from DESC (later ranges first)
                // to prevent overlapping constraint violations during execution.
                // Non-MOVEs sort ASC.
                let a_is_move = a.update_effect == Some(UpdateEffect::Move);
                let b_is_move = b.update_effect == Some(UpdateEffect::Move);
                let a_from = a.old_valid_from.as_deref().or(a.new_valid_from.as_deref()).unwrap_or("");
                let b_from = b.old_valid_from.as_deref().or(b.new_valid_from.as_deref()).unwrap_or("");
                if a_is_move && b_is_move {
                    // MOVEs: DESC by old_valid_from
                    temporal_cmp(b_from, a_from, is_numeric)
                } else {
                    // Non-MOVEs: ASC
                    temporal_cmp(a_from, b_from, is_numeric)
                }
            })
            .then_with(|| {
                // new_valid_from ASC — deterministic tiebreaker for INSERT rows
                // that share the same old_valid_from (from the same ancestor target row)
                let a_nf = a.new_valid_from.as_deref().unwrap_or("");
                let b_nf = b.new_valid_from.as_deref().unwrap_or("");
                temporal_cmp(a_nf, b_nf, is_numeric)
            })
            .then_with(|| {
                let a_rid = a.row_ids.first().copied().unwrap_or(0);
                let b_rid = b.row_ids.first().copied().unwrap_or(0);
                a_rid.cmp(&b_rid)
            })
    });

    // Assign plan_op_seq sequentially
    for (i, row) in plan_rows.iter_mut().enumerate() {
        row.plan_op_seq = (i + 1) as i64;
    }

    // Compute statement_seq based on operation category (execution order).
    // Categories: DELETE=1, UPDATE(NONE/SHRINK)=2, UPDATE(MOVE)=3, UPDATE(GROW)=4, INSERT=5
    let op_category = |p: &PlanRow| -> i32 {
        match p.operation {
            PlanAction::Delete => 1,
            PlanAction::Update => match p.update_effect {
                Some(UpdateEffect::None) | Some(UpdateEffect::Shrink) => 2,
                Some(UpdateEffect::Move) => 3,
                Some(UpdateEffect::Grow) => 4,
                None => 2,
            },
            PlanAction::Insert => 5,
            _ => 0, // SKIPs / ERRORs get statement_seq 0
        }
    };

    // Collect distinct DML categories present, in execution order
    let mut categories: Vec<i32> = plan_rows
        .iter()
        .filter(|r| r.operation.is_dml())
        .map(|r| op_category(r))
        .collect();
    categories.sort_unstable();
    categories.dedup();

    // Assign statement_seq: same category → same seq, MOVE each gets own seq
    let mut move_count = 0i32;
    let base_move_seq = categories.iter().position(|&c| c == 3);

    // Compute the max statement_seq that DML rows will get (for SKIP/ERROR placement)
    let max_dml_seq = categories.len() as i32;

    for row in plan_rows.iter_mut() {
        if !row.operation.is_dml() {
            // PL/pgSQL: SKIP/ERROR rows get NULL raw_statement_seq → dense_rank places them
            // AFTER all DML categories
            row.statement_seq = max_dml_seq + 1;
            continue;
        }
        let cat = op_category(row);
        let base_seq = categories.iter().position(|&c| c == cat).unwrap() as i32 + 1;

        if cat == 3 {
            // Each MOVE gets its own statement
            move_count += 1;
            row.statement_seq = if move_count == 1 {
                base_seq
            } else {
                base_seq + move_count - 1
            };
        } else if base_move_seq.is_some() && cat > 3 && move_count > 1 {
            // Categories after MOVE need adjustment for extra MOVE statements
            row.statement_seq = base_seq + move_count - 1;
        } else {
            row.statement_seq = base_seq;
        }
    }
}

// ── Utility functions ──

fn make_feedback_plan_row(sr: &MatchedSourceRow, fb: &EarlyFeedback, ctx: &PlannerContext) -> PlanRow {
    let feedback_json = if fb.action == PlanAction::SkipNoTarget || fb.action == PlanAction::SkipFiltered {
        serde_json::json!({
            "info": "Source row was correctly filtered by the mode's logic and did not result in a DML operation."
        })
    } else {
        // PL/pgSQL ERROR feedback uses {"error": "message"} — the executor checks
        // feedback ? 'error' to extract the error message for source feedback.
        serde_json::json!({
            "error": fb.message.as_deref().unwrap_or("")
        })
    };

    // SKIP_NO_TARGET, SKIP_FILTERED, and ERROR don't emit temporal bounds (matches PL/pgSQL)
    let emit_temporal = fb.action != PlanAction::SkipNoTarget
        && fb.action != PlanAction::SkipFiltered
        && fb.action != PlanAction::Error;

    // PL/pgSQL uses v_grouping_key_expr_for_union for SKIP/ERROR rows.
    // For new entities without lookup keys, this uses causal_id (not identity values).
    let display_grouping_key = if sr.is_new_entity && sr.source.lookup_keys.is_empty() && ctx.all_lookup_cols.is_empty() {
        format!("new_entity__{}", sr.source.causal_id)
    } else {
        sr.grouping_key.clone()
    };

    // Build entity_keys: COALESCE source identity with discovered target identity (PL/pgSQL behavior).
    // Also include lookup column values for the entity_keys object.
    let mut ek = sr.source.identity_keys.clone();
    if let Some(ref disc) = sr.discovered_identity {
        for (k, v) in disc {
            if ek.get(k).map_or(true, |sv| sv.is_null()) {
                ek.insert(k.clone(), v.clone());
            }
        }
    }
    // Include lookup keys in entity_keys
    for (k, v) in &sr.source.lookup_keys {
        if !ek.contains_key(k) {
            ek.insert(k.clone(), v.clone());
        }
    }

    // identity_keys: COALESCE source identity with discovered
    let mut ik = sr.source.identity_keys.clone();
    if let Some(ref disc) = sr.discovered_identity {
        for (k, v) in disc {
            if ik.get(k).map_or(true, |sv| sv.is_null()) {
                ik.insert(k.clone(), v.clone());
            }
        }
    }

    PlanRow {
        plan_op_seq: 0, // Set in sequencing
        statement_seq: 0,
        row_ids: vec![sr.source.row_id],
        operation: fb.action,
        update_effect: None,
        causal_id: Some(sr.source.causal_id.clone()),
        is_new_entity: sr.is_new_entity,
        entity_keys: Some(serde_json::Value::Object(ek)),
        identity_keys: Some(serde_json::Value::Object(ik)),
        lookup_keys: Some(serde_json::Value::Object(sr.source.lookup_keys.clone())),
        s_t_relation: None,
        b_a_relation: None,
        old_valid_from: None,
        old_valid_until: None,
        new_valid_from: if emit_temporal { Some(sr.source.valid_from.clone()) } else { None },
        new_valid_until: if emit_temporal { Some(sr.source.valid_until.clone()) } else { None },
        old_valid_range: None,
        new_valid_range: if emit_temporal { Some(format_range(&sr.source.valid_from, &sr.source.valid_until)) } else { None },
        data: None,
        feedback: Some(feedback_json),
        trace: None,
        grouping_key: display_grouping_key,
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

/// Format a range string, quoting values that contain spaces (e.g., timestamps).
/// Produces `[2024-01-01,2025-01-01)` for dates and `["2023-12-31 16:00:00-08","2024-12-30 16:00:00-08")` for timestamps.
fn format_range(from: &str, until: &str) -> String {
    let q = |s: &str| {
        if s.contains(' ') {
            format!("\"{}\"", s)
        } else {
            s.to_string()
        }
    };
    format!("[{},{})", q(from), q(until))
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

/// Format a JSON value in PostgreSQL's jsonb text style (spaces after `:` and `,`).
fn json_to_pg_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Object(map) => {
            let entries: Vec<String> = map.iter()
                .map(|(k, val)| format!("\"{}\": {}", k, json_to_pg_text(val)))
                .collect();
            format!("{{{}}}", entries.join(", "))
        }
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(json_to_pg_text).collect();
            format!("[{}]", items.join(", "))
        }
        serde_json::Value::String(s) => format!("\"{}\"", s),
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
