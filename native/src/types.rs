#![allow(dead_code)]

use std::cmp::Ordering;
use std::collections::BTreeSet;

// ── Merge mode (mirrors sql_saga.temporal_merge_mode) ──

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MergeMode {
    MergeEntityUpsert,
    UpdateForPortionOf,
    MergeEntityPatch,
    PatchForPortionOf,
    MergeEntityReplace,
    ReplaceForPortionOf,
    InsertNewEntities,
    DeleteForPortionOf,
}

impl MergeMode {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "MERGE_ENTITY_UPSERT" => Some(Self::MergeEntityUpsert),
            "UPDATE_FOR_PORTION_OF" => Some(Self::UpdateForPortionOf),
            "MERGE_ENTITY_PATCH" => Some(Self::MergeEntityPatch),
            "PATCH_FOR_PORTION_OF" => Some(Self::PatchForPortionOf),
            "MERGE_ENTITY_REPLACE" => Some(Self::MergeEntityReplace),
            "REPLACE_FOR_PORTION_OF" => Some(Self::ReplaceForPortionOf),
            "INSERT_NEW_ENTITIES" => Some(Self::InsertNewEntities),
            "DELETE_FOR_PORTION_OF" => Some(Self::DeleteForPortionOf),
            _ => None,
        }
    }

    pub fn is_patch(&self) -> bool {
        matches!(self, Self::MergeEntityPatch | Self::PatchForPortionOf)
    }

    pub fn is_replace(&self) -> bool {
        matches!(self, Self::MergeEntityReplace | Self::ReplaceForPortionOf)
    }

    /// REPLACE-family modes use "last-writer-wins": only the highest source_row_id
    /// contributes to row_ids per atomic segment. PATCH/UPSERT modes accumulate all.
    pub fn is_last_writer_wins(&self) -> bool {
        matches!(
            self,
            Self::MergeEntityReplace
                | Self::ReplaceForPortionOf
                | Self::InsertNewEntities
                | Self::DeleteForPortionOf
        )
    }

    pub fn is_for_portion_of(&self) -> bool {
        matches!(
            self,
            Self::UpdateForPortionOf
                | Self::PatchForPortionOf
                | Self::ReplaceForPortionOf
                | Self::DeleteForPortionOf
        )
    }

    pub fn is_entity_scope(&self) -> bool {
        matches!(
            self,
            Self::MergeEntityUpsert
                | Self::MergeEntityPatch
                | Self::MergeEntityReplace
                | Self::InsertNewEntities
        )
    }
}

// ── Delete mode (mirrors sql_saga.temporal_merge_delete_mode) ──

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DeleteMode {
    None,
    DeleteMissingTimeline,
    DeleteMissingEntities,
    DeleteMissingTimelineAndEntities,
}

impl DeleteMode {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "NONE" => Some(Self::None),
            "DELETE_MISSING_TIMELINE" => Some(Self::DeleteMissingTimeline),
            "DELETE_MISSING_ENTITIES" => Some(Self::DeleteMissingEntities),
            "DELETE_MISSING_TIMELINE_AND_ENTITIES" => Some(Self::DeleteMissingTimelineAndEntities),
            _ => Option::None,
        }
    }

    pub fn deletes_entities(&self) -> bool {
        matches!(
            self,
            Self::DeleteMissingEntities | Self::DeleteMissingTimelineAndEntities
        )
    }

    pub fn deletes_timeline(&self) -> bool {
        matches!(
            self,
            Self::DeleteMissingTimeline | Self::DeleteMissingTimelineAndEntities
        )
    }
}

// ── Plan action (mirrors sql_saga.temporal_merge_plan_action) ──

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PlanAction {
    Delete,
    Update,
    Insert,
    SkipIdentical,
    SkipNoTarget,
    SkipFiltered,
    SkipEclipsed,
    Error,
}

impl PlanAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::SkipIdentical => "SKIP_IDENTICAL",
            Self::SkipNoTarget => "SKIP_NO_TARGET",
            Self::SkipFiltered => "SKIP_FILTERED",
            Self::SkipEclipsed => "SKIP_ECLIPSED",
            Self::Error => "ERROR",
        }
    }

    pub fn is_dml(&self) -> bool {
        matches!(self, Self::Insert | Self::Update | Self::Delete)
    }
}

// ── Update effect (mirrors sql_saga.temporal_merge_update_effect) ──

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum UpdateEffect {
    None,
    Shrink,
    Move,
    Grow,
}

impl UpdateEffect {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Shrink => "SHRINK",
            Self::Move => "MOVE",
            Self::Grow => "GROW",
        }
    }
}

// ── Allen's Interval Algebra (mirrors sql_saga.allen_interval_relation) ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllenRelation {
    Precedes,
    Meets,
    Overlaps,
    Starts,
    During,
    Finishes,
    Equals,
    PrecededBy,
    MetBy,
    OverlappedBy,
    StartedBy,
    Contains,
    FinishedBy,
}

impl AllenRelation {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Precedes => "precedes",
            Self::Meets => "meets",
            Self::Overlaps => "overlaps",
            Self::Starts => "starts",
            Self::During => "during",
            Self::Finishes => "finishes",
            Self::Equals => "equals",
            Self::PrecededBy => "preceded_by",
            Self::MetBy => "met_by",
            Self::OverlappedBy => "overlapped_by",
            Self::StartedBy => "started_by",
            Self::Contains => "contains",
            Self::FinishedBy => "finished_by",
        }
    }

    /// Compute Allen relation between intervals [x_from, x_until) and [y_from, y_until).
    /// Returns None if any input is None.
    pub fn compute(x_from: &str, x_until: &str, y_from: &str, y_until: &str, is_numeric: bool) -> Option<Self> {
        let lt = |a: &str, b: &str| temporal_cmp(a, b, is_numeric) == Ordering::Less;
        let gt = |a: &str, b: &str| temporal_cmp(a, b, is_numeric) == Ordering::Greater;
        let eq = |a: &str, b: &str| temporal_cmp(a, b, is_numeric) == Ordering::Equal;

        if lt(x_until, y_from) {
            Some(Self::Precedes)
        } else if eq(x_until, y_from) {
            Some(Self::Meets)
        } else if lt(x_from, y_from) && lt(y_from, x_until) && lt(x_until, y_until) {
            Some(Self::Overlaps)
        } else if eq(x_from, y_from) && lt(x_until, y_until) {
            Some(Self::Starts)
        } else if gt(x_from, y_from) && lt(x_until, y_until) {
            Some(Self::During)
        } else if gt(x_from, y_from) && eq(x_until, y_until) {
            Some(Self::Finishes)
        } else if eq(x_from, y_from) && eq(x_until, y_until) {
            Some(Self::Equals)
        } else if lt(y_until, x_from) {
            Some(Self::PrecededBy)
        } else if eq(y_until, x_from) {
            Some(Self::MetBy)
        } else if lt(y_from, x_from) && lt(x_from, y_until) && lt(y_until, x_until) {
            Some(Self::OverlappedBy)
        } else if eq(x_from, y_from) && gt(x_until, y_until) {
            Some(Self::StartedBy)
        } else if lt(x_from, y_from) && gt(x_until, y_until) {
            Some(Self::Contains)
        } else if lt(x_from, y_from) && eq(x_until, y_until) {
            Some(Self::FinishedBy)
        } else {
            Option::None
        }
    }
}

/// Compare temporal boundary values with awareness of range subtype.
/// For numeric range subtypes (int4range, int8range, numrange), parse as f64.
/// For date/time subtypes, lexicographic string comparison is correct.
pub fn temporal_cmp(a: &str, b: &str, is_numeric: bool) -> Ordering {
    if is_numeric {
        let a_num = parse_temporal_numeric(a);
        let b_num = parse_temporal_numeric(b);
        a_num.partial_cmp(&b_num).unwrap_or(Ordering::Equal)
    } else {
        a.cmp(b)
    }
}

fn parse_temporal_numeric(s: &str) -> f64 {
    match s {
        "infinity" => f64::INFINITY,
        "-infinity" => f64::NEG_INFINITY,
        _ => s.parse::<f64>().unwrap_or(0.0),
    }
}

// ── Identification strategy (which keys are available) ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityStrategy {
    Hybrid,           // Both identity_columns and lookup_keys
    IdentityKeyOnly,  // Only identity_columns
    LookupKeyOnly,    // Only lookup_keys
    Undefined,        // Neither (error state)
}

// ── Era metadata (from sql_saga.era) ──

#[derive(Debug, Clone)]
pub struct EraMetadata {
    pub range_col: String,
    pub valid_from_col: String,
    pub valid_until_col: String,
    pub valid_to_col: Option<String>,
    pub range_type: String,
    pub multirange_type: String,
    pub range_subtype: String,
    pub range_subtype_category: char,
    pub ephemeral_columns: Vec<String>,
}

// ── Source row (read from source table via SPI) ──

#[derive(Debug, Clone)]
pub struct SourceRow {
    pub row_id: i64,
    pub causal_id: String,
    pub valid_from: String,
    pub valid_until: String,
    pub identity_keys: serde_json::Map<String, serde_json::Value>,
    pub lookup_keys: serde_json::Map<String, serde_json::Value>,
    pub data_payload: serde_json::Map<String, serde_json::Value>,
    pub ephemeral_payload: serde_json::Map<String, serde_json::Value>,
    pub stable_pk_payload: serde_json::Map<String, serde_json::Value>,
    pub is_identifiable: bool,
    pub lookup_cols_are_null: bool,
}

// ── Target row (read from target table via SPI) ──

#[derive(Debug, Clone)]
pub struct TargetRow {
    pub valid_from: String,
    pub valid_until: String,
    pub identity_keys: serde_json::Map<String, serde_json::Value>,
    pub lookup_keys: serde_json::Map<String, serde_json::Value>,
    pub data_payload: serde_json::Map<String, serde_json::Value>,
    pub ephemeral_payload: serde_json::Map<String, serde_json::Value>,
    /// PK-only columns (pk_cols minus identity/lookup/temporal) for entity_keys propagation
    pub pk_payload: serde_json::Map<String, serde_json::Value>,
}

// ── Matched source row (after entity correlation) ──

#[derive(Debug, Clone)]
pub struct MatchedSourceRow {
    pub source: SourceRow,
    pub is_new_entity: bool,
    pub grouping_key: String,
    /// Discovered identity from target match (None for new entities)
    pub discovered_identity: Option<serde_json::Map<String, serde_json::Value>>,
    /// Canonical NK JSON after matching
    pub canonical_nk_json: Option<serde_json::Map<String, serde_json::Value>>,
    /// Early feedback for error/skip conditions
    pub early_feedback: Option<EarlyFeedback>,
    /// Whether this row was eclipsed by another source row
    pub is_eclipsed: bool,
}

#[derive(Debug, Clone)]
pub struct EarlyFeedback {
    pub action: PlanAction,
    pub message: Option<String>,
}

// ── Atomic segment (time slice between consecutive boundaries) ──

#[derive(Debug, Clone)]
pub struct AtomicSegment {
    pub grouping_key: String,
    pub valid_from: String,
    pub valid_until: String,
    pub is_new_entity: bool,
    pub identity_keys: serde_json::Map<String, serde_json::Value>,
    pub causal_id: Option<String>,
}

// ── Resolved segment (after payload resolution) ──

#[derive(Debug, Clone)]
pub struct ResolvedSegment {
    pub grouping_key: String,
    pub valid_from: String,
    pub valid_until: String,
    pub is_new_entity: bool,
    pub identity_keys: serde_json::Map<String, serde_json::Value>,
    pub causal_id: Option<String>,
    pub row_ids: Vec<i64>,
    pub source_valid_from: Option<String>,
    pub source_valid_until: Option<String>,
    pub target_valid_from: Option<String>,
    pub target_valid_until: Option<String>,
    pub data_payload: Option<serde_json::Map<String, serde_json::Value>>,
    pub ephemeral_payload: Option<serde_json::Map<String, serde_json::Value>>,
    pub target_data_payload: Option<serde_json::Map<String, serde_json::Value>>,
    pub data_hash: Option<String>,
    /// True if at least one source row covers this segment
    pub has_source_coverage: bool,
    /// True if a target row covers this segment
    pub has_target_coverage: bool,
    /// Allen relation between source row range and covering target row range (per-segment)
    pub s_t_relation: Option<AllenRelation>,
}

// ── Coalesced segment (after merging adjacent identical segments) ──

#[derive(Debug, Clone)]
pub struct CoalescedSegment {
    pub grouping_key: String,
    pub valid_from: String,
    pub valid_until: String,
    pub is_new_entity: bool,
    pub identity_keys: serde_json::Map<String, serde_json::Value>,
    pub causal_id: Option<String>,
    pub row_ids: Vec<i64>,
    pub data_payload: Option<serde_json::Map<String, serde_json::Value>>,
    pub ephemeral_payload: Option<serde_json::Map<String, serde_json::Value>>,
    /// The original target valid_from for diff join (ancestor tracking)
    pub ancestor_valid_from: Option<String>,
    /// Pre-computed hash of data_payload (excluding nulls), carried from resolve phase.
    /// Invariant: data_payload is never modified during coalescing, so this stays valid.
    pub data_hash: Option<String>,
    /// True if at least one source row covers any part of this coalesced segment
    pub has_source_coverage: bool,
    /// True if at least one target row covers any part of this coalesced segment
    pub has_target_coverage: bool,
    /// Allen relation between source row range and covering target row range (first value)
    pub s_t_relation: Option<AllenRelation>,
}

// ── Diff row (result of FULL OUTER JOIN between coalesced and target) ──

#[derive(Debug, Clone)]
pub struct DiffRow {
    pub grouping_key: String,
    pub is_new_entity: bool,
    pub identity_keys: serde_json::Map<String, serde_json::Value>,
    pub causal_id: Option<String>,
    pub row_ids: Vec<i64>,
    // Final (coalesced) segment
    pub final_valid_from: Option<String>,
    pub final_valid_until: Option<String>,
    pub final_payload: Option<serde_json::Map<String, serde_json::Value>>,
    // Target segment
    pub target_valid_from: Option<String>,
    pub target_valid_until: Option<String>,
    pub target_payload: Option<serde_json::Map<String, serde_json::Value>>,
    // Ephemeral payload (carried for plan row output, not for comparison)
    pub ephemeral_payload: Option<serde_json::Map<String, serde_json::Value>>,
    /// True if at least one source row covers this segment
    pub has_source_coverage: bool,
    /// Allen relation between source row range and covering target row range
    pub s_t_relation: Option<AllenRelation>,
    /// Target row's ephemeral payload (for comparison in classify_single_diff)
    pub target_ephemeral: Option<serde_json::Map<String, serde_json::Value>>,
    /// Target row's lookup keys (for including in INSERT data when source doesn't have them)
    pub target_lookup_keys: Option<serde_json::Map<String, serde_json::Value>>,
    /// Target's PK-only columns (for entity_keys in existing entity operations)
    pub target_pk_payload: Option<serde_json::Map<String, serde_json::Value>>,
}

// ── Plan row (final output, matches sql_saga.temporal_merge_plan) ──

#[derive(Debug, Clone)]
pub struct PlanRow {
    pub plan_op_seq: i64,
    pub statement_seq: i32,
    pub row_ids: Vec<i64>,
    pub operation: PlanAction,
    pub update_effect: Option<UpdateEffect>,
    pub causal_id: Option<String>,
    pub is_new_entity: bool,
    pub entity_keys: Option<serde_json::Value>,
    pub identity_keys: Option<serde_json::Value>,
    pub lookup_keys: Option<serde_json::Value>,
    pub s_t_relation: Option<AllenRelation>,
    pub b_a_relation: Option<AllenRelation>,
    pub old_valid_from: Option<String>,
    pub old_valid_until: Option<String>,
    pub new_valid_from: Option<String>,
    pub new_valid_until: Option<String>,
    pub old_valid_range: Option<String>,
    pub new_valid_range: Option<String>,
    pub data: Option<serde_json::Value>,
    pub feedback: Option<serde_json::Value>,
    pub trace: Option<serde_json::Value>,
    pub grouping_key: String,
}

// ── Planner context (holds all metadata needed throughout planning) ──

#[derive(Debug, Clone)]
pub struct PlannerContext {
    pub mode: MergeMode,
    pub delete_mode: DeleteMode,
    pub era: EraMetadata,
    pub identity_columns: Vec<String>,
    pub all_lookup_cols: Vec<String>,
    /// Individual natural key sets for independent entity matching.
    /// PL/pgSQL tries each set with OR logic: match on ANY set succeeds.
    pub lookup_key_sets: Vec<Vec<String>>,
    pub original_entity_key_cols: Vec<String>,
    pub original_entity_segment_key_cols: Vec<String>,
    pub temporal_cols: Vec<String>,
    pub pk_cols: Vec<String>,
    pub strategy: IdentityStrategy,
    pub ephemeral_columns: Vec<String>,
    pub founding_id_column: Option<String>,
    pub row_id_column: String,
    pub log_trace: bool,
    /// Columns where NULL source values should be stripped in UPSERT/REPLACE modes.
    pub exclude_if_null_columns: std::collections::HashSet<String>,
}

impl PlannerContext {
    pub fn is_founding_mode(&self) -> bool {
        self.founding_id_column.is_some()
    }
}

// ── Entity group (all rows belonging to one entity, for sweep-line processing) ──

#[derive(Debug)]
pub struct EntityGroup {
    pub grouping_key: String,
    pub is_new_entity: bool,
    pub identity_keys: serde_json::Map<String, serde_json::Value>,
    pub source_rows: Vec<MatchedSourceRow>,
    pub target_rows: Vec<TargetRow>,
    pub time_boundaries: BTreeSet<String>,
}

// ── Column layout for ordinal-based reading (eliminates JSON parsing) ──

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColCategory {
    Identity,
    Lookup,
    Data,
    Ephemeral,
    /// PK columns that are NOT identity/lookup/temporal/ephemeral.
    /// These flow through as entity_keys but are excluded from data_payload.
    StablePk,
}

/// Describes one column in the SELECT list for ordinal-based reading.
/// Columns start at ordinal `first_ordinal` (after the fixed prefix columns).
#[derive(Debug, Clone)]
pub struct ColMapping {
    pub col_name: String,
    pub category: ColCategory,
    /// PostgreSQL type name (e.g., "integer", "text") for native JSON type parsing
    pub pg_type: String,
}

// ── Parameterized filter for target read ──

/// Describes a single parameterized filter column for the target read query.
/// Single-column key sets use: WHERE t."col" = ANY($N::text::type[])
/// Multi-column key sets use: WHERE (t."c1", t."c2") IN (SELECT ... FROM unnest($N1::type[], $N2::type[]))
#[derive(Debug, Clone)]
pub struct FilterParam {
    /// Column name in the target table (and source row identity_keys/lookup_keys)
    pub col_name: String,
    /// PostgreSQL type name (e.g., "integer", "text", "uuid")
    pub pg_type: String,
    /// 1-based parameter index ($1, $2, ...)
    pub param_index: usize,
    /// Whether this is from identity_columns (true) or all_lookup_cols (false)
    pub is_identity: bool,
    /// Group ID: columns with the same key_set_id form one composite key filter.
    /// Single-column key sets have unique IDs; multi-column share one ID.
    pub key_set_id: usize,
}

// ── Cached state for the planner (reused across batches within one session) ──

#[derive(Debug, Clone)]
pub struct CachedState {
    pub cache_key: u64,
    pub ctx: PlannerContext,
    pub target_ident: String,
    pub source_sql_template: String,
    pub target_sql_template: String,
    /// Column layout for source rows (ordinals start at 5, after row_id/causal/from/until).
    pub source_col_layout: Vec<ColMapping>,
    /// Column layout for target rows (ordinals start at 3, after valid_from/valid_until).
    pub target_col_layout: Vec<ColMapping>,
    /// If Some, target_sql_template uses $N parameters for the WHERE filter.
    /// Each FilterParam describes one = ANY($N::text::type[]) condition.
    /// If None, target_sql_template uses __SOURCE_IDENT__ subquery (dynamic SQL).
    pub target_filter_params: Option<Vec<FilterParam>>,
    /// Hash of source column names — used to detect when source structure changes
    /// (e.g., different source tables wrapped in the same CREATE OR REPLACE view).
    pub source_cols_hash: u64,
    /// Source table OID — used to invalidate cache when source table changes.
    /// Critical for test scenarios that drop and recreate source tables with different columns.
    pub source_oid: u32,
}
