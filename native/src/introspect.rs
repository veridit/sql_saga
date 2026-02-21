use pgrx::prelude::*;

use crate::types::{DeleteMode, EraMetadata, IdentityStrategy, MergeMode, PlannerContext};

/// Result of all introspection queries needed on cache miss.
pub struct IntrospectionResult {
    pub era: EraMetadata,
    pub pk_cols: Vec<String>,
    pub target_ident: String,
    pub source_cols: Vec<String>,
    pub target_cols: Vec<String>,
    /// Map of target column name → PostgreSQL type name (e.g., "integer", "text")
    pub target_col_types: std::collections::HashMap<String, String>,
    /// Map of source column name → PostgreSQL type name
    pub source_col_types: std::collections::HashMap<String, String>,
    /// Columns where NULL source values should be stripped in UPSERT/REPLACE modes.
    /// Includes: NOT NULL with default, nullable with default, NOT NULL without default.
    /// Excludes: identity, generated, and nullable-without-default (REGULAR) columns.
    pub exclude_if_null_columns: std::collections::HashSet<String>,
}

/// Perform all introspection in a single SPI connection scope.
/// On cache miss, this replaces 5 separate Spi::connect() calls with 1.
pub fn introspect_all(
    target_table: pg_sys::Oid,
    source_table: pg_sys::Oid,
    era_name: &str,
) -> Result<IntrospectionResult, String> {
    let target_oid = u32::from(target_table);
    let source_oid = u32::from(source_table);
    let era_escaped = era_name.replace('\'', "''");

    Spi::connect(|client| {
        // 1. Era metadata
        let era_query = format!(
            r#"SELECT
                e.range_column_name::text,
                e.valid_from_column_name::text,
                e.valid_until_column_name::text,
                e.valid_to_column_name::text,
                e.range_type::text,
                e.multirange_type::text,
                e.range_subtype::text,
                e.range_subtype_category::text,
                COALESCE(e.ephemeral_columns::text[], '{{}}'::text[])
            FROM sql_saga.era AS e
            JOIN pg_class c ON c.relname = e.table_name
            JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = e.table_schema
            WHERE c.oid = {oid}::oid AND e.era_name = '{era}'"#,
            oid = target_oid,
            era = era_escaped,
        );
        let era_row = client
            .select(&era_query, Some(1), &[])
            .map_err(|e| format!("SPI error introspecting era: {e}"))?
            .first();

        let range_col: String = era_row
            .get::<String>(1)
            .map_err(|e| format!("era query failed: {e}"))?
            .ok_or_else(|| format!("No era named \"{}\" found for target table", era_name))?;
        let valid_from: String = era_row
            .get::<String>(2)
            .map_err(|e| format!("{e}"))?
            .unwrap_or_default();
        let valid_until: String = era_row
            .get::<String>(3)
            .map_err(|e| format!("{e}"))?
            .unwrap_or_default();
        let valid_to: Option<String> = era_row.get::<String>(4).map_err(|e| format!("{e}"))?;
        let range_type: String = era_row
            .get::<String>(5)
            .map_err(|e| format!("{e}"))?
            .unwrap_or_default();
        let multirange_type: String = era_row
            .get::<String>(6)
            .map_err(|e| format!("{e}"))?
            .unwrap_or_default();
        let range_subtype: String = era_row
            .get::<String>(7)
            .map_err(|e| format!("{e}"))?
            .unwrap_or_default();
        let subtype_cat_str: String = era_row
            .get::<String>(8)
            .map_err(|e| format!("{e}"))?
            .unwrap_or_default();
        let ephemeral_cols: Vec<String> = era_row
            .get::<Vec<String>>(9)
            .map_err(|e| format!("{e}"))?
            .unwrap_or_default();

        let era = EraMetadata {
            range_col,
            valid_from_col: valid_from,
            valid_until_col: valid_until,
            valid_to_col: valid_to,
            range_type,
            multirange_type,
            range_subtype,
            range_subtype_category: subtype_cat_str.chars().next().unwrap_or(' '),
            ephemeral_columns: ephemeral_cols,
        };

        // 2. PK columns (filtered by temporal cols derived from era)
        let mut temporal_cols = vec![era.range_col.clone(), era.valid_from_col.clone()];
        if let Some(ref vt) = era.valid_to_col {
            temporal_cols.push(vt.clone());
        }
        temporal_cols.push(era.valid_until_col.clone());

        let pk_query = format!(
            "SELECT COALESCE(array_agg(a.attname::text), '{{}}'::text[]) \
             FROM pg_constraint c \
             JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey) \
             WHERE c.conrelid = {}::oid AND c.contype = 'p'",
            target_oid
        );
        let pk_cols: Vec<String> = client
            .select(&pk_query, None, &[])
            .ok()
            .and_then(|table| table.first().get_one::<Vec<String>>().ok().flatten())
            .unwrap_or_default()
            .into_iter()
            .filter(|c| !temporal_cols.contains(c))
            .collect();

        // 3. Target table name
        let name_query = format!("SELECT {}::regclass::text", target_oid);
        let target_ident: String = client
            .select(&name_query, None, &[])
            .map_err(|e| format!("SPI error: {e}"))?
            .first()
            .get_one::<String>()
            .map_err(|e| format!("SPI error: {e}"))?
            .ok_or_else(|| "Could not resolve target table name".to_string())?;

        // 4. Source columns (all, including generated) with their types
        let src_cols_query = format!(
            "SELECT attname::text, atttypid::regtype::text FROM pg_attribute \
             WHERE attrelid = {}::oid AND attnum > 0 AND NOT attisdropped \
             ORDER BY attnum",
            source_oid
        );
        let (source_cols, source_col_types) = {
            let table = client
                .select(&src_cols_query, None, &[])
                .map_err(|e| format!("SPI error: {e}"))?;
            let mut cols = Vec::new();
            let mut types = std::collections::HashMap::new();
            for row in table {
                if let Some(name) = row.get::<String>(1).unwrap_or(None) {
                    if let Some(typ) = row.get::<String>(2).unwrap_or(None) {
                        types.insert(name.clone(), typ);
                    }
                    cols.push(name);
                }
            }
            (cols, types)
        };

        // 5. Target columns (excluding generated) with their types
        let tgt_cols_query = format!(
            "SELECT attname::text, atttypid::regtype::text FROM pg_attribute \
             WHERE attrelid = {}::oid AND attnum > 0 AND NOT attisdropped \
             AND attgenerated = '' ORDER BY attnum",
            target_oid
        );
        let (target_cols, target_col_types) = {
            let table = client
                .select(&tgt_cols_query, None, &[])
                .map_err(|e| format!("SPI error: {e}"))?;
            let mut cols = Vec::new();
            let mut types = std::collections::HashMap::new();
            for row in table {
                if let Some(name) = row.get::<String>(1).unwrap_or(None) {
                    if let Some(typ) = row.get::<String>(2).unwrap_or(None) {
                        types.insert(name.clone(), typ);
                    }
                    cols.push(name);
                }
            }
            (cols, types)
        };

        // 6. Exclude-if-null columns (for UPSERT/REPLACE NULL stripping)
        // Mirrors PL/pgSQL: NOT_NULL_DEFAULT + NULLABLE_DEFAULT + NOT_NULL_NO_DEFAULT
        // = all non-identity, non-generated columns where (attnotnull OR atthasdef)
        let exclude_null_query = format!(
            "SELECT COALESCE(array_agg(a.attname::text), '{{}}'::text[]) \
             FROM pg_attribute a \
             WHERE a.attrelid = {}::oid AND a.attnum > 0 AND NOT a.attisdropped \
             AND a.attidentity = '' AND a.attgenerated = '' \
             AND (a.attnotnull OR a.atthasdef)",
            target_oid
        );
        let exclude_if_null_columns: std::collections::HashSet<String> = client
            .select(&exclude_null_query, None, &[])
            .ok()
            .and_then(|table| table.first().get_one::<Vec<String>>().ok().flatten())
            .unwrap_or_default()
            .into_iter()
            .collect();

        Ok(IntrospectionResult {
            era,
            pk_cols,
            target_ident,
            source_cols,
            target_cols,
            target_col_types,
            source_col_types,
            exclude_if_null_columns,
        })
    })
}

/// Build all canonical column lists for the planner context.
pub fn build_planner_context(
    mode: MergeMode,
    delete_mode: DeleteMode,
    era: EraMetadata,
    identity_columns: Option<Vec<String>>,
    all_lookup_cols: Option<Vec<String>>,
    lookup_key_sets: Vec<Vec<String>>,
    pk_cols: Vec<String>,
    ephemeral_columns: Vec<String>,
    founding_id_column: Option<String>,
    row_id_column: String,
    log_trace: bool,
    exclude_if_null_columns: std::collections::HashSet<String>,
) -> PlannerContext {
    // Exclude all temporal columns from data payload: range, valid_from, valid_until, valid_to.
    // valid_to is computed from valid_until after coalescing (in classify_operations).
    let mut temporal_cols = vec![era.range_col.clone(), era.valid_from_col.clone()];
    if let Some(ref vt) = era.valid_to_col {
        temporal_cols.push(vt.clone());
    }
    temporal_cols.push(era.valid_until_col.clone());

    let identity_columns = identity_columns.unwrap_or_default();
    let all_lookup_cols = all_lookup_cols.unwrap_or_default();

    let has_identity = !identity_columns.is_empty();
    let has_lookup = !all_lookup_cols.is_empty();
    let strategy = match (has_identity, has_lookup) {
        (true, true) => IdentityStrategy::Hybrid,
        (true, false) => IdentityStrategy::IdentityKeyOnly,
        (false, true) => IdentityStrategy::LookupKeyOnly,
        (false, false) => IdentityStrategy::Undefined,
    };

    let mut segment_key_cols: Vec<String> = identity_columns
        .iter()
        .chain(all_lookup_cols.iter())
        .chain(pk_cols.iter())
        .cloned()
        .collect::<std::collections::BTreeSet<String>>()
        .into_iter()
        .collect();
    segment_key_cols.sort();

    let entity_key_cols: Vec<String> = segment_key_cols
        .iter()
        .filter(|c| !temporal_cols.contains(c))
        .cloned()
        .collect();

    let mut all_ephemeral = ephemeral_columns;
    for col in &era.ephemeral_columns {
        if !all_ephemeral.contains(col) {
            all_ephemeral.push(col.clone());
        }
    }
    all_ephemeral.sort();
    all_ephemeral.dedup();

    PlannerContext {
        mode,
        delete_mode,
        era,
        identity_columns,
        all_lookup_cols,
        lookup_key_sets,
        original_entity_key_cols: entity_key_cols,
        original_entity_segment_key_cols: segment_key_cols,
        temporal_cols,
        pk_cols,
        strategy,
        ephemeral_columns: all_ephemeral,
        founding_id_column,
        row_id_column,
        log_trace,
        exclude_if_null_columns,
    }
}
