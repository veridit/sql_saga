use pgrx::prelude::*;

use crate::types::{DeleteMode, EraMetadata, IdentityStrategy, MergeMode, PlannerContext};

/// Introspect era metadata from sql_saga.era for the given target table and era name.
pub fn introspect_era(
    target_table: pg_sys::Oid,
    era_name: &str,
) -> Result<EraMetadata, String> {
    let oid_val = u32::from(target_table);
    let era_escaped = era_name.replace('\'', "''");
    let query = format!(
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
        oid = oid_val,
        era = era_escaped,
    );

    Spi::connect(|client| {
        let row = client
            .select(&query, Some(1), &[])
            .map_err(|e| format!("SPI error introspecting era: {e}"))?
            .first();

        let range_col: String = row
            .get::<String>(1)
            .map_err(|e| format!("era query failed: {e}"))?
            .ok_or_else(|| format!("No era named \"{}\" found for target table", era_name))?;

        let valid_from: String = row.get::<String>(2).map_err(|e| format!("{e}"))?.unwrap_or_default();
        let valid_until: String = row.get::<String>(3).map_err(|e| format!("{e}"))?.unwrap_or_default();
        let valid_to: Option<String> = row.get::<String>(4).map_err(|e| format!("{e}"))?;
        let range_type: String = row.get::<String>(5).map_err(|e| format!("{e}"))?.unwrap_or_default();
        let multirange_type: String = row.get::<String>(6).map_err(|e| format!("{e}"))?.unwrap_or_default();
        let range_subtype: String = row.get::<String>(7).map_err(|e| format!("{e}"))?.unwrap_or_default();
        let subtype_cat_str: String = row.get::<String>(8).map_err(|e| format!("{e}"))?.unwrap_or_default();
        let ephemeral_cols: Vec<String> = row.get::<Vec<String>>(9).map_err(|e| format!("{e}"))?.unwrap_or_default();

        Ok(EraMetadata {
            range_col,
            valid_from_col: valid_from,
            valid_until_col: valid_until,
            valid_to_col: valid_to,
            range_type,
            multirange_type,
            range_subtype,
            range_subtype_category: subtype_cat_str.chars().next().unwrap_or(' '),
            ephemeral_columns: ephemeral_cols,
        })
    })
}

/// Introspect primary key columns for the target table (excluding temporal columns).
pub fn introspect_pk_cols(target_table: pg_sys::Oid, temporal_cols: &[String]) -> Vec<String> {
    let oid_val = u32::from(target_table);
    let query = format!(
        "SELECT COALESCE(array_agg(a.attname::text), '{{}}'::text[]) \
         FROM pg_constraint c \
         JOIN pg_attribute a ON a.attrelid = c.conrelid AND a.attnum = ANY(c.conkey) \
         WHERE c.conrelid = {}::oid AND c.contype = 'p'",
        oid_val
    );

    Spi::connect(|client| {
        let cols: Vec<String> = client
            .select(&query, None, &[])
            .ok()
            .and_then(|table| table.first().get_one::<Vec<String>>().ok().flatten())
            .unwrap_or_default();

        cols.into_iter()
            .filter(|c| !temporal_cols.contains(c))
            .collect()
    })
}

/// Build all canonical column lists for the planner context.
pub fn build_planner_context(
    mode: MergeMode,
    delete_mode: DeleteMode,
    era: EraMetadata,
    identity_columns: Option<Vec<String>>,
    all_lookup_cols: Option<Vec<String>>,
    pk_cols: Vec<String>,
    ephemeral_columns: Vec<String>,
    founding_id_column: Option<String>,
    row_id_column: String,
    log_trace: bool,
) -> PlannerContext {
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
        original_entity_key_cols: entity_key_cols,
        original_entity_segment_key_cols: segment_key_cols,
        temporal_cols,
        pk_cols,
        strategy,
        ephemeral_columns: all_ephemeral,
        founding_id_column,
        row_id_column,
        log_trace,
    }
}
