use pgrx::prelude::*;

use crate::types::{PlannerContext, SourceRow, TargetRow};

/// Read all source rows via a single bulk SPI query.
pub fn read_source_rows(
    source_table: pg_sys::Oid,
    ctx: &PlannerContext,
) -> Result<Vec<SourceRow>, String> {
    let source_ident = resolve_table_name(source_table)?;
    let source_cols = list_table_columns(source_table)?;

    // Determine temporal column availability in source
    let has_range = source_cols.contains(&ctx.era.range_col);
    let has_from = source_cols.contains(&ctx.era.valid_from_col);
    let has_until = source_cols.contains(&ctx.era.valid_until_col);
    let has_to = ctx
        .era
        .valid_to_col
        .as_ref()
        .map(|c| source_cols.contains(c))
        .unwrap_or(false);

    if !has_from && !has_range {
        return Err(format!(
            "Source table must have either \"{}\" or \"{}\"",
            ctx.era.range_col, ctx.era.valid_from_col
        ));
    }

    let interval_expr = match ctx.era.range_subtype_category {
        'D' => "'1 day'::interval",
        'N' => "1",
        _ => return Err(format!("Unsupported range subtype category: {}", ctx.era.range_subtype_category)),
    };

    // Build valid_from expression
    let from_expr = if has_range {
        format!(
            "COALESCE(lower(s.{rc}), {fb})",
            rc = qi(&ctx.era.range_col),
            fb = if has_from { format!("s.{}", qi(&ctx.era.valid_from_col)) } else { "NULL".into() },
        )
    } else {
        format!("s.{}", qi(&ctx.era.valid_from_col))
    };

    // Build valid_until expression
    let until_expr = build_until_expr("s", has_range, has_until, has_to, ctx, interval_expr);

    // Build causal_id expression
    let causal_expr = if ctx.is_founding_mode() {
        format!(
            "COALESCE(s.{}::text, s.{}::text)",
            qi(ctx.founding_id_column.as_ref().unwrap()),
            qi(&ctx.row_id_column)
        )
    } else {
        format!("s.{}::text", qi(&ctx.row_id_column))
    };

    let identity_jsonb = build_jsonb_expr("s", &ctx.identity_columns);
    let lookup_jsonb = build_jsonb_expr("s", &ctx.all_lookup_cols);

    // Data columns: source cols that exist in source, excluding identity/temporal/ephemeral/row_id
    let exclude: Vec<&str> = ctx
        .original_entity_segment_key_cols
        .iter()
        .chain(ctx.temporal_cols.iter())
        .chain(ctx.ephemeral_columns.iter())
        .chain(std::iter::once(&ctx.row_id_column))
        .map(|s| s.as_str())
        .collect();

    let data_cols: Vec<String> = source_cols
        .iter()
        .filter(|c| !exclude.contains(&c.as_str()) && *c != "era_id" && *c != "era_name")
        .cloned()
        .collect();
    let data_jsonb = build_jsonb_expr("s", &data_cols);

    let eph_in_source: Vec<String> = ctx
        .ephemeral_columns
        .iter()
        .filter(|c| source_cols.contains(c))
        .cloned()
        .collect();
    let ephemeral_jsonb = build_jsonb_expr("s", &eph_in_source);

    let stable_pk_jsonb = build_jsonb_expr_nullable("s", &ctx.identity_columns, &source_cols);

    let id_not_all_null = if ctx.identity_columns.is_empty() {
        "true".into()
    } else {
        ctx.identity_columns
            .iter()
            .map(|c| format!("s.{} IS NOT NULL", qi(c)))
            .collect::<Vec<_>>()
            .join(" OR ")
    };

    let lookup_all_null = if ctx.all_lookup_cols.is_empty() {
        "true".into()
    } else {
        ctx.all_lookup_cols
            .iter()
            .map(|c| format!("s.{} IS NULL", qi(c)))
            .collect::<Vec<_>>()
            .join(" AND ")
    };

    let sql = format!(
        "SELECT s.{rid}::bigint, ({causal}), ({from_e})::text, ({until_e})::text, \
         {id_j}, {lk_j}, {data_j}, {eph_j}, {pk_j}, ({id_check}), ({lk_check}) \
         FROM {src} AS s",
        rid = qi(&ctx.row_id_column),
        causal = causal_expr,
        from_e = from_expr,
        until_e = until_expr,
        id_j = identity_jsonb,
        lk_j = lookup_jsonb,
        data_j = data_jsonb,
        eph_j = ephemeral_jsonb,
        pk_j = stable_pk_jsonb,
        id_check = id_not_all_null,
        lk_check = lookup_all_null,
        src = source_ident,
    );

    Spi::connect(|client| {
        let table = client
            .select(&sql, None, &[])
            .map_err(|e| format!("SPI error reading source rows: {e}"))?;

        let mut rows = Vec::with_capacity(table.len());
        for row in table {
            let row_id: i64 = row.get::<i64>(1).unwrap_or(Some(0)).unwrap_or(0);
            let causal_id: String = row.get::<String>(2).unwrap_or(Some(String::new())).unwrap_or_default();
            let valid_from: String = row.get::<String>(3).unwrap_or(Some(String::new())).unwrap_or_default();
            let valid_until: String = row.get::<String>(4).unwrap_or(Some(String::new())).unwrap_or_default();
            let identity_keys = get_jsonb_map(&row, 5);
            let lookup_keys = get_jsonb_map(&row, 6);
            let data_payload = get_jsonb_map(&row, 7);
            let ephemeral_payload = get_jsonb_map(&row, 8);
            let stable_pk_payload = get_jsonb_map(&row, 9);
            let is_identifiable: bool = row.get::<bool>(10).unwrap_or(Some(false)).unwrap_or(false);
            let lookup_cols_are_null: bool = row.get::<bool>(11).unwrap_or(Some(true)).unwrap_or(true);

            rows.push(SourceRow {
                row_id,
                causal_id,
                valid_from,
                valid_until,
                identity_keys,
                lookup_keys,
                data_payload,
                ephemeral_payload,
                stable_pk_payload,
                is_identifiable,
                lookup_cols_are_null,
            });
        }
        Ok(rows)
    })
}

/// Read all target rows for entities that might be affected by the merge.
pub fn read_target_rows(
    target_table: pg_sys::Oid,
    ctx: &PlannerContext,
) -> Result<Vec<TargetRow>, String> {
    let target_ident = resolve_table_name(target_table)?;

    let target_cols = list_table_columns_no_generated(target_table)?;

    let identity_jsonb = build_jsonb_expr("t", &ctx.identity_columns);
    let lookup_jsonb = build_jsonb_expr("t", &ctx.all_lookup_cols);

    let exclude: Vec<&str> = ctx
        .original_entity_segment_key_cols
        .iter()
        .chain(ctx.temporal_cols.iter())
        .chain(ctx.ephemeral_columns.iter())
        .map(|s| s.as_str())
        .collect();

    let data_cols: Vec<String> = target_cols
        .iter()
        .filter(|c| !exclude.contains(&c.as_str()) && *c != "era_id" && *c != "era_name")
        .cloned()
        .collect();
    let data_jsonb = build_jsonb_expr("t", &data_cols);

    let eph_in_target: Vec<String> = ctx
        .ephemeral_columns
        .iter()
        .filter(|c| target_cols.contains(c))
        .cloned()
        .collect();
    let ephemeral_jsonb = build_jsonb_expr("t", &eph_in_target);

    let sql = format!(
        "SELECT lower(t.{rc})::text, upper(t.{rc})::text, {id_j}, {lk_j}, {data_j}, {eph_j} \
         FROM {tgt} AS t",
        rc = qi(&ctx.era.range_col),
        id_j = identity_jsonb,
        lk_j = lookup_jsonb,
        data_j = data_jsonb,
        eph_j = ephemeral_jsonb,
        tgt = target_ident,
    );

    Spi::connect(|client| {
        let table = client
            .select(&sql, None, &[])
            .map_err(|e| format!("SPI error reading target rows: {e}"))?;

        let mut rows = Vec::with_capacity(table.len());
        for row in table {
            let valid_from: String = row.get::<String>(1).unwrap_or(Some(String::new())).unwrap_or_default();
            let valid_until: String = row.get::<String>(2).unwrap_or(Some(String::new())).unwrap_or_default();
            let identity_keys = get_jsonb_map(&row, 3);
            let lookup_keys = get_jsonb_map(&row, 4);
            let data_payload = get_jsonb_map(&row, 5);
            let ephemeral_payload = get_jsonb_map(&row, 6);

            rows.push(TargetRow {
                valid_from,
                valid_until,
                identity_keys,
                lookup_keys,
                data_payload,
                ephemeral_payload,
            });
        }
        Ok(rows)
    })
}

// ── Helpers ──

fn qi(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn build_jsonb_expr(alias: &str, cols: &[String]) -> String {
    if cols.is_empty() {
        return "'{}'::jsonb".to_string();
    }
    let pairs: Vec<String> = cols
        .iter()
        .map(|c| format!("'{}', {}.{}", c.replace('\'', "''"), alias, qi(c)))
        .collect();
    format!("jsonb_build_object({})", pairs.join(", "))
}

fn build_jsonb_expr_nullable(alias: &str, cols: &[String], available: &[String]) -> String {
    if cols.is_empty() {
        return "'{}'::jsonb".to_string();
    }
    let pairs: Vec<String> = cols
        .iter()
        .map(|c| {
            if available.contains(c) {
                format!("'{}', {}.{}", c.replace('\'', "''"), alias, qi(c))
            } else {
                format!("'{}', NULL", c.replace('\'', "''"))
            }
        })
        .collect();
    format!("jsonb_build_object({})", pairs.join(", "))
}

fn build_until_expr(
    alias: &str,
    has_range: bool,
    has_until: bool,
    has_to: bool,
    ctx: &PlannerContext,
    interval_expr: &str,
) -> String {
    let to_expr = |a: &str| -> String {
        format!(
            "({a}.{vt} + {iv})::{st}",
            a = a,
            vt = qi(ctx.era.valid_to_col.as_ref().unwrap()),
            iv = interval_expr,
            st = ctx.era.range_subtype,
        )
    };

    if has_range {
        let fallback = if has_until && has_to {
            format!("COALESCE({a}.{vu}, {to})",
                a = alias, vu = qi(&ctx.era.valid_until_col), to = to_expr(alias))
        } else if has_until {
            format!("{}.{}", alias, qi(&ctx.era.valid_until_col))
        } else if has_to {
            to_expr(alias)
        } else {
            "NULL".into()
        };
        format!("COALESCE(upper({a}.{rc}), {fb})", a = alias, rc = qi(&ctx.era.range_col), fb = fallback)
    } else if has_until && has_to {
        format!("COALESCE({a}.{vu}, {to})",
            a = alias, vu = qi(&ctx.era.valid_until_col), to = to_expr(alias))
    } else if has_until {
        format!("{}.{}", alias, qi(&ctx.era.valid_until_col))
    } else if has_to {
        to_expr(alias)
    } else {
        "NULL".into()
    }
}

fn resolve_table_name(table_oid: pg_sys::Oid) -> Result<String, String> {
    let sql = format!("SELECT {}::regclass::text", u32::from(table_oid));
    Spi::connect(|client| {
        client
            .select(&sql, None, &[])
            .map_err(|e| format!("SPI error: {e}"))?
            .first()
            .get_one::<String>()
            .map_err(|e| format!("SPI error: {e}"))
    })?
    .ok_or_else(|| "Could not resolve table name".to_string())
}

fn list_table_columns(table_oid: pg_sys::Oid) -> Result<Vec<String>, String> {
    let sql = format!(
        "SELECT attname::text FROM pg_attribute WHERE attrelid = {}::oid AND attnum > 0 AND NOT attisdropped ORDER BY attnum",
        u32::from(table_oid)
    );
    Spi::connect(|client| {
        let table = client
            .select(&sql, None, &[])
            .map_err(|e| format!("SPI error: {e}"))?;

        let mut cols = Vec::new();
        for row in table {
            if let Some(name) = row.get::<String>(1).unwrap_or(None) {
                cols.push(name);
            }
        }
        Ok(cols)
    })
}

fn list_table_columns_no_generated(table_oid: pg_sys::Oid) -> Result<Vec<String>, String> {
    let sql = format!(
        "SELECT attname::text FROM pg_attribute WHERE attrelid = {}::oid AND attnum > 0 AND NOT attisdropped AND attgenerated = '' ORDER BY attnum",
        u32::from(table_oid)
    );
    Spi::connect(|client| {
        let table = client
            .select(&sql, None, &[])
            .map_err(|e| format!("SPI error: {e}"))?;

        let mut cols = Vec::new();
        for row in table {
            if let Some(name) = row.get::<String>(1).unwrap_or(None) {
                cols.push(name);
            }
        }
        Ok(cols)
    })
}

fn get_jsonb_map(
    row: &pgrx::spi::SpiHeapTupleData,
    ordinal: usize,
) -> serde_json::Map<String, serde_json::Value> {
    match row.get::<pgrx::JsonB>(ordinal) {
        Ok(Some(pgrx::JsonB(serde_json::Value::Object(map)))) => map,
        _ => serde_json::Map::new(),
    }
}
