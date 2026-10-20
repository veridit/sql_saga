//! Schema fingerprint used to validate the per-backend native caches.
//!
//! Both the planner cache (lib.rs) and the executor cache (executor_cache.rs) hold SQL
//! fragments derived from the catalog definitions of the source and the target. A cached
//! entry is only reused when the fingerprint computed on the current call, from the
//! catalog as seen by the current snapshot, equals the one recorded when it was built.
//! Because the catalog is read on every call, DDL in this transaction, DDL undone by a
//! ROLLBACK and DDL committed by another session are all seen exactly like a cold cache
//! would see them.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use pgrx::prelude::*;

/// Hash the catalog definitions the cached templates depend on:
/// - source: live column names, order, type OID and typmod;
/// - target: qualified name, live column names, order, type OID, typmod, NOT NULL,
///   default expression, identity and generated kind, and the primary key columns.
///
/// Each field is a separate JSON array element and the whole value is serialized as JSON, so
/// a name containing delimiter characters can never spell another column list.
///
/// Any query failure raises an error: a fingerprint that cannot be computed must never
/// be confused with an unchanged schema.
pub fn schema_fingerprint(source_oid: u32, target_oid: u32) -> u64 {
    let query = format!(
        "SELECT pg_catalog.jsonb_build_array( \
           (SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(a.attname, a.atttypid, a.atttypmod) \
                        ORDER BY a.attnum), '[]'::jsonb) \
              FROM pg_catalog.pg_attribute AS a \
              WHERE a.attrelid = {src}::oid AND a.attnum > 0 AND NOT a.attisdropped), \
           (SELECT pg_catalog.jsonb_build_array(n.nspname, c.relname) \
              FROM pg_catalog.pg_class AS c JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
              WHERE c.oid = {tgt}::oid), \
           (SELECT COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(a.attname, a.atttypid, a.atttypmod, \
                        a.attnotnull, a.attidentity, a.attgenerated, pg_catalog.pg_get_expr(d.adbin, d.adrelid)) \
                        ORDER BY a.attnum), '[]'::jsonb) \
              FROM pg_catalog.pg_attribute AS a \
              LEFT JOIN pg_catalog.pg_attrdef AS d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
              WHERE a.attrelid = {tgt}::oid AND a.attnum > 0 AND NOT a.attisdropped), \
           (SELECT pg_catalog.to_jsonb(i.indkey::int2[]) FROM pg_catalog.pg_index AS i \
              WHERE i.indrelid = {tgt}::oid AND i.indisprimary) \
         )::text",
        src = source_oid,
        tgt = target_oid,
    );
    let text: String = Spi::connect(|client| {
        client
            .select(&query, None, &[])
            .map_err(|e| format!("{e}"))
            .and_then(|t| t.first().get_one::<String>().map_err(|e| format!("{e}")))
    })
    .unwrap_or_else(|e| pgrx::error!("sql_saga: failed to read the schema fingerprint for the native cache: {}", e))
    .unwrap_or_else(|| pgrx::error!("sql_saga: the schema fingerprint for the native cache is NULL"));
    let mut h = DefaultHasher::new();
    text.hash(&mut h);
    h.finish()
}
