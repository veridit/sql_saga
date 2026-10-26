/*
 * sql_saga 1.4 update: repairs after the 1.4 definitions are loaded (repairs fragment of
 * devops/generate_update_scripts.py, 1.4 delta). The generator emits the final integrity gate right after this.
 *
 * 1. Regular foreign keys 1.4 no longer accepts refuse the update, naming each one and what to do:
 *    a partitioned referencing table (its trigger was cloned to the partitions, which 1.4 refuses to run), and a
 *    reference to a predicated unique key (whose lookups ignored the predicate).
 * 2. Each regular foreign key gets its TRUNCATE trigger on the referenced table (foreign_keys.uk_truncate_trigger),
 *    named as a fresh 1.4 add_foreign_key names it. A table that already has a trigger of that name refuses.
 * Every statement runs inside the update's transaction: any refusal rolls the whole update back.
 */
DO $refuse_unsupported_regular_fks$
DECLARE
    fk record;
BEGIN
    FOR fk IN
        SELECT f.foreign_key_name, pg_catalog.format('%I.%I', f.table_schema, f.table_name) AS child
        FROM sql_saga.foreign_keys AS f
        JOIN pg_catalog.pg_namespace AS n ON n.nspname = f.table_schema
        JOIN pg_catalog.pg_class AS c ON (c.relnamespace, c.relname) = (n.oid, f.table_name)
        WHERE f.type = 'regular_to_temporal' AND c.relkind = 'p'
        ORDER BY f.foreign_key_name
        LIMIT 1
    LOOP
        RAISE EXCEPTION 'sql_saga: regular foreign key % references from the partitioned table %, which sql_saga 1.4 does not support',
            pg_catalog.quote_ident(fk.foreign_key_name), fk.child
            USING ERRCODE = 'feature_not_supported',
                  HINT = pg_catalog.format('Before updating, on the installed version: SELECT sql_saga.drop_foreign_key_by_name(%L, %L); then update, and add the foreign key to each partition with sql_saga.add_foreign_key.',
                      fk.child, fk.foreign_key_name);
    END LOOP;
    FOR fk IN
        SELECT f.foreign_key_name, pg_catalog.format('%I.%I', f.table_schema, f.table_name) AS child, u.unique_key_name
        FROM sql_saga.foreign_keys AS f
        JOIN sql_saga.unique_keys AS u ON u.unique_key_name = f.unique_key_name
        WHERE f.type = 'regular_to_temporal' AND u.key_type = 'predicated'
        ORDER BY f.foreign_key_name
        LIMIT 1
    LOOP
        RAISE EXCEPTION 'sql_saga: regular foreign key % references the predicated unique key %, which sql_saga 1.4 does not support',
            pg_catalog.quote_ident(fk.foreign_key_name), pg_catalog.quote_ident(fk.unique_key_name)
            USING ERRCODE = 'feature_not_supported',
                  HINT = pg_catalog.format('Its checks ignored the predicate. Before updating, on the installed version: SELECT sql_saga.drop_foreign_key_by_name(%L, %L); then update, and reference a primary or natural key instead.',
                      fk.child, fk.foreign_key_name);
    END LOOP;
END;
$refuse_unsupported_regular_fks$;

/* 1.4 stores predicated keys as PostgreSQL's canonical deparse, so function calls and qualified names compare
 * correctly in integrity checks. Existing catalogs hold the user's original text; normalize it from the constraint
 * itself, without rebuilding user constraints or evaluating their predicates. */
DO $normalize_predicated_key_predicates$
DECLARE
    k record;
    canonical text;
    n integer := 0;
BEGIN
    FOR k IN
        SELECT uk.unique_key_name, uk.table_schema, uk.table_name, uk.exclude_constraint, uk.predicate
        FROM sql_saga.unique_keys AS uk
        WHERE uk.key_type = 'predicated' AND uk.predicate IS NOT NULL
        ORDER BY uk.unique_key_name
    LOOP
        SELECT pg_catalog.pg_get_expr(i.indpred, i.indrelid) INTO canonical
        FROM pg_catalog.pg_constraint AS c
        JOIN pg_catalog.pg_index AS i ON i.indexrelid = c.conindid
        WHERE c.conrelid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', k.table_schema, k.table_name))
          AND c.conname = k.exclude_constraint AND c.contype = 'x';
        IF canonical IS NOT NULL AND canonical IS DISTINCT FROM k.predicate THEN
            UPDATE sql_saga.unique_keys AS uk SET predicate = canonical WHERE uk.unique_key_name = k.unique_key_name;
            n := n + 1;
        END IF;
    END LOOP;
    IF n > 0 THEN
        RAISE NOTICE 'sql_saga: Canonicalized % predicated unique-key predicates', n;
    END IF;
END;
$normalize_predicated_key_predicates$;

DO $create_regular_fk_truncate_triggers$
DECLARE
    fk record;
    n integer := 0;
BEGIN
    FOR fk IN
        SELECT f.foreign_key_name FROM sql_saga.foreign_keys AS f
        WHERE f.type = 'regular_to_temporal'
        ORDER BY f.foreign_key_name
    LOOP
        UPDATE sql_saga.foreign_keys AS f
        SET uk_truncate_trigger = sql_saga.__internal_make_name(ARRAY[f.foreign_key_name], 'uk_truncate')
        WHERE f.foreign_key_name = fk.foreign_key_name;
        PERFORM sql_saga.__internal_create_regular_fk_truncate_trigger(fk.foreign_key_name);
        n := n + 1;
    END LOOP;
    IF n > 0 THEN
        RAISE NOTICE 'sql_saga: Created the TRUNCATE trigger of % regular foreign keys on their referenced tables', n;
    END IF;
END;
$create_regular_fk_truncate_triggers$;
