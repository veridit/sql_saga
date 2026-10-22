CREATE OR REPLACE FUNCTION sql_saga.drop_protection()
 RETURNS event_trigger
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path = sql_saga, pg_catalog, public
AS
$function$
#variable_conflict use_variable
DECLARE
    r record;
    table_oid regclass;
    era_name name;
    v_teardown boolean := sql_saga.__internal_is_clean_teardown();
    v_dropped_objects jsonb[];
    v_sanctioned_bases jsonb[];
    v_refusal_objects jsonb[];
    v_candidate record;
    v_object_types text[];
    v_teardown_family boolean := false;
BEGIN
    /* Materialize this invocation once. Nested DDL gets its own function frame. */
    SELECT coalesce(
        array_agg(to_jsonb(dobj) ORDER BY dobj.ordinality),
        ARRAY[]::jsonb[])
    INTO v_dropped_objects
    FROM pg_catalog.pg_event_trigger_dropped_objects() WITH ORDINALITY AS dobj;

    SELECT pg_catalog.array_agg(DISTINCT u.data->>'object_type' ORDER BY u.data->>'object_type')
    INTO v_object_types
    FROM unnest(v_dropped_objects) AS u(data);

    /*
     * During a recognized clean teardown, any non-policy object addressed on a system-versioned base or
     * history table must reach the teardown cleanup below (it drops the installation-token history
     * policies). Neither early exit may skip it. Outside a teardown this costs nothing.
     */
    IF v_teardown THEN
        SELECT EXISTS (
            SELECT 1
            FROM unnest(v_dropped_objects) AS u(data)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(object_type text, address_names text[])
            JOIN sql_saga.system_versioning AS sv
              ON (d.address_names[1], d.address_names[2]) IN (
                  (sv.table_schema, sv.table_name), (sv.history_schema_name, sv.history_table_name))
            WHERE d.object_type <> 'policy')
        INTO v_teardown_family;
    END IF;

    IF v_teardown_family THEN
        NULL; /* Admitted: see above. */
    ELSIF v_object_types = ARRAY['function'] AND NOT EXISTS (
        SELECT 1
        FROM unnest(v_dropped_objects) AS u(data)
        JOIN sql_saga.system_versioning AS sv
          ON u.data->>'object_identity' = ANY (
              ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to])) THEN
        RETURN;
    ELSIF v_object_types = ARRAY['view'] AND NOT EXISTS (
        SELECT 1
        FROM unnest(v_dropped_objects) AS u(data)
        CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(address_names text[])
        WHERE EXISTS (
                  SELECT FROM sql_saga.updatable_view AS v
                  WHERE (v.view_schema, v.view_name) = (d.address_names[1], d.address_names[2]))
           OR EXISTS (
                  SELECT FROM sql_saga.system_versioning AS sv
                  WHERE (d.address_names[1], d.address_names[2]) IN (
                      (sv.view_schema_name, sv.view_table_name),
                      (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name))))) THEN
        RETURN;
    ELSIF v_object_types = ARRAY['table'] AND NOT EXISTS (
        SELECT 1
        FROM unnest(v_dropped_objects) AS u(data)
        CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(address_names text[])
        WHERE EXISTS (
                  SELECT FROM sql_saga.era AS e
                  WHERE (e.table_schema, e.table_name) = (d.address_names[1], d.address_names[2]))
           OR EXISTS (
                  SELECT FROM sql_saga.system_versioning AS sv
                  WHERE (sv.history_schema_name, sv.history_table_name) =
                        (d.address_names[1], d.address_names[2]))) THEN
        RETURN;
    ELSIF v_object_types = ARRAY['trigger'] AND NOT EXISTS (
        SELECT 1
        FROM unnest(v_dropped_objects) AS u(data)
        CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(address_names text[])
        WHERE EXISTS (
                  SELECT FROM sql_saga.system_time_era AS ste
                  WHERE (ste.table_schema, ste.table_name) = (d.address_names[1], d.address_names[2])
                    AND d.address_names[3] IN (
                        ste.generated_always_trigger, ste.write_history_trigger, ste.truncate_trigger))
           OR EXISTS (
                  SELECT FROM sql_saga.updatable_view AS v
                  WHERE (v.view_schema, v.view_name, v.trigger_name) =
                        (d.address_names[1], d.address_names[2], d.address_names[3]))
           OR EXISTS (
                  SELECT FROM sql_saga.foreign_keys AS fk
                  JOIN sql_saga.unique_keys AS uk USING (unique_key_name)
                  WHERE (uk.table_schema, uk.table_name) = (d.address_names[1], d.address_names[2])
                    AND d.address_names[3] IN (fk.uk_update_trigger, fk.uk_delete_trigger, fk.uk_truncate_trigger))
           OR EXISTS (
                  SELECT FROM sql_saga.foreign_keys AS fk
                  WHERE fk.type = 'regular_to_temporal'
                    AND (fk.table_schema, fk.table_name, fk.fk_check_constraint) =
                        (d.address_names[1], d.address_names[2], d.address_names[3]))) THEN
        RETURN;
    END IF;

    /* Ordinary and metadata-first DDL must not pay for protected-object analysis. */
    IF NOT v_teardown_family AND NOT EXISTS (
        WITH dropped AS MATERIALIZED (
            SELECT d.*
            FROM unnest(v_dropped_objects) AS u(data)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                object_type text, object_identity text, address_names text[], objid oid)
        )
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.era AS e
          ON (d.object_type = 'table'
              AND (d.address_names[1], d.address_names[2]) = (e.table_schema, e.table_name))
          OR (d.object_type = 'table column'
              AND (d.address_names[1], d.address_names[2]) = (e.table_schema, e.table_name)
              AND d.address_names[3] IN (e.valid_from_column_name, e.valid_until_column_name))
          OR d.objid = e.range_type::oid
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.system_time_era AS ste
          ON (d.object_type = 'table constraint'
              AND (d.address_names[1], d.address_names[2], d.address_names[3]) =
                  (ste.table_schema, ste.table_name, ste.infinity_check_constraint))
          OR (d.object_type = 'trigger'
              AND (d.address_names[1], d.address_names[2]) = (ste.table_schema, ste.table_name)
              AND d.address_names[3] IN (ste.generated_always_trigger, ste.write_history_trigger, ste.truncate_trigger))
          OR (d.object_type = 'table column'
              AND (d.address_names[1], d.address_names[2]) = (ste.table_schema, ste.table_name)
              AND d.address_names[3] = ANY (ste.excluded_column_names))
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.updatable_view AS v
          ON (d.object_type = 'view'
              AND (d.address_names[1], d.address_names[2]) = (v.view_schema, v.view_name))
          OR (d.object_type = 'trigger'
              AND (d.address_names[1], d.address_names[2], d.address_names[3]) =
                  (v.view_schema, v.view_name, v.trigger_name))
          OR (d.object_type IN ('table column', 'table constraint', 'index')
              AND (d.address_names[1], d.address_names[2]) = (v.table_schema, v.table_name))
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.unique_keys AS uk
          ON (d.object_type = 'table constraint'
              AND (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.table_name)
              AND (d.address_names[3] IN (uk.unique_constraint, uk.exclude_constraint, uk.check_constraint)
                   OR d.address_names[3] = ANY (coalesce(uk.pk_consistency_constraint_names, '{}'))))
          OR (d.object_type = 'index'
              AND (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.unique_constraint))
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.foreign_keys AS fk
          ON d.object_type = 'trigger'
         AND d.address_names[3] IN (fk.uk_update_trigger, fk.uk_delete_trigger, fk.uk_truncate_trigger)
        JOIN sql_saga.unique_keys AS uk
          ON uk.unique_key_name = fk.unique_key_name
         AND (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.table_name)
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.foreign_keys AS fk
          ON d.object_type = 'trigger'
         AND fk.type = 'regular_to_temporal'
         AND (d.address_names[1], d.address_names[2], d.address_names[3]) =
             (fk.table_schema, fk.table_name, fk.fk_check_constraint)
        UNION ALL
        SELECT 1
        FROM dropped AS d
        JOIN sql_saga.system_versioning AS sv
          ON (d.object_type = 'table'
              AND (d.address_names[1], d.address_names[2]) = (sv.history_schema_name, sv.history_table_name))
          OR (d.object_type = 'view'
              AND (d.address_names[1], d.address_names[2]) IN (
                  (sv.view_schema_name, sv.view_table_name),
                  (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name))))
          OR (d.object_type = 'function'
              AND d.object_identity = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]))
        UNION ALL
        SELECT 1
        FROM dropped AS d
        CROSS JOIN LATERAL sql_saga.__internal_history_family(
            to_regclass(format('%I.%I', d.address_names[1], d.address_names[2]))) AS f
        WHERE d.object_type = 'policy'
    ) THEN
        RETURN;
    END IF;

    /*
     * Choose exactly one refusal from this statement's payload. Priority preserves the historical
     * source-class order; ordinality preserves PostgreSQL's dependency order within a class.
     */
    IF NOT v_teardown THEN
        /*
         * Remove complete families before constructing refusal candidates.  The ordinalities
         * identify rows in this invocation's materialized payload, so no dropped-object OID is
         * retained beyond the statement.  Objects not belonging to a dropped base family remain
         * available for mixed sanctioned/unsanctioned statements.
         */
        IF EXISTS (
            SELECT 1
            FROM unnest(v_dropped_objects) AS u(data)
            WHERE u.data->>'object_type' = 'table'
        ) THEN
            SELECT coalesce(
                array_agg(DISTINCT jsonb_build_array(e.table_schema, e.table_name)),
                ARRAY[]::jsonb[])
            INTO v_sanctioned_bases
            FROM unnest(v_dropped_objects) AS u(data)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                object_type text, address_names text[])
            JOIN sql_saga.era AS e
              ON d.object_type = 'table'
             AND (d.address_names[1], d.address_names[2]) = (e.table_schema, e.table_name);
        ELSE
            v_sanctioned_bases := ARRAY[]::jsonb[];
        END IF;

        IF cardinality(v_sanctioned_bases) = 0 THEN
            v_refusal_objects := v_dropped_objects;
        ELSIF EXISTS (
            SELECT 1
            FROM unnest(v_dropped_objects) AS u(data)
            WHERE u.data->>'object_type' = 'schema'
        ) AND NOT EXISTS (
            WITH dropped AS MATERIALIZED (
                SELECT d.*
                FROM unnest(v_dropped_objects) AS u(data)
                CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                    object_type text, object_identity text, address_names text[])
            )
            SELECT 1
            FROM dropped AS d
            JOIN sql_saga.updatable_view AS v
              ON ((d.object_type = 'view'
                   AND (d.address_names[1], d.address_names[2]) = (v.view_schema, v.view_name))
                  OR (d.object_type = 'trigger'
                      AND (d.address_names[1], d.address_names[2], d.address_names[3]) =
                          (v.view_schema, v.view_name, v.trigger_name)))
            WHERE NOT jsonb_build_array(v.table_schema, v.table_name) = ANY (v_sanctioned_bases)
            UNION ALL
            SELECT 1
            FROM dropped AS d
            JOIN sql_saga.system_versioning AS sv
              ON (d.object_type = 'table'
                  AND (d.address_names[1], d.address_names[2]) =
                      (sv.history_schema_name, sv.history_table_name))
              OR (d.object_type = 'view'
                  AND (d.address_names[1], d.address_names[2]) IN (
                      (sv.view_schema_name, sv.view_table_name),
                      (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name))))
              OR (d.object_type = 'function'
                  AND d.object_identity = ANY (ARRAY[
                      sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to]))
            WHERE NOT jsonb_build_array(sv.table_schema, sv.table_name) = ANY (v_sanctioned_bases)
        ) THEN
            v_refusal_objects := ARRAY[]::jsonb[];
        ELSE
        WITH dropped AS MATERIALIZED (
            SELECT u.ordinality AS dropped_ordinality, u.data, d.*
            FROM unnest(v_dropped_objects) WITH ORDINALITY AS u(data, ordinality)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                object_type text, object_identity text, address_names text[], objid oid)
        ), sanctioned_bases AS MATERIALIZED (
            SELECT b.data->>0 AS table_schema, b.data->>1 AS table_name
            FROM unnest(v_sanctioned_bases) AS b(data)
        )
        SELECT coalesce(
            array_agg(d.data ORDER BY d.dropped_ordinality) FILTER (WHERE NOT EXISTS (
                SELECT 1
                FROM sanctioned_bases AS b
                WHERE (d.address_names[1], d.address_names[2]) = (b.table_schema, b.table_name)
                   OR EXISTS (
                        SELECT 1
                        FROM sql_saga.era AS e
                        WHERE (e.table_schema, e.table_name) = (b.table_schema, b.table_name)
                          AND d.objid = e.range_type::oid)
                   OR (d.object_type = 'index' AND EXISTS (
                        SELECT 1
                        FROM sql_saga.unique_keys AS uk
                        WHERE (uk.table_schema, uk.table_name) = (b.table_schema, b.table_name)
                          AND (d.address_names[1], d.address_names[2]) =
                              (uk.table_schema, uk.unique_constraint)))
                   OR (d.object_type = 'trigger' AND EXISTS (
                        /* A regular FK's child-side trigger depends on (FROM) the dropped referenced table. */
                        SELECT 1
                        FROM sql_saga.foreign_keys AS fk
                        JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
                        WHERE (uk.table_schema, uk.table_name) = (b.table_schema, b.table_name)
                          AND fk.type = 'regular_to_temporal'
                          AND (d.address_names[1], d.address_names[2], d.address_names[3]) =
                              (fk.table_schema, fk.table_name, fk.fk_check_constraint)))
                   OR EXISTS (
                        SELECT 1
                        FROM sql_saga.updatable_view AS v
                        WHERE (v.table_schema, v.table_name) = (b.table_schema, b.table_name)
                          AND (d.address_names[1], d.address_names[2]) = (v.view_schema, v.view_name))
                   OR EXISTS (
                        SELECT 1
                        FROM sql_saga.system_versioning AS sv
                        WHERE (sv.table_schema, sv.table_name) = (b.table_schema, b.table_name)
                          AND ((d.address_names[1], d.address_names[2]) IN (
                                  (sv.history_schema_name, sv.history_table_name),
                                  (sv.view_schema_name, sv.view_table_name),
                                  (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name)))
                               OR d.object_identity = ANY (ARRAY[
                                  sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to])))
            )),
            ARRAY[]::jsonb[])
        INTO v_refusal_objects
        FROM dropped AS d;
        END IF;

        IF cardinality(v_refusal_objects) > 0 THEN
        IF NOT EXISTS (
            SELECT 1
            FROM unnest(v_refusal_objects) AS u(data)
            WHERE u.data->>'object_type' <> 'trigger'
        ) THEN
        WITH dropped AS (
            SELECT u.ordinality AS dropped_ordinality, d.*
            FROM unnest(v_refusal_objects) WITH ORDINALITY AS u(data, ordinality)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                object_type text, address_names text[])
        ), candidates AS (
            SELECT CASE d.address_names[3]
                       WHEN ste.generated_always_trigger THEN 40
                       WHEN ste.write_history_trigger THEN 50 ELSE 60 END AS priority,
                   d.dropped_ordinality,
                   format('cannot drop trigger "%s" on table "%s" because it is used in SYSTEM_TIME period',
                          d.address_names[3],
                          sql_saga.__internal_display_name(ste.table_schema, ste.table_name)) AS message
            FROM dropped AS d JOIN sql_saga.system_time_era AS ste
             ON (d.address_names[1], d.address_names[2]) = (ste.table_schema, ste.table_name)
             AND d.address_names[3] IN (ste.generated_always_trigger, ste.write_history_trigger, ste.truncate_trigger)
            UNION ALL
            SELECT 90, d.dropped_ordinality,
                   format('cannot drop trigger "%s" on view "%s" because it is part of an updatable view for era "%s" on table "%s"',
                          v.trigger_name,
                          sql_saga.__internal_display_name(d.address_names[1]::name, d.address_names[2]::name),
                          v.era_name,
                          sql_saga.__internal_display_name(v.table_schema, v.table_name))
            FROM dropped AS d JOIN sql_saga.updatable_view AS v
             ON (d.address_names[1], d.address_names[2], d.address_names[3]) = (v.view_schema, v.view_name, v.trigger_name)
            UNION ALL
            SELECT CASE WHEN d.address_names[3] = fk.uk_update_trigger THEN 120 ELSE 130 END,
                   d.dropped_ordinality,
                   format('cannot drop trigger "%s" on table "%s" because it is used in era foreign key "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(uk.table_schema, uk.table_name),
                          fk.foreign_key_name)
            FROM dropped AS d
            JOIN sql_saga.foreign_keys AS fk ON d.address_names[3] IN (fk.uk_update_trigger, fk.uk_delete_trigger, fk.uk_truncate_trigger)
            JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
            WHERE (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.table_name)
            UNION ALL
            SELECT 125, d.dropped_ordinality,
                   format('cannot drop trigger "%s" on table "%s" because it is used in era foreign key "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(fk.table_schema, fk.table_name),
                          fk.foreign_key_name)
            FROM dropped AS d
            JOIN sql_saga.foreign_keys AS fk
              ON fk.type = 'regular_to_temporal'
             AND (d.address_names[1], d.address_names[2], d.address_names[3]) =
                 (fk.table_schema, fk.table_name, fk.fk_check_constraint)
        )
        SELECT c.message INTO v_candidate
        FROM candidates AS c
        ORDER BY c.priority, c.dropped_ordinality
        LIMIT 1;
        ELSE
        WITH dropped AS (
            SELECT u.ordinality AS dropped_ordinality, d.*
            FROM unnest(v_refusal_objects) WITH ORDINALITY AS u(data, ordinality)
            CROSS JOIN LATERAL jsonb_to_record(u.data) AS d(
                object_type text, object_identity text, address_names text[], objid oid)
        ), candidates AS (
            SELECT 10 AS priority, d.dropped_ordinality,
                   format('cannot drop column "%s" because it is part of the period "%s"', d.object_identity, e.era_name) AS message
            FROM dropped AS d JOIN sql_saga.era AS e
             ON d.object_type = 'table column'
             AND (d.address_names[1], d.address_names[2]) = (e.table_schema, e.table_name)
             AND d.address_names[3] IN (e.valid_from_column_name, e.valid_until_column_name)
            UNION ALL
            SELECT 20, d.dropped_ordinality,
                   format('cannot drop rangetype "%s" because it is used in period "%s" on table "%s"',
                          d.object_identity, e.era_name,
                          sql_saga.__internal_display_name(e.table_schema, e.table_name))
            FROM dropped AS d JOIN sql_saga.era AS e ON d.objid = e.range_type::oid
            UNION ALL
            SELECT 30, d.dropped_ordinality,
                   format('cannot drop constraint "%s" on table "%s" because it is used in SYSTEM_TIME period',
                          ste.infinity_check_constraint,
                          sql_saga.__internal_display_name(ste.table_schema, ste.table_name))
            FROM dropped AS d JOIN sql_saga.system_time_era AS ste
             ON d.object_type = 'table constraint'
             AND (d.address_names[1], d.address_names[2], d.address_names[3]) = (ste.table_schema, ste.table_name, ste.infinity_check_constraint)
            UNION ALL
            SELECT CASE d.address_names[3]
                       WHEN ste.generated_always_trigger THEN 40
                       WHEN ste.write_history_trigger THEN 50 ELSE 60 END,
                   d.dropped_ordinality,
                   format('cannot drop trigger "%s" on table "%s" because it is used in SYSTEM_TIME period',
                          d.address_names[3],
                          sql_saga.__internal_display_name(ste.table_schema, ste.table_name))
            FROM dropped AS d JOIN sql_saga.system_time_era AS ste
             ON d.object_type = 'trigger'
             AND (d.address_names[1], d.address_names[2]) = (ste.table_schema, ste.table_name)
             AND d.address_names[3] IN (ste.generated_always_trigger, ste.write_history_trigger, ste.truncate_trigger)
            UNION ALL
            SELECT 70, d.dropped_ordinality,
                   format('cannot drop or rename column "%s" on table "%s" because it is excluded from system_time era',
                          d.address_names[3],
                          sql_saga.__internal_display_name(ste.table_schema, ste.table_name))
            FROM dropped AS d JOIN sql_saga.system_time_era AS ste
             ON d.object_type = 'table column'
             AND (d.address_names[1], d.address_names[2]) = (ste.table_schema, ste.table_name)
             AND d.address_names[3] = ANY (ste.excluded_column_names)
            UNION ALL
            SELECT 80, d.dropped_ordinality,
                   format('cannot drop view "%s", call "sql_saga.drop_%s_view()" instead',
                          sql_saga.__internal_display_name(d.object_identity), v.view_type)
            FROM dropped AS d JOIN sql_saga.updatable_view AS v
             ON d.object_type = 'view'
             AND (d.address_names[1], d.address_names[2]) = (v.view_schema, v.view_name)
            UNION ALL
            SELECT 90, d.dropped_ordinality,
                   format('cannot drop trigger "%s" on view "%s" because it is part of an updatable view for era "%s" on table "%s"',
                          v.trigger_name,
                          sql_saga.__internal_display_name(d.address_names[1]::name, d.address_names[2]::name),
                          v.era_name,
                          sql_saga.__internal_display_name(v.table_schema, v.table_name))
            FROM dropped AS d JOIN sql_saga.updatable_view AS v
             ON d.object_type = 'trigger'
             AND (d.address_names[1], d.address_names[2], d.address_names[3]) = (v.view_schema, v.view_name, v.trigger_name)
            UNION ALL
            SELECT 100, d.dropped_ordinality,
                   format('table "%s" must have a primary key or a single-column temporal unique key to support its %s view "%s" for era "%s"; drop that view first with sql_saga.drop_%s_view()',
                          sql_saga.__internal_display_name(v.table_schema, v.table_name), v.view_type,
                          sql_saga.__internal_display_name(v.view_schema, v.view_name), v.era_name, v.view_type)
            FROM dropped AS d JOIN sql_saga.updatable_view AS v
             ON d.object_type IN ('table column', 'table constraint', 'index')
             AND (d.address_names[1], d.address_names[2]) = (v.table_schema, v.table_name)
            WHERE NOT EXISTS (
                      SELECT 1 FROM pg_catalog.pg_constraint AS c
                      WHERE (c.conrelid, c.contype) =
                            (to_regclass(format('%I.%I', v.table_schema, v.table_name)), 'p'))
              AND NOT EXISTS (
                      SELECT 1 FROM sql_saga.unique_keys AS identifier_uk
                      WHERE (identifier_uk.table_schema, identifier_uk.table_name, identifier_uk.era_name) =
                            (v.table_schema, v.table_name, v.era_name)
                        AND array_length(identifier_uk.column_names, 1) = 1
                        AND NOT EXISTS (
                                SELECT 1
                                FROM dropped AS identifier_drop
                                WHERE (identifier_drop.object_type = 'table constraint'
                                       AND (identifier_drop.address_names[1], identifier_drop.address_names[2], identifier_drop.address_names[3]) =
                                           (identifier_uk.table_schema, identifier_uk.table_name, identifier_uk.unique_constraint))
                                   OR (identifier_drop.object_type = 'index'
                                       AND (identifier_drop.address_names[1], identifier_drop.address_names[2]) =
                                           (identifier_uk.table_schema, identifier_uk.unique_constraint))))
            UNION ALL
            SELECT 110, d.dropped_ordinality,
                   format('cannot drop constraint "%s" on table "%s" because it is used in era unique key "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(uk.table_schema, uk.table_name),
                          uk.unique_key_name)
            FROM dropped AS d JOIN sql_saga.unique_keys AS uk
             ON d.object_type = 'table constraint'
             AND (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.table_name)
             AND (d.address_names[3] IN (uk.unique_constraint, uk.exclude_constraint, uk.check_constraint)
                  OR d.address_names[3] = ANY (coalesce(uk.pk_consistency_constraint_names, '{}')))
            UNION ALL
            SELECT 110, d.dropped_ordinality,
                   format('cannot drop index "%s" on table "%s" because it is used in era unique key "%s"',
                          uk.unique_constraint,
                          sql_saga.__internal_display_name(uk.table_schema, uk.table_name),
                          uk.unique_key_name)
            FROM dropped AS d JOIN sql_saga.unique_keys AS uk
             ON d.object_type = 'index'
             AND (d.address_names[1], d.address_names[2]) =
                 (uk.table_schema, uk.unique_constraint)
            UNION ALL
            SELECT CASE WHEN d.address_names[3] = fk.uk_update_trigger THEN 120 ELSE 130 END,
                   d.dropped_ordinality,
                   format('cannot drop trigger "%s" on table "%s" because it is used in era foreign key "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(uk.table_schema, uk.table_name),
                          fk.foreign_key_name)
            FROM dropped AS d
            JOIN sql_saga.foreign_keys AS fk ON d.address_names[3] IN (fk.uk_update_trigger, fk.uk_delete_trigger, fk.uk_truncate_trigger)
            JOIN sql_saga.unique_keys AS uk ON uk.unique_key_name = fk.unique_key_name
            WHERE d.object_type = 'trigger'
              AND (d.address_names[1], d.address_names[2]) = (uk.table_schema, uk.table_name)
            UNION ALL
            SELECT 125, d.dropped_ordinality,
                   format('cannot drop trigger "%s" on table "%s" because it is used in era foreign key "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(fk.table_schema, fk.table_name),
                          fk.foreign_key_name)
            FROM dropped AS d
            JOIN sql_saga.foreign_keys AS fk
              ON fk.type = 'regular_to_temporal'
             AND (d.address_names[1], d.address_names[2], d.address_names[3]) =
                 (fk.table_schema, fk.table_name, fk.fk_check_constraint)
            WHERE d.object_type = 'trigger'
              AND true
            UNION ALL
            SELECT 140, d.dropped_ordinality,
                   format('cannot drop table "%s" because it is used in SYSTEM VERSIONING for table "%s"',
                          sql_saga.__internal_display_name(d.object_identity),
                          sql_saga.__internal_display_name(sv.table_schema, sv.table_name))
            FROM dropped AS d JOIN sql_saga.system_versioning AS sv
              ON d.object_type = 'table'
             AND (d.address_names[1], d.address_names[2]) = (sv.history_schema_name, sv.history_table_name)
            UNION ALL
            SELECT 150, d.dropped_ordinality,
                   format('cannot drop view "%s" because it is used in SYSTEM VERSIONING for table "%s"',
                          sql_saga.__internal_display_name(d.object_identity),
                          sql_saga.__internal_display_name(sv.table_schema, sv.table_name))
            FROM dropped AS d JOIN sql_saga.system_versioning AS sv
              ON d.object_type = 'view'
             AND (d.address_names[1], d.address_names[2]) IN (
                    (sv.view_schema_name, sv.view_table_name),
                    (sv.history_schema_name, sql_saga.__internal_history_gated_name(sv.history_table_name)))
             AND sql_saga.__internal_rebuilding_system_versioning_views(
                     to_regclass(format('%I.%I', sv.table_schema, sv.table_name))::oid) IS NOT TRUE
            UNION ALL
            SELECT 160, d.dropped_ordinality,
                   format('cannot drop function "%s" because it is used in SYSTEM VERSIONING for table "%s"',
                          sql_saga.__internal_display_function_name(d.object_identity),
                          sql_saga.__internal_display_name(sv.table_schema, sv.table_name))
            FROM dropped AS d JOIN sql_saga.system_versioning AS sv
              ON d.object_type = 'function'
             AND d.object_identity = ANY (ARRAY[sv.func_as_of, sv.func_between, sv.func_between_symmetric, sv.func_from_to])
             AND sql_saga.__internal_rebuilding_system_versioning_views(
                     to_regclass(format('%I.%I', sv.table_schema, sv.table_name))::oid) IS NOT TRUE
            UNION ALL
            SELECT 170, d.dropped_ordinality,
                   format('cannot drop or alter policy "%s" on "%s" because it is managed by sql_saga for table "%s"',
                          d.address_names[3],
                          sql_saga.__internal_display_name(f.history_oid::regclass),
                          sql_saga.__internal_display_name(f.base_oid::regclass))
            FROM dropped AS d
            CROSS JOIN LATERAL sql_saga.__internal_history_family(
                to_regclass(format('%I.%I', d.address_names[1], d.address_names[2]))) AS f
            WHERE d.object_type = 'policy'
              AND to_regclass(format('%I.%I', d.address_names[1], d.address_names[2]))::oid = f.history_oid
              AND sql_saga.__internal_is_managed_history_policy(d.address_names[3]::name)
              AND NOT sql_saga.__internal_history_policy_sync_active(f.base_oid)
        )
        SELECT c.message INTO v_candidate
        FROM candidates AS c
        ORDER BY c.priority, c.dropped_ordinality
        LIMIT 1;
        END IF;

        IF FOUND THEN
            RAISE EXCEPTION '%', v_candidate.message;
        END IF;
        END IF;
    END IF;

    ---
    --- periods
    ---

    /*
     * Teardown by pg_restore --clean (sql_saga.__internal_is_clean_teardown, tmp/design_restore_clean.md).
     * The checks below that complain about missing objects are skipped, and so is cleanup of objects
     * pg_restore drops itself. One thing pg_restore cannot do: drop the managed history policies,
     * whose names carry this installation's token while the dump carries another. They depend on the
     * base table, so they are dropped here, as soon as any object of their family goes (DROP TABLE of
     * the base would fail otherwise when the base is dropped before its history). Not on pg_restore's
     * own DROP POLICY statements: those come first, and a later DROP POLICY (without IF EXISTS) of a
     * dumped name already gone would fail. Restore plumbing the caller asked for: reported at DEBUG.
     * The nested DROP POLICY statements below drop only policies, which the family lookup ignores, so
     * they never re-enter this cleanup.
     */
    IF v_teardown THEN
        DECLARE
            family_row record;
            policy_row record;
        BEGIN
            FOR family_row IN
                SELECT DISTINCT to_regclass(format('%I.%I', sv.history_schema_name /* %I */, sv.history_table_name /* %I */)) AS history_oid
                FROM unnest(v_dropped_objects) AS u(data)
                CROSS JOIN LATERAL jsonb_to_record(u.data) AS dobj(
                    object_type text, address_names text[])
                JOIN sql_saga.system_versioning AS sv
                  ON (dobj.address_names[1], dobj.address_names[2]) IN ((sv.table_schema, sv.table_name), (sv.history_schema_name, sv.history_table_name))
                WHERE dobj.object_type <> 'policy'
            LOOP
                CONTINUE WHEN family_row.history_oid IS NULL;
                FOR policy_row IN
                    SELECT p.polname
                    FROM pg_catalog.pg_policy AS p
                    WHERE p.polrelid = family_row.history_oid
                      AND sql_saga.__internal_is_managed_history_policy(p.polname)
                    ORDER BY p.polname
                LOOP
                    EXECUTE format('DROP POLICY IF EXISTS %I ON %s', policy_row.polname /* %I */, family_row.history_oid::regclass /* %s */);
                    RAISE DEBUG 'sql_saga: Dropped policy % on %, part of the teardown by pg_restore --clean',
                        policy_row.polname, family_row.history_oid::regclass;
                END LOOP;
            END LOOP;
        END;
    END IF;

    /* If one of our tables is being dropped, remove references to it */
    FOR r IN
        SELECT dobj.schema_name, dobj.object_name
        FROM unnest(v_dropped_objects) AS u(data)
        CROSS JOIN LATERAL jsonb_to_record(u.data) AS dobj(
            object_type text, schema_name text, object_name text)
        WHERE dobj.object_type = 'table'
          AND EXISTS (SELECT 1 FROM sql_saga.era e WHERE (e.table_schema, e.table_name) = (dobj.schema_name, dobj.object_name))
    LOOP
        -- This table was dropped. Cascade-delete its metadata and any dependent triggers on other tables.
        -- This logic mirrors the metadata-first era drop for a dropped table.

        -- 1. For FKs that reference the dropped table, drop their triggers.
        -- The metadata will be cleaned up through the unique key's cascading foreign key.
        DECLARE
            fk_row record;
            fk_table_oid regclass;
        BEGIN
            FOR fk_row IN
                SELECT fk.*
                FROM sql_saga.foreign_keys fk
                JOIN sql_saga.unique_keys uk ON fk.unique_key_name = uk.unique_key_name
                WHERE (uk.table_schema, uk.table_name) = (r.schema_name, r.object_name)
            LOOP
                fk_table_oid := pg_catalog.to_regclass(format('%I.%I', fk_row.table_schema /* %I */, fk_row.table_name /* %I */));
                -- The referencing table might have been dropped in the same cascading statement. If it still exists, drop its triggers/constraints.
                -- Not during pg_restore --clean: it drops the CHECK (with the table) and the helper function itself.
                IF fk_table_oid IS NOT NULL AND NOT v_teardown THEN
                    IF fk_row.type = 'regular_to_temporal' THEN
                        IF fk_row.fk_check_constraint IS NOT NULL AND EXISTS (
                            SELECT 1 FROM pg_catalog.pg_trigger
                            WHERE tgrelid = fk_table_oid AND tgname = fk_row.fk_check_constraint
                              AND tgfoid = pg_catalog.to_regprocedure('sql_saga.regular_fk_check_c()')) THEN
                            EXECUTE format('DROP TRIGGER %I ON %s', fk_row.fk_check_constraint /* %I */, fk_table_oid /* %s */);
                        /* Historical (pre-1.3) CHECK form. */
                        ELSIF fk_row.fk_check_constraint IS NOT NULL AND fk_row.fk_helper_function IS NOT NULL AND EXISTS (
                            /* Only the released CHECK: it depends on (calls) the catalogued helper function. */
                            SELECT 1 FROM pg_catalog.pg_constraint AS c
                            JOIN pg_catalog.pg_depend AS d
                              ON d.classid = 'pg_catalog.pg_constraint'::regclass AND d.objid = c.oid
                             AND d.refclassid = 'pg_catalog.pg_proc'::regclass
                             AND d.refobjid = pg_catalog.to_regprocedure(fk_row.fk_helper_function)
                            WHERE c.conrelid = fk_table_oid AND c.conname = fk_row.fk_check_constraint AND c.contype = 'c') THEN
                            EXECUTE format('ALTER TABLE %s DROP CONSTRAINT %I', fk_table_oid /* %s */, fk_row.fk_check_constraint /* %I */);
                        END IF;
                        IF fk_row.fk_helper_function IS NOT NULL THEN
                           EXECUTE format('DROP FUNCTION IF EXISTS %s', fk_row.fk_helper_function /* %s */);
                        END IF;
                    END IF;
                END IF;
            END LOOP;
        END;

        -- 2. For FKs ON the dropped table, remove triggers from the referenced UK table.
        DECLARE
            fk_row record;
            uk_table_oid regclass;
        BEGIN
            FOR fk_row IN
                SELECT fk.*, uk.table_schema AS uk_schema, uk.table_name AS uk_table
                FROM sql_saga.foreign_keys fk JOIN sql_saga.unique_keys uk ON fk.unique_key_name = uk.unique_key_name
                WHERE (fk.table_schema, fk.table_name) = (r.schema_name, r.object_name)
            LOOP
                uk_table_oid := pg_catalog.to_regclass(format('%I.%I', fk_row.uk_schema /* %I */, fk_row.uk_table /* %I */));
                -- Use DROP IF EXISTS because the UK table might have been dropped in the same command.
                IF uk_table_oid IS NOT NULL THEN
                    IF EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = uk_table_oid AND tgname = fk_row.uk_update_trigger) THEN
                        EXECUTE format('DROP TRIGGER %I ON %s', fk_row.uk_update_trigger /* %I */, uk_table_oid /* %s */);
                    END IF;
                    IF EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = uk_table_oid AND tgname = fk_row.uk_delete_trigger) THEN
                        EXECUTE format('DROP TRIGGER %I ON %s', fk_row.uk_delete_trigger /* %I */, uk_table_oid /* %s */);
                    END IF;
                    IF fk_row.uk_truncate_trigger IS NOT NULL
                       AND EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = uk_table_oid AND tgname = fk_row.uk_truncate_trigger) THEN
                        EXECUTE format('DROP TRIGGER %I ON %s', fk_row.uk_truncate_trigger /* %I */, uk_table_oid /* %s */);
                    END IF;
                END IF;
            END LOOP;
        END;

        -- 3. Clean up template sync triggers/functions for each era on this table BEFORE deleting metadata
        DECLARE
            era_row record;
            func_name name;
        BEGIN
            -- Table has been dropped, but we still need to clean up the functions
            FOR era_row IN
                SELECT e.era_name, e.sync_temporal_trg_function_name
                FROM sql_saga.era e
                WHERE (e.table_schema, e.table_name) = (r.schema_name, r.object_name)
                  AND e.sync_temporal_trg_function_name IS NOT NULL  -- Only eras with sync triggers
                  -- Not during pg_restore --clean: it drops the (dumped) function itself, without IF EXISTS.
                  AND NOT v_teardown
            LOOP
                -- Drop the function directly (trigger already gone with table)
                func_name := era_row.sync_temporal_trg_function_name;
                IF to_regprocedure(format('sql_saga.%I()', func_name)) IS NOT NULL THEN
                    -- Do not silently remove a user trigger that reuses this function.
                    EXECUTE format($$DROP FUNCTION sql_saga.%I()$$, func_name);
                END IF;
            END LOOP;
        END;

        -- 4. Delete all metadata for the dropped table.
        DELETE FROM sql_saga.updatable_view WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
        DELETE FROM sql_saga.foreign_keys WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
        DELETE FROM sql_saga.unique_keys WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
        DELETE FROM sql_saga.era WHERE (table_schema, table_name) = (r.schema_name, r.object_name);
    END LOOP;

    /*
     * Policies (tmp/design_final.md sections 8, 13). A dropped policy on a versioned base table
     * drops its history copy. Policies sql_saga manages on a history table cannot be dropped
     * directly. address_names of a policy are {schema, table, policy}.
     */
    DECLARE
    BEGIN
    FOR r IN
        SELECT f.base_oid, f.history_oid,
               to_regclass(format('%I.%I', dobj.address_names[1], dobj.address_names[2]))::oid AS rel_oid,
               array_agg(dobj.address_names[3]::name ORDER BY dobj.ordinality) AS policy_names
        FROM unnest(v_dropped_objects) AS u(data)
        CROSS JOIN LATERAL jsonb_to_record(u.data) AS dobj(
            object_type text, address_names text[], ordinality bigint)
        CROSS JOIN LATERAL sql_saga.__internal_history_family(
            to_regclass(format('%I.%I', dobj.address_names[1], dobj.address_names[2]))) AS f
        WHERE dobj.object_type = 'policy'
        GROUP BY 1, 2, 3
    LOOP
        -- Not during pg_restore --clean: it drops the dumped copies itself, without IF EXISTS.
        IF r.rel_oid = r.base_oid AND NOT v_teardown
           AND NOT sql_saga.__internal_history_policy_sync_active(r.base_oid) THEN
            PERFORM sql_saga.__internal_sync_history_policies(r.base_oid, r.policy_names);
        END IF;
    END LOOP;
    END;
END;
$function$;

COMMENT ON FUNCTION sql_saga.drop_protection() IS
'An event trigger function that prevents accidental dropping of sql_saga-managed objects. During the teardown phase of pg_restore --clean (see sql_saga.__internal_is_clean_teardown) it lets the drops through and removes the managed history policies the dump cannot name.';
