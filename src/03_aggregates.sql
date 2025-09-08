/*
 * covers_without_gaps(period anyrange, target anyrange) -
 * Returns true if the collected `period` ranges are contiguous (have no gaps)
 * and completely cover the fixed `target` range.
 */
CREATE AGGREGATE sql_saga.covers_without_gaps(anyrange, anyrange) (
  sfunc = sql_saga.covers_without_gaps_transfn,
  stype = internal,
  finalfunc = sql_saga.covers_without_gaps_finalfn,
  finalfunc_extra
);

CREATE FUNCTION sql_saga.first_sfunc(agg_state anyelement, next_val anyelement)
RETURNS anyelement
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS
$function$
BEGIN
    IF agg_state IS NULL THEN
        RETURN next_val;
    END IF;
    RETURN agg_state;
END;
$function$;

/*
 * first(anyelement ORDER BY sort_expression) -
 * A simple aggregate to get the first element from a group based on an
 * ordering expression. This is more efficient than the common pattern of
 * `(array_agg(... ORDER BY ...))[1]`.
 */
CREATE AGGREGATE sql_saga.first(anyelement) (
    sfunc = sql_saga.first_sfunc,
    stype = anyelement,
    parallel = safe
);
