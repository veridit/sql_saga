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
