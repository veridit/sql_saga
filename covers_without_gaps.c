/**
 * covers_without_gaps.c -
 * Provides an aggregate function
 * that tells if a bunch of input ranges completely cover a target range.
 */

#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/uio.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <float.h>

#include <postgres.h>
#include <fmgr.h>
#include <pg_config.h>
#include <miscadmin.h>
#include <utils/array.h>
#include <utils/datum.h>
#include <utils/guc.h>
#include <utils/acl.h>
#include <utils/lsyscache.h>
#include <utils/builtins.h>
#include <utils/rangetypes.h>
#include <utils/float.h>
#include <utils/numeric.h>
#include <utils/date.h>
#include <utils/timestamp.h>
#include <catalog/pg_type.h>
#include <catalog/catalog.h>
#include <catalog/pg_tablespace.h>
#include <commands/tablespace.h>

#include "covers_without_gaps.h"

// Declarations/Prototypes
char *DatumGetString(Oid elem_oid, RangeBound bound);
Datum DatumNegativeInfinity(Oid elem_oid);

Datum covers_without_gaps_transfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(covers_without_gaps_transfn);
Datum covers_without_gaps_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(covers_without_gaps_finalfn);


// Types
typedef struct covers_without_gaps_state {
  RangeBound covered_to;
  RangeType *target;  // Assuming that the target range does not need to be modified and is not large
  RangeBound target_start, target_end; // Cache computed values
  bool target_empty; // Cache computed value
  bool answer_is_null;
  bool finished;    // Used to avoid further processing if we have already succeeded/failed.
  bool is_covered;
  RangeBound prev_start; // To check if input is sorted
  bool first_row_processed; // To check if input is sorted
} covers_without_gaps_state;


// Implementations

/*
 * State transition logic for covers_without_gaps aggregate:
 *
 * The goal is to determine if a sorted series of input ranges fully covers a
 * target range without any gaps.
 *
 * State variables and their purpose:
 *
 *  - target_start, target_end: These hold the boundaries of the target range
 *    we are trying to cover. They are cached in the state to avoid repeated
 *    deserialization and are constant throughout the aggregation for a group.
 *
 *  - covered_to: This is the core of the state machine. It tracks the "high-water
 *    mark" of continuous coverage, starting from `target_start`. As each new
 *    input range is processed, if it is contiguous with the existing coverage,
 *    `covered_to` is advanced to the end of that new range. It always
 *    represents the exclusive upper bound of the covered area.
 *
 *  - first_row_processed: A boolean flag to distinguish the first relevant
 *    input range from subsequent ranges. The first range has a special role:
 *    it must cover `target_start` without a gap. Subsequent ranges are checked
 *    for contiguity against the `covered_to` mark.
 *
 *  - finished: A boolean flag used to short-circuit the aggregation. Once a
 *    definitive answer is found (either a gap is detected, making coverage
 *    impossible, or the `target_end` is reached), this is set to `true`, and
 *    all subsequent input rows for the group are ignored.
 *
 *  - is_covered: The boolean result of the aggregation. It is initialized to
 *    `false` and is only set to `true` if `covered_to` is confirmed to be
 *    greater than or equal to `target_end`.
 *
 * Initialization (first call):
 *  - `covered_to` is initialized to `target_start`. This represents the point
 *    from which coverage must begin.
 *  - `first_row_processed` is `false`.
 *
 * Processing each input range (current_start, current_end):
 *
 * 1. Ignore irrelevant ranges:
 *    - If `current_end` <= `target_start`, the range is entirely before the
 *      target and cannot contribute. It is skipped.
 *
 * 2. First relevant range (`first_row_processed` is false):
 *    - This is the first range that extends into or beyond the target's start.
 *    - Check for a gap at the beginning using range_cmp_bounds, which correctly
 *      compares two lower bounds:
 *      - If `current_start` > `target_start`, a gap exists. Coverage is
 *        impossible. Set `finished=true`, `is_covered=false`.
 *    - Otherwise (no gap):
 *      - The first part of the target is covered.
 *      - Update `covered_to` with `current_end`.
 *      - Set `first_row_processed = true`.
 *
 * 3. Subsequent ranges (`first_row_processed` is true):
 *    - Check for a gap between the previous coverage and the current range.
 *      This requires careful comparison of `covered_to` (an upper bound) and
 *      `current_start` (a lower bound). A gap exists if:
 *        a) The value of `current_start` is greater than `covered_to`.
 *        b) The values are equal, but both bounds are exclusive.
 *      - If a gap exists, set `finished=true`, `is_covered=false`.
 *    - Otherwise (no gap):
 *      - Extend coverage: update `covered_to` if `current_end` is greater.
 *
 * 4. Check for completion:
 *    - After any update to `covered_to`, check if `covered_to` >= `target_end`.
 *    - If it is, the target is fully covered. Set `finished=true`, `is_covered=true`.
 *
 * Example: target = [10, 20), input = [8,12), [12,18)
 *
 * - Init: `covered_to` = [10, `first_row_processed` = false.
 * - Row [8,12): First relevant row. `current_start`([8) <= `target_start`([10). No gap.
 *   `covered_to` becomes 12). `first_row_processed` = true.
 * - Row [12,18): Subsequent row. `current_start`([12) vs `covered_to`(12)). No gap.
 *   `covered_to` becomes 18).
 * - Final check: `covered_to`(18)) < `target_end`(20)). is_covered remains false.
 */
Datum covers_without_gaps_transfn(PG_FUNCTION_ARGS)
{
  MemoryContext aggContext, oldContext;
  covers_without_gaps_state *state;
  RangeType *current_range,
            *target_range;
  RangeBound current_start, current_end;
  TypeCacheEntry *typcache, *elem_typcache;
  Oid elem_oid;
  bool current_empty;
  bool is_first_row;

  if (!AggCheckCallContext(fcinfo, &aggContext)) {
    elog(ERROR, "covers_without_gaps called in non-aggregate context");
  }

  // First run of the aggregate function.
  // Create the state and analyse the input arguments.
  if (PG_ARGISNULL(0)) {
    // Need to use MemoryContextAlloc with aggContext, not just palloc0,
    // or the state will get cleared in between invocations:
    state = (covers_without_gaps_state *)MemoryContextAlloc(aggContext, sizeof(covers_without_gaps_state));
    state->finished = false;
    state->is_covered = false;
    state->first_row_processed = false;
    is_first_row = true;

    // Technically this will fail to detect an inconsistent target
    // if only the first row is NULL or has an empty range, however,
    // any target problem will be detected when the data is present.
    // e.g. SELECT sql_saga.covers_without_gaps(tstzrange('2024-01-01', '2024-01-02'), NULL)
    if (PG_ARGISNULL(2) || RangeIsEmpty(target_range = PG_GETARG_RANGE_P(2))) {
      // return NULL from the whole thing
      state->answer_is_null = true;
      state->finished = true;
      PG_RETURN_POINTER(state);
    }
    state->answer_is_null = false;

    state->target = (RangeType *)MemoryContextAlloc(aggContext, VARSIZE(PG_GETARG_RANGE_P(2)));
    memcpy(state->target, target_range, VARSIZE(target_range));
    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(state->target));
    range_deserialize(typcache, state->target, &state->target_start, &state->target_end, &state->target_empty);
    elem_oid = typcache->rngelemtype->type_id;
    elem_typcache = lookup_type_cache(elem_oid, 0);

    // Deep copy the initial covered_to bound
    state->covered_to = state->target_start;
    if (!elem_typcache->typbyval && !state->target_start.infinite) {
        oldContext = MemoryContextSwitchTo(aggContext);
        state->covered_to.val = datumCopy(state->target_start.val, elem_typcache->typbyval, elem_typcache->typlen);
        MemoryContextSwitchTo(oldContext);
    }
  } else {
    // ereport(NOTICE, (errmsg("looking up state....")));
    state = (covers_without_gaps_state *)PG_GETARG_POINTER(0);
    is_first_row = !state->first_row_processed;

    // TODO: Is there any better way to exit an aggregation early?
    // Even https://pgxn.org/dist/first_last_agg/ hits all the input rows:
    if (state->finished) PG_RETURN_POINTER(state);

    // Make sure the second arg is always the same:
    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(state->target));
    elem_oid = typcache->rngelemtype->type_id;
    elem_typcache = lookup_type_cache(elem_oid, 0);

    if (PG_ARGISNULL(2) || range_ne_internal(typcache, state->target, PG_GETARG_RANGE_P(2))) {
      ereport(ERROR, (errmsg("covers_without_gaps second argument must be constant across the group")));
    }
  }

  // e.g. SELECT sql_saga.covers_without_gaps(NULL, tstzrange('2024-01-01', '2024-01-10'))
  if (PG_ARGISNULL(1)) PG_RETURN_POINTER(state);

  current_range = PG_GETARG_RANGE_P(1);
  if (RangeTypeGetOid(current_range) != RangeTypeGetOid(state->target)) {
    elog(ERROR, "range types do not match");
  }

  range_deserialize(typcache, current_range, &current_start, &current_end, &current_empty);

  /*
   * If the current range ends before our target starts, it cannot contribute
   * to coverage, so we can ignore it. This is the key to handling ranges
   * that start before the target.
   */
  if (range_cmp_bounds(typcache, &current_end, &state->target_start) <= 0) {
      PG_RETURN_POINTER(state);
  }

  if (state->first_row_processed) {
      // Subsequent row logic: Check for sortedness and for gaps between ranges.
      bool gap;
      int32 cmp;

      if (range_cmp_bounds(typcache, &current_start, &state->prev_start) < 0) {
          ereport(ERROR, (errmsg("covers_without_gaps first argument must be sorted by range start")));
      }
      // Check for a gap. A gap exists if the current range starts after the
      // previously covered range ends. This check is complex because it compares
      // a lower bound (current_start) with an upper bound (covered_to).
      // A gap exists if:
      //  1. The value of current_start is strictly greater than covered_to.
      //  2. The values are equal, and both bounds are exclusive (e.g., ...12) and (12...).
      cmp = DatumGetInt32(FunctionCall2Coll(&elem_typcache->cmp_proc_finfo,
                                            typcache->rng_collation,
                                            current_start.val,
                                            state->covered_to.val));
      gap = (cmp > 0) || (cmp == 0 && !current_start.inclusive && !state->covered_to.inclusive);

      if (gap) {
          state->finished = true;
          state->is_covered = false;
          PG_RETURN_POINTER(state);
      }

  } else {
      // First row logic: Check for a gap at the very start of the target range.
      if (range_cmp_bounds(typcache, &current_start, &state->target_start) > 0) {
          state->finished = true;
          state->is_covered = false;
          PG_RETURN_POINTER(state);
      }
      state->first_row_processed = true;
  }

  // Update prev_start for the next iteration. We must copy pass-by-reference
  // values into the aggregate context to ensure they are not lost. This logic
  // mirrors the update for `covered_to` to be robust.
  // The old `prev_start.val` is freed only on subsequent rows. `is_first_row`
  // captures the state at the beginning of the function, preventing a bug where
  // we would pfree uninitialized memory on the first row.
  if (!is_first_row && !elem_typcache->typbyval && !state->prev_start.infinite) {
    pfree(DatumGetPointer(state->prev_start.val));
  }
  state->prev_start.inclusive = current_start.inclusive;
  state->prev_start.infinite = current_start.infinite;
  state->prev_start.lower = current_start.lower;

  if (!elem_typcache->typbyval && !current_start.infinite) {
      oldContext = MemoryContextSwitchTo(aggContext);
      state->prev_start.val = datumCopy(current_start.val, elem_typcache->typbyval, elem_typcache->typlen);
      MemoryContextSwitchTo(oldContext);
  } else {
      state->prev_start.val = current_start.val;
  }
  
  
  // Update the covered range if the current range extends beyond it
  if (range_cmp_bounds(typcache, &current_end, &state->covered_to) > 0) {
      // Free the old value if it was pass-by-ref and not infinite
      if (!elem_typcache->typbyval && !state->covered_to.infinite) {
          pfree(DatumGetPointer(state->covered_to.val));
      }

      // Copy the new end bound into the state
      state->covered_to.inclusive = current_end.inclusive;
      state->covered_to.infinite = current_end.infinite;
      state->covered_to.lower = current_end.lower;

      if (!elem_typcache->typbyval && !current_end.infinite) {
          oldContext = MemoryContextSwitchTo(aggContext);
          state->covered_to.val = datumCopy(current_end.val, elem_typcache->typbyval, elem_typcache->typlen);
          MemoryContextSwitchTo(oldContext);
      } else {
          state->covered_to.val = current_end.val;
      }
  }
  
  // If the covered range now extends to or beyond the target end, we have full coverage
  if (range_cmp_bounds(typcache, &state->covered_to, &state->target_end) >= 0) {
    state->is_covered = true;
    state->finished = true;
  }
  //oldContext = MemoryContextSwitchTo(aggContext);
  //ereport(DEBUG1, (errmsg("post state->covered_to is %s", DatumGetString(elem_oid, state->covered_to))));
  //MemoryContextSwitchTo(oldContext);
  
  PG_RETURN_POINTER(state);
}

Datum covers_without_gaps_finalfn(PG_FUNCTION_ARGS)
{
  covers_without_gaps_state *state;
  TypeCacheEntry *typcache;

  if (PG_ARGISNULL(0)) {
    /*
     * transfn was never called (no input rows).
     * e.g. SELECT sql_saga.covers_without_gaps(range_col, '[1,10)') FROM my_table WHERE false;
     * The result depends only on the target range (arg 2).
     * The period range (arg 1) is a dummy value.
     */
    if (PG_ARGISNULL(2) || RangeIsEmpty(PG_GETARG_RANGE_P(2)))
      PG_RETURN_NULL(); /* NULL target -> NULL result */
    else
      PG_RETURN_BOOL(false); /* No ranges can't cover a valid target */
  }

  state = (covers_without_gaps_state *)PG_GETARG_POINTER(0);
  if (state->answer_is_null) {
    PG_RETURN_NULL();
  }

  /* If we've already determined the answer, just return it. */
  if (state->finished) {
    PG_RETURN_BOOL(state->is_covered);
  }

  /* Otherwise, perform the final check. */
  typcache = range_get_typcache(fcinfo, RangeTypeGetOid(state->target));
  if (range_cmp_bounds(typcache, &state->covered_to, &state->target_end) >= 0) {
    state->is_covered = true;
  }
  
  PG_RETURN_BOOL(state->is_covered);
}


Datum DatumNegativeInfinity(Oid elem_oid)
{
    switch (elem_oid)
    {
        case INT4OID:
            return Int32GetDatum(INT32_MIN);
        case INT8OID:
            return Int64GetDatum(INT64_MIN);
        case DATEOID:
            return DateADTGetDatum(DATEVAL_NOBEGIN);
        case NUMERICOID:
        {
            text* negativeInfinityText = cstring_to_text("-Infinity");
            Datum negativeInfinityDatum = DirectFunctionCall1(numeric_in, PointerGetDatum(negativeInfinityText));
            pfree(negativeInfinityText);
            return negativeInfinityDatum;
        }
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            return DatumGetTimestampTz(DT_NOBEGIN);
        default:
            elog(ERROR, "Unsupported range type: %u", elem_oid);
            return (Datum) 0;  // This line will not be reached due to the elog(ERROR) above
    }
}


char *DatumGetString(Oid elem_oid, RangeBound bound) {
    char *result;

    switch (elem_oid) {
        case INT4OID:
            result = psprintf("%d", DatumGetInt32(bound.val));
            break;
        case INT8OID:
            result = psprintf("%ld", DatumGetInt64(bound.val));
            break;
        case DATEOID: {
            char *dateStr = DatumGetCString(DirectFunctionCall1(date_out, bound.val));
            result = psprintf("%s", dateStr);
            //pfree(dateStr);
            break;
        }
        case NUMERICOID: {
            char *numericStr = DatumGetCString(DirectFunctionCall1(numeric_out, bound.val));
            result = psprintf("%s", numericStr);
            //pfree(numericStr);
            break;
        }
        case TIMESTAMPOID: {
            char *timestampStr = DatumGetCString(DirectFunctionCall1(timestamp_out, bound.val));
            result = psprintf("%s", timestampStr);
            //pfree(timestampStr);
            break;
        }
        case TIMESTAMPTZOID: {
            char *timestamptzStr = DatumGetCString(DirectFunctionCall1(timestamptz_out, bound.val));
            result = psprintf("%s", timestamptzStr);
            //pfree(timestamptzStr);
            break;
        }
        default:
            elog(ERROR, "Unsupported element type id: %u", elem_oid);
            return NULL; // This line will not be reached due to the elog(ERROR) above
    }
    return result;
}
