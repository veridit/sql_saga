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
  /*
   * The upper bound of the contiguous range covered so far. This is updated
   * with the upper bound of each new range that extends the coverage.
   */
  RangeBound covered_to;

  /*
   * A copy of the target range, held in the aggregate's memory context to
   * persist across transition function calls.
   */
  RangeType *target;

  /* Deserialized and cached bounds of the target range for efficiency. */
  RangeBound target_start, target_end;
  /* Cached emptiness of the target range. */
  bool target_empty;

  /*
   * Flag to indicate if the aggregate should return NULL. This is set if the
   * target range is NULL or empty on the first call.
   */
  bool answer_is_null;
  /*
   * Optimization flag to stop processing further rows once a definitive answer
   * (either full coverage or a gap) has been found.
   */
  bool finished;
  /* The current answer. Becomes true only when full coverage is confirmed. */
  bool is_covered;
  /*
   * Memory management flag. True if `covered_to.val` points to memory
   * allocated with `datumCopy` in the aggregate's context, which must be
   * `pfree`d before reallocation. This is only relevant for pass-by-reference
   * range-element types (e.g., numeric, text, timestamp).
   */
  bool covered_to_is_palloced;
  /*
   * The start bound of the previously processed range, used to verify that
   * the input is correctly sorted.
   */
  RangeBound previous_start;
  /* Memory management flag for previous_start.val. */
  bool previous_start_is_palloced;
} covers_without_gaps_state;


// Implementations
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
  bool first_time;
  Datum neg_inf;

  if (!AggCheckCallContext(fcinfo, &aggContext)) {
    elog(ERROR, "covers_without_gaps called in non-aggregate context");
  }

  // First run of the aggregate function.
  // Create the state and analyse the input arguments.
  if (PG_ARGISNULL(0)) {
    // Need to use MemoryContextAlloc with aggContext, not just palloc0,
    // or the state will get cleared in between invocations:
    state = (covers_without_gaps_state *)MemoryContextAlloc(aggContext, sizeof(covers_without_gaps_state));
    state->covered_to_is_palloced = false;
    state->previous_start_is_palloced = false;
    state->finished = false;
    state->is_covered = false;
    first_time = true;

    // If the target range is NULL, the result is NULL.
    if (PG_ARGISNULL(2))
    {
        state->answer_is_null = true;
        state->finished = true;
        PG_RETURN_POINTER(state);
    }

    target_range = PG_GETARG_RANGE_P(2);

    // If the target range is empty, the result is TRUE.
    if (RangeIsEmpty(target_range))
    {
        state->is_covered = true;
        state->finished = true;
        state->answer_is_null = false; /* Ensure this is not set */
        PG_RETURN_POINTER(state);
    }
    state->answer_is_null = false;

    state->target = (RangeType *)MemoryContextAlloc(aggContext, VARSIZE(PG_GETARG_RANGE_P(2)));
    memcpy(state->target, target_range, VARSIZE(target_range));
    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(state->target));
    range_deserialize(typcache, state->target, &state->target_start, &state->target_end, &state->target_empty);
    elem_oid = typcache->rngelemtype->type_id;
    elem_typcache = lookup_type_cache(elem_oid, 0);

    //ereport(DEBUG1, (errmsg("target is [%s, %s)", DatumGetString(elem_oid, state->target_start), DatumGetString(elem_oid, state->target_end))));

    // Initialize covered_to to negative infinity bound, and make sure it is allocated for regular free.
    neg_inf = DatumNegativeInfinity(elem_oid);
    if (!elem_typcache->typbyval && elem_typcache->typlen == -1)
    {
        oldContext = MemoryContextSwitchTo(aggContext);
        state->covered_to.val = datumCopy(neg_inf, false, -1);
        MemoryContextSwitchTo(oldContext);
        pfree(DatumGetPointer(neg_inf));
        state->covered_to_is_palloced = true;
    }
    else
    {
        state->covered_to.val = neg_inf;
        state->covered_to_is_palloced = false;
    }
    state->covered_to.infinite = true;
    state->covered_to.inclusive = true;
    state->covered_to.lower = true;

    // Initialize previous_start to negative infinity as well.
    state->previous_start = state->covered_to;
    if (state->covered_to_is_palloced)
    {
        // covered_to.val was palloc'd. Need a new copy for previous_start.
        oldContext = MemoryContextSwitchTo(aggContext);
        state->previous_start.val = datumCopy(state->covered_to.val, false, -1);
        MemoryContextSwitchTo(oldContext);
        state->previous_start_is_palloced = true;
    }

    //ereport(DEBUG1, (errmsg("initial covered_to is %s", DatumGetString(elem_oid, state->covered_to))));
  } else {
    // ereport(NOTICE, (errmsg("looking up state....")));
    state = (covers_without_gaps_state *)PG_GETARG_POINTER(0);

    // TODO: Is there any better way to exit an aggregation early?
    // Even https://pgxn.org/dist/first_last_agg/ hits all the input rows:
    if (state->finished) PG_RETURN_POINTER(state);

    first_time = false;

    // Make sure the second arg is always the same:
    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(state->target));
    elem_oid = typcache->rngelemtype->type_id;
    elem_typcache = lookup_type_cache(elem_oid, 0);

    if (PG_ARGISNULL(2) || range_ne_internal(typcache, state->target, PG_GETARG_RANGE_P(2))) {
      ereport(ERROR, (errmsg("covers_without_gaps second argument must be constant across the group")));
    }
  }

  if (PG_ARGISNULL(1)) PG_RETURN_POINTER(state);

  current_range = PG_GETARG_RANGE_P(1);
  if (first_time) {
    if (RangeTypeGetOid(current_range) != RangeTypeGetOid(state->target)
        ) {
      elog(ERROR, "range types do not match");
    }
  }

  range_deserialize(typcache, current_range, &current_start, &current_end, &current_empty);

  oldContext = MemoryContextSwitchTo(aggContext);
  //ereport(DEBUG1, (errmsg("current is [%s, %s)", DatumGetString(elem_oid, current_start), DatumGetString(elem_oid, current_end))));
  //ereport(DEBUG1, (errmsg("pre state->covered_to is %s", DatumGetString(elem_oid, state->covered_to))));
  MemoryContextSwitchTo(oldContext);

  if (first_time) {
    // If the target range start is unbounded, but the current range start is not, then we cannot have full coverage
    if (state->target_start.infinite && !current_start.infinite) {
      state->finished = true;
      state->is_covered = false;
      PG_RETURN_POINTER(state);
    }
    // If the current range starts after the target range starts, then we have a gap
    if (range_cmp_bounds(typcache, &current_start, &state->target_start) > 0) {
      state->finished = true;
      state->is_covered = false;
      PG_RETURN_POINTER(state);
    }
  } else {
    /*
     * For subsequent ranges, check if there is a gap between the end of the
     * covered range and the start of the current range. The range_cmp_bounds
     * function is for sorting, not contiguity checking, because it considers
     * any upper bound to be greater than any lower bound of the same value
     * (e.g., `(b < b]`), which would incorrectly be seen as a non-gap. A manual
     * check is required to correctly handle all boundary conditions.
     */
    int cmp = DatumGetInt32(FunctionCall2Coll(&typcache->rng_cmp_proc_finfo,
                                          typcache->rng_collation,
                                          state->covered_to.val, current_start.val));
    if (cmp < 0)
    {
        // A clear gap, e.g., [..., 5] and [7, ...]
        state->finished = true;
        state->is_covered = false;
        PG_RETURN_POINTER(state);
    }
    else if (cmp == 0 && !state->covered_to.inclusive && !current_start.inclusive)
    {
        /*
         * An adjacent gap, e.g., `(..., 5)` and `(5, ...)` where the boundary
         * value itself is not included by either range. This is only a true
         * gap for continuous range types (e.g., numeric, timestamp). For
         * discrete types (e.g., integer, date), `(..., 5)` is equivalent to
         * `(..., 4]` and is contiguous with `[5, ...)`. We identify discrete
         * types by checking for the existence of a `range_canonical` function.
         */
        if (!OidIsValid(typcache->rng_canonical_finfo.fn_oid))
        {
            state->finished = true;
            state->is_covered = false;
            PG_RETURN_POINTER(state);
        }
    }
  }
  
  /*
   * The logic requires that input ranges are sorted by their start bound.
   * We check that the current start bound is not less than the previous start
   * bound. This correctly handles overlapping ranges.
   */
  if (!first_time && range_cmp_bounds(typcache, &current_start, &state->previous_start) < 0) {
    //ereport(ERROR, (errmsg(
    //    "covers_without_gaps first argument should be sorted but got %s after covering up to %s",
    //    DatumGetString(elem_oid, current_start),
    //    DatumGetString(elem_oid, state->covered_to)
    //)));
    ereport(ERROR, (errmsg(
      "input to covers_without_gaps must be sorted by range start"
    )));
  }
  
  // Update the covered range if the current range extends beyond it
  if (range_cmp_bounds(typcache, &current_end, &state->covered_to) > 0) {
    if (!elem_typcache->typbyval && elem_typcache->typlen == -1) {
        oldContext = MemoryContextSwitchTo(aggContext);
        if (state->covered_to_is_palloced)
            pfree(DatumGetPointer(state->covered_to.val));
        
        state->covered_to.val = datumCopy(current_end.val, false, -1);
        state->covered_to_is_palloced = true;
        MemoryContextSwitchTo(oldContext);
    } else {
        state->covered_to.val = current_end.val;
    }
    
    // Copy other bound properties
    state->covered_to.infinite = current_end.infinite;
    state->covered_to.lower = current_end.lower;

    if (OidIsValid(typcache->rng_canonical_finfo.fn_oid))
    {
        /*
         * For discrete types, an exclusive end like in `[1,6)` is conceptually
         * contiguous with an inclusive start `[6,12)`. To make range_cmp_bounds
         * see this as non-gapped, we treat the covered_to as inclusive. This
         * also works for `(valid_after, valid_to]` style ranges.
         */
        state->covered_to.inclusive = true;
    }
    else
    {
        /*
         * For continuous types, we must respect the bound's own inclusivity
         * to correctly detect gaps between ranges like (10,12) and (12,14).
         */
        state->covered_to.inclusive = current_end.inclusive;
    }
  }
  
  // If the covered range now extends to or beyond the target end, we have full coverage
  if (!state->target_end.infinite && range_cmp_bounds(typcache, &state->covered_to, &state->target_end) >= 0) {
    state->is_covered = true;
    state->finished = true;
  }
  //oldContext = MemoryContextSwitchTo(aggContext);
  //ereport(DEBUG1, (errmsg("post state->covered_to is %s", DatumGetString(elem_oid, state->covered_to))));
  //MemoryContextSwitchTo(oldContext);

  // Update previous_start for the next iteration's sort check
  if (!elem_typcache->typbyval && elem_typcache->typlen == -1) {
      oldContext = MemoryContextSwitchTo(aggContext);
      if (state->previous_start_is_palloced)
          pfree(DatumGetPointer(state->previous_start.val));

      state->previous_start.val = datumCopy(current_start.val, false, -1);
      state->previous_start_is_palloced = true;
      MemoryContextSwitchTo(oldContext);
  } else {
      state->previous_start.val = current_start.val;
  }
  state->previous_start.infinite = current_start.infinite;
  state->previous_start.inclusive = current_start.inclusive;
  state->previous_start.lower = current_start.lower;
  
  PG_RETURN_POINTER(state);
}

Datum covers_without_gaps_finalfn(PG_FUNCTION_ARGS)
{
    covers_without_gaps_state *state;
    TypeCacheEntry *typcache;

    /*
     * The final function is called after all rows have been processed.
     * `PG_ARGISNULL(0)` is true if the aggregate received zero input rows.
     * In this case, we can only determine coverage if the target is also empty.
     * The target is passed as an extra argument, so we check PG_ARGISNULL(2).
     */
    if (PG_ARGISNULL(0))
    {
        if (PG_ARGISNULL(2))
            PG_RETURN_NULL();
        else if (RangeIsEmpty(PG_GETARG_RANGE_P(2)))
            PG_RETURN_BOOL(true);
        else
            PG_RETURN_BOOL(false);
    }

    state = (covers_without_gaps_state *)PG_GETARG_POINTER(0);
    if (state->answer_is_null)
    {
        PG_RETURN_NULL();
    }
    else
    {
        /*
         * If the transition function did not set `finished` to true (which happens
         * when it finds a gap or confirms full coverage early), we must perform a
         * final check to see if the total covered range extends to the target's
         * end. This handles cases where the input rows are exhausted before the
         * target is fully covered.
         */
        if (!state->finished)
        {
            typcache = range_get_typcache(fcinfo, RangeTypeGetOid(state->target));
            if (range_cmp_bounds(typcache, &state->covered_to, &state->target_end) >= 0)
            {
                state->is_covered = true;
            }
        }
        PG_RETURN_BOOL(state->is_covered);
    }
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
