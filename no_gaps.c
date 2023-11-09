/**
 * no_gaps.c -
 * Provides an aggregate function
 * that tells if a bunch of input ranges competely cover a target range.
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

#include "no_gaps.h"

// Taken from 'https://github.com/postgres/postgres/raw/master/src/backend/utils/adt/numeric.c',
// since it is not exposed in a headerfile.
#define NUMERIC_NINF      0xF000


// Declarations/Prototypes
char *DatumGetString(Oid elem_oid, RangeBound bound);
Datum DatumNegativeInfinity(Oid elem_oid);

Datum no_gaps_transfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(no_gaps_transfn);
Datum no_gaps_finalfn(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(no_gaps_finalfn);


// Types
typedef struct no_gaps_state {
  RangeBound covered_to;
  RangeType *target;  // Assuming that the target range does not need to be modified and is not large
  RangeBound target_start, target_end; // Cache computed values
  bool target_empty; // Cache computed value
  bool answer_is_null;
  bool finished;    // Used to avoid further processing if we have already succeeded/failed.
  bool no_gaps;
} no_gaps_state;


// Implementations
Datum no_gaps_transfn(PG_FUNCTION_ARGS)
{
  MemoryContext aggContext, oldContext;
  no_gaps_state *state;
  RangeType *current_range,
            *target_range;
  RangeBound current_start, current_end;
  TypeCacheEntry *typcache, *elem_typcache;
  Oid elem_oid;
  bool current_empty;
  bool first_time;

  if (!AggCheckCallContext(fcinfo, &aggContext)) {
    elog(ERROR, "no_gaps called in non-aggregate context");
  }

  // First run of the aggregate function.
  // Create the state and analyse the input arguments.
  if (PG_ARGISNULL(0)) {
    // Need to use MemoryContextAlloc with aggContext, not just palloc0,
    // or the state will get cleared in between invocations:
    state = (no_gaps_state *)MemoryContextAlloc(aggContext, sizeof(no_gaps_state));
    state->finished = false;
    state->no_gaps = false;
    first_time = true;

    // Technically this will fail to detect an inconsistent target
    // if only the first row is NULL or has an empty range, however,
    // any target problem will be detected when the data is present.
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

    //ereport(DEBUG1, (errmsg("target is [%s, %s)", DatumGetString(elem_oid, state->target_start), DatumGetString(elem_oid, state->target_end))));

    // Initialize covered_to to negative infinity bound, and make sure it is allocated for regular free.
    //if (!elem_typcache->typbyval && elem_typcache->typlen == -1) {
    //  ereport(DEBUG1, (errmsg("Performing memory copy.")));  
    //  oldContext = MemoryContextSwitchTo(aggContext);
    //  state->covered_to.val = datumCopy(DatumNegativeInfinity(elem_oid), /*pass by reference when false*/ false, /*dynamic length when -1*/ -1);
    //  MemoryContextSwitchTo(oldContext);
    //} else {
    //    ereport(DEBUG1, (errmsg("Skipping memory copy.")));
        state->covered_to.val = DatumNegativeInfinity(elem_oid);
    //}
    state->covered_to.infinite = true;
    state->covered_to.inclusive = true;
    state->covered_to.lower = true;
    //ereport(DEBUG1, (errmsg("initial covered_to is %s", DatumGetString(elem_oid, state->covered_to))));
  } else {
    // ereport(NOTICE, (errmsg("looking up state....")));
    state = (no_gaps_state *)PG_GETARG_POINTER(0);

    // TODO: Is there any better way to exit an aggregation early?
    // Even https://pgxn.org/dist/first_last_agg/ hits all the input rows:
    if (state->finished) PG_RETURN_POINTER(state);

    first_time = false;

    // Make sure the second arg is always the same:
    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(state->target));
    elem_oid = typcache->rngelemtype->type_id;
    elem_typcache = lookup_type_cache(elem_oid, 0);

    if (PG_ARGISNULL(2) || range_ne_internal(typcache, state->target, PG_GETARG_RANGE_P(2))) {
      ereport(ERROR, (errmsg("no_gaps second argument must be constant across the group")));
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
      state->no_gaps = false;
      PG_RETURN_POINTER(state);
    }
    // If the current range starts after the target range starts, then we have a gap
    if (range_cmp_bounds(typcache, &current_start, &state->target_start) > 0) {
      state->finished = true;
      state->no_gaps = false;
      PG_RETURN_POINTER(state);
    }
  } else {
    // For subsequent ranges, check if there is a gap between the end of the covered range and the start of the current range
    if (range_cmp_bounds(typcache, &state->covered_to, &current_start) < 0) {
      state->finished = true;
      state->no_gaps = false;
      PG_RETURN_POINTER(state);
    }
  }
  
  // If the current range starts after the last covered range, it means the ranges are not sorted
  if (range_cmp_bounds(typcache, &current_start, &state->covered_to) < 0) {
    //ereport(ERROR, (errmsg(
    //    "no_gaps first argument should be sorted but got %s after covering up to %s",
    //    DatumGetString(elem_oid, current_start),
    //    DatumGetString(elem_oid, state->covered_to)
    //)));
    ereport(ERROR, (errmsg(
      "no_gaps first argument should be sorted but got a range ending before the last covered_to"
    )));
  }
  
  // Update the covered range if the current range extends beyond it
  if (range_cmp_bounds(typcache, &current_end, &state->covered_to) > 0) {
    state->covered_to = current_end;
    if (!elem_typcache->typbyval && elem_typcache->typlen == -1) {
        //ereport(DEBUG1, (errmsg("Performing memory copy.")));
        //ereport(DEBUG1, (errmsg("Before pfree(state->covered_to.val)")));
        oldContext = MemoryContextSwitchTo(aggContext);
        // The first covered_to is not allocated on the stack, so it can not be freed...
        //pfree(state->covered_to.val);
        //ereport(DEBUG1, (errmsg("Before datumCopy.")));
        state->covered_to.val = datumCopy(current_end.val, /*pass by reference when false*/ false, /*dynamic length when -1*/ -1);
        //ereport(DEBUG1, (errmsg("After datumCopy.")));
        MemoryContextSwitchTo(oldContext);
    } else {
        //ereport(DEBUG1, (errmsg("Skipping memory copy.")));
    }
    
    // Notice that the previous non-inclusive end is included in the next start.
    state->covered_to.inclusive = true;
  }
  
  // If the covered range now extends to or beyond the target end, we have full coverage
  if (!state->target_end.infinite && range_cmp_bounds(typcache, &state->covered_to, &state->target_end) >= 0) {
    state->no_gaps = true;
    state->finished = true;
  }
  //oldContext = MemoryContextSwitchTo(aggContext);
  //ereport(DEBUG1, (errmsg("post state->covered_to is %s", DatumGetString(elem_oid, state->covered_to))));
  //MemoryContextSwitchTo(oldContext);
  
  PG_RETURN_POINTER(state);
}

Datum no_gaps_finalfn(PG_FUNCTION_ARGS)
{
  no_gaps_state *state;

  if (PG_ARGISNULL(0)) PG_RETURN_NULL();

  state = (no_gaps_state *)PG_GETARG_POINTER(0);
  if (state->answer_is_null) {
    PG_RETURN_NULL();
  } else {
    PG_RETURN_BOOL(state->no_gaps);
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
            return NumericGetDatum(NUMERIC_NINF);
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
