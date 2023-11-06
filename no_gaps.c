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
Datum DatumGet(TypeCacheEntry *typcache, RangeBound bound);
Datum DatumNegativeInfinity(TypeCacheEntry *typcache);

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
  MemoryContext aggContext;
  no_gaps_state *state;
  RangeType *current_range,
            *target_range;
  RangeBound current_start, current_end;
  TypeCacheEntry *typcache;
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

    //ereport(NOTICE, (errmsg("target is [%ld, %ld)", DatumGet(typcache, state->target_start), DatumGet(typcache, state->target_end))));

    // Initialize covered_to to negative infinity bound
    state->covered_to.val = DatumNegativeInfinity(typcache);
    state->covered_to.infinite = true;
    state->covered_to.inclusive = true;
    state->covered_to.lower = true;

    //ereport(NOTICE, (errmsg("initial covered_to is %ld", DatumGet(typcache, state->covered_to))));
  } else {
    // ereport(NOTICE, (errmsg("looking up state....")));
    state = (no_gaps_state *)PG_GETARG_POINTER(0);

    // TODO: Is there any better way to exit an aggregation early?
    // Even https://pgxn.org/dist/first_last_agg/ hits all the input rows:
    if (state->finished) PG_RETURN_POINTER(state);

    first_time = false;

    // Make sure the second arg is always the same:
    typcache = range_get_typcache(fcinfo, RangeTypeGetOid(state->target));
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

  //ereport(NOTICE, (errmsg("current is [%ld, %ld)", DatumGet(typcache, current_start), DatumGet(typcache, current_end))));
  //ereport(NOTICE, (errmsg("pre state->covered_to is %ld", DatumGet(typcache, state->covered_to))));

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
    //    "no_gaps first argument should be sorted but got %ld after covering up to %ld",
    //    DatumGet(typcache, current_start),
    //    DatumGet(typcache, state->covered_to)
    //)));
    ereport(ERROR, (errmsg(
      "no_gaps first argument should be sorted but got a range ending before the last covered_to"
    )));
  }
  
  // Update the covered range if the current range extends beyond it
  if (range_cmp_bounds(typcache, &current_end, &state->covered_to) > 0) {
    state->covered_to = current_end;
    // Notice that the previous non inclusive end is included in the next start.
    state->covered_to.inclusive = true;
  }
  
  // If the covered range now extends to or beyond the target end, we have full coverage
  if (!state->target_end.infinite && range_cmp_bounds(typcache, &state->covered_to, &state->target_end) >= 0) {
    state->no_gaps = true;
    state->finished = true;
  }
  //ereport(NOTICE, (errmsg("post state->covered_to is %ld", DatumGet(typcache, state->covered_to))));
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


Datum DatumGet(TypeCacheEntry *typcache, RangeBound bound)
{
    Oid elem_type_id = typcache->rngelemtype->type_id;
    switch (elem_type_id)
    {
        case INT4OID:
            return DatumGetInt32(bound.val);
        case INT8OID:
            return DatumGetInt64(bound.val);
        case FLOAT4OID:
            return DatumGetFloat4(bound.val);
        case FLOAT8OID:
            return DatumGetFloat8(bound.val);
        case DATERANGEOID:
            return DatumGetDateADT(bound.val);
        case NUMRANGEOID:
            return PointerGetDatum(bound.val);
        case TSRANGEOID:
            return DatumGetTimestamp(bound.val);
        case TIMESTAMPTZOID:
            return DatumGetTimestampTz(bound.val);
        default:
            elog(ERROR, "Unsupported element type id: %u", elem_type_id);
            return (Datum) 0;  // This line will not be reached due to the elog(ERROR) above
    }
}


Datum DatumNegativeInfinity(TypeCacheEntry *typcache)
{
    Oid elem_type_id = typcache->rngelemtype->type_id;
    switch (elem_type_id)
    {
        case INT4OID:
            return Int32GetDatum(INT32_MIN);
        case INT8OID:
            return Int64GetDatum(INT64_MIN);
        case FLOAT4OID:
            return Float4GetDatum(FLT_MIN);
        case FLOAT8OID:
            return Float8GetDatum(-DBL_MAX);
        case DATERANGEOID:
            return DateADTGetDatum(DATEVAL_NOBEGIN);
        case NUMRANGEOID:
            return PointerGetDatum(NUMERIC_NINF);
        case TSRANGEOID:
        case TIMESTAMPTZOID:
            return DatumGetTimestampTz(DT_NOBEGIN);
        default:
            elog(ERROR, "Unsupported range type: %u", elem_type_id);
            return (Datum) 0;  // This line will not be reached due to the elog(ERROR) above
    }
}
