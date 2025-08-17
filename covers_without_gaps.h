#ifndef COVERS_WITHOUT_GAPS_H
#define COVERS_WITHOUT_GAPS_H

#include "postgres.h"
#include "fmgr.h"

Datum covers_without_gaps_transfn(PG_FUNCTION_ARGS);
Datum covers_without_gaps_finalfn(PG_FUNCTION_ARGS);

#endif /* COVERS_WITHOUT_GAPS_H */
