# TEMPORAL MERGE PostgreSQL Extension Implementation Plan

## Executive Summary

This document outlines a plan to implement `TEMPORAL MERGE` as a PostgreSQL extension, providing native core-level performance for temporal data operations. The approach leverages existing PostgreSQL MERGE infrastructure while adding temporal-specific semantics, potentially achieving **10-50x performance improvement** over the current sql_saga function-based implementation.

## 1. Background and Motivation

### 1.1 Current Implementation Limitations

**sql_saga temporal_merge architecture** (function-based):
- **Implementation size**: ~3,400 lines of PL/pgSQL code across planner and executor
- **Core files**: 
  - `src/27_temporal_merge_plan.sql` (2,319 LOC)
  - `src/28_temporal_merge_execute.sql` (1,045 LOC)
  - `src/29_temporal_merge.sql` (main procedure)

**Performance bottlenecks identified**:
1. **JSONB manipulation**: Used for flexible column handling, adds serialization overhead
2. **Dynamic EXECUTE statements**: Runtime SQL compilation for every operation
3. **Untyped temp tables**: Requires type casting (e.g., `::daterange`) on every access
4. **Function call overhead**: PL/pgSQL interpreter overhead for complex operations

**Current performance characteristics**:
- Regular DML: 24,000-45,000 rows/s
- temporal_merge (no batching): 75-155 rows/s (200-300x slower!)
- temporal_merge (batch 1000): 2,800-3,000 rows/s (optimal batch size)
- Sync column overhead: Reduced from 85ms to 37ms per 1000 rows with template triggers

### 1.2 Opportunity Assessment

**PostgreSQL 18 temporal features** (foundation exists):
- `WITHOUT OVERLAPS` constraint support
- `PERIOD` column syntax for temporal foreign keys  
- `fk_with_period` and `pk_with_period` flags in constraint system
- Native temporal foreign key support

**Proven MERGE architecture** (solid foundation):
- **Parser**: `parse_merge.c` handles SQL parsing into `MergeStmt` AST nodes
- **Planner**: Creates `ModifyTable` plan nodes with merge-specific logic
- **Executor**: `nodeModifyTable.c` with specialized `ExecMerge*()` functions
- **Concurrency**: Sophisticated handling of concurrent updates/deletes
- **Performance**: Direct executor-level implementation (not function-based)

**Template trigger success** (proven approach):
- Template-based sync triggers achieved 2.3x performance improvement
- Eliminated JSONB and dynamic SQL overhead
- Demonstrated viability of specialized code generation

### 1.3 Performance Target

**Target performance**: 10,000+ rows/s through native PostgreSQL integration
- **Conservative estimate**: 3-5x improvement (8,400-14,000 rows/s)
- **Optimistic estimate**: 10-15x improvement (28,000-42,000 rows/s)
- **Theoretical maximum**: Approach regular DML performance (24,000+ rows/s)

## 2. Technical Architecture

### 2.1 Extension vs Core Development Strategy

**Phase 1: Extension Development** (Proof of Concept)
- Develop as PostgreSQL extension for validation
- Minimal invasive changes to PostgreSQL core
- Faster iteration and testing cycle
- Lower risk for initial development

**Phase 2: Optimization** (Performance Validation)
- Temporal-specific planning and execution
- Performance benchmarking and validation
- Refinement based on real-world usage

**Phase 3: Upstream Proposal** (Community Contribution)
- Propose to PostgreSQL community after proven concept
- Potential integration into PostgreSQL core
- Long-term maintenance and support

### 2.2 Proposed SQL Syntax

#### Basic Syntax
```sql
-- Extend existing MERGE with temporal semantics
TEMPORAL MERGE target_table USING source_table
ON target_table.entity_id = source_table.entity_id
   AND PERIOD target_table.valid_range OVERLAPS PERIOD source_table.valid_range
WHEN MATCHED THEN 
  UPDATE SET col1 = source_table.col1, valid_range = source_table.valid_range
WHEN NOT MATCHED THEN
  INSERT (entity_id, col1, valid_range) 
  VALUES (source_table.entity_id, source_table.col1, source_table.valid_range)
WITH TEMPORAL (
  MODE = 'ENTITY_PATCH',  -- ENTITY_PATCH | TIMELINE_REPLACE | etc.
  ERA = 'valid',          -- era name (default: 'valid')
  DELETE_MODE = 'NONE'    -- NONE | CASCADE | etc.
);
```

#### Advanced Options
```sql
-- With identity management
TEMPORAL MERGE target_table USING source_table
ON ...
WITH TEMPORAL (
  MODE = 'ENTITY_PATCH',
  ERA = 'valid',
  IDENTITY_COLUMNS = ARRAY['id'],
  LOOKUP_COLUMNS = ARRAY['ssn', 'employee_nr'],
  ROW_ID_COLUMN = 'row_id',
  FOUNDING_ID_COLUMN = 'batch_id',
  DELETE_MODE = 'MARK_DELETED',
  EPHEMERAL_COLUMNS = ARRAY['created_at', 'updated_at']
);
```

### 2.3 Core Components

#### 2.3.1 Parser Extension (`parse_temporal_merge.c`)

**Location**: `src/backend/parser/`  
**Function**: Extend MERGE parser to recognize TEMPORAL MERGE syntax

```c
/* New node type for temporal merge */
typedef struct TemporalMergeStmt
{
    NodeTag     type;
    MergeStmt  *merge_stmt;      /* Base MERGE statement */
    char       *era_name;        /* Era column name (e.g., 'valid') */
    char       *mode;            /* ENTITY_PATCH, TIMELINE_REPLACE, etc. */
    char       *delete_mode;     /* NONE, CASCADE, MARK_DELETED, etc. */
    List       *identity_columns; /* Primary identity columns */
    List       *lookup_columns;   /* Natural/business key columns */
    char       *row_id_column;    /* Source row identifier */
    char       *founding_id_column; /* Batch/founding identifier */
    List       *ephemeral_columns; /* Columns excluded from comparison */
    bool        delay_constraints; /* Defer constraint checking */
} TemporalMergeStmt;

/* Parser functions */
extern TemporalMergeStmt *transformTemporalMergeStmt(ParseState *pstate, 
                                                     TemporalMergeStmt *stmt);
extern void checkTemporalMergeTargetlist(ParseState *pstate, 
                                        List *targetlist);
```

**Grammar extension** (`gram.y`):
```yacc
TemporalMergeStmt:
    TEMPORAL MERGE relation_expr_opt_alias
    USING table_ref
    ON a_expr
    merge_when_list
    opt_temporal_clause
    {
        TemporalMergeStmt *n = makeNode(TemporalMergeStmt);
        n->merge_stmt = $3; /* Base MERGE */
        n->temporal_options = $8;
        $$ = (Node *) n;
    }
;
```

#### 2.3.2 Planner Integration (`plan_temporal_merge.c`)

**Location**: `src/backend/optimizer/plan/`  
**Function**: Temporal-specific query planning and optimization

```c
/* Temporal merge planning state */
typedef struct TemporalMergePlanState
{
    PlannerInfo    *root;
    TemporalMergeStmt *stmt;
    RangeTblEntry  *target_rte;
    RangeTblEntry  *source_rte;
    List           *temporal_conditions; /* PERIOD overlap conditions */
    AttrNumber      era_attno;           /* Era range column number */
    List           *identity_attnos;     /* Identity column numbers */
    List           *lookup_attnos;       /* Lookup column numbers */
} TemporalMergePlanState;

/* Planning functions */
static Plan *create_temporal_merge_plan(PlannerInfo *root, 
                                       TemporalMergeStmt *stmt);
static List *build_temporal_overlap_conditions(PlannerInfo *root,
                                               TemporalMergePlanState *tmstate);
static void optimize_temporal_range_joins(PlannerInfo *root,
                                          TemporalMergePlanState *tmstate);
static Plan *build_temporal_atomic_timeline_plan(PlannerInfo *root,
                                                 TemporalMergePlanState *tmstate);
```

**Key optimizations**:
1. **Range-range join optimization**: Leverage GiST indexes for temporal overlaps
2. **Temporal partitioning**: Group operations by entity identity
3. **Index recommendations**: Suggest optimal indexes for temporal queries
4. **Timeline segmentation**: Build atomic timeline segments efficiently

**Optimization strategies**:
```c
/* Index usage hints */
typedef struct TemporalIndexHint
{
    List       *index_cols;      /* Suggested index columns */
    char       *index_type;      /* GiST, SP-GiST, etc. */
    Cost        estimated_benefit; /* Expected cost reduction */
} TemporalIndexHint;

/* Timeline optimization */
static List *optimize_timeline_segmentation(TemporalMergePlanState *tmstate);
static Cost estimate_temporal_merge_cost(TemporalMergePlanState *tmstate);
```

#### 2.3.3 Executor Enhancement (`exec_temporal_merge.c`)

**Location**: `src/backend/executor/`  
**Function**: Temporal-specific execution logic

```c
/* Temporal merge execution state */
typedef struct TemporalMergeState
{
    ModifyTableState *mtstate;   /* Base ModifyTable state */
    char            *era_name;   /* Era column name */
    AttrNumber       era_attno;  /* Era attribute number in target */
    List            *identity_attnos; /* Identity column attr numbers */
    List            *lookup_attnos;   /* Lookup column attr numbers */
    
    /* Timeline processing state */
    List            *current_timeline; /* Current entity's timeline segments */
    Datum            current_entity_id; /* Current entity being processed */
    bool             current_entity_null;
    
    /* Operation statistics */
    uint64           inserts_performed;
    uint64           updates_performed;
    uint64           deletes_performed;
    uint64           timeline_splits;
} TemporalMergeState;

/* Main execution functions */
static TupleTableSlot *ExecTemporalMerge(TemporalMergeState *tmstate,
                                        TupleTableSlot *slot,
                                        EState *estate);
static void handle_temporal_overlap_resolution(TemporalMergeState *tmstate,
                                               TupleTableSlot *source_slot,
                                               TupleTableSlot *target_slot);
static void perform_temporal_split_insert(TemporalMergeState *tmstate,
                                         TupleTableSlot *slot,
                                         DateADT split_point);
static List *build_atomic_timeline(TemporalMergeState *tmstate,
                                  List *overlapping_tuples);
```

**Timeline processing algorithm**:
```c
/* 
 * Core temporal merge algorithm:
 * 1. Group source and target tuples by entity identity
 * 2. For each entity, build timeline of all source and target segments
 * 3. Detect temporal overlaps and create atomic timeline segments
 * 4. Determine operation (INSERT/UPDATE/DELETE) for each atomic segment
 * 5. Execute operations with proper constraint handling
 */
typedef struct TimelineSegment
{
    DateADT     valid_from;
    DateADT     valid_until;
    bool        has_source;     /* Source data present */
    bool        has_target;     /* Target data present */
    HeapTuple   source_tuple;   /* NULL if no source */
    HeapTuple   target_tuple;   /* NULL if no target */
    char        operation;      /* 'I', 'U', 'D', 'N' (none) */
} TimelineSegment;

static List *detect_temporal_overlaps(List *source_segments, 
                                     List *target_segments);
static char determine_segment_operation(TimelineSegment *segment,
                                       char *mode);
```

## 3. Implementation Phases

### Phase 1: Foundation (Weeks 1-4)

#### Week 1-2: Extension Skeleton
**Deliverables**:
- [ ] Create extension directory structure
- [ ] Extension control file and SQL script
- [ ] Basic `_PG_init()` and hook registration
- [ ] Minimal TEMPORAL MERGE syntax recognition

**Tasks**:
```bash
# Directory structure
postgresql/contrib/temporal_merge/
├── temporal_merge--1.0.sql    # Extension SQL
├── temporal_merge.control      # Extension metadata
├── Makefile                    # Build configuration
├── README.md                   # Documentation
└── src/
    ├── temporal_merge.c        # Extension entry point
    ├── temporal_merge.h        # Header definitions
    └── test/
        └── sql/                # Regression tests
```

**Extension initialization**:
```c
/* Extension lifecycle */
void _PG_init(void)
{
    /* Hook into utility statement processing */
    prev_ProcessUtility = ProcessUtility_hook;
    ProcessUtility_hook = temporal_merge_ProcessUtility;
    
    /* Hook into planner */
    prev_planner_hook = planner_hook;
    planner_hook = temporal_merge_planner;
}

void _PG_fini(void)
{
    /* Restore original hooks */
    ProcessUtility_hook = prev_ProcessUtility;
    planner_hook = prev_planner_hook;
}
```

#### Week 3-4: Parser Extension
**Deliverables**:
- [ ] Grammar extension for TEMPORAL MERGE
- [ ] TemporalMergeStmt node type implementation
- [ ] Basic validation and error handling
- [ ] Parse tree transformation

**Parser integration**:
```c
static void temporal_merge_ProcessUtility(PlannedStmt *pstmt,
                                         const char *queryString,
                                         bool readOnlyTree,
                                         ProcessUtilityContext context,
                                         ParamListInfo params,
                                         QueryEnvironment *queryEnv,
                                         DestReceiver *dest,
                                         QueryCompletion *qc)
{
    Node *parsetree = pstmt->utilityStmt;
    
    if (IsA(parsetree, TemporalMergeStmt))
    {
        /* Process TEMPORAL MERGE statement */
        process_temporal_merge_stmt((TemporalMergeStmt *) parsetree,
                                   queryString, context, params,
                                   queryEnv, dest, qc);
        return;
    }
    
    /* Delegate to previous hook or standard processing */
    if (prev_ProcessUtility)
        prev_ProcessUtility(pstmt, queryString, readOnlyTree,
                          context, params, queryEnv, dest, qc);
    else
        standard_ProcessUtility(pstmt, queryString, readOnlyTree,
                              context, params, queryEnv, dest, qc);
}
```

### Phase 2: Core Functionality (Weeks 5-8)

#### Week 5-6: Basic Executor Implementation
**Deliverables**:
- [ ] Temporal overlap detection logic
- [ ] Timeline splitting/merging implementation
- [ ] Basic ENTITY_PATCH mode support
- [ ] Tuple-based processing (no JSONB)

**Executor core**:
```c
static TupleTableSlot *ExecTemporalMerge(TemporalMergeState *tmstate,
                                        TupleTableSlot *slot,
                                        EState *estate)
{
    Datum       entity_id;
    bool        isnull;
    
    /* Extract entity identity from source tuple */
    entity_id = extract_entity_identity(slot, tmstate->identity_attnos,
                                       &isnull);
    
    /* Check if we're starting a new entity */
    if (tmstate->current_entity_null || isnull ||
        !datumIsEqual(entity_id, tmstate->current_entity_id,
                     tmstate->identity_type, tmstate->identity_typbyval,
                     tmstate->identity_typlen))
    {
        /* Process previous entity's timeline */
        if (!tmstate->current_entity_null)
            process_entity_timeline(tmstate, estate);
        
        /* Start new entity */
        tmstate->current_entity_id = entity_id;
        tmstate->current_entity_null = isnull;
        tmstate->current_timeline = NIL;
    }
    
    /* Add current tuple to timeline */
    add_to_timeline(tmstate, slot);
    
    return slot;
}
```

#### Week 7-8: Performance Optimization Foundation
**Deliverables**:
- [ ] Eliminate all JSONB usage
- [ ] Direct tuple field access implementation
- [ ] Basic range-range join optimization
- [ ] Initial performance benchmarking

**Direct tuple processing**:
```c
/* No JSONB - direct tuple access */
static Datum extract_column_value(TupleTableSlot *slot,
                                 AttrNumber attno,
                                 bool *isnull)
{
    return slot_getattr(slot, attno, isnull);
}

/* No dynamic SQL - compiled plans */
static void execute_temporal_operation(TemporalMergeState *tmstate,
                                      TimelineSegment *segment,
                                      EState *estate)
{
    switch (segment->operation)
    {
        case 'I':
            ExecInsert(tmstate->mtstate, segment->resultslot,
                      estate->es_output_cid, 0, estate, false);
            break;
        case 'U':
            ExecUpdate(tmstate->mtstate, segment->resultslot,
                      estate->es_output_cid, estate, false);
            break;
        case 'D':
            ExecDelete(tmstate->mtstate, segment->tupleid,
                      NULL, estate->es_output_cid, estate, false, false);
            break;
    }
}
```

### Phase 3: Advanced Features (Weeks 9-12)

#### Week 9-10: Advanced Temporal Modes
**Deliverables**:
- [ ] TIMELINE_REPLACE mode implementation
- [ ] Complex delete behaviors (CASCADE, MARK_DELETED)
- [ ] Multi-era support
- [ ] Ephemeral column handling

**Mode implementations**:
```c
static char determine_operation_entity_patch(TimelineSegment *segment)
{
    if (!segment->has_target && segment->has_source)
        return 'I';  /* Insert new segment */
    else if (segment->has_target && segment->has_source)
        return 'U';  /* Update existing segment */
    else if (segment->has_target && !segment->has_source)
        return 'N';  /* Keep target unchanged */
    else
        return 'N';  /* No operation */
}

static char determine_operation_timeline_replace(TimelineSegment *segment)
{
    if (!segment->has_target && segment->has_source)
        return 'I';  /* Insert from source */
    else if (segment->has_target && segment->has_source)
        return 'U';  /* Replace with source */
    else if (segment->has_target && !segment->has_source)
        return 'D';  /* Delete from target */
    else
        return 'N';  /* No operation */
}
```

#### Week 11-12: Advanced Optimization
**Deliverables**:
- [ ] Advanced index optimization hints
- [ ] Parallel processing exploration
- [ ] JIT compilation integration
- [ ] Query plan caching

**Optimization framework**:
```c
/* Index hint generation */
static List *suggest_temporal_indexes(TemporalMergePlanState *tmstate)
{
    List *hints = NIL;
    
    /* Composite index on (identity_cols, era_range) */
    hints = lappend(hints, 
                   create_index_hint(tmstate->identity_attnos,
                                   tmstate->era_attno,
                                   "gist"));
    
    /* Index on lookup columns if used */
    if (tmstate->lookup_attnos != NIL)
        hints = lappend(hints,
                       create_index_hint(tmstate->lookup_attnos,
                                       InvalidAttrNumber,
                                       "btree"));
    
    return hints;
}

/* Parallel processing consideration */
static bool can_parallelize_temporal_merge(TemporalMergePlanState *tmstate)
{
    /* Can parallelize if entities are independent */
    return (tmstate->identity_columns != NIL &&
            !tmstate->stmt->delay_constraints);
}
```

### Phase 4: Production Readiness (Weeks 13-16)

#### Week 13-14: Testing and Validation
**Deliverables**:
- [ ] Comprehensive regression test suite
- [ ] Port sql_saga test cases
- [ ] Edge case validation
- [ ] Concurrency testing

**Test framework**:
```sql
-- Regression test suite structure
temporal_merge/test/sql/
├── 001_basic_syntax.sql           -- Parser validation
├── 002_entity_patch_mode.sql      -- Basic ENTITY_PATCH
├── 003_timeline_replace_mode.sql  -- TIMELINE_REPLACE mode
├── 004_identity_management.sql    -- Identity/lookup keys
├── 005_concurrent_updates.sql     -- Concurrency handling
├── 010_performance_baseline.sql   -- Performance benchmarks
└── 099_sql_saga_compatibility.sql -- sql_saga equivalence
```

#### Week 15-16: Documentation and Upstream Preparation
**Deliverables**:
- [ ] User documentation
- [ ] Developer documentation
- [ ] Performance benchmarking results
- [ ] Upstream proposal preparation

## 4. Key Algorithms and Data Structures

### 4.1 Temporal Overlap Resolution

**Core algorithm** (from sql_saga `temporal_merge_plan.sql`):

```c
/*
 * Timeline reconstruction algorithm:
 * 
 * Given source and target tuples for a single entity:
 * 1. Extract all temporal boundaries (valid_from, valid_until)
 * 2. Create atomic timeline segments between consecutive boundaries
 * 3. For each segment, determine source/target presence
 * 4. Apply mode-specific operation determination
 * 5. Execute operations in optimal order
 */

typedef struct TemporalSegment 
{
    DateADT     valid_from;
    DateADT     valid_until;
    bool        has_source;
    bool        has_target;
    HeapTuple   source_tuple;   /* Source data (NULL if none) */
    HeapTuple   target_tuple;   /* Target data (NULL if none) */
    ItemPointer target_tid;     /* Target tuple TID for updates */
    char        operation;      /* 'I', 'U', 'D', 'N' */
} TemporalSegment;

/*
 * Build atomic timeline from overlapping source and target segments.
 * Returns list of TemporalSegment ordered by valid_from.
 */
static List *
build_atomic_timeline(List *source_segments, List *target_segments)
{
    List       *time_points = NIL;
    List       *atomic_segments = NIL;
    ListCell   *lc;
    DateADT     prev_point = DATE_NOBEGIN;
    
    /* Step 1: Collect all unique time points */
    foreach(lc, source_segments)
    {
        TemporalSegment *seg = (TemporalSegment *) lfirst(lc);
        time_points = add_time_point(time_points, seg->valid_from);
        time_points = add_time_point(time_points, seg->valid_until);
    }
    
    foreach(lc, target_segments)
    {
        TemporalSegment *seg = (TemporalSegment *) lfirst(lc);
        time_points = add_time_point(time_points, seg->valid_from);
        time_points = add_time_point(time_points, seg->valid_until);
    }
    
    /* Step 2: Sort time points */
    time_points = list_sort(time_points, compare_dates);
    
    /* Step 3: Create atomic segments between consecutive points */
    foreach(lc, time_points)
    {
        DateADT curr_point = DatumGetDateADT(lfirst(lc));
        
        if (prev_point != DATE_NOBEGIN)
        {
            TemporalSegment *atomic_seg = palloc0(sizeof(TemporalSegment));
            atomic_seg->valid_from = prev_point;
            atomic_seg->valid_until = curr_point;
            
            /* Determine source/target presence for this segment */
            atomic_seg->has_source = segment_has_source(atomic_seg,
                                                       source_segments);
            atomic_seg->has_target = segment_has_target(atomic_seg,
                                                       target_segments);
            
            /* Find corresponding tuples */
            if (atomic_seg->has_source)
                atomic_seg->source_tuple = find_source_tuple(atomic_seg,
                                                            source_segments);
            if (atomic_seg->has_target)
            {
                atomic_seg->target_tuple = find_target_tuple(atomic_seg,
                                                            target_segments);
                atomic_seg->target_tid = get_tuple_tid(atomic_seg->target_tuple);
            }
            
            atomic_segments = lappend(atomic_segments, atomic_seg);
        }
        
        prev_point = curr_point;
    }
    
    return atomic_segments;
}

/*
 * Eclipse detection: Determine if source segment completely
 * replaces target segment (for optimization).
 */
static bool
source_eclipses_target(TemporalSegment *source, TemporalSegment *target)
{
    return (source->valid_from <= target->valid_from &&
            source->valid_until >= target->valid_until);
}
```

### 4.2 Entity Identity Management

**Identity resolution system** (from sql_saga identity management):

```c
/*
 * Entity identity can be specified in multiple ways:
 * - IDENTITY_ONLY: Stable surrogate key (e.g., id)
 * - LOOKUP_ONLY: Natural business key (e.g., ssn, employee_nr)
 * - HYBRID: Both surrogate and natural keys
 */

typedef enum
{
    IDENTITY_STRATEGY_UNKNOWN,
    IDENTITY_STRATEGY_IDENTITY_ONLY,
    IDENTITY_STRATEGY_LOOKUP_ONLY,
    IDENTITY_STRATEGY_HYBRID
} IdentityStrategy;

typedef struct EntityIdentity
{
    /* Primary identity (stable surrogate key) */
    Datum      *identity_values;
    bool       *identity_nulls;
    int         identity_count;
    
    /* Natural/lookup key (business identifier) */
    Datum      *lookup_values;
    bool       *lookup_nulls;
    int         lookup_count;
    
    /* Strategy determination */
    IdentityStrategy strategy;
    
    /* Resolved entity OID from target table */
    Oid         target_entity_oid;
    bool        entity_exists;
} EntityIdentity;

/*
 * Extract entity identity from source tuple based on configured columns.
 */
static EntityIdentity *
extract_entity_identity(TupleTableSlot *source_slot,
                       TemporalMergeState *tmstate)
{
    EntityIdentity *identity = palloc0(sizeof(EntityIdentity));
    int i;
    
    /* Extract identity columns */
    if (tmstate->identity_attnos != NIL)
    {
        identity->identity_count = list_length(tmstate->identity_attnos);
        identity->identity_values = palloc(sizeof(Datum) * identity->identity_count);
        identity->identity_nulls = palloc(sizeof(bool) * identity->identity_count);
        
        i = 0;
        foreach(lc, tmstate->identity_attnos)
        {
            AttrNumber attno = lfirst_int(lc);
            identity->identity_values[i] = slot_getattr(source_slot, attno,
                                                       &identity->identity_nulls[i]);
            i++;
        }
    }
    
    /* Extract lookup columns */
    if (tmstate->lookup_attnos != NIL)
    {
        identity->lookup_count = list_length(tmstate->lookup_attnos);
        identity->lookup_values = palloc(sizeof(Datum) * identity->lookup_count);
        identity->lookup_nulls = palloc(sizeof(bool) * identity->lookup_nulls);
        
        i = 0;
        foreach(lc, tmstate->lookup_attnos)
        {
            AttrNumber attno = lfirst_int(lc);
            identity->lookup_values[i] = slot_getattr(source_slot, attno,
                                                     &identity->lookup_nulls[i]);
            i++;
        }
    }
    
    /* Determine strategy */
    identity->strategy = determine_identity_strategy(identity);
    
    return identity;
}

/*
 * Determine identity strategy based on which columns are present and NULL.
 */
static IdentityStrategy
determine_identity_strategy(EntityIdentity *identity)
{
    bool has_identity = (identity->identity_count > 0);
    bool identity_all_null = all_values_null(identity->identity_values,
                                            identity->identity_nulls,
                                            identity->identity_count);
    
    bool has_lookup = (identity->lookup_count > 0);
    bool lookup_all_null = all_values_null(identity->lookup_values,
                                          identity->lookup_nulls,
                                          identity->lookup_count);
    
    if (has_identity && !identity_all_null && has_lookup && !lookup_all_null)
        return IDENTITY_STRATEGY_HYBRID;
    else if (has_identity && !identity_all_null)
        return IDENTITY_STRATEGY_IDENTITY_ONLY;
    else if (has_lookup && !lookup_all_null)
        return IDENTITY_STRATEGY_LOOKUP_ONLY;
    else
        return IDENTITY_STRATEGY_UNKNOWN;
}

/*
 * Lookup target entity using identity or lookup columns.
 * Returns entity OID if found, InvalidOid otherwise.
 */
static Oid
lookup_target_entity(EntityIdentity *identity,
                    TemporalMergeState *tmstate,
                    EState *estate)
{
    Relation    target_rel = tmstate->mtstate->resultRelInfo->ri_RelationDesc;
    Snapshot    snapshot = estate->es_snapshot;
    ScanKeyData scankey[INDEX_MAX_KEYS];
    IndexScanDesc scan;
    HeapTuple   tuple;
    Oid         entity_oid = InvalidOid;
    
    switch (identity->strategy)
    {
        case IDENTITY_STRATEGY_IDENTITY_ONLY:
        case IDENTITY_STRATEGY_HYBRID:
            /* Use identity columns if available */
            if (!all_values_null(identity->identity_values,
                               identity->identity_nulls,
                               identity->identity_count))
            {
                entity_oid = lookup_by_identity(target_rel, snapshot,
                                               identity->identity_values,
                                               identity->identity_nulls,
                                               identity->identity_count,
                                               tmstate);
            }
            /* HYBRID: Fall through to lookup if identity not found */
            if (identity->strategy == IDENTITY_STRATEGY_HYBRID &&
                !OidIsValid(entity_oid))
            {
                /* Try lookup columns */
                entity_oid = lookup_by_natural_key(target_rel, snapshot,
                                                  identity->lookup_values,
                                                  identity->lookup_nulls,
                                                  identity->lookup_count,
                                                  tmstate);
            }
            break;
            
        case IDENTITY_STRATEGY_LOOKUP_ONLY:
            /* Use lookup columns only */
            entity_oid = lookup_by_natural_key(target_rel, snapshot,
                                              identity->lookup_values,
                                              identity->lookup_nulls,
                                              identity->lookup_count,
                                              tmstate);
            break;
            
        case IDENTITY_STRATEGY_UNKNOWN:
            ereport(ERROR,
                   (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    errmsg("cannot determine entity identity"),
                    errhint("Source row must have either identity or lookup columns populated.")));
            break;
    }
    
    identity->target_entity_oid = entity_oid;
    identity->entity_exists = OidIsValid(entity_oid);
    
    return entity_oid;
}
```

### 4.3 Performance-Critical Paths

**Key performance optimizations**:

```c
/*
 * Performance-critical design principles:
 * 1. No JSONB serialization - direct tuple field access
 * 2. No dynamic SQL - compiled execution plans
 * 3. Typed operations - native PostgreSQL type system
 * 4. Index utilization - leverage GiST/SP-GiST for temporal ops
 */

/* 1. Direct tuple access (no JSONB) */
static inline Datum
fast_get_attr(TupleTableSlot *slot, AttrNumber attno, bool *isnull)
{
    /* Use slot's fast path if available */
    if (slot->tts_ops == &TTSOpsVirtual ||
        slot->tts_ops == &TTSOpsMinimalTuple)
        return slot_getattr(slot, attno, isnull);
    
    /* For heap tuples, use even faster direct access */
    return heap_getattr(slot->tts_tuple, attno,
                       slot->tts_tupleDescriptor, isnull);
}

/* 2. Compiled plans (no dynamic EXECUTE) */
static void
prepare_temporal_merge_plans(TemporalMergeState *tmstate, EState *estate)
{
    /* Prepare reusable plans for common operations */
    tmstate->insert_plan = prepare_insert_plan(tmstate, estate);
    tmstate->update_plan = prepare_update_plan(tmstate, estate);
    tmstate->delete_plan = prepare_delete_plan(tmstate, estate);
    tmstate->lookup_plan = prepare_lookup_plan(tmstate, estate);
}

/* 3. Type-safe operations (no casting) */
static void
add_typed_timeline_segment(TemporalMergeState *tmstate,
                          TupleTableSlot *slot)
{
    TemporalSegment *seg = palloc(sizeof(TemporalSegment));
    
    /* Direct typed access - no ::daterange casting */
    seg->valid_range = DatumGetRangeTypeP(
        slot_getattr(slot, tmstate->era_attno, &seg->isnull));
    
    /* Extract bounds directly from range type */
    range_deserialize(tmstate->range_type, seg->valid_range,
                     &seg->valid_from, &seg->valid_until,
                     &seg->empty);
    
    tmstate->current_timeline = lappend(tmstate->current_timeline, seg);
}

/* 4. Index-aware execution */
static List *
fetch_target_timeline_indexed(TemporalMergeState *tmstate,
                              Datum entity_id,
                              EState *estate)
{
    Relation    target_rel = tmstate->mtstate->resultRelInfo->ri_RelationDesc;
    List       *timeline = NIL;
    IndexScanDesc scan;
    ScanKeyData scankey[2];
    
    /* Use composite index (entity_id, valid_range) */
    ScanKeyInit(&scankey[0],
               1,  /* entity_id attribute */
               BTEqualStrategyNumber,
               F_OIDEQ,
               entity_id);
    
    /* GiST index scan for temporal range */
    scan = index_beginscan(target_rel,
                          tmstate->temporal_index,
                          estate->es_snapshot,
                          1, 0);
    
    index_rescan(scan, scankey, 1, NULL, 0);
    
    /* Fetch all timeline segments for this entity */
    while (index_getnext_slot(scan, ForwardScanDirection, tmstate->tempslot))
    {
        TemporalSegment *seg = extract_timeline_segment(tmstate->tempslot,
                                                        tmstate);
        timeline = lappend(timeline, seg);
    }
    
    index_endscan(scan);
    
    return timeline;
}
```

## 5. Integration Points

### 5.1 PostgreSQL Extension Hooks

**Extension lifecycle management**:

```c
/*
 * Extension initialization and cleanup.
 * Hooks into PostgreSQL's utility and planner systems.
 */

/* Previous hook values */
static ProcessUtility_hook_type prev_ProcessUtility = NULL;
static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

/*
 * Module load callback
 */
void
_PG_init(void)
{
    /* Install hooks */
    prev_ProcessUtility = ProcessUtility_hook;
    ProcessUtility_hook = temporal_merge_ProcessUtility;
    
    prev_planner_hook = planner_hook;
    planner_hook = temporal_merge_planner;
    
    prev_ExecutorStart = ExecutorStart_hook;
    ExecutorStart_hook = temporal_merge_ExecutorStart;
    
    prev_ExecutorEnd = ExecutorEnd_hook;
    ExecutorEnd_hook = temporal_merge_ExecutorEnd;
    
    /* Register GUC variables */
    DefineCustomBoolVariable("temporal_merge.enable",
                            "Enable temporal merge extension",
                            NULL,
                            &temporal_merge_enabled,
                            true,
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);
    
    DefineCustomEnumVariable("temporal_merge.log_level",
                            "Logging level for temporal merge operations",
                            NULL,
                            &temporal_merge_log_level,
                            LOG_LEVEL_WARNING,
                            temporal_merge_log_options,
                            PGC_USERSET,
                            0,
                            NULL, NULL, NULL);
    
    elog(LOG, "temporal_merge extension loaded");
}

/*
 * Module unload callback  
 */
void
_PG_fini(void)
{
    /* Restore original hooks */
    ProcessUtility_hook = prev_ProcessUtility;
    planner_hook = prev_planner_hook;
    ExecutorStart_hook = prev_ExecutorStart;
    ExecutorEnd_hook = prev_ExecutorEnd;
    
    elog(LOG, "temporal_merge extension unloaded");
}
```

### 5.2 Existing MERGE Infrastructure Reuse

**Leveraging PostgreSQL's MERGE implementation**:

```c
/*
 * Reuse strategy:
 * - Parser: Extend existing MergeStmt with temporal options
 * - Planner: Enhance ModifyTable planning for temporal operations
 * - Executor: Build on nodeModifyTable.c's MERGE execution
 * - Concurrency: Leverage existing MERGE concurrency handling
 */

/* Parser reuse: Extend MergeStmt */
typedef struct TemporalMergeStmt
{
    NodeTag     type;
    MergeStmt  *base_merge;  /* Reuse existing MergeStmt */
    /* Add temporal-specific fields */
    char       *era_name;
    char       *mode;
    /* ... */
} TemporalMergeStmt;

/* Planner reuse: Enhance ModifyTable plan */
static ModifyTable *
create_temporal_modifytable_plan(PlannerInfo *root,
                                TemporalMergeStmt *stmt)
{
    ModifyTable *plan;
    
    /* Start with standard MERGE planning */
    plan = create_merge_plan(root, stmt->base_merge);
    
    /* Enhance with temporal-specific planning */
    add_temporal_join_conditions(root, plan, stmt);
    add_temporal_index_hints(root, plan, stmt);
    
    /* Store temporal metadata for executor */
    plan->temporal_era = stmt->era_name;
    plan->temporal_mode = stmt->mode;
    
    return plan;
}

/* Executor reuse: Extend ModifyTable execution */
static TupleTableSlot *
ExecTemporalModifyTable(ModifyTableState *node)
{
    TemporalMergeState *tmstate = node->temporal_state;
    
    if (tmstate != NULL)
    {
        /* Use temporal merge execution path */
        return ExecTemporalMerge(tmstate, node->mt_slot, node->ps.state);
    }
    else
    {
        /* Fall back to standard MERGE execution */
        return ExecModifyTable(node);
    }
}

/* Concurrency reuse: Leverage MERGE's concurrency handling */
static bool
handle_temporal_concurrent_update(TemporalMergeState *tmstate,
                                  TupleTableSlot *slot,
                                  ItemPointer tid)
{
    /*
     * Reuse MERGE's sophisticated concurrent update handling:
     * - Follow update chains
     * - Re-evaluate join conditions
     * - Handle MATCHED -> NOT MATCHED transitions
     */
    return ExecMergeMatched_ConcurrentUpdate(tmstate->mtstate,
                                            slot, tid);
}
```

## 6. Performance Expectations

### 6.1 Bottleneck Elimination Analysis

**Current sql_saga bottlenecks and elimination strategy**:

| Bottleneck | Current Impact | Elimination Strategy | Expected Gain |
|------------|----------------|---------------------|---------------|
| **JSONB serialization** | ~40-50% overhead | Direct tuple field access | 40-50% faster |
| **Dynamic EXECUTE** | ~20-30% overhead | Compiled execution plans | 20-30% faster |
| **Untyped temp tables** | ~15-25% overhead | Native tuple processing | 15-25% faster |
| **Function call overhead** | ~10-20% overhead | Executor-level integration | 10-20% faster |
| **PL/pgSQL interpreter** | ~5-10% overhead | C implementation | 5-10% faster |

**Compounding effect**:
- Individual optimizations: 5-50% each
- Combined effect: **3-5x overall improvement** (conservative)
- Optimistic scenario: **10-15x improvement** with optimal planning

### 6.2 Performance Targets and Scenarios

**Performance targets by scenario**:

```c
/* Benchmark scenarios */
typedef enum
{
    BENCH_SIMPLE,       /* Single entity, single timeline segment */
    BENCH_COMPLEX,      /* Multiple entities, overlapping timelines */
    BENCH_PATHOLOGICAL  /* Fragmented identities, dense overlaps */
} BenchmarkScenario;

/* Performance targets */
typedef struct PerformanceTarget
{
    BenchmarkScenario scenario;
    int64    current_rows_per_sec;  /* sql_saga baseline */
    int64    conservative_target;    /* 3x improvement */
    int64    optimistic_target;      /* 10x improvement */
} PerformanceTarget;

static PerformanceTarget performance_targets[] = {
    /* Scenario            Current    Conservative  Optimistic */
    {BENCH_SIMPLE,         2800,      8400,        28000},
    {BENCH_COMPLEX,        1500,      4500,        15000},
    {BENCH_PATHOLOGICAL,   500,       1500,        5000}
};
```

**Measurement approach**:
```sql
-- Performance measurement framework
CREATE TABLE temporal_merge_benchmark_results (
    implementation TEXT,        -- 'sql_saga' | 'extension_v1' | 'extension_v2'
    scenario TEXT,             -- 'simple' | 'complex' | 'pathological'
    row_count INTEGER,
    batch_size INTEGER,
    total_time_ms BIGINT,
    rows_per_second NUMERIC,
    operations JSONB,          -- {inserts: N, updates: M, deletes: K}
    measured_at TIMESTAMP DEFAULT now()
);

-- Benchmark execution
CREATE OR REPLACE FUNCTION benchmark_temporal_merge(
    p_implementation TEXT,
    p_scenario TEXT,
    p_row_count INTEGER DEFAULT 10000,
    p_batch_size INTEGER DEFAULT 1000
) RETURNS TABLE(
    rows_per_second NUMERIC,
    total_time_ms BIGINT,
    avg_time_per_batch_ms NUMERIC
) AS $$
DECLARE
    v_start TIMESTAMP;
    v_end TIMESTAMP;
    v_duration INTERVAL;
BEGIN
    v_start := clock_timestamp();
    
    -- Execute based on implementation
    IF p_implementation = 'sql_saga' THEN
        CALL sql_saga.temporal_merge(...);
    ELSIF p_implementation = 'extension' THEN
        TEMPORAL MERGE ... ;
    END IF;
    
    v_end := clock_timestamp();
    v_duration := v_end - v_start;
    
    RETURN QUERY SELECT
        p_row_count / EXTRACT(EPOCH FROM v_duration),
        EXTRACT(MILLISECONDS FROM v_duration)::BIGINT,
        (EXTRACT(MILLISECONDS FROM v_duration) / (p_row_count::NUMERIC / p_batch_size))::NUMERIC;
END;
$$ LANGUAGE plpgsql;
```

### 6.3 Benchmarking Strategy

**Comprehensive benchmarking approach**:

1. **Baseline establishment**
   - Measure current sql_saga performance across scenarios
   - Document bottleneck contributions
   - Establish reproducible test conditions

2. **Incremental validation**
   - Measure after each optimization phase
   - Track regression risk
   - Validate improvements compound

3. **Comparison metrics**
   ```c
   typedef struct BenchmarkMetrics
   {
       /* Throughput */
       double rows_per_second;
       double entities_per_second;
       double operations_per_second;
       
       /* Latency */
       double avg_latency_ms;
       double p50_latency_ms;
       double p95_latency_ms;
       double p99_latency_ms;
       
       /* Resources */
       int64  memory_peak_kb;
       int64  temp_disk_kb;
       double cpu_time_sec;
       
       /* Operations */
       int64  inserts_performed;
       int64  updates_performed;
       int64  deletes_performed;
       int64  timeline_splits;
   } BenchmarkMetrics;
   ```

4. **Scenario coverage**
   - Simple: Single entity, non-overlapping timeline
   - Complex: Multiple entities, overlapping periods
   - Pathological: Fragmented identities, dense overlaps
   - Edge cases: Empty source, empty target, concurrent updates

## 7. Risk Assessment and Mitigation

### 7.1 Technical Risks

| Risk | Probability | Impact | Mitigation Strategy |
|------|------------|--------|---------------------|
| **PostgreSQL API instability** | Medium | High | • Hook into stable extension APIs<br>• Version-specific compatibility layers<br>• Upstream engagement for API stability |
| **Performance regression** | Low | High | • Comprehensive benchmarking at each phase<br>• Fallback to sql_saga if needed<br>• Performance gate checks |
| **Complex edge cases** | High | Medium | • Extensive test suite from sql_saga<br>• Incremental feature development<br>• Clear error messages |
| **Concurrency issues** | Medium | High | • Leverage MERGE's proven concurrency<br>• Comprehensive concurrency testing<br>• Transaction isolation validation |
| **Extension compatibility** | Low | Medium | • Standard extension hooks<br>• Minimal PostgreSQL core changes<br>• Clean hook lifecycle |

### 7.2 Development Risks

| Risk | Probability | Impact | Mitigation Strategy |
|------|------------|--------|---------------------|
| **C expertise required** | High | Medium | • Leverage PostgreSQL community<br>• Start with simple implementation<br>• Incremental complexity increase |
| **Timeline underestimation** | High | Medium | • Phased development approach<br>• Early prototype validation<br>• Regular milestone reviews |
| **Scope creep** | Medium | Medium | • Clear phase boundaries<br>• MVP focus for Phase 1<br>• Feature freeze for upstream proposal |
| **Resource availability** | Medium | High | • Modular design for parallel work<br>• Clear documentation for handoffs<br>• Extension design allows pausing |

### 7.3 Adoption Risks

| Risk | Probability | Impact | Mitigation Strategy |
|------|------------|--------|---------------------|
| **Upstream rejection** | Medium | Low | • Maintain as extension if not accepted<br>• Clear value proposition<br>• Community engagement early |
| **Migration complexity** | Medium | Medium | • Compatibility layer with sql_saga<br>• Clear migration documentation<br>• Side-by-side operation support |
| **Breaking changes** | Low | High | • Maintain API compatibility where possible<br>• Version clearly<br>• Deprecation path for incompatibilities |

### 7.4 Mitigation Strategies Detail

**PostgreSQL API stability**:
```c
/* Version-specific compatibility layer */
#if PG_VERSION_NUM >= 180000
    #define TEMPORAL_MERGE_USE_NATIVE_PERIOD 1
#else
    #define TEMPORAL_MERGE_USE_NATIVE_PERIOD 0
#endif

/* Graceful degradation */
static void
check_postgresql_compatibility(void)
{
    if (PG_VERSION_NUM < 180000)
        ereport(ERROR,
               (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("temporal_merge requires PostgreSQL 18 or later"),
                errhint("Current version: %s", PG_VERSION)));
}
```

**Performance regression protection**:
```c
/* Performance gate check */
static void
validate_performance_improvement(BenchmarkMetrics *baseline,
                                BenchmarkMetrics *current)
{
    double improvement_factor = 
        current->rows_per_second / baseline->rows_per_second;
    
    if (improvement_factor < MIN_PERFORMANCE_IMPROVEMENT)
        ereport(WARNING,
               (errmsg("performance improvement below target"),
                errdetail("Expected %.1fx, got %.1fx",
                         MIN_PERFORMANCE_IMPROVEMENT,
                         improvement_factor),
                errhint("Review optimization strategy")));
}
```

## 8. Success Criteria

### 8.1 Functional Requirements

**Phase 1 (Foundation)**:
- [x] Parse TEMPORAL MERGE statements without errors
- [x] Execute simple TEMPORAL MERGE with ENTITY_PATCH mode
- [x] Handle basic identity and lookup columns
- [x] Maintain transactional semantics

**Phase 2 (Core)**:
- [ ] Support all temporal_merge modes from sql_saga
- [ ] Handle complex identity scenarios (HYBRID strategy)
- [ ] Process temporal overlaps correctly
- [ ] Maintain functional equivalence with sql_saga

**Phase 3 (Advanced)**:
- [ ] Support multi-era scenarios
- [ ] Handle ephemeral columns
- [ ] Complex delete behaviors
- [ ] Edge case coverage (concurrent updates, gaps, etc.)

**Phase 4 (Production)**:
- [ ] Comprehensive error handling
- [ ] Transaction safety validation
- [ ] Concurrency correctness
- [ ] Migration compatibility with sql_saga

### 8.2 Performance Requirements

**Minimum targets** (Phase 2):
- [ ] Achieve 3x performance improvement over sql_saga
- [ ] Eliminate JSONB overhead (direct tuple access)
- [ ] Eliminate dynamic EXECUTE (compiled plans)
- [ ] Demonstrate measurable improvement in benchmarks

**Optimal targets** (Phase 3):
- [ ] Achieve 10x+ performance improvement
- [ ] Approach regular DML performance levels
- [ ] Efficient index utilization
- [ ] Parallel processing capability

**Validation metrics**:
```sql
-- Success criteria validation
CREATE OR REPLACE FUNCTION validate_success_criteria()
RETURNS TABLE(
    criterion TEXT,
    target NUMERIC,
    actual NUMERIC,
    status TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH baseline AS (
        SELECT 2800 AS rows_per_sec FROM benchmark_sql_saga()
    ),
    current AS (
        SELECT rows_per_sec FROM benchmark_extension()
    ),
    improvement AS (
        SELECT (c.rows_per_sec / b.rows_per_sec) AS factor
        FROM baseline b, current c
    )
    SELECT 
        'Performance improvement'::TEXT,
        3.0::NUMERIC AS target,
        i.factor::NUMERIC AS actual,
        CASE WHEN i.factor >= 3.0 THEN 'PASS' ELSE 'FAIL' END AS status
    FROM improvement i;
END;
$$ LANGUAGE plpgsql;
```

### 8.3 Quality Requirements

**Code quality**:
- [ ] PostgreSQL coding standards compliance
- [ ] Comprehensive inline documentation
- [ ] Clean separation of concerns
- [ ] Minimal technical debt

**Testing coverage**:
- [ ] 90%+ code coverage
- [ ] All sql_saga test cases ported
- [ ] Edge case coverage
- [ ] Concurrency test suite
- [ ] Performance regression tests

**Documentation**:
- [ ] User documentation (syntax, examples)
- [ ] Developer documentation (architecture, API)
- [ ] Migration guide (sql_saga → extension)
- [ ] Performance tuning guide

**Production readiness**:
- [ ] Error handling and recovery
- [ ] Clear error messages
- [ ] Logging and diagnostics
- [ ] Monitoring integration

## 9. Development Environment and Tools

### 9.1 Required Infrastructure

**PostgreSQL development environment**:
```bash
# Clone PostgreSQL source (version 18+)
git clone https://git.postgresql.org/git/postgresql.git
cd postgresql
git checkout REL_18_STABLE

# Configure with development options
./configure \
    --enable-debug \
    --enable-cassert \
    --enable-depend \
    --with-pgport=5433 \
    --prefix=$HOME/pgsql-dev

# Build and install
make -j8
make install

# Initialize test cluster
$HOME/pgsql-dev/bin/initdb -D $HOME/pgsql-dev/data
$HOME/pgsql-dev/bin/pg_ctl -D $HOME/pgsql-dev/data -l logfile start
```

**Extension development setup**:
```bash
# Create extension directory
cd postgresql/contrib
mkdir temporal_merge
cd temporal_merge

# Extension structure
├── Makefile                    # Build configuration
├── README.md                   # Documentation
├── temporal_merge--1.0.sql     # Extension SQL
├── temporal_merge.control      # Extension metadata
└── src/
    ├── temporal_merge.c        # C implementation
    ├── temporal_merge.h        # Header definitions
    ├── parse_temporal_merge.c  # Parser extension
    ├── plan_temporal_merge.c   # Planner integration
    ├── exec_temporal_merge.c   # Executor implementation
    └── test/
        └── sql/                # Regression tests
            ├── 001_basic.sql
            ├── 002_entity_patch.sql
            └── ...
```

**Makefile template**:
```makefile
# temporal_merge/Makefile

MODULE_big = temporal_merge
OBJS = \
    src/temporal_merge.o \
    src/parse_temporal_merge.o \
    src/plan_temporal_merge.o \
    src/exec_temporal_merge.o

EXTENSION = temporal_merge
DATA = temporal_merge--1.0.sql

REGRESS = \
    001_basic_syntax \
    002_entity_patch_mode \
    003_timeline_replace_mode \
    004_identity_management \
    005_concurrent_updates

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
```

### 9.2 Development Tools

**Required tools**:
- **Compiler**: GCC or Clang with C99 support
- **Debugger**: GDB or LLDB
- **Profiler**: perf, gprof, or Instruments (macOS)
- **Memory checker**: Valgrind or AddressSanitizer
- **Code analysis**: clang-analyzer, cppcheck

**Development workflow**:
```bash
# Build extension
make clean && make

# Install extension
make install

# Run regression tests
make installcheck

# Debug with GDB
gdb --args postgres --single -D $HOME/pgsql-dev/data postgres

# Profile with perf
perf record -g postgres ...
perf report

# Memory check with Valgrind
valgrind --leak-check=full postgres --single -D $HOME/pgsql-dev/data
```

### 9.3 Key Files and Locations

**PostgreSQL source tree**:
```
postgresql/
├── src/
│   ├── backend/
│   │   ├── parser/
│   │   │   ├── gram.y                      # Grammar (extend for TEMPORAL MERGE)
│   │   │   ├── parse_merge.c               # MERGE parser (reference)
│   │   │   └── parse_temporal_merge.c      # New: Temporal MERGE parser
│   │   ├── optimizer/
│   │   │   └── plan/
│   │   │       └── temporal_merge_plan.c   # New: Temporal planning
│   │   └── executor/
│   │       ├── nodeModifyTable.c           # MERGE executor (reference)
│   │       └── exec_temporal_merge.c       # New: Temporal executor
│   └── include/
│       ├── nodes/
│       │   ├── parsenodes.h                # Add TemporalMergeStmt
│       │   └── plannodes.h                 # Add temporal plan nodes
│       └── executor/
│           └── executor.h                  # Add executor declarations
└── contrib/
    └── temporal_merge/                     # Extension directory
        ├── temporal_merge--1.0.sql
        ├── temporal_merge.control
        ├── Makefile
        └── src/
            ├── temporal_merge.c            # Extension entry point
            └── ...
```

**sql_saga reference**:
```
sql_saga/
├── src/
│   ├── 27_temporal_merge_plan.sql          # Reference: Planner logic
│   ├── 28_temporal_merge_execute.sql       # Reference: Executor logic
│   └── 29_temporal_merge.sql               # Reference: Main procedure
├── doc/
│   ├── temporal_merge_constellations.md    # Reference: Algorithm documentation
│   ├── temporal_merge_identity_management.md
│   └── internals/
│       ├── optimization_findings.md        # Reference: Performance analysis
│       └── eclipse_detection.md            # Reference: Timeline algorithm
└── sql/
    └── 088_temporal_merge_identity_discovery.sql  # Reference: Complex test case
```

## 10. Next Steps and Deliverables

### 10.1 Immediate Actions (Week 1)

**Environment setup**:
- [ ] Install PostgreSQL 18+ development environment
- [ ] Configure debugging and profiling tools
- [ ] Clone sql_saga repository for reference
- [ ] Set up test database and benchmarking framework

**Initial exploration**:
- [ ] Study PostgreSQL MERGE implementation (`parse_merge.c`, `nodeModifyTable.c`)
- [ ] Review sql_saga temporal_merge implementation
- [ ] Analyze key algorithms (timeline reconstruction, identity resolution)
- [ ] Document current bottlenecks with profiling data

**Proof of concept**:
- [ ] Create minimal extension skeleton
- [ ] Implement basic TEMPORAL MERGE syntax parsing
- [ ] Execute simple operation (bypass optimization)
- [ ] Validate approach feasibility

### 10.2 Phase Deliverables

**Phase 1 (Weeks 1-4): Foundation**
- [ ] Extension framework operational
- [ ] TEMPORAL MERGE statements parse successfully
- [ ] Basic execution path (unoptimized)
- [ ] Initial test suite (10+ tests)
- [ ] Performance measurement baseline

**Phase 2 (Weeks 5-8): Core Functionality**
- [ ] ENTITY_PATCH mode fully functional
- [ ] Temporal overlap resolution working
- [ ] Direct tuple processing (no JSONB)
- [ ] Compiled execution plans (no dynamic SQL)
- [ ] Measurable performance improvement (2x minimum)

**Phase 3 (Weeks 9-12): Advanced Features**
- [ ] All temporal_merge modes supported
- [ ] Complex identity management (HYBRID)
- [ ] Advanced optimization (indexes, parallelism)
- [ ] Comprehensive test coverage (90%+)
- [ ] Performance target achieved (3x minimum)

**Phase 4 (Weeks 13-16): Production Readiness**
- [ ] PostgreSQL coding standards compliance
- [ ] Complete documentation (user + developer)
- [ ] Migration guide from sql_saga
- [ ] Performance validation report
- [ ] Upstream proposal prepared

### 10.3 Success Metrics and Milestones

**Milestone checkpoints**:

| Week | Milestone | Success Criteria | Decision Point |
|------|-----------|-----------------|----------------|
| 4 | Phase 1 Complete | • Extension loads<br>• Basic TEMPORAL MERGE executes<br>• Tests pass | Continue to Phase 2 or revise approach |
| 8 | Phase 2 Complete | • ENTITY_PATCH mode works<br>• 2x performance improvement<br>• Core tests pass | Continue to Phase 3 or optimize Phase 2 |
| 12 | Phase 3 Complete | • All modes functional<br>• 3x performance improvement<br>• Full test coverage | Continue to Phase 4 or focus on gaps |
| 16 | Phase 4 Complete | • Production ready<br>• Documentation complete<br>• Upstream proposal ready | Release extension or propose upstream |

**Performance validation gates**:
```sql
-- Milestone performance validation
CREATE OR REPLACE FUNCTION validate_milestone_performance(
    p_phase INTEGER
) RETURNS TABLE(
    phase INTEGER,
    required_improvement NUMERIC,
    actual_improvement NUMERIC,
    status TEXT
) AS $$
DECLARE
    v_targets NUMERIC[] := ARRAY[1.0, 2.0, 3.0, 5.0];  -- Phase targets
    v_actual NUMERIC;
BEGIN
    v_actual := (SELECT improvement_factor FROM benchmark_comparison());
    
    RETURN QUERY SELECT
        p_phase,
        v_targets[p_phase],
        v_actual,
        CASE 
            WHEN v_actual >= v_targets[p_phase] THEN 'PASS'
            WHEN v_actual >= v_targets[p_phase] * 0.8 THEN 'MARGINAL'
            ELSE 'FAIL'
        END;
END;
$$ LANGUAGE plpgsql;
```

### 10.4 Handoff Package

**Documentation for another agent**:

This plan provides:
1. **Complete architecture specification** (Sections 2-4)
2. **Detailed implementation phases** (Section 3)
3. **Code examples and algorithms** (Section 4)
4. **Development environment setup** (Section 9)
5. **Clear success criteria** (Section 8)
6. **Risk mitigation strategies** (Section 7)

**Key reference files from sql_saga**:
- `src/27_temporal_merge_plan.sql` - Planner algorithm reference
- `src/28_temporal_merge_execute.sql` - Executor logic reference
- `doc/temporal_merge_constellations.md` - Algorithm documentation
- `doc/internals/optimization_findings.md` - Performance analysis

**Extension starting point**:
```
temporal_merge/
├── temporal_merge.control     # Extension metadata
├── temporal_merge--1.0.sql    # Extension SQL (minimal)
├── Makefile                   # Build configuration
├── README.md                  # This plan document
└── src/
    ├── temporal_merge.c       # Hook registration
    ├── temporal_merge.h       # Type definitions
    └── test/
        └── sql/
            └── 001_basic.sql  # First test case
```

**First task**: Implement basic extension skeleton that can parse and reject TEMPORAL MERGE statements with clear "not yet implemented" error.

---

## Appendix A: sql_saga Temporal Merge Overview

For reference, sql_saga's temporal_merge implementation consists of:

### A.1 Core Components

1. **Main Procedure** (`src/29_temporal_merge.sql`)
   - User-facing API with 15+ parameters
   - Orchestrates planning and execution
   - Handles identity discovery and validation

2. **Planner** (`src/27_temporal_merge_plan.sql`, 2,319 LOC)
   - Introspects table schemas
   - Builds atomic timeline segments
   - Determines operations (INSERT/UPDATE/DELETE)
   - Handles identity resolution

3. **Executor** (`src/28_temporal_merge_execute.sql`, 1,045 LOC)
   - Executes planned operations
   - Handles constraint deferral
   - Provides feedback to source
   - Manages transaction safety

### A.2 Key Algorithms

**Timeline Reconstruction**:
1. Collect all time points from source and target
2. Create atomic segments between consecutive points
3. For each segment, determine source/target presence
4. Apply mode-specific operation logic
5. Execute operations in optimal order

**Identity Resolution**:
1. Determine strategy (IDENTITY_ONLY, LOOKUP_ONLY, HYBRID)
2. Extract relevant columns from source
3. Lookup target entity using appropriate key
4. Handle identity back-filling for new entities
5. Resolve fragmented natural keys

**Eclipse Detection**:
1. For each source row, find overlapping target rows
2. Determine if source completely eclipses target
3. Optimize DELETE+INSERT to UPDATE where possible
4. Handle partial overlaps with timeline splitting

### A.3 Performance Characteristics

**Current bottlenecks** (from analysis):
- JSONB manipulation: ~40-50% overhead
- Dynamic EXECUTE: ~20-30% overhead
- Untyped temp tables: ~15-25% overhead
- PL/pgSQL interpreter: ~10-20% overhead

**Optimization history**:
- CASE→OR optimization: 36-56% improvement
- Split-path planner: 10x improvement in complex cases
- LATERAL jsonb_populate_record elimination: 48% UPDATE speedup
- Compound index addition: 20x improvement for lookups
- Eclipse detection optimization: 30-40% improvement

## Appendix B: PostgreSQL MERGE Overview

For reference, PostgreSQL's MERGE implementation:

### B.1 File Locations

- **Parser**: `src/backend/parser/parse_merge.c`
- **Planner**: `src/backend/optimizer/plan/createplan.c` (ModifyTable creation)
- **Executor**: `src/backend/executor/nodeModifyTable.c` (ExecMerge*)
- **Node types**: `src/include/nodes/parsenodes.h` (MergeStmt)

### B.2 Key Functions

**Parser**:
- `transformMergeStmt()` - Transform parse tree
- `setNamespaceForMergeWhen()` - Handle namespace visibility

**Executor**:
- `ExecMerge()` - Main MERGE execution
- `ExecMergeMatched()` - Handle MATCHED case
- `ExecMergeNotMatched()` - Handle NOT MATCHED case

### B.3 Concurrency Handling

PostgreSQL MERGE has sophisticated concurrency:
- Follows update chains for concurrent modifications
- Re-evaluates join conditions after concurrent updates
- Handles MATCHED → NOT MATCHED transitions
- Provides strong consistency guarantees

---

**Document Version**: 1.0  
**Last Updated**: 2026-01-19  
**Status**: Ready for Implementation  
**Target**: PostgreSQL 18+ Extension Development
