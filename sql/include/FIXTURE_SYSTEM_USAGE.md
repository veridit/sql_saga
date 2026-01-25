# Fixture System Usage Guide

## Overview

The fixture system provides fast data generation and loading for benchmark tests. Instead of generating millions of rows on every test run, you can generate fixtures once and load them instantly.

## Quick Start

```sql
-- Include the fixture system in your test
\i sql/include/benchmark_fixture_system_simple.sql
\i sql/include/benchmark_fixture_integration.sql

-- Generate a fixture (one time)
SELECT sql_saga_fixtures.generate_temporal_merge_fixture('my_test_1K', 1000, false, true);

-- Load fixture instantly in tests  
SELECT sql_saga_fixtures.load_temporal_merge_fixture('my_test_1K', 'public');

-- Your test tables now contain 2000 rows (1K legal_unit + 1K establishment)
SELECT COUNT(*) FROM legal_unit_tm;   -- 1000
SELECT COUNT(*) FROM establishment_tm; -- 1000
```

## Common Patterns

### 1. Auto-Generation with Standard Naming

```sql
-- Auto-generate if missing, using standard naming conventions
SELECT benchmark_ensure_temporal_merge_fixture('10K', 'basic', true);
SELECT benchmark_load_temporal_merge_fixture('10K', 'basic', 'public');
```

This creates fixture `temporal_merge_10K_basic` with 10,000 entities.

### 2. ETL Benchmark Fixtures

```sql
-- Generate ETL fixture with 1000 entities, 3 batches per entity, 5 rows per batch = 15,000 rows
SELECT sql_saga_fixtures.generate_etl_fixture('etl_test_1K', 1000, 3, 5, false);

-- Load into your ETL table
SELECT sql_saga_fixtures.load_etl_fixture('etl_test_1K', 'my_etl_table'::regclass);
```

### 3. Performance Testing Multiple Scales

```sql
-- Prepare fixtures for multiple scales
SELECT benchmark_prepare_all_fixtures('temporal_merge', ARRAY['1K', '10K', '100K'], false);

-- Test each scale
SELECT benchmark_load_temporal_merge_fixture('1K', 'basic');
-- ... run your benchmark ...

SELECT benchmark_load_temporal_merge_fixture('10K', 'basic');  
-- ... run your benchmark ...
```

## Fixture Management

### List Available Fixtures

```sql
SELECT fixture_name, fixture_type, entities, total_rows, load_count 
FROM sql_saga_fixtures.list_fixtures();
```

### Check if Fixture Exists

```sql
SELECT sql_saga_fixtures.fixture_exists('my_fixture_name');
```

### Get Detailed Info

```sql
SELECT sql_saga_fixtures.get_fixture_info('my_fixture_name');
```

### Delete Fixture

```sql
SELECT sql_saga_fixtures.delete_fixture('my_fixture_name');
```

## Standard Naming Conventions

| Pattern | Example | Description |
|---------|---------|-------------|
| `{type}_{scale}_{variant}` | `temporal_merge_1K_basic` | 1K entities, basic setup |
| `{type}_{scale}_{variant}` | `temporal_merge_10K_with_fk` | 10K entities with foreign keys |
| `{type}_{scale}_{variant}` | `etl_bench_100K_standard` | 100K entities, standard config |
| `{type}_{scale}_{variant}` | `etl_bench_1M_high_batch` | 1M entities, high batch count |

### Scale Values
- `1K` = 1,000 entities
- `10K` = 10,000 entities  
- `100K` = 100,000 entities
- `1M` = 1,000,000 entities

### Variant Types
- `basic` = Standard configuration
- `with_fk` = Includes foreign key constraints
- `parent_only` = Legal units only, no establishments
- `standard` = Default ETL configuration
- `high_batch` = More batches per entity
- `low_batch` = Fewer batches per entity

## Integration in Benchmark Tests

### Template Pattern

```sql
\i sql/include/test_setup.sql
\i sql/include/benchmark_setup.sql
\i sql/include/benchmark_fixture_system_simple.sql
\i sql/include/benchmark_fixture_integration.sql

SET ROLE TO sql_saga_unprivileged_user;

-- Ensure fixtures exist (auto-generate if missing)
SELECT benchmark_ensure_temporal_merge_fixture('1K', 'basic', true);

-- Create target tables
CREATE TABLE legal_unit_tm (...);
CREATE TABLE establishment_tm (...);

-- Load fixture data
SELECT benchmark_load_temporal_merge_fixture('1K', 'basic', 'public');

-- Run your benchmarks on pre-loaded data
-- ... benchmark code ...

-- Cleanup
DROP TABLE legal_unit_tm;
DROP TABLE establishment_tm;
```

## Performance Benefits

| Operation | Traditional | With Fixtures | Speedup |
|-----------|-------------|---------------|---------|
| 1K entities (2K rows) | ~100ms generation | ~10ms loading | 10x faster |
| 10K entities (20K rows) | ~1s generation | ~50ms loading | 20x faster |
| 100K entities (200K rows) | ~10s generation | ~500ms loading | 20x faster |
| 1M entities (2M rows) | ~100s generation | ~2s loading | 50x faster |

The larger the dataset, the greater the performance improvement.

## Storage and Persistence

- **CSV Files**: Fixtures are saved as CSV files in the `fixtures/` directory
- **Metadata**: Fixture metadata is stored in `sql_saga_fixtures.fixture_registry` table
- **Fallback**: System automatically falls back to in-memory generation if CSV files aren't available
- **Cross-Session**: Fixtures persist between PostgreSQL sessions and test runs

## Best Practices

1. **Generate Once**: Create fixtures once and reuse them across multiple test runs
2. **Standard Names**: Use the standard naming conventions for consistency
3. **Right Size**: Don't create fixtures larger than needed for your tests
4. **Auto-Generation**: Enable auto-generation in production tests for reliability
5. **Cleanup**: Delete unused fixtures to save disk space
6. **Version Control**: Consider committing frequently-used fixtures to version control

## Troubleshooting

### "Fixture not found" Error
```sql
-- Check if fixture exists
SELECT sql_saga_fixtures.fixture_exists('my_fixture');

-- List all fixtures  
SELECT * FROM sql_saga_fixtures.list_fixtures();

-- Auto-generate missing fixture
SELECT benchmark_ensure_temporal_merge_fixture('1K', 'basic', true);
```

### CSV Loading Fails
- System automatically falls back to in-memory generation
- Check that `fixtures/` directory exists and is writable
- Verify PostgreSQL has permission to read/write CSV files

### Performance Issues
```sql
-- Check fixture generation performance
SELECT * FROM benchmark_compare_generation_vs_loading('1K', 'temporal_merge');

-- Health check
SELECT * FROM benchmark_check_fixture_health();
```

## Advanced Usage

### Custom Fixture Generation

```sql
-- Create custom fixture with specific parameters
SELECT sql_saga_fixtures.generate_temporal_merge_fixture(
    'custom_test',     -- fixture name
    50000,             -- 50K entities 
    true,              -- force regenerate
    false              -- legal units only, no establishments
);
```

### Batch Operations

```sql
-- Generate fixtures for multiple scales
SELECT benchmark_prepare_all_fixtures(
    'temporal_merge',                    -- fixture type
    ARRAY['1K', '10K', '100K'],         -- scales to generate
    false                                -- don't force regenerate existing
);
```

### Performance Analysis

```sql
-- Compare generation vs loading performance
SELECT * FROM benchmark_compare_generation_vs_loading('10K', 'temporal_merge');

-- Fixture usage statistics
SELECT fixture_name, load_count, 
       generation_time_sec, last_loaded_at
FROM sql_saga_fixtures.list_fixtures()
ORDER BY load_count DESC;
```