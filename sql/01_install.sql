\i sql/include/test_setup.sql

SELECT extversion
FROM pg_extension
WHERE extname = 'sql_saga';

\i sql/include/test_teardown.sql
