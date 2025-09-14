CREATE TEMPORARY TABLE benchmark (
  seq_id BIGINT GENERATED ALWAYS AS IDENTITY,
  timestamp TIMESTAMPTZ DEFAULT clock_timestamp(),
  event TEXT,
  row_count INTEGER,
  is_performance_benchmark BOOLEAN NOT NULL
);

INSERT INTO benchmark (event, row_count, is_performance_benchmark) VALUES ('BEGIN', 0, false);
