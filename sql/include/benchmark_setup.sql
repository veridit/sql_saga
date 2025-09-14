CREATE TEMPORARY TABLE benchmark (
  seq_id BIGINT GENERATED ALWAYS AS IDENTITY,
  timestamp TIMESTAMPTZ DEFAULT clock_timestamp(),
  event TEXT,
  row_count INTEGER
);

INSERT INTO benchmark (event, row_count) VALUES ('BEGIN', 0);
