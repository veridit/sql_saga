--
-- Creates the shifts, houses, and rooms tables used by many legacy tests.
--
CREATE TABLE shifts (
  job_id INTEGER,
  worker_id INTEGER,
  valid_range DATERANGE,
  valid_from DATE,
  valid_until DATE
);

CREATE TABLE houses (
  id INTEGER,
  assessment FLOAT,
  valid_range DATERANGE,
  valid_from DATE,
  valid_until DATE
);

CREATE TABLE rooms (
  id INTEGER,
  house_id INTEGER,
  valid_range DATERANGE,
  valid_from DATE,
  valid_until DATE
);
