--
-- Inserts a common dataset into the `houses` table, used by tests
-- 24, 25, 26, 27.
--
INSERT INTO houses (id, assessment, valid_from, valid_until) VALUES
  (1, 150000, '2015-01-01'::DATE, '2016-01-01'::DATE),
  (1, 200000, '2016-01-01'::DATE, '2017-01-01'::DATE),
  (2, 300000, '2015-01-01'::DATE, '2016-01-01'::DATE),
  (3, 100000, '2014-01-01'::DATE, '2015-01-01'::DATE),
  (3, 200000, '2015-01-01'::DATE, 'infinity'::DATE),
  (4, 200000, '-infinity'::DATE, '2014-01-01'::DATE)
;
