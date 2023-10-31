/* Run tests as unprivileged user */
SET ROLE TO sql_saga_unprivileged_user;

CREATE TABLE preds (s integer, e integer);
SELECT sql_saga.add_era('preds', 's', 'e', 'p');

INSERT INTO preds (s, e) VALUES (100, 200);
ANALYZE preds;

/* Ensure the functions are inlined. */

EXPLAIN (COSTS OFF) SELECT * FROM preds WHERE sql_saga.contains(s, e, 100);
EXPLAIN (COSTS OFF) SELECT * FROM preds WHERE sql_saga.contains(s, e, 100, 200);
EXPLAIN (COSTS OFF) SELECT * FROM preds WHERE sql_saga.equals(s, e, 100, 200);
EXPLAIN (COSTS OFF) SELECT * FROM preds WHERE sql_saga.overlaps(s, e, 100, 200);
EXPLAIN (COSTS OFF) SELECT * FROM preds WHERE sql_saga.precedes(s, e, 100, 200);
EXPLAIN (COSTS OFF) SELECT * FROM preds WHERE sql_saga.succeeds(s, e, 100, 200);
EXPLAIN (COSTS OFF) SELECT * FROM preds WHERE sql_saga.immediately_precedes(s, e, 100, 200);
EXPLAIN (COSTS OFF) SELECT * FROM preds WHERE sql_saga.immediately_succeeds(s, e, 100, 200);

/* Now make sure they work! */

SELECT * FROM preds WHERE sql_saga.contains(s, e, 0);
SELECT * FROM preds WHERE sql_saga.contains(s, e, 150);
SELECT * FROM preds WHERE sql_saga.contains(s, e, 300);

SELECT * FROM preds WHERE sql_saga.contains(s, e, 0, 50);
SELECT * FROM preds WHERE sql_saga.contains(s, e, 50, 100);
SELECT * FROM preds WHERE sql_saga.contains(s, e, 100, 150);
SELECT * FROM preds WHERE sql_saga.contains(s, e, 150, 200);
SELECT * FROM preds WHERE sql_saga.contains(s, e, 200, 250);
SELECT * FROM preds WHERE sql_saga.contains(s, e, 250, 300);
SELECT * FROM preds WHERE sql_saga.contains(s, e, 125, 175);
SELECT * FROM preds WHERE sql_saga.contains(s, e, 0, 300);

SELECT * FROM preds WHERE sql_saga.equals(s, e, 0, 100);
SELECT * FROM preds WHERE sql_saga.equals(s, e, 100, 200);
SELECT * FROM preds WHERE sql_saga.equals(s, e, 200, 300);

SELECT * FROM preds WHERE sql_saga.overlaps(s, e, 0, 50);
SELECT * FROM preds WHERE sql_saga.overlaps(s, e, 50, 100);
SELECT * FROM preds WHERE sql_saga.overlaps(s, e, 100, 150);
SELECT * FROM preds WHERE sql_saga.overlaps(s, e, 150, 200);
SELECT * FROM preds WHERE sql_saga.overlaps(s, e, 200, 250);
SELECT * FROM preds WHERE sql_saga.overlaps(s, e, 250, 300);
SELECT * FROM preds WHERE sql_saga.overlaps(s, e, 125, 175);
SELECT * FROM preds WHERE sql_saga.overlaps(s, e, 0, 300);

SELECT * FROM preds WHERE sql_saga.precedes(s, e, 0, 50);
SELECT * FROM preds WHERE sql_saga.precedes(s, e, 50, 100);
SELECT * FROM preds WHERE sql_saga.precedes(s, e, 100, 150);
SELECT * FROM preds WHERE sql_saga.precedes(s, e, 150, 200);
SELECT * FROM preds WHERE sql_saga.precedes(s, e, 200, 250);
SELECT * FROM preds WHERE sql_saga.precedes(s, e, 250, 300);
SELECT * FROM preds WHERE sql_saga.precedes(s, e, 125, 175);
SELECT * FROM preds WHERE sql_saga.precedes(s, e, 0, 300);

SELECT * FROM preds WHERE sql_saga.succeeds(s, e, 0, 50);
SELECT * FROM preds WHERE sql_saga.succeeds(s, e, 50, 100);
SELECT * FROM preds WHERE sql_saga.succeeds(s, e, 100, 150);
SELECT * FROM preds WHERE sql_saga.succeeds(s, e, 150, 200);
SELECT * FROM preds WHERE sql_saga.succeeds(s, e, 200, 250);
SELECT * FROM preds WHERE sql_saga.succeeds(s, e, 250, 300);
SELECT * FROM preds WHERE sql_saga.succeeds(s, e, 125, 175);
SELECT * FROM preds WHERE sql_saga.succeeds(s, e, 0, 300);

SELECT * FROM preds WHERE sql_saga.immediately_precedes(s, e, 0, 50);
SELECT * FROM preds WHERE sql_saga.immediately_precedes(s, e, 50, 100);
SELECT * FROM preds WHERE sql_saga.immediately_precedes(s, e, 100, 150);
SELECT * FROM preds WHERE sql_saga.immediately_precedes(s, e, 150, 200);
SELECT * FROM preds WHERE sql_saga.immediately_precedes(s, e, 200, 250);
SELECT * FROM preds WHERE sql_saga.immediately_precedes(s, e, 250, 300);
SELECT * FROM preds WHERE sql_saga.immediately_precedes(s, e, 125, 175);
SELECT * FROM preds WHERE sql_saga.immediately_precedes(s, e, 0, 300);

SELECT * FROM preds WHERE sql_saga.immediately_succeeds(s, e, 0, 50);
SELECT * FROM preds WHERE sql_saga.immediately_succeeds(s, e, 50, 100);
SELECT * FROM preds WHERE sql_saga.immediately_succeeds(s, e, 100, 150);
SELECT * FROM preds WHERE sql_saga.immediately_succeeds(s, e, 150, 200);
SELECT * FROM preds WHERE sql_saga.immediately_succeeds(s, e, 200, 250);
SELECT * FROM preds WHERE sql_saga.immediately_succeeds(s, e, 250, 300);
SELECT * FROM preds WHERE sql_saga.immediately_succeeds(s, e, 125, 175);
SELECT * FROM preds WHERE sql_saga.immediately_succeeds(s, e, 0, 300);

DROP TABLE preds;
