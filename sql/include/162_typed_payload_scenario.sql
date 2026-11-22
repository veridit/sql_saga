BEGIN;
INSERT INTO w (id, t, j, c, n, i, valid_from, valid_until)
VALUES (1, '{a}', '{"o": 0}', ROW(0, 'z'), 0, '{0}', '2024-01-01', 'infinity');
-- Entity 1 changes from 2025 (an UPDATE that shrinks the old piece and an INSERT); entity 2 is new.
INSERT INTO src VALUES
    (1, 1, '{b,"c,d"}', '{"x": [1, "y"]}', ROW(1, 'x,y'), 1.10, '{1,2}', '2025-01-01', 'infinity'),
    (2, 2, '{e}', '{"n": 2}', ROW(2, 'w'), 2.50, '{3}', '2024-01-01', 'infinity');
CALL sql_saga.temporal_merge(target_table => 'w'::regclass, source_table => 'src'::regclass,
                             primary_identity_columns => ARRAY['id']);
SELECT id, t, j, jsonb_typeof(j) AS j_kind, c, n, i, valid_range FROM w ORDER BY id, valid_range;
-- A data-only UPDATE of the whole timeline of entity 2, with arrays holding NULL and quotes.
TRUNCATE src;
INSERT INTO src VALUES (3, 2, '{"q\"x",NULL}', '[1, {"k": "v"}, "s"]', ROW(NULL, ''), -0.5, '{NULL,4}', '2024-01-01', 'infinity');
CALL sql_saga.temporal_merge(target_table => 'w'::regclass, source_table => 'src'::regclass,
                             primary_identity_columns => ARRAY['id']);
SELECT id, t, j, jsonb_typeof(j) AS j_kind, c, n, i, valid_range FROM w WHERE id = 2;
-- numeric keeps its exact digits and scale (hug nw3qmt: the native planner rounded through a 64-bit float).
TRUNCATE src;
INSERT INTO src (row_id, id, n, valid_from, valid_until) VALUES
    (4, 3, 12345678901234567890.123456789012345678900, '2024-01-01', 'infinity'),
    (5, 3, 0.1000, '2025-01-01', 'infinity');
CALL sql_saga.temporal_merge(target_table => 'w'::regclass, source_table => 'src'::regclass,
                             primary_identity_columns => ARRAY['id']);
SELECT id, n, valid_range FROM w WHERE id = 3 ORDER BY valid_range;
-- A null nested inside a jsonb value is data: PATCH keeps it (hug y37p3x) ...
TRUNCATE src;
INSERT INTO src (row_id, id, j, valid_from, valid_until) VALUES (6, 4, '[1, {"k": null}]', '2024-01-01', 'infinity');
CALL sql_saga.temporal_merge(target_table => 'w'::regclass, source_table => 'src'::regclass,
                             primary_identity_columns => ARRAY['id'], mode => 'MERGE_ENTITY_PATCH');
SELECT id, j, valid_range FROM w WHERE id = 4;
-- ... and two adjacent pieces that differ only by one stay two pieces.
TRUNCATE src;
INSERT INTO src (row_id, id, j, valid_from, valid_until) VALUES
    (7, 5, '{"k": null}', '2024-01-01', '2025-01-01'),
    (8, 5, '{}', '2025-01-01', 'infinity');
CALL sql_saga.temporal_merge(target_table => 'w'::regclass, source_table => 'src'::regclass,
                             primary_identity_columns => ARRAY['id']);
SELECT id, j, valid_range FROM w WHERE id = 5 ORDER BY valid_range;
ROLLBACK;
