\set ON_ERROR_STOP on
SET client_min_messages = warning;
CREATE EXTENSION btree_gist;
CREATE EXTENSION sql_saga VERSION :'extversion';
CREATE SCHEMA bench;
SET search_path = bench, public;

CREATE TABLE sv_base (id integer PRIMARY KEY, payload text);
CREATE TABLE uk_base (id integer, valid daterange);
SELECT sql_saga.add_era('uk_base', 'valid', 'business');
CREATE TABLE fk_parent (id integer, valid daterange);
CREATE TABLE fk_child (id integer, parent_id integer, valid daterange);
SELECT sql_saga.add_era('fk_parent', 'valid', 'business');
SELECT sql_saga.add_unique_key('fk_parent', ARRAY['id']::name[], 'business', key_type => 'primary', unique_key_name => 'fk_parent_primary');
SELECT sql_saga.add_era('fk_child', 'valid', 'business');
SELECT sql_saga.add_unique_key('fk_child', ARRAY['id']::name[], 'business', key_type => 'primary', unique_key_name => 'fk_child_primary');

CREATE TABLE samples (round integer, api text, elapsed_ms double precision);
CREATE FUNCTION bench.round_once(p_round integer) RETURNS void LANGUAGE plpgsql AS $round_once$
DECLARE
    started_at timestamptz;
BEGIN
    PERFORM sql_saga.add_system_versioning('bench.sv_base');
    started_at := clock_timestamp();
    PERFORM sql_saga.drop_system_versioning('bench.sv_base');
    INSERT INTO bench.samples VALUES (p_round, 'drop_system_versioning', extract(epoch FROM clock_timestamp() - started_at) * 1000.0);

    PERFORM sql_saga.add_unique_key('bench.uk_base', ARRAY['id']::name[], 'business', key_type => 'primary', unique_key_name => 'uk_base_primary');
    started_at := clock_timestamp();
    PERFORM sql_saga.drop_unique_key_by_name('bench.uk_base', 'uk_base_primary');
    INSERT INTO bench.samples VALUES (p_round, 'drop_unique_key', extract(epoch FROM clock_timestamp() - started_at) * 1000.0);

    PERFORM sql_saga.add_foreign_key('bench.fk_child', ARRAY['parent_id']::name[], 'bench.fk_parent', ARRAY['id']::name[]);
    started_at := clock_timestamp();
    PERFORM sql_saga.drop_foreign_key('bench.fk_child', ARRAY['parent_id']::name[]);
    INSERT INTO bench.samples VALUES (p_round, 'drop_foreign_key', extract(epoch FROM clock_timestamp() - started_at) * 1000.0);
END;
$round_once$;
