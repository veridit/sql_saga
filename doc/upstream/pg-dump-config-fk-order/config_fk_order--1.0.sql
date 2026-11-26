CREATE SCHEMA config_fk_order;
CREATE TABLE config_fk_order.parent (
    id integer PRIMARY KEY
);
CREATE TABLE config_fk_order.child (
    id integer PRIMARY KEY,
    parent_id integer NOT NULL REFERENCES config_fk_order.parent (id)
);
SELECT pg_catalog.pg_extension_config_dump('config_fk_order.parent', '');
SELECT pg_catalog.pg_extension_config_dump('config_fk_order.child', '');
