\c src;
drop table orders;
CREATE TABLE orders (
    id INT PRIMARY KEY,
    description TEXT,
    source_system TEXT
);
drop table customers;
CREATE TABLE customers (
    id INT PRIMARY KEY,
    name TEXT,
    email TEXT,
    source_system TEXT
);
DROP ROLE host_src;
CREATE ROLE host_src with replication login superuser;
GRANT ALL ON orders to host_src;
GRANT ALL ON customers to host_src;


CREATE OR REPLACE FUNCTION set_source_system()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.source_system IS NULL THEN
        NEW.source_system := 'src';
    END IF;
    IF CURRENT_USER = 'host_src' THEN
        NEW.source_system := 'src';
    END IF;
    IF CURRENT_USER = 'host_dst' THEN
        NEW.source_system := 'dst';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_set_src_orders BEFORE INSERT OR UPDATE ON orders
FOR EACH ROW EXECUTE FUNCTION set_source_system();
CREATE OR REPLACE TRIGGER trg_set_src_orders BEFORE INSERT OR UPDATE ON customers
FOR EACH ROW EXECUTE FUNCTION set_source_system();

drop publication src_pub;
CREATE PUBLICATION src_pub FOR TABLE orders, customers;

select pg_drop_replication_slot('slot_src');
select pg_create_logical_replication_slot('slot_src', 'wal2json');






\c dst;
drop table orders;
CREATE TABLE orders (
    id INT PRIMARY KEY,
    description TEXT,
    source_system TEXT
);
drop table customers;
CREATE TABLE customers (
    id INT PRIMARY KEY,
    name TEXT,
    email TEXT,
    source_system TEXT
);
CREATE OR REPLACE FUNCTION set_source_system()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.source_system IS NULL THEN
        NEW.source_system := 'dst';
    END IF;
    IF CURRENT_USER = 'host_src' THEN
        NEW.source_system := 'src';
    END IF;
    IF CURRENT_USER = 'host_dst' THEN
        NEW.source_system := 'dst';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP ROLE host_dst;
CREATE ROLE host_dst with replication login superuser;
GRANT ALL ON orders to host_dst;
GRANT ALL ON customers to host_dst;

CREATE OR REPLACE TRIGGER trg_set_src_orders BEFORE INSERT OR UPDATE ON orders
FOR EACH ROW EXECUTE FUNCTION set_source_system();
CREATE OR REPLACE TRIGGER trg_set_src_orders BEFORE INSERT OR UPDATE ON customers
FOR EACH ROW EXECUTE FUNCTION set_source_system();

drop publication dst_pub;
CREATE PUBLICATION dst_pub FOR TABLE orders, customers;

select pg_drop_replication_slot('slot_dst');
select pg_create_logical_replication_slot('slot_dst', 'wal2json');