-- Demo schema for the docker-compose PostgreSQL. Data comes from the CSV files written
-- by examples/generate_demo_data.py, mounted at /demo.

CREATE SCHEMA sales;
COMMENT ON SCHEMA sales IS 'Demo online shop';

CREATE TABLE sales.customers (
    id integer PRIMARY KEY,
    name text NOT NULL,
    region text NOT NULL,
    segment text NOT NULL,
    signup_date date NOT NULL
);
COMMENT ON TABLE sales.customers IS 'One row per customer';
COMMENT ON COLUMN sales.customers.segment IS 'Consumer, SMB or Enterprise';

CREATE TABLE sales.products (
    id integer PRIMARY KEY,
    name text NOT NULL,
    category text NOT NULL,
    unit_price numeric(10, 2) NOT NULL
);
COMMENT ON TABLE sales.products IS 'Product catalogue';

CREATE TABLE sales.orders (
    id integer PRIMARY KEY,
    customer_id integer NOT NULL REFERENCES sales.customers (id),
    order_date date NOT NULL,
    status text NOT NULL,
    channel text NOT NULL
);
COMMENT ON TABLE sales.orders IS 'One row per order; revenue is in order_items';
COMMENT ON COLUMN sales.orders.status IS 'paid, refunded or cancelled; only paid orders count as revenue';

CREATE TABLE sales.order_items (
    order_id integer NOT NULL REFERENCES sales.orders (id),
    line_no integer NOT NULL,
    product_id integer NOT NULL REFERENCES sales.products (id),
    quantity integer NOT NULL,
    unit_price numeric(10, 2) NOT NULL,
    discount numeric(4, 2) NOT NULL,
    PRIMARY KEY (order_id, line_no)
);
COMMENT ON TABLE sales.order_items IS 'Order lines; line revenue = quantity * unit_price * (1 - discount)';

COPY sales.customers FROM '/demo/customers.csv' WITH (FORMAT csv, HEADER true);
COPY sales.products FROM '/demo/products.csv' WITH (FORMAT csv, HEADER true);
COPY sales.orders FROM '/demo/orders.csv' WITH (FORMAT csv, HEADER true);
COPY sales.order_items FROM '/demo/order_items.csv' WITH (FORMAT csv, HEADER true);

ANALYZE;
