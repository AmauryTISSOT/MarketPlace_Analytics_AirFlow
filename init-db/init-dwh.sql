-- =============================================================================
-- Init DWH PostgreSQL - Formation Airflow IPSSI
-- Schemas : staging, dwh (star schema Kimball), analytics
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS analytics;

-- =============================================================================
-- STAGING
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.orders (
    order_id      VARCHAR(30) NOT NULL PRIMARY KEY,
    seller_id     VARCHAR(30) NOT NULL,
    customer_id   VARCHAR(30) NOT NULL,
    product_id    VARCHAR(30) NOT NULL,
    dt            DATE NOT NULL,
    quantity      INTEGER NOT NULL,
    total_amount  DECIMAL(10,2) NOT NULL,
    status        VARCHAR(20) NOT NULL,
    loaded_at     TIMESTAMP DEFAULT NOW()
    );

CREATE TABLE IF NOT EXISTS staging.sellers (
    seller_id   VARCHAR(30) NOT NULL,
    name  VARCHAR(100)    NOT NULL,
    country VARCHAR(100),
    joined_date DATE,
    loaded_at   TIMESTAMP DEFAULT NOW()
    );

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id  VARCHAR(30)    NOT NULL,
    email  VARCHAR(200),
    city VARCHAR(100),
    signup_date  DATE,
    loaded_at    TIMESTAMP  DEFAULT NOW()
    );

CREATE TABLE IF NOT EXISTS staging.products (
    product_id  VARCHAR(30) NOT NULL,
    name  VARCHAR(100)    NOT NULL,
    category    VARCHAR(50),
    base_price  DECIMAL(10,2),
    loaded_at   TIMESTAMP DEFAULT NOW()
    );

-- =============================================================================
-- DWH - dimensions et table de faits
-- =============================================================================

CREATE TABLE IF NOT EXISTS dwh.dim_seller (
    seller_id   VARCHAR(30) PRIMARY KEY,
    name  VARCHAR(100)    NOT NULL,
    country VARCHAR(100),
    joined_date DATE
    );

CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_id VARCHAR(30) PRIMARY KEY,
    email VARCHAR(200),
    city  VARCHAR(100),
    signup_date DATE
    );

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_id  VARCHAR(30) PRIMARY KEY,
    name  VARCHAR(100)    NOT NULL,
    category    VARCHAR(50),
    base_price  DECIMAL(10,2)
    );

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    dt  DATE    PRIMARY KEY,
    year  INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_id    VARCHAR(30) PRIMARY KEY,
    seller_id   VARCHAR(30) NOT NULL REFERENCES dwh.dim_seller(seller_id),
    customer_id VARCHAR(30) NOT NULL REFERENCES dwh.dim_customer(customer_id),
    product_id  VARCHAR(30) NOT NULL REFERENCES dwh.dim_product(product_id),
    dt  DATE    NOT NULL REFERENCES dwh.dim_date(dt),
    quantity    INTEGER NOT NULL,
    total_amount DECIMAL(10,2)  NOT NULL,
    status  VARCHAR(20) NOT NULL,
    loaded_at   TIMESTAMP DEFAULT NOW()
    );

CREATE INDEX IF NOT EXISTS idx_fact_orders_dt  ON dwh.fact_orders(dt);
CREATE INDEX IF NOT EXISTS idx_fact_orders_seller_id   ON dwh.fact_orders(seller_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer_id ON dwh.fact_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product_id  ON dwh.fact_orders(product_id);

-- =============================================================================
-- ANALYTICS
-- =============================================================================

CREATE TABLE IF NOT EXISTS  analytics.daily_metrics (
    dt DATE PRIMARY KEY,
    orders_count INT,
    total_quantity INT,
    gmv NUMERIC,
    avg_order_value NUMERIC
);

CREATE TABLE IF NOT EXISTS analytics.seller_metrics (
    dt              DATE NOT NULL,
    seller_id       VARCHAR(30) NOT NULL,
    seller_name     TEXT,
    orders_count    INTEGER,
    revenue   DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    loaded_at       TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (dt, seller_id)
    );

CREATE TABLE IF NOT EXISTS analytics.category_metrics (
    dt              DATE NOT NULL,
    category        VARCHAR(50) NOT NULL,
    orders_count    INTEGER,
    revenue         DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    loaded_at       TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (dt, category)
    );


CREATE TABLE IF NOT EXISTS analytics.customer_metrics (
    dt DATE PRIMARY KEY,
    active_customers INT
);