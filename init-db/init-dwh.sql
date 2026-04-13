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
    id VARCHAR(30) PRIMARY KEY,
    date DATE NOT NULL,
    seller_id VARCHAR(20) NOT NULL,
    customer_id VARCHAR(20) NOT NULL,
    product_id VARCHAR(20) NOT NULL,
    product VARCHAR(100) NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.customers (
    id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200),
    city VARCHAR(100),
    registered_date DATE,
    active BOOLEAN DEFAULT TRUE,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.products (
    id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10,2),
    stock INTEGER,
    active BOOLEAN DEFAULT TRUE,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS staging.metrics (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    hour INTEGER NOT NULL,
    metric_type VARCHAR(50) NOT NULL,
    value DECIMAL(12,2) NOT NULL,
    loaded_at TIMESTAMP DEFAULT NOW()
);

-- =============================================================================
-- DWH — Star Schema (Kimball)
-- =============================================================================

-- ----- Dimensions -----

CREATE TABLE IF NOT EXISTS dwh.dim_seller (
    seller_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    country VARCHAR(50),
    joined_date DATE
);

CREATE TABLE IF NOT EXISTS dwh.dim_customer (
    customer_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(200),
    city VARCHAR(100),
    registered_date DATE,
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_product (
    product_id VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    category VARCHAR(50),
    price DECIMAL(10,2),
    stock INTEGER,
    active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    dt DATE PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL
);

-- ----- Fact table -----

CREATE TABLE IF NOT EXISTS dwh.fact_orders (
    order_id VARCHAR(30) PRIMARY KEY,
    seller_id VARCHAR(20) NOT NULL REFERENCES dwh.dim_seller(seller_id),
    customer_id VARCHAR(20) NOT NULL REFERENCES dwh.dim_customer(customer_id),
    product_id VARCHAR(20) NOT NULL REFERENCES dwh.dim_product(product_id),
    dt DATE NOT NULL REFERENCES dwh.dim_date(dt),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fact_orders_dt ON dwh.fact_orders(dt);

-- =============================================================================
-- ANALYTICS — Tables d'agrégation pour Metabase
-- =============================================================================

CREATE TABLE IF NOT EXISTS analytics.daily_summary (
    dt DATE NOT NULL PRIMARY KEY,
    total_orders INTEGER,
    total_revenue DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    unique_customers INTEGER,
    top_product VARCHAR(100),
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS analytics.seller_daily (
    dt DATE NOT NULL,
    seller_id VARCHAR(20) NOT NULL,
    total_orders INTEGER,
    total_revenue DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    loaded_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (dt, seller_id)
);

CREATE TABLE IF NOT EXISTS analytics.category_daily (
    dt DATE NOT NULL,
    category VARCHAR(50) NOT NULL,
    total_orders INTEGER,
    total_revenue DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    loaded_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (dt, category)
);
