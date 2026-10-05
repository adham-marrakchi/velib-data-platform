-- =============================================================
-- DATA WAREHOUSE SCHEMA - Star Schema for Orders Analytics
-- =============================================================
-- Run against PostgreSQL (processed_db)

-- Dimension: Customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id INT UNIQUE NOT NULL,
    customer_name VARCHAR(100),
    customer_segment VARCHAR(50) DEFAULT 'Standard',
    join_date TIMESTAMP,
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    effective_to TIMESTAMP DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE
);

-- Dimension: Date
CREATE TABLE IF NOT EXISTS dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    day_of_week INT,
    day_name VARCHAR(10),
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month_num INT,
    month_name VARCHAR(10),
    quarter INT,
    year INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN DEFAULT FALSE
);

-- Dimension: Products (extensible)
CREATE TABLE IF NOT EXISTS dim_products (
    product_key SERIAL PRIMARY KEY,
    product_id INT UNIQUE,
    product_name VARCHAR(200),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    unit_price DECIMAL(10,2),
    effective_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current BOOLEAN DEFAULT TRUE
);

-- Dimension: IoT Devices
CREATE TABLE IF NOT EXISTS dim_devices (
    device_key SERIAL PRIMARY KEY,
    device_id INT UNIQUE NOT NULL,
    device_name VARCHAR(100),
    device_type VARCHAR(50) DEFAULT 'sensor',
    location VARCHAR(200),
    install_date TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Fact: Orders
CREATE TABLE IF NOT EXISTS fact_orders (
    order_key BIGSERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    customer_key INT REFERENCES dim_customers(customer_key),
    date_key INT REFERENCES dim_date(date_key),
    order_amount DECIMAL(10,2) NOT NULL,
    quantity INT DEFAULT 1,
    discount_amount DECIMAL(10,2) DEFAULT 0,
    net_amount DECIMAL(10,2),
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_system VARCHAR(50) DEFAULT 'mysql'
);

-- Fact: Sensor Readings (for IoT streaming data)
CREATE TABLE IF NOT EXISTS fact_sensor_readings (
    reading_key BIGSERIAL PRIMARY KEY,
    device_key INT REFERENCES dim_devices(device_key),
    date_key INT REFERENCES dim_date(date_key),
    event_id VARCHAR(50),
    reading_value DOUBLE PRECISION NOT NULL,
    is_anomaly BOOLEAN DEFAULT FALSE,
    anomaly_score DOUBLE PRECISION,
    reading_timestamp TIMESTAMP NOT NULL,
    processed_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Pipeline Runs (operational metadata)
CREATE TABLE IF NOT EXISTS fact_pipeline_runs (
    run_key BIGSERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    run_id VARCHAR(200),
    run_type VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds INT,
    status VARCHAR(20),
    records_processed INT DEFAULT 0,
    records_failed INT DEFAULT 0,
    error_message TEXT
);

-- Aggregation: Daily Order Summary (materialized view pattern)
CREATE TABLE IF NOT EXISTS agg_daily_orders (
    date_key INT PRIMARY KEY REFERENCES dim_date(date_key),
    total_orders INT,
    total_revenue DECIMAL(12,2),
    avg_order_value DECIMAL(10,2),
    unique_customers INT,
    max_order_value DECIMAL(10,2),
    min_order_value DECIMAL(10,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Aggregation: Hourly Sensor Summary
CREATE TABLE IF NOT EXISTS agg_hourly_sensors (
    id BIGSERIAL PRIMARY KEY,
    device_key INT REFERENCES dim_devices(device_key),
    hour_start TIMESTAMP NOT NULL,
    reading_count INT,
    avg_reading DOUBLE PRECISION,
    min_reading DOUBLE PRECISION,
    max_reading DOUBLE PRECISION,
    anomaly_count INT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(device_key, hour_start)
);

-- Populate dim_date for 2024-2026
INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, day_of_month, day_of_year, week_of_year, month_num, month_name, quarter, year, is_weekend)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
    d AS full_date,
    EXTRACT(DOW FROM d)::INT AS day_of_week,
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(DAY FROM d)::INT AS day_of_month,
    EXTRACT(DOY FROM d)::INT AS day_of_year,
    EXTRACT(WEEK FROM d)::INT AS week_of_year,
    EXTRACT(MONTH FROM d)::INT AS month_num,
    TO_CHAR(d, 'Month') AS month_name,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(DOW FROM d) IN (0, 6) AS is_weekend
FROM generate_series('2024-01-01'::date, '2026-12-31'::date, '1 day'::interval) AS d
ON CONFLICT (date_key) DO NOTHING;

-- Create indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON fact_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON fact_orders(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_amount ON fact_orders(order_amount);
CREATE INDEX IF NOT EXISTS idx_fact_sensors_device ON fact_sensor_readings(device_key);
CREATE INDEX IF NOT EXISTS idx_fact_sensors_date ON fact_sensor_readings(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sensors_anomaly ON fact_sensor_readings(is_anomaly) WHERE is_anomaly = TRUE;
CREATE INDEX IF NOT EXISTS idx_fact_pipeline_dag ON fact_pipeline_runs(dag_id, start_time);
CREATE INDEX IF NOT EXISTS idx_agg_hourly_device ON agg_hourly_sensors(device_key, hour_start);
