-- ============================================
-- ETL Target Database Schema
-- Created: 2026-04-16
-- ============================================

-- Create target database user and database
-- (docker-compose handles airflow DB, this creates etl_target)
SELECT 'CREATE DATABASE etl_target'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'etl_target')\gexec

-- Connect to target DB
\c etl_target;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS production;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- ============================================
-- STAGING TABLES (raw extracted data)
-- ============================================

-- Exchange rates raw data
CREATE TABLE IF NOT EXISTS staging.exchange_rates (
    id SERIAL PRIMARY KEY,
    base_currency VARCHAR(3) NOT NULL,
    target_currency VARCHAR(3) NOT NULL,
    rate NUMERIC(20, 8) NOT NULL,
    extraction_timestamp TIMESTAMPTZ DEFAULT NOW(),
    source VARCHAR(50) DEFAULT 'open.er-api.com',
    raw_response JSONB
);

-- World Bank indicators raw data
CREATE TABLE IF NOT EXISTS staging.worldbank_indicators (
    id SERIAL PRIMARY KEY,
    country_code VARCHAR(3) NOT NULL,
    country_name VARCHAR(100),
    indicator_code VARCHAR(50) NOT NULL,
    indicator_name VARCHAR(255),
    year INTEGER,
    value NUMERIC(20, 4),
    extraction_timestamp TIMESTAMPTZ DEFAULT NOW(),
    source VARCHAR(50) DEFAULT 'worldbank.org'
);

-- JSONPlaceholder users staging
CREATE TABLE IF NOT EXISTS staging.api_users (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    username VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    company_name VARCHAR(255),
    extraction_timestamp TIMESTAMPTZ DEFAULT NOW(),
    raw_response JSONB
);

-- CSV file ingestion staging
CREATE TABLE IF NOT EXISTS staging.csv_ingestions (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(255) NOT NULL,
    row_data JSONB NOT NULL,
    ingestion_timestamp TIMESTAMPTZ DEFAULT NOW(),
    row_number INTEGER,
    source VARCHAR(100)
);

-- ============================================
-- PRODUCTION TABLES (cleaned, transformed data)
-- ============================================

-- Exchange rates production (deduplicated)
CREATE TABLE IF NOT EXISTS production.exchange_rates (
    id SERIAL PRIMARY KEY,
    base_currency VARCHAR(3) NOT NULL,
    target_currency VARCHAR(3) NOT NULL,
    rate NUMERIC(20, 8) NOT NULL,
    effective_date DATE NOT NULL,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(base_currency, target_currency, effective_date)
);

-- World Bank indicators production (pivoted)
CREATE TABLE IF NOT EXISTS production.country_indicators (
    id SERIAL PRIMARY KEY,
    country_code VARCHAR(3) NOT NULL,
    country_name VARCHAR(100),
    year INTEGER NOT NULL,
    gdp_usd NUMERIC(20, 2),
    population_total BIGINT,
    inflation_rate NUMERIC(10, 4),
    unemployment_rate NUMERIC(10, 4),
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(country_code, year)
);

-- Derived metrics
CREATE TABLE IF NOT EXISTS production.derived_metrics (
    id SERIAL PRIMARY KEY,
    metric_name VARCHAR(100) NOT NULL,
    metric_value NUMERIC(20, 4),
    calculated_at TIMESTAMPTZ DEFAULT NOW(),
    source_tables TEXT[]
);

-- ============================================
-- MONITORING TABLES (pipeline health)
-- ============================================

-- Pipeline run logs
CREATE TABLE IF NOT EXISTS monitoring.pipeline_runs (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(250) NOT NULL,
    run_id VARCHAR(250),
    state VARCHAR(50),
    start_time TIMESTAMPTZ,
    end_time TIMESTAMPTZ,
    duration_seconds NUMERIC(10, 2),
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);

-- Table row counts over time
CREATE TABLE IF NOT EXISTS monitoring.table_stats (
    id SERIAL PRIMARY KEY,
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    row_count BIGINT,
    table_size_bytes BIGINT,
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);

-- Data quality checks
CREATE TABLE IF NOT EXISTS monitoring.quality_checks (
    id SERIAL PRIMARY KEY,
    check_name VARCHAR(255) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL, -- PASS, FAIL, WARN
    message TEXT,
    checked_at TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================
-- INDEXES
-- ============================================

CREATE INDEX IF NOT EXISTS idx_staging_er_timestamp
    ON staging.exchange_rates(extraction_timestamp);
CREATE INDEX IF NOT EXISTS idx_staging_wb_country
    ON staging.worldbank_indicators(country_code);
CREATE INDEX IF NOT EXISTS idx_production_er_date
    ON production.exchange_rates(effective_date);
CREATE INDEX IF NOT EXISTS idx_production_ci_country_year
    ON production.country_indicators(country_code, year);
CREATE INDEX IF NOT EXISTS idx_monitoring_runs_dag
    ON monitoring.pipeline_runs(dag_id, start_time);

-- ============================================
-- GRANTS (for airflow user)
-- ============================================

GRANT USAGE ON SCHEMA staging, production, monitoring TO target_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO target_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA production TO target_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA monitoring TO target_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA staging TO target_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA production TO target_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA monitoring TO target_user;
