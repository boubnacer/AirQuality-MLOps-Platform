-- Create databases
SELECT 'CREATE DATABASE airflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
SELECT 'CREATE DATABASE mlflow' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'mlflow')\gexec
SELECT 'CREATE DATABASE metabase' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec

-- Grant privileges so airquality user can use all databases
GRANT ALL PRIVILEGES ON DATABASE airflow TO airquality;
GRANT ALL PRIVILEGES ON DATABASE mlflow TO airquality;
GRANT ALL PRIVILEGES ON DATABASE metabase TO airquality;

\c airquality

-- Sensors reference table
CREATE TABLE IF NOT EXISTS sensors (
    sensor_id VARCHAR(20) PRIMARY KEY,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    city VARCHAR(100) NOT NULL,
    zone VARCHAR(50) NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Raw measurements
CREATE TABLE IF NOT EXISTS measurements (
    id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(20) NOT NULL REFERENCES sensors(sensor_id),
    timestamp TIMESTAMPTZ NOT NULL,
    pm2_5 DOUBLE PRECISION,
    pm10 DOUBLE PRECISION,
    no2 DOUBLE PRECISION,
    o3 DOUBLE PRECISION,
    co DOUBLE PRECISION,
    so2 DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    battery_level INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_measurements_sensor_ts ON measurements(sensor_id, timestamp);
CREATE INDEX IF NOT EXISTS idx_measurements_ts ON measurements(timestamp);

-- Aggregates (1-min and 5-min rolling windows)
CREATE TABLE IF NOT EXISTS aggregates (
    id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(20) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    window_type VARCHAR(10) NOT NULL,  -- '1min' or '5min'
    pm2_5_mean DOUBLE PRECISION,
    pm2_5_std DOUBLE PRECISION,
    pm2_5_min DOUBLE PRECISION,
    pm2_5_max DOUBLE PRECISION,
    pm10_mean DOUBLE PRECISION,
    pm10_std DOUBLE PRECISION,
    no2_mean DOUBLE PRECISION,
    no2_std DOUBLE PRECISION,
    o3_mean DOUBLE PRECISION,
    co_mean DOUBLE PRECISION,
    so2_mean DOUBLE PRECISION,
    record_count INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_aggregates_sensor_window ON aggregates(sensor_id, window_start);

-- Anomalies
CREATE TABLE IF NOT EXISTS anomalies (
    id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(20) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    anomaly_type VARCHAR(20) NOT NULL,  -- 'rule_based' or 'ml_based'
    severity VARCHAR(10) NOT NULL,       -- 'warning' or 'critical'
    pollutant VARCHAR(20),
    value DOUBLE PRECISION,
    threshold DOUBLE PRECISION,
    ml_probability DOUBLE PRECISION,
    raw_payload JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_anomalies_sensor_ts ON anomalies(sensor_id, timestamp);

-- ETL heartbeat (for StreamingDoneSensor)
CREATE TABLE IF NOT EXISTS etl_heartbeat (
    id BIGSERIAL PRIMARY KEY,
    pipeline_date DATE NOT NULL,
    last_batch_ts TIMESTAMPTZ NOT NULL,
    records_processed INTEGER DEFAULT 0,
    status VARCHAR(20) DEFAULT 'running',
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_heartbeat_date ON etl_heartbeat(pipeline_date);

-- Daily summaries (for dashboards)
CREATE TABLE IF NOT EXISTS daily_summaries (
    id BIGSERIAL PRIMARY KEY,
    sensor_id VARCHAR(20) NOT NULL,
    summary_date DATE NOT NULL,
    pm2_5_avg DOUBLE PRECISION,
    pm2_5_max DOUBLE PRECISION,
    pm10_avg DOUBLE PRECISION,
    no2_avg DOUBLE PRECISION,
    o3_avg DOUBLE PRECISION,
    co_avg DOUBLE PRECISION,
    so2_avg DOUBLE PRECISION,
    anomaly_count INTEGER DEFAULT 0,
    record_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(sensor_id, summary_date)
);

-- Catalog schema
CREATE SCHEMA IF NOT EXISTS catalog;

CREATE TABLE IF NOT EXISTS catalog.data_sources (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL UNIQUE,
    source_type VARCHAR(50) NOT NULL,
    connection_string TEXT,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS catalog.tables (
    id SERIAL PRIMARY KEY,
    source_id INTEGER REFERENCES catalog.data_sources(id),
    table_name VARCHAR(100) NOT NULL,
    schema_name VARCHAR(50) DEFAULT 'public',
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS catalog.columns (
    id SERIAL PRIMARY KEY,
    table_id INTEGER REFERENCES catalog.tables(id),
    column_name VARCHAR(100) NOT NULL,
    data_type VARCHAR(50),
    description TEXT,
    is_nullable BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS catalog.lineage_edges (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(200) NOT NULL,
    target_name VARCHAR(200) NOT NULL,
    transformation VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS catalog.quality_metrics (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(100) NOT NULL,
    pipeline_date DATE NOT NULL,
    row_count BIGINT,
    null_rate DOUBLE PRECISION,
    freshness_minutes INTEGER,
    schema_drift BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(table_name, pipeline_date)
);