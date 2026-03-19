-- DataOps Metrics Dashboard Views

-- ETL pipeline health: daily records processed and heartbeat status
CREATE OR REPLACE VIEW v_etl_pipeline_health AS
SELECT
    pipeline_date,
    last_batch_ts,
    records_processed,
    status,
    EXTRACT(EPOCH FROM (NOW() - last_batch_ts)) / 60 AS minutes_since_last_batch
FROM etl_heartbeat
ORDER BY pipeline_date DESC
LIMIT 30;

-- ETL throughput: records per hour over last 7 days
CREATE OR REPLACE VIEW v_etl_throughput_hourly AS
SELECT
    date_trunc('hour', created_at) AS hour,
    COUNT(*) AS records_inserted,
    COUNT(*) / 3600.0 AS records_per_second
FROM measurements
WHERE created_at >= NOW() - INTERVAL '7 days'
GROUP BY 1
ORDER BY 1 DESC;

-- Daily data volume trend (last 30 days)
CREATE OR REPLACE VIEW v_daily_volume AS
SELECT
    created_at::date AS report_date,
    COUNT(*) AS total_records,
    COUNT(DISTINCT sensor_id) AS active_sensors,
    AVG(pm2_5) AS avg_pm25,
    MAX(pm2_5) AS max_pm25
FROM measurements
WHERE created_at >= NOW() - INTERVAL '30 days'
GROUP BY 1
ORDER BY 1 DESC;

-- Sensor activity: how many sensors are reporting in the last hour
CREATE OR REPLACE VIEW v_sensor_activity AS
SELECT
    s.sensor_id,
    s.city,
    s.zone,
    s.status,
    COUNT(m.id) AS readings_last_hour,
    MAX(m.timestamp) AS last_seen,
    CASE
        WHEN MAX(m.timestamp) >= NOW() - INTERVAL '5 minutes' THEN 'active'
        WHEN MAX(m.timestamp) >= NOW() - INTERVAL '1 hour' THEN 'delayed'
        ELSE 'offline'
    END AS reporting_status
FROM sensors s
LEFT JOIN measurements m ON s.sensor_id = m.sensor_id AND m.timestamp >= NOW() - INTERVAL '1 hour'
GROUP BY s.sensor_id, s.city, s.zone, s.status
ORDER BY readings_last_hour DESC;

-- Data quality metrics over time (null rates, schema drift)
CREATE OR REPLACE VIEW v_data_quality_trend AS
SELECT
    table_name,
    pipeline_date,
    row_count,
    ROUND((null_rate * 100)::numeric, 2) AS null_rate_pct,
    freshness_minutes,
    schema_drift,
    LAG(row_count) OVER (PARTITION BY table_name ORDER BY pipeline_date) AS prev_row_count,
    row_count - LAG(row_count) OVER (PARTITION BY table_name ORDER BY pipeline_date) AS row_count_delta
FROM catalog.quality_metrics
WHERE pipeline_date >= CURRENT_DATE - 30
ORDER BY table_name, pipeline_date DESC;

-- Anomaly detection rate: what % of sensor readings result in anomaly flags
CREATE OR REPLACE VIEW v_anomaly_rate_daily AS
SELECT
    m_counts.report_date,
    m_counts.total_measurements,
    COALESCE(a_counts.total_anomalies, 0) AS total_anomalies,
    ROUND(
        (COALESCE(a_counts.total_anomalies, 0)::float / NULLIF(m_counts.total_measurements, 0) * 100)::numeric,
        3
    ) AS anomaly_rate_pct
FROM (
    SELECT timestamp::date AS report_date, COUNT(*) AS total_measurements
    FROM measurements
    WHERE timestamp >= NOW() - INTERVAL '30 days'
    GROUP BY 1
) m_counts
LEFT JOIN (
    SELECT timestamp::date AS report_date, COUNT(*) AS total_anomalies
    FROM anomalies
    WHERE timestamp >= NOW() - INTERVAL '30 days'
    GROUP BY 1
) a_counts ON m_counts.report_date = a_counts.report_date
ORDER BY 1 DESC;

-- Kafka consumer lag proxy: gap between latest raw message and latest processed record
CREATE OR REPLACE VIEW v_kafka_lag_proxy AS
SELECT
    NOW() - MAX(timestamp) AS estimated_consumer_lag,
    MAX(timestamp) AS latest_processed_ts,
    COUNT(*) FILTER (WHERE created_at >= NOW() - INTERVAL '1 minute') AS records_last_minute
FROM measurements;

-- Per-city daily summary for DataOps monitoring
CREATE OR REPLACE VIEW v_city_daily_summary AS
SELECT
    s.city,
    ds.summary_date,
    COUNT(ds.sensor_id) AS reporting_sensors,
    AVG(ds.pm2_5_avg) AS city_avg_pm25,
    MAX(ds.pm2_5_max) AS city_max_pm25,
    SUM(ds.anomaly_count) AS total_anomalies,
    SUM(ds.record_count) AS total_records
FROM daily_summaries ds
JOIN sensors s ON ds.sensor_id = s.sensor_id
WHERE ds.summary_date >= CURRENT_DATE - 30
GROUP BY s.city, ds.summary_date
ORDER BY ds.summary_date DESC, s.city;