-- Real-Time Air Quality Monitor Dashboard Views

-- Current sensor status with latest readings
CREATE OR REPLACE VIEW v_sensor_latest AS
SELECT DISTINCT ON (m.sensor_id)
    m.sensor_id,
    s.lat,
    s.lon,
    s.city,
    s.zone,
    m.timestamp,
    m.pm2_5,
    m.pm10,
    m.no2,
    m.o3,
    m.co,
    m.so2,
    m.temperature,
    m.humidity,
    CASE
        WHEN m.pm2_5 > 75 OR m.pm10 > 150 OR m.no2 > 200 THEN 'critical'
        WHEN m.pm2_5 > 25 OR m.pm10 > 50 OR m.no2 > 100 THEN 'warning'
        ELSE 'good'
    END AS aqi_status,
    CASE
        WHEN m.pm2_5 > 75 THEN 5
        WHEN m.pm2_5 > 55 THEN 4
        WHEN m.pm2_5 > 35 THEN 3
        WHEN m.pm2_5 > 12 THEN 2
        ELSE 1
    END AS aqi_level
FROM measurements m
JOIN sensors s ON m.sensor_id = s.sensor_id
ORDER BY m.sensor_id, m.timestamp DESC;

-- Last 24h time series per pollutant (5-min buckets)
CREATE OR REPLACE VIEW v_pollutant_timeseries_24h AS
SELECT
    date_trunc('hour', timestamp) + INTERVAL '5 min' * FLOOR(date_part('minute', timestamp) / 5) AS bucket,
    AVG(pm2_5) AS avg_pm2_5,
    AVG(pm10) AS avg_pm10,
    AVG(no2) AS avg_no2,
    AVG(o3) AS avg_o3,
    AVG(co) AS avg_co,
    AVG(so2) AS avg_so2,
    COUNT(*) AS record_count
FROM measurements
WHERE timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY 1
ORDER BY 1;

-- Per-sensor pollutant timeseries (last 24h)
CREATE OR REPLACE VIEW v_sensor_pollutant_24h AS
SELECT
    sensor_id,
    date_trunc('hour', timestamp) + INTERVAL '5 min' * FLOOR(date_part('minute', timestamp) / 5) AS bucket,
    AVG(pm2_5) AS avg_pm2_5,
    AVG(no2) AS avg_no2,
    AVG(o3) AS avg_o3
FROM measurements
WHERE timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY 1, 2
ORDER BY 1, 2;

-- Anomaly alert feed (last 48h)
CREATE OR REPLACE VIEW v_anomaly_feed AS
SELECT
    a.id,
    a.sensor_id,
    s.city,
    s.zone,
    a.timestamp,
    a.anomaly_type,
    a.severity,
    a.pollutant,
    a.value,
    a.threshold,
    ROUND((a.value / NULLIF(a.threshold, 0) * 100)::numeric, 1) AS pct_over_threshold
FROM anomalies a
JOIN sensors s ON a.sensor_id = s.sensor_id
WHERE a.timestamp >= NOW() - INTERVAL '48 hours'
ORDER BY a.timestamp DESC;

-- Anomaly count per city (last 24h)
CREATE OR REPLACE VIEW v_anomaly_by_city_24h AS
SELECT
    s.city,
    a.severity,
    COUNT(*) AS anomaly_count
FROM anomalies a
JOIN sensors s ON a.sensor_id = s.sensor_id
WHERE a.timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY s.city, a.severity
ORDER BY anomaly_count DESC;

-- Hourly average by zone type (last 7 days)
CREATE OR REPLACE VIEW v_zone_hourly_avg AS
SELECT
    s.zone,
    date_trunc('hour', m.timestamp) AS hour,
    AVG(m.pm2_5) AS avg_pm2_5,
    AVG(m.no2) AS avg_no2,
    COUNT(*) AS readings
FROM measurements m
JOIN sensors s ON m.sensor_id = s.sensor_id
WHERE m.timestamp >= NOW() - INTERVAL '7 days'
GROUP BY s.zone, date_trunc('hour', m.timestamp)
ORDER BY s.zone, hour;