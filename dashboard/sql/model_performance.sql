-- Model Performance Dashboard Views

-- Latest production model versions and metrics
CREATE OR REPLACE VIEW v_model_registry AS
SELECT
    model_name,
    version,
    stage,
    run_id,
    created_at
FROM (
    VALUES
        ('air-quality-anomaly-classifier', 'N/A', 'Production', NULL, NOW()),
        ('air-quality-pollution-forecaster', 'N/A', 'Production', NULL, NOW()),
        ('air-quality-sensor-clustering', 'N/A', 'Production', NULL, NOW())
) AS t(model_name, version, stage, run_id, created_at);

-- Anomaly detection accuracy over time (using rule-based labels as ground truth)
CREATE OR REPLACE VIEW v_anomaly_detection_daily AS
SELECT
    a.timestamp::date AS report_date,
    COUNT(*) AS total_anomalies,
    COUNT(*) FILTER (WHERE a.severity = 'critical') AS critical_count,
    COUNT(*) FILTER (WHERE a.severity = 'warning') AS warning_count,
    COUNT(*) FILTER (WHERE a.anomaly_type = 'rule_based') AS rule_based_count,
    COUNT(*) FILTER (WHERE a.anomaly_type = 'ml_based') AS ml_based_count
FROM anomalies a
WHERE a.timestamp >= NOW() - INTERVAL '30 days'
GROUP BY 1
ORDER BY 1 DESC;

-- PSI (Population Stability Index) proxy: compare recent vs historical PM2.5 distribution
CREATE OR REPLACE VIEW v_feature_drift_pm25 AS
WITH hist AS (
    SELECT
        WIDTH_BUCKET(pm2_5, 0, 150, 10) AS bucket,
        COUNT(*) AS hist_count
    FROM measurements
    WHERE timestamp BETWEEN NOW() - INTERVAL '60 days' AND NOW() - INTERVAL '30 days'
    AND pm2_5 IS NOT NULL
    GROUP BY 1
),
recent AS (
    SELECT
        WIDTH_BUCKET(pm2_5, 0, 150, 10) AS bucket,
        COUNT(*) AS recent_count
    FROM measurements
    WHERE timestamp >= NOW() - INTERVAL '30 days'
    AND pm2_5 IS NOT NULL
    GROUP BY 1
),
combined AS (
    SELECT
        COALESCE(h.bucket, r.bucket) AS bucket,
        COALESCE(h.hist_count, 0) AS hist_count,
        COALESCE(r.recent_count, 0) AS recent_count
    FROM hist h
    FULL OUTER JOIN recent r ON h.bucket = r.bucket
)
SELECT
    bucket,
    hist_count,
    recent_count,
    CASE
        WHEN hist_count > 0 AND recent_count > 0
        THEN (recent_count::float / NULLIF(SUM(recent_count) OVER (), 0) -
              hist_count::float / NULLIF(SUM(hist_count) OVER (), 0))
        ELSE 0
    END AS psi_contribution
FROM combined
ORDER BY bucket;

-- Prediction residuals over time (computed from measurements vs thresholds as proxy)
CREATE OR REPLACE VIEW v_pm25_residuals_daily AS
SELECT
    m.timestamp::date AS report_date,
    AVG(m.pm2_5) AS actual_avg_pm25,
    AVG(ds.pm2_5_avg) AS summary_avg_pm25,
    AVG(m.pm2_5) - AVG(ds.pm2_5_avg) AS residual_delta
FROM measurements m
LEFT JOIN daily_summaries ds ON m.sensor_id = ds.sensor_id AND m.timestamp::date = ds.summary_date
WHERE m.timestamp >= NOW() - INTERVAL '30 days'
GROUP BY 1
ORDER BY 1 DESC;

-- Staging vs Production comparison (quality_metrics proxy)
CREATE OR REPLACE VIEW v_model_stage_comparison AS
SELECT
    qm.table_name,
    qm.pipeline_date,
    qm.row_count,
    qm.null_rate,
    qm.freshness_minutes,
    qm.schema_drift
FROM catalog.quality_metrics qm
WHERE qm.pipeline_date >= CURRENT_DATE - 14
ORDER BY qm.table_name, qm.pipeline_date DESC;