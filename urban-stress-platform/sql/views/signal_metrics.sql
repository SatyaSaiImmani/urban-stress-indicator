CREATE OR REPLACE VIEW v_signal_metrics_latest AS
SELECT
    city,
    metric_date,
    metric_name,
    value_num,
    value_text,
    delta_pct,
    baseline
FROM signal_metrics
WHERE (city, metric_date) IN (
    SELECT city, MAX(metric_date)
    FROM signal_metrics
    GROUP BY city
);