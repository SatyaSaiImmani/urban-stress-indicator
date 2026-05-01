CREATE OR REPLACE VIEW v_composite_scores AS
SELECT
    city,
    score_date,
    composite,
    composite_baseline_30d,
    composite - composite_baseline_30d AS delta,
    readiness_mode,
    anomaly_score,
    heat_score,
    alert_score,
    context_score
FROM signal_scores
ORDER BY city, score_date DESC;