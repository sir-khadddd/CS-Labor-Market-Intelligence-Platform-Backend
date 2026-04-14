CREATE OR REPLACE TABLE marts.trajectory_labels AS
SELECT
    tf.entity_type,
    tf.entity_id,
    tf.month,
    CASE
        WHEN tf.acceleration >= 5
            AND tf.posting_count < 200
            AND tf.yoy_growth >= 10 THEN 'emerging'
        WHEN tf.yoy_growth >= 5
            AND COALESCE(tf.volatility_12m, 0) < 15 THEN 'stable_growth'
        WHEN tf.yoy_growth BETWEEN -2 AND 5
            AND tf.acceleration < 0 THEN 'plateau'
        WHEN tf.yoy_growth < -2 THEN 'declining'
        ELSE 'uncertain'
    END AS trajectory_class,
    tf.momentum_score AS trajectory_score,
    LEAST(1.0, GREATEST(0.0, 1.0 - COALESCE(tf.volatility_12m, 0) / 100.0)) AS confidence,
    'phase1_rules'::VARCHAR AS method,
    rc.label_version,
    rc.method_version,
    rc.run_timestamp
FROM marts.trajectory_features tf
CROSS JOIN stage.run_context rc;
