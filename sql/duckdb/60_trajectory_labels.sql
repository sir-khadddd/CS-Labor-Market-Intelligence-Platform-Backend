-- v2 relaxes phase1 rule thresholds to increase trajectory_class label diversity
CREATE OR REPLACE TABLE marts.trajectory_labels AS
SELECT
    tf.entity_type,
    tf.entity_id,
    tf.month,
    CASE
        WHEN (tf.acceleration >= 3
                AND tf.posting_count < 500
                AND tf.yoy_growth >= 8)
            OR (tf.yoy_growth >= 12 AND tf.acceleration >= 0) THEN 'emerging'
        WHEN tf.yoy_growth >= 3
            AND COALESCE(tf.volatility_12m, 0) < 25 THEN 'stable_growth'
        WHEN tf.yoy_growth BETWEEN -3 AND 3
            AND tf.acceleration <= 0 THEN 'plateau'
        WHEN tf.yoy_growth < -3 THEN 'declining'
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
