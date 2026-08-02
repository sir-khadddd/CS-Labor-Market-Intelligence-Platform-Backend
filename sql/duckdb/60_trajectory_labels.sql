-- v3 makes the target forward-looking to remove circularity.
-- v2 derived trajectory_class from a CASE over the *same* month's feature columns, which
-- are also model inputs, so the classifier only had to re-learn the rule. In v3 the label
-- attached to month t describes the entity's state at t + LABEL_HORIZON_MONTHS (3) plus the
-- realized posting growth from t to t + 3. Model inputs stay point-in-time at t, so the
-- classifier must forecast rather than re-derive its own inputs.
--
-- Horizon is fixed at 3 months and hardcoded here; keep it in sync with
-- LABEL_HORIZON_MONTHS in ml/constants.py.
--
-- Entity-months whose t + 3 observation is missing (panel tail, or a gap in the monthly
-- grid) get no label row. That is intentional: the outcome has not happened yet.
CREATE OR REPLACE TABLE marts.trajectory_labels AS
WITH forward AS (
    SELECT
        tf.entity_type,
        tf.entity_id,
        tf.month,
        fwd.yoy_growth AS fwd_yoy_growth,
        fwd.acceleration AS fwd_acceleration,
        fwd.volatility_12m AS fwd_volatility_12m,
        fwd.posting_count AS fwd_posting_count,
        fwd.momentum_score AS fwd_momentum_score,
        100.0 * (fwd.posting_count - tf.posting_count)
            / NULLIF(tf.posting_count, 0) AS fwd_3m_growth
    FROM marts.trajectory_features tf
    JOIN marts.trajectory_features fwd
        ON fwd.entity_type = tf.entity_type
        AND fwd.entity_id = tf.entity_id
        AND fwd.month = CAST(tf.month + INTERVAL 3 MONTH AS DATE)
)
SELECT
    f.entity_type,
    f.entity_id,
    f.month,
    CASE
        WHEN (f.fwd_acceleration >= 3
                AND f.fwd_posting_count < 500
                AND f.fwd_yoy_growth >= 8)
            OR (f.fwd_yoy_growth >= 12 AND f.fwd_acceleration >= 0)
            OR (f.fwd_3m_growth >= 15 AND f.fwd_yoy_growth >= 8) THEN 'emerging'
        WHEN f.fwd_yoy_growth >= 3
            AND COALESCE(f.fwd_volatility_12m, 0) < 25 THEN 'stable_growth'
        WHEN f.fwd_yoy_growth BETWEEN -3 AND 3
            AND f.fwd_acceleration <= 0 THEN 'plateau'
        WHEN f.fwd_yoy_growth < -3
            OR f.fwd_3m_growth <= -20 THEN 'declining'
        ELSE 'uncertain'
    END AS trajectory_class,
    f.fwd_momentum_score AS trajectory_score,
    LEAST(1.0, GREATEST(0.0, 1.0 - COALESCE(f.fwd_volatility_12m, 0) / 100.0)) AS confidence,
    'phase1_rules'::VARCHAR AS method,
    rc.label_version,
    rc.method_version,
    rc.run_timestamp
FROM forward f
CROSS JOIN stage.run_context rc;
