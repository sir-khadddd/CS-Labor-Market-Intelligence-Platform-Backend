CREATE OR REPLACE TABLE marts.trajectory_features AS
WITH role_monthly AS (
    SELECT
        role_id AS entity_id,
        month,
        SUM(posting_count) AS posting_count
    FROM marts.cs_job_demand
    GROUP BY 1, 2
),
ts AS (
    SELECT
        'role'::VARCHAR AS entity_type,
        entity_id,
        month,
        posting_count,
        100.0 * (posting_count - LAG(posting_count, 12) OVER w)
            / NULLIF(LAG(posting_count, 12) OVER w, 0) AS yoy_growth,
        100.0 * (posting_count - LAG(posting_count, 3) OVER w)
            / NULLIF(LAG(posting_count, 3) OVER w, 0) AS rolling_3m_growth
    FROM role_monthly
    WINDOW w AS (PARTITION BY entity_id ORDER BY month)
),
ts_with_accel AS (
    SELECT
        *,
        yoy_growth - LAG(yoy_growth, 1) OVER (
            PARTITION BY entity_id ORDER BY month
        ) AS acceleration
    FROM ts
),
windowed AS (
    SELECT
        entity_type,
        entity_id,
        month,
        posting_count,
        yoy_growth,
        rolling_3m_growth,
        acceleration,
        stddev_pop(yoy_growth) OVER (
            PARTITION BY entity_type, entity_id
            ORDER BY month
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS volatility_12m,
        posting_count::DOUBLE / NULLIF(SUM(posting_count) OVER (PARTITION BY month), 0) AS demand_concentration_index,
        0.5 * COALESCE(yoy_growth, 0)
            + 0.3 * COALESCE(rolling_3m_growth, 0)
            + 0.2 * COALESCE(acceleration, 0) AS momentum_score,
        COUNT(*) OVER (
            PARTITION BY entity_type, entity_id
            ORDER BY month
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS observed_months
    FROM ts_with_accel
)
SELECT
    w.entity_type,
    w.entity_id,
    w.month,
    w.posting_count,
    w.yoy_growth,
    w.rolling_3m_growth,
    w.acceleration,
    w.volatility_12m,
    w.demand_concentration_index,
    w.momentum_score,
    rc.feature_version,
    rc.run_timestamp,
    rc.cs_allowlist_version,
    '2023-01-01'::DATE AS train_start_month,
    '2025-01-01'::DATE AS validation_start_month
FROM windowed w
CROSS JOIN stage.run_context rc
WHERE w.observed_months >= 12;
