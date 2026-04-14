CREATE OR REPLACE TABLE marts.cs_job_demand AS
WITH monthly AS (
    SELECT
        p.month,
        p.geo_id,
        any_value(p.geo_name) AS geo_name,
        p.industry_id,
        any_value(p.industry_name) AS industry_name,
        p.role_id,
        any_value(p.role_name) AS role_name,
        SUM(p.posting_count) AS posting_count
    FROM stage.cs_postings p
    GROUP BY 1, 2, 4, 6
),
panel AS (
    SELECT
        em.month,
        em.geo_id,
        em.industry_id,
        em.role_id
    FROM stage.entity_month_panel em
),
filled AS (
    SELECT
        pa.month,
        pa.geo_id,
        COALESCE(m.geo_name, 'Unknown') AS geo_name,
        pa.industry_id,
        COALESCE(m.industry_name, 'Unknown') AS industry_name,
        pa.role_id,
        COALESCE(m.role_name, 'Unknown') AS role_name,
        COALESCE(m.posting_count, 0) AS posting_count
    FROM panel pa
    LEFT JOIN monthly m
      ON pa.month = m.month
     AND pa.geo_id = m.geo_id
     AND pa.industry_id = m.industry_id
     AND pa.role_id = m.role_id
),
metrics AS (
    SELECT
        *,
        100.0 * (posting_count - LAG(posting_count, 12) OVER w) / NULLIF(LAG(posting_count, 12) OVER w, 0) AS yoy_growth,
        100.0 * (posting_count - LAG(posting_count, 3) OVER w) / NULLIF(LAG(posting_count, 3) OVER w, 0) AS rolling_3m_growth
    FROM filled
    WINDOW w AS (PARTITION BY geo_id, industry_id, role_id ORDER BY month)
)
SELECT
    m.*,
    m.yoy_growth - LAG(m.yoy_growth, 1) OVER (
        PARTITION BY m.geo_id, m.industry_id, m.role_id ORDER BY m.month
    ) AS acceleration
FROM metrics m;
