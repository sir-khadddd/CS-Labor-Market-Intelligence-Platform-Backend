CREATE OR REPLACE TABLE marts.cs_skill_demand AS
WITH skill_monthly AS (
    SELECT
        month,
        geo_id,
        role_id,
        skill_id,
        any_value(skill_name) AS skill_name,
        SUM(posting_count) AS skill_posting_count
    FROM stage.cs_postings
    GROUP BY 1, 2, 3, 4
),
role_totals AS (
    SELECT
        month,
        geo_id,
        role_id,
        SUM(posting_count) AS role_posting_count
    FROM stage.cs_postings
    GROUP BY 1, 2, 3
),
joined AS (
    SELECT
        s.month,
        s.geo_id,
        s.role_id,
        s.skill_id,
        s.skill_name,
        s.skill_posting_count,
        r.role_posting_count,
        s.skill_posting_count::DOUBLE / NULLIF(r.role_posting_count, 0) AS share_within_role
    FROM skill_monthly s
    JOIN role_totals r
      ON s.month = r.month
     AND s.geo_id = r.geo_id
     AND s.role_id = r.role_id
)
SELECT
    j.*,
    100.0 * (j.skill_posting_count - LAG(j.skill_posting_count, 12) OVER w)
        / NULLIF(LAG(j.skill_posting_count, 12) OVER w, 0) AS yoy_growth
FROM joined j
WINDOW w AS (PARTITION BY j.geo_id, j.role_id, j.skill_id ORDER BY j.month);
