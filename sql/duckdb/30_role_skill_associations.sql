CREATE OR REPLACE TABLE marts.role_skill_associations AS
WITH monthly AS (
    SELECT
        month,
        role_id,
        skill_id,
        COUNT(*) AS co_occurrence_count
    FROM stage.cs_postings
    GROUP BY 1, 2, 3
),
rolling AS (
    SELECT
        month,
        role_id,
        skill_id,
        SUM(co_occurrence_count) OVER (
            PARTITION BY role_id, skill_id
            ORDER BY month
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS co_occurrence_12m
    FROM monthly
),
denom AS (
    SELECT
        month,
        role_id,
        SUM(co_occurrence_count) AS role_total
    FROM monthly
    GROUP BY 1, 2
),
skill_base AS (
    SELECT
        month,
        skill_id,
        SUM(co_occurrence_count) AS skill_total
    FROM monthly
    GROUP BY 1, 2
),
all_total AS (
    SELECT
        month,
        SUM(co_occurrence_count) AS all_total
    FROM monthly
    GROUP BY 1
)
SELECT
    r.month,
    r.role_id,
    r.skill_id,
    r.co_occurrence_12m AS co_occurrence_count,
    r.co_occurrence_12m::DOUBLE / NULLIF(d.role_total, 0) AS p_skill_given_role,
    (r.co_occurrence_12m::DOUBLE / NULLIF(d.role_total, 0))
        / NULLIF(sb.skill_total::DOUBLE / NULLIF(a.all_total, 0), 0) AS lift
FROM rolling r
JOIN denom d
  ON r.month = d.month
 AND r.role_id = d.role_id
JOIN skill_base sb
  ON r.month = sb.month
 AND r.skill_id = sb.skill_id
JOIN all_total a
  ON r.month = a.month;
