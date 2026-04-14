CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE OR REPLACE TABLE stage.run_context AS
SELECT
    CAST(current_timestamp AS TIMESTAMP) AS run_timestamp,
    'phase1-v1'::VARCHAR AS feature_version,
    'phase1-v1'::VARCHAR AS label_version,
    'rules-v1'::VARCHAR AS method_version,
    '2026.04'::VARCHAR AS cs_allowlist_version;

CREATE OR REPLACE TABLE stage.dim_role AS
SELECT DISTINCT role_id, role_name
FROM stage.allowlist_roles;

CREATE OR REPLACE TABLE stage.dim_skill AS
SELECT DISTINCT skill_id, skill_name
FROM stage.allowlist_skills;

CREATE OR REPLACE TABLE stage.base_postings AS
SELECT
    date_trunc('month', CAST(post_date AS DATE))::DATE AS month,
    COALESCE(geo_id, 'UNK')::VARCHAR AS geo_id,
    COALESCE(geo_name, 'Unknown')::VARCHAR AS geo_name,
    COALESCE(industry_id, 'UNK')::VARCHAR AS industry_id,
    COALESCE(industry_name, 'Unknown')::VARCHAR AS industry_name,
    UPPER(COALESCE(role_id, 'UNK'))::VARCHAR AS role_id,
    COALESCE(role_name, 'Unknown')::VARCHAR AS role_name,
    UPPER(COALESCE(skill_id, 'UNK'))::VARCHAR AS skill_id,
    COALESCE(skill_name, 'Unknown')::VARCHAR AS skill_name,
    TRY_CAST(salary_usd AS DOUBLE) AS salary_usd,
    1 AS posting_count
FROM stage.raw_postings
WHERE post_date IS NOT NULL;

CREATE OR REPLACE TABLE stage.cs_postings AS
WITH allowlist_sizes AS (
    SELECT
        (SELECT COUNT(*) FROM stage.dim_role) AS role_allowlist_size,
        (SELECT COUNT(*) FROM stage.dim_skill) AS skill_allowlist_size
)
SELECT p.*
FROM stage.base_postings p
LEFT JOIN stage.dim_role r USING (role_id)
LEFT JOIN stage.dim_skill s USING (skill_id)
CROSS JOIN allowlist_sizes a
WHERE
    (a.role_allowlist_size = 0 AND a.skill_allowlist_size = 0)
    OR r.role_id IS NOT NULL
    OR s.skill_id IS NOT NULL;

CREATE OR REPLACE TABLE stage.entity_month_panel AS
WITH entities AS (
    SELECT DISTINCT
        role_id,
        geo_id,
        industry_id
    FROM stage.cs_postings
),
months AS (
    SELECT DISTINCT month
    FROM stage.cs_postings
)
SELECT
    m.month,
    e.role_id,
    e.geo_id,
    e.industry_id
FROM entities e
CROSS JOIN months m;
