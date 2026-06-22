-- Join inferred skill hits onto job-level postings (one row per job x skill).
-- stage.raw_postings_jobs and stage.posting_skill_hits must exist.

CREATE OR REPLACE TABLE stage.raw_postings AS
SELECT
    j.job_id,
    j.post_date,
    j.remove_date,
    j.rcid,
    j.country,
    j.state,
    j.metro_area,
    j.role_id,
    j.role_name,
    h.skill_id,
    h.skill_name,
    j.salary_usd,
    j.industry_id,
    j.industry_name,
    j.geo_id,
    j.geo_name
FROM stage.raw_postings_jobs j
INNER JOIN stage.posting_skill_hits h
  ON j.job_id = h.job_id;
