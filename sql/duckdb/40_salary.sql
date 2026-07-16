CREATE OR REPLACE TABLE marts.salary_distribution AS
SELECT
    month,
    geo_id,
    any_value(geo_name) AS geo_name,
    industry_id,
    any_value(industry_name) AS industry_name,
    role_id,
    any_value(role_name) AS role_name,
    quantile_cont(salary_usd, 0.25) AS salary_p25,
    quantile_cont(salary_usd, 0.50) AS salary_p50,
    quantile_cont(salary_usd, 0.75) AS salary_p75
FROM stage.cs_postings
WHERE salary_usd IS NOT NULL AND salary_usd > 0
GROUP BY 1, 2, 4, 6;
