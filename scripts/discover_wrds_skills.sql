-- WRDS skill discovery (Revelio April 2026+ taxonomy)
-- Run in WRDS Query tool or via scripts/discover_wrds_skills.py
-- Replace schema names if your subscription uses revelio_job_postings / revelio_individual, etc.

-- 1) All Revelio schemas with skill in the table name
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema ILIKE 'revelio%'
  AND table_name ILIKE '%skill%'
ORDER BY 1, 2;

-- 2) Columns on individual_user_skills (confirm skill_k35000 exists; skill_mapped deprecated)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'revelio'
  AND table_name = 'individual_user_skills'
ORDER BY ordinal_position;

-- 3) Columns on individual_user_skill_lookup (broader skill_k* rollups)
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'revelio'
  AND table_name = 'individual_user_skill_lookup'
ORDER BY ordinal_position;

-- 4) Top granular skills for allowlist candidates (profile grain)
SELECT skill_k35000, COUNT(*) AS user_skill_rows
FROM revelio.individual_user_skills
WHERE skill_k35000 IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC
LIMIT 500;

-- 5) Example: skill_k35000 with skill_k15 label (adjust lookup column names to match step 3)
SELECT s.skill_k35000, l.skill_k15, COUNT(*) AS n
FROM revelio.individual_user_skills AS s
LEFT JOIN revelio.individual_user_skill_lookup AS l
  ON s.skill_k35000 = l.skill_k35000
WHERE s.skill_k35000 IS NOT NULL
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 200;

-- 6) Posting-level skill columns (demand marts) on postings_cosmos
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'revelio_job_postings'
  AND table_name = 'postings_cosmos'
  AND column_name ILIKE '%skill%'
ORDER BY ordinal_position;

-- 7) Posting <-> skill bridge tables (if step 6 is empty)
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema ILIKE 'revelio%'
  AND (
    table_name ILIKE '%posting%skill%'
    OR table_name ILIKE '%job%skill%'
    OR table_name ILIKE '%cosmos%skill%'
  )
ORDER BY 1, 2;
