# WRDS Raw Snapshot Contract

This document defines the transform-ready raw snapshot contract for monthly WRDS ingestion.

## Primary source

- Table: `revelio_job_postings.postings_cosmos`
- Extract output root: `data/raw_cs_snapshot/revelio_job_postings/postings_cosmos/`
- Partitioning: `year=YYYY/month=MM`

## Required posting fields

- `job_id`
- `post_date`
- `remove_date`
- `rcid`
- `company`
- `ultimate_parent_rcid`
- `ultimate_parent_company_name`
- `country`
- `state`
- `metro_area`
- `role_k17000_v3`
- `role_k1500_v2`
- `onet_code`
- `salary`
- `salary_min`
- `salary_max`
- `salary_predicted`
- `remote_type`
- `expected_hires`
- `source_*` booleans

## Industry enrichment source

Industry dimensions are attached from:

- Table: `revelio_common.company_mapping`
- Extract output path: `data/raw_cs_snapshot/revelio_common/company_mapping/company_mapping.parquet`
- Fields used: `rcid`, `rics_k50`, `rics_k200`, `rics_k400`, `naics_code`

If posting rows have null `rcid`, industry remains unknown.

## Regional enrichment source

- Table: `revelio_common.regions`
- Path: `data/raw_cs_snapshot/revelio_common/regions/regions.parquet`
- Join key: `country`

## Staging normalization

`scripts/build_duckdb.py` maps WRDS columns into canonical staging fields used by transforms:

- `role_id <- role_k17000_v3`
- `role_name <- role_k1500_v2`
- `industry_id <- company_mapping.rics_k200`
- `industry_name <- company_mapping.rics_k50`
- `salary_usd <- salary`
- `geo_id/geo_name <- coalesce(metro_area, state, country)`

## CS filtering contract

`config/cs_universe.yml` values must use real WRDS taxonomy keys:

- `roles.role_id` values map to `role_k17000_v3`
- `skills.skill_id` values map to structured skill keys when enabled

If both role and skill allowlists are empty, transforms run in pass-through mode.

## Lineage and QA artifacts

Each extraction run writes:

- Metadata manifest: `data/raw_cs_snapshot/metadata/<run_id>.json`
- Taxonomy frequency report:
  `data/raw_cs_snapshot/metadata/taxonomy_reports/role_frequency_<run_id>.csv`

The monthly ingestion job uses the frequency report to surface candidate role IDs
for allowlist maintenance on the same schedule as WRDS extraction.
