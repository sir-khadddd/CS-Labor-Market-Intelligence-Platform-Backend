# CS Labor Market Intelligence Backend

Backend data pipeline for CS-focused labor market intelligence using a DuckDB transform layer and a Postgres serving mart.

## What This Includes

- Revelio-compatible source config aligned with cloner table patterns.
- Early CS filtering from role/skill allowlists.
- WRDS ingestion script for monthly CS snapshots from `postings_cosmos`.
- Monthly aggregated fact tables for demand, skills, associations, and salary.
- Phase 1 trajectory features + rule-based trajectory labels.
- Shareable aggregated dev snapshot in `data/dev_processed`.
- Postgres schemas with metadata contracts for Phase 2 ML compatibility.

## Repository Layout

- `config/revelio_sources.py` source and path configuration.
- `config/wrds_extract.yml` WRDS extraction settings.
- `config/cs_universe.yml` CS allowlists and thresholds.
- `sql/duckdb/*.sql` transform pipeline.
- `sql/postgres/*.sql` serving and metadata schema.
- `scripts/extract_wrds_cs_snapshot.py` WRDS extract + monthly taxonomy frequency report.
- `scripts/build_duckdb.py` DuckDB build + processed exports.
- `scripts/load_postgres.py` load processed outputs into Postgres.
- `scripts/make_dev_snapshot.py` generate GitHub-safe dev data.
- `scripts/validate_outputs.py` sanity checks.

## Setup

```bash
python -m venv .venv
. .venv/Scripts/activate
pip install -r requirements.txt
```

## Environment Variables

- `WRDS_USERNAME` required for WRDS extraction
- `WRDS_PASSWORD` optional for WRDS extraction (or use pgpass)
- `CS_LMI_REVELIO_ROOT` default: `data/raw_cs_snapshot`
- `CS_LMI_DUCKDB_PATH` default: `data/local/cs_lmi.duckdb`
- `CS_LMI_PROCESSED_DIR` default: `data/processed`
- `CS_LMI_POSTGRES_DSN` default: `postgresql://postgres:postgres@localhost:5432/cs_lmi`

## End-to-End Local Run

1) Extract monthly WRDS snapshot (postings + optional company mapping/regions):

```bash
python scripts/extract_wrds_cs_snapshot.py
```

This writes raw snapshot files to `data/raw_cs_snapshot` and a monthly role-frequency report
under `data/raw_cs_snapshot/metadata/taxonomy_reports/` so allowlist expansion can run on the
same monthly ingestion cadence.

2) Build DuckDB marts and export processed outputs:

```bash
python scripts/build_duckdb.py
```

3) Validate processed outputs:

```bash
python scripts/validate_outputs.py
```

4) Generate/update shareable dev snapshot:

```bash
python scripts/make_dev_snapshot.py
```

5) Load analytics mart into Postgres:

```bash
python scripts/load_postgres.py
```

## Phase 1 Lock-Ins Included

- Stable entity keys across tables (`role_id`, `skill_id`, `geo_id`, `industry_id`).
- Entity-month panel support for consistent time grids.
- Point-in-time feature outputs for leakage-safe modeling.
- Versioned artifacts (`feature_version`, `label_version`, `method_version`, `run_timestamp`).
- Lineage and split metadata via `metadata` schema contracts.
- Idempotent, partition-friendly rebuild design.

## Dev Snapshot Guidance

`data/dev_processed` is safe for GitHub sharing because it contains only aggregated outputs.
Do not commit raw posting-level exports or individual-level data.

## Industry Dimension Note

`rics_k50`, `rics_k200`, and `rics_k400` are attached via `revelio_common.company_mapping`.
They are not assumed to be present on `postings_cosmos` in this WRDS configuration.