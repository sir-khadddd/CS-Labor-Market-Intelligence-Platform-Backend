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
- `api/main.py` FastAPI app; `api/routers/*.py` Postgres-backed routes (develop on **`feature/fastapi-setup`**).

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
- `DATABASE_URL` optional alias for the API Postgres pool (default: `postgresql://localhost/analytics` on this branch)

## API (FastAPI)

**Active endpoint branch:** `feature/fastapi-setup` (merge target: `main`). Serving layer reads `analytics.*` in Postgres after `load_postgres.py`.

**Run locally:**

```bash
pip install -r requirements.txt
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

- Interactive docs: `http://localhost:8000/docs`
- OpenAPI JSON: `http://localhost:8000/openapi.json`

**Branch coverage**

| Capability | Where |
|------------|--------|
| Job, skill, salary, role-skills routes | **`feature/fastapi-setup`** (this branch) |
| Postgres pool (`Depends(get_postgres_connection)`) | **`feature/fastapi-setup`** |
| `/health` with Postgres status, `/health/db`, `config/postgres.py` DSN | **`main`** (`feature/api-postgres-health`, merged) — not on this branch yet |
| Trajectory routes | **`feature/ml-trajectory-layer`** |

### Implemented on `feature/fastapi-setup`

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness (`{"status": "healthy"}`) |
| `GET` | `/api/v1/info` | API name, version, route prefixes |
| `GET` | `/api/v1/job-demand` | CS job demand (`month`, `geo_id`, `industry_id`, `role_id`, pagination) |
| `GET` | `/api/v1/job-demand/by-geo` | Top demand by geo for a required `month` |
| `GET` | `/api/v1/job-demand/by-role` | Demand for a required `role_id` |
| `GET` | `/api/v1/skill-demand` | Skill demand (`month`, `geo_id`, `role_id`, `skill_id`, `sort_by`) |
| `GET` | `/api/v1/skill-demand/by-role/{role_id}` | Top skills for a role |
| `GET` | `/api/v1/skill-demand/trending` | Skills above `min_yoy_growth` for a `month` |
| `GET` | `/api/v1/salaries` | Salary percentiles (`month`, `geo_id`, `role_id`, `industry_id`) |
| `GET` | `/api/v1/salaries/by-role/{role_id}` | Salaries for a role |
| `GET` | `/api/v1/salaries/by-geo/{geo_id}` | Salaries for a geography |
| `GET` | `/api/v1/role-skills` | Role–skill associations (`month`, `role_id`, `skill_id`, `sort_by`) |
| `GET` | `/api/v1/role-skills/strong-associations` | Associations with `lift` above `min_lift` for a `month` |
| `GET` | `/api/v1/role-skills/{role_id}` | Top skills for a role by association strength |

**Data quality:** Skill and role–skill marts often have `skill_id = UNK` until the inferred skill pilot lands; treat skill-demand as plumbing only for now.

### Planned next (on `feature/fastapi-setup` or follow-ups)

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/health/db` | Postgres health + `analytics.*` row counts (already on `main`; merge or re-implement here) |
| `GET` | `/api/v1/info` (extend) | `skills_status: placeholder \| inferred` |
| `GET` | `/api/v1/skill-demand` (behavior) | Default exclude `UNK` / `Unknown` |
| `GET` | `/api/v1/trajectory/*` | Feature/label routes — see **`feature/ml-trajectory-layer`** |

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