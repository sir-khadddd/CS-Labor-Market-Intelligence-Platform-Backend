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
# Or reload a subset without blocking on large tables:
python scripts/load_postgres.py --tables trajectory_features,trajectory_labels
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

## Trajectory ML Data Requirements

Two related but distinct settings govern trajectory data, both in `config/cs_universe.yml`:

- `minimum_months_for_trajectory: 36` -- target extract/panel length for credible ML
  training and evaluation. `config/wrds_extract.yml` `extract.start_date` should span at
  least this many months up to `end_date`.
- `min_observed_months_for_features: 12` -- minimum trailing observed months required
  per entity before a `trajectory_features` row is emitted, since YoY growth needs a
  12-month lag. Enforced in `sql/duckdb/50_trajectory_features.sql` via
  `WHERE observed_months >= 12`. This value is hardcoded in SQL (not read from the YAML)
  because `scripts/build_duckdb.py` does not template config values into SQL text today.
  If you change the YAML value, update the SQL literal to match.

## Forward-Looking Trajectory Labels

`trajectory_class` is a forecasting target, not a restatement of the current month. The label
attached to month `t` describes the entity's state at `t + 3` months (plus realized posting
growth from `t` to `t + 3`), while model inputs stay point-in-time at `t`. Earlier versions
(`phase1-v2` and before) derived the class from a `CASE` over the same month's feature
columns, which made the classifier circular: it could only re-learn the rule it was trained
from, and reported accuracy was meaningless.

Consequences to be aware of:

- The last 3 months of the panel have features but no labels, so they are trainable inputs
  for prediction only, not for supervised fitting.
- Label semantics changed, so labels are versioned `phase1-v3` (rule `method_version`
  `rules-v3`, ML `method_version` `ml-v2`). Rows written under older versions are not
  comparable and should not be mixed into a single training set.
- Existing DuckDB output, processed CSVs, dev snapshot, and Postgres rows still carry the
  old labels until you rebuild:

```bash
python scripts/build_duckdb.py
python scripts/make_dev_snapshot.py
python scripts/load_postgres.py --tables trajectory_features,trajectory_labels
python scripts/train_trajectory_model.py --source postgres
```

## Industry Dimension Note

`rics_k50`, `rics_k200`, and `rics_k400` are attached via `revelio_common.company_mapping`.
They are not assumed to be present on `postings_cosmos` in this WRDS configuration.