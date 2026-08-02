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
- `scripts/discover_wrds_skills.py` monthly WRDS skill taxonomy discovery (Apr 2026+: `skill_k35000`).
- `scripts/discover_wrds_posting_skills.py` scan WRDS for posting-level skill tables (see `docs/skills_ingest_plan.md`).
- `scripts/train_trajectory_model.py` train role trajectory classifier (`ml/`, `requirements-ml.txt`).
- `docs/skills_ingest_plan.md` skill taxonomy notes, WRDS scan results, and ingest phases.

## Roadmap / TODO

Product goal: **which skillsets are growing in posting demand, and where those skillsets are hiring** (plus CS role/geo/industry context and role trajectories).

Approximate progress: **~90%** role demand stack, **~15%** inferred skill demand pilot, **~65%** role trajectory ML scaffold, **~80%** discovery/docs. See checkboxes below.

### Done (or mostly done)

- [x] `postings_cosmos` extract — time (`post_date`), geo, industry, role, salary
- [x] DuckDB marts + Postgres load for job demand, salary, trajectory features/labels
- [x] FastAPI: job demand, salary, role-skills, skill-demand on **`feature/fastapi-setup`**; trajectory on **`feature/ml-trajectory-layer`**; richer health on **`main`**
- [x] Trajectory features at role-month grain (dedup fix); Phase 1 rule labels
- [x] WRDS skill taxonomy discovery (`skill_k35000`, `individual_user_skill_lookup`)
- [x] WRDS posting-skill scan — no native `skill_k35000` on `postings_cosmos`; text only in `postings_cosmos_raw`
- [x] `ml/` package + `train_trajectory_model.py` + sample classifier artifact (thin training panel today)

### Next — inferred skill demand pilot (critical path for skillset views)

- [ ] Add `config/skill_keywords.yml` — 50–200 `skill_k35000` terms + aliases; map to `skill_k15` via lookup
- [ ] Job_id-limited extract of `postings_cosmos_raw` (`job_id`, `title_raw`, `description`) for CS postings only (do not bulk-pull full raw ~13TB; join dates from `postings_cosmos`)
- [ ] DuckDB stage: posting text → keyword hits → `stage.posting_skill_hits` (or equivalent)
- [ ] Wire hits into `build_duckdb.py` (replace `NULL` / `UNK` skills in staging) and rebuild `cs_skill_demand` / `role_skill_associations`
- [ ] Re-run `validate_outputs.py` → `load_postgres.py`; confirm non-`UNK` skill rows and coverage stats
- [ ] API honesty: `skills_status: inferred` on `/api/v1/info`; filter `UNK` by default; optional `skill_k15` rollup endpoints
- [ ] Add `docs/skill_demand_methodology.md` (or section in `skills_ingest_plan.md`) for UI copy: inferred, CS role universe, keyword method
- [ ] Frontend (separate repo): Skillset demand charts — trend, geo map, role breakdown with methodology tooltips

### Parallel — role trajectory ML (does not block skill pilot)

Current state (smoke test only): with **5 roles** and a short extract, Postgres often holds ~**80** role-month rows (~16 months × 5 roles after the 12-month warmup). Training split at `2025-01-01` can leave **~5 train rows** and a validation set that is **~87% `uncertain`**, so high accuracy is misleading and macro F1 stays low. Treat `ml/artifacts/` output as pipeline validation until the items below are done.

#### Rough targets for a first credible baseline

- [ ] **More entity-months** — expand `roles:` in `config/cs_universe.yml` (or train at geo×role grain if the product needs it). Five roles caps trajectory rows at roughly `(months_in_extract − 12) × role_count`.
- [ ] **More train-period history** — temporal split uses `train_start_month` / `validation_start_month` from features (currently `2023-01-01` / `2025-01-01` in SQL). You need **many months before validation** (ideally **24+ labeled months per role**), not a single pre-validation month.
- [ ] **Class diversity** — Phase 1 rules label most rows `uncertain`. Improve via more roles/history, revisiting thresholds in `sql/duckdb/60_trajectory_labels.sql`, and/or stratified evaluation; `class_weight` in the trainer is not enough on its own.
- [ ] **Longer WRDS extract window** — `50_trajectory_features.sql` requires **≥12 observed months** per entity before emitting a row; a short extract shrinks the panel. Align extract length with `minimum_months_for_trajectory` in `cs_universe.yml` (see below).

#### Next steps for `config/cs_universe.yml`

`cs_universe.yml` is the main lever for trajectory **volume** and dev-snapshot **thresholds**. Skills ingest is separate but uses the same file for the `skills:` block.

1. **Discover real role keys (monthly, after extract)**  
   - Run `python scripts/extract_wrds_cs_snapshot.py` (or `--no-role-filter --last-months 1` for discovery).  
   - Open `data/raw_cs_snapshot/metadata/taxonomy_reports/role_frequency_*.csv`.  
   - Add CS-relevant rows to `roles:` — `role_id` must match `postings_cosmos.role_k17000_v3` **exactly** (not display titles). Wrong keys extract zero rows.

2. **Expand the role allowlist gradually**  
   - Start with high-posting CS roles from the frequency report; keep `role_name` as a human label only.  
   - Re-run extract → `build_duckdb.py` → `make_dev_snapshot.py` → `load_postgres.py` after each expansion.  
   - Check panel size: `SELECT COUNT(*), COUNT(DISTINCT entity_id) FROM analytics.trajectory_features`.

3. **Reconcile trajectory month settings**  
   - `minimum_months_for_trajectory: 36` in YAML is the **intended** panel length; SQL currently gates on **12** observed months (`50_trajectory_features.sql`). Decide one source of truth and align YAML, SQL, and WRDS `--last-months` / date range so extracts cover at least that many months.

4. **`min_row_threshold`**  
   - Used by `make_dev_snapshot.py` to filter low-volume rows in shareable CSVs (default `5`). Lower only for local debugging; it does not increase Postgres trajectory rows.

5. **`skills:` block (parallel track)**  
   - Populate from `python scripts/discover_wrds_skills.py` reports (`skill_k35000_frequency_*`). Keys must be `skill_k35000` values, not free-text names. Posting skill demand marts still need the inferred-skill pilot or a posting-level WRDS table — profile skills alone do not fill `cs_skill_demand`.

6. **Retrain and record eval**  
   ```bash
   pip install -r requirements-ml.txt
   python scripts/train_trajectory_model.py --source postgres
   ```  
   - Aim for **hundreds+ train rows** and **multiple examples per class** before trusting metrics.  
   - [ ] Register splits and run metadata in `metadata.model_eval_splits`.  
   - [ ] Trajectory API: document rule vs ML labels; do not ship a mostly-`uncertain` model as the primary signal.

### Data universe and quality

- [ ] Expand `config/cs_universe.yml` **roles** from monthly `role_frequency_*` reports
- [ ] Populate `skills:` allowlist from `skill_k35000_frequency_*` / lookup samples (profile taxonomy keys)
- [ ] Monthly cadence: `discover_wrds_skills.py` after extract; review taxonomy reports before allowlist edits

### Long term

- [ ] Ask WRDS / Revelio for **posting-level** `skill_k35000` (or job↔skill bridge table); swap ingest when available, keep marts/API shape
- [ ] Optional: Redis caching for hot leaderboard queries

### Do not ship without labeling

- `analytics.cs_skill_demand` and `role_skill_associations` while `skill_id = 'UNK'` — treat as non-production
- Profile skills (`individual_user_skills`) are **not** posting demand; do not present as job skill demand

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
- `CS_LMI_POSTGRES_DSN` default: `postgresql://postgres:postgres@localhost:5432/cs_lmi` (also used by the API via `config/postgres.py`)
- `DATABASE_URL` optional alias for API if set (otherwise `CS_LMI_POSTGRES_DSN` applies)
- `ML_MODEL_PATH` optional trajectory model artifact override. The resolved path must stay
  inside `ml/artifacts`; the API loads models with `joblib`, so pointing it elsewhere would
  let any writable file be unpickled. Values outside that directory are ignored with a
  warning and `/api/v1/trajectory/predict` answers `503`.

## API (FastAPI)

Serving layer reads aggregated tables from Postgres (`analytics.*`). **Endpoint development branch:** `feature/fastapi-setup` (see that branch’s README for the canonical route list). This doc also tracks `main` and ML branches.

**Run locally** (after `load_postgres.py`):

```bash
pip install -r requirements.txt
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000
```

- Interactive docs: `http://localhost:8000/docs`
- OpenAPI JSON: `http://localhost:8000/openapi.json`

**Branch coverage**

| Capability | Branch / merge target |
|------------|------------------------|
| Core analytics routes (job, skill, salary, role-skills) | **`feature/fastapi-setup`** → `main` |
| Postgres pool | **`feature/fastapi-setup`** |
| `/health` with Postgres status, `/health/db`, `config/postgres.py` | **`main`** (`feature/api-postgres-health`, merged) |
| Trajectory routes | **`feature/ml-trajectory-layer`** |

### Implemented endpoints

Analytics routes below are on **`feature/fastapi-setup`** unless noted.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness on **`feature/fastapi-setup`**; on **`main`**, also reports Postgres connectivity |
| `GET` | `/health/db` | Postgres health + `analytics.*` row counts (**`main` only**) |
| `GET` | `/api/v1/info` | API name, version, top-level route prefixes |
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
| `GET` | `/api/v1/trajectory/features` | Trajectory feature mart (`entity_type`, `entity_id`, `month`) — **`feature/ml-trajectory-layer`** |
| `GET` | `/api/v1/trajectory/labels` | Trajectory labels (`method`, e.g. `phase1_rules`) — **`feature/ml-trajectory-layer`** |

**Data quality notes**

- Skill and role–skill routes are wired but **`skill_id` is often `UNK`** until the inferred skill pilot lands; do not treat as production skill demand yet.
- Trajectory panel is thin until role allowlist and extract history expand (see Roadmap).

### Proposed endpoints (not implemented yet)

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/api/v1/info` (extend) | Add `skills_status: placeholder \| inferred` and methodology link |
| `GET` | `/api/v1/skill-demand` (behavior) | Default exclude `skill_id IN ('UNK', 'Unknown')` |
| `GET` | `/api/v1/skill-demand/by-skillset/{skill_k15}` | Roll up granular skills to Revelio `skill_k15` “skillset” |
| `GET` | `/api/v1/skill-demand/by-geo/{geo_id}` | Skill demand map slice for Skillset dashboard |
| `GET` | `/api/v1/trajectory/predictions` | Optional ML classifier output alongside rule labels |
| `GET` | `/api/v1/market-summary` | Composite leaderboard (roles, skills, geos) for home view |

## End-to-End Local Run

1) Extract monthly WRDS snapshot (postings + optional company mapping/regions):

```bash
python scripts/extract_wrds_cs_snapshot.py
```

This writes raw snapshot files to `data/raw_cs_snapshot` and a monthly role-frequency report
under `data/raw_cs_snapshot/metadata/taxonomy_reports/` so allowlist expansion can run on the
same monthly ingestion cadence.

Postings are written as `year=/month=` parquet partitions and the run resumes by skipping
partitions that already exist. A partition counts as existing when it holds a `_SUCCESS`
sentinel (written by current runs) or, for partitions predating the sentinel, any parquet
file. Delete a `year=/month=` directory to force that month to be refetched.

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

`trajectory_ml.train_start_month` and `trajectory_ml.validation_start_month` are the
authoritative temporal split. `scripts/build_duckdb.py` stamps them into every
`trajectory_features` row, and `ml.data.temporal_split` reads them back from the config so a
stale rebuild cannot silently move the evaluation boundary. The embedded columns are only
used when the config keys are absent, and a disagreement is logged as a warning. Both values
must be ISO dates (`YYYY-MM-DD`); anything else fails the build instead of being spliced
into SQL.

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
python scripts/predict_trajectory_model.py --source postgres
```

## ML Label Rows Are Not Durable Across Loads

`analytics.trajectory_labels` holds both rule labels (`method=phase1_rules`, produced by
the DuckDB pipeline) and ML predictions (`method=ml_classifier`, produced by
`scripts/predict_trajectory_model.py`). Only the rule labels exist in
`data/processed/trajectory_labels.csv`.

`scripts/load_postgres.py` reloads a table by `TRUNCATE` then `COPY`, so any run that
includes `trajectory_labels` (including a full load with no `--tables`) deletes every
`ml_classifier` row. `GET /api/v1/trajectory/labels?method=ml_classifier` returns nothing
until predictions are regenerated:

```bash
python scripts/predict_trajectory_model.py --source postgres
```

Training is unaffected: `ml.data.join_trajectory_dataset` filters labels to
`method=phase1_rules` before deduping, so ML rows are never used as training targets.

## Industry Dimension Note

`rics_k50`, `rics_k200`, and `rics_k400` are attached via `revelio_common.company_mapping`.
They are not assumed to be present on `postings_cosmos` in this WRDS configuration.