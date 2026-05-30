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
- [x] FastAPI: job demand, salary, role-skills, skill-demand (routes exist), trajectory, health
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