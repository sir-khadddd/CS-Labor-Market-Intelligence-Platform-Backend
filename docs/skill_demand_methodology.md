# Inferred skill demand methodology

## Status

Skill demand in this platform is **inferred**, not sourced from structured posting-level `skill_k35000` on WRDS.

- API field: `skills_status: inferred`
- Method: keyword matching on posting title + description
- Config: `config/skill_keywords.yml` (Revelio `skill_k35000` ids + aliases)

WRDS `postings_cosmos` has roles, geo, salary, and dates but **no skill columns**. Skills appear only in `postings_cosmos_raw.description` (free text). See `docs/skills_ingest_plan.md` for the WRDS scan conclusion.

## Pipeline

1. **CS job universe** — `postings_cosmos` extract filtered by `config/cs_universe.yml` `roles:` (`role_k17000_v3`).
2. **Posting text** — `scripts/extract_wrds_cs_posting_text.py` fetches `title_raw` + `description` for those `job_id`s only.
3. **Keyword map** — `config/skill_keywords.yml` built from `individual_user_skill_lookup` via `scripts/build_skill_keywords.py`.
4. **Match** — `scripts/build_duckdb.py` scans text per month, writes `stage.posting_skill_hits`, explodes to job×skill rows.
5. **Marts** — `cs_skill_demand`, `role_skill_associations` in DuckDB → Postgres.

Job demand (`cs_job_demand`) stays at **job grain** (one row per posting). Skill marts use **job×skill grain** (a posting with three skill hits contributes to three skills).

## Metrics

| Mart column | Meaning |
|-------------|---------|
| `skill_posting_count` | Postings in month×geo×role with at least one keyword hit for the skill |
| `role_posting_count` | Total postings in month×geo×role (job grain) |
| `share_within_role` | `skill_posting_count / role_posting_count` |
| `yoy_growth` | 12-month YoY on `skill_posting_count` |
| `lift` (associations) | Strength of role↔skill co-occurrence vs baseline |

## Limitations (show in UI)

- **Coverage** — Only skills in the keyword config are measured (~200 pilot terms). Unmatched config skills produce no rows.
- **False positives** — Short tokens (`sql`, `r`, `go`, `c`) can match inside unrelated words; review `scripts/review_skill_quality.py` reports.
- **False negatives** — Synonyms not in `keywords:` are missed; no embedding or ML matcher yet.
- **CS role scope** — Metrics reflect the current `roles:` allowlist, not all labor-market postings.
- **Not profile skills** — `individual_user_skills` is workforce grain; it is not joined to postings.

## Quality review

After each build:

```bash
python scripts/review_skill_quality.py
```

Reports land in `data/raw_cs_snapshot/metadata/skill_quality_review_*.md`.

## Disk usage

Intermediate artifacts can be large. Defaults are tuned to minimize peak disk:

| Step | What uses disk | Mitigation |
|------|----------------|------------|
| `extract_wrds_cs_posting_text.py` | `_checkpoints/` during WRDS fetch | Auto-deleted after merge; default **only fetches job_ids missing** from existing `posting_text.parquet` |
| `build_duckdb.py` | DuckDB temp spill | Capped at **12 GiB**; temp + staging cleaned after build |
| `build_duckdb.py` | `data/processed/` | **Parquet only** by default (no duplicate CSV); use `--export-csv` if needed |
| `build_duckdb.py` | `cs_lmi.duckdb` | Deleted after export unless `--keep-duckdb` |

Free space after a run:

```bash
python scripts/cleanup_pipeline_artifacts.py --all-safe
```

Remove WRDS checkpoints only after a successful text merge:

```bash
python scripts/cleanup_pipeline_artifacts.py --include-checkpoints
```

## API behavior

- `/api/v1/info` returns `skills_status: inferred` and method metadata.
- Skill routes exclude `UNK` / `Unknown` by default; pass `include_unknown=true` to include placeholders (legacy runs).

## Future

If WRDS/Revelio ships posting-level `skill_k35000`, swap the staging source and keep mart/API shapes. Until then, label dashboards **“Inferred from posting text”**.
