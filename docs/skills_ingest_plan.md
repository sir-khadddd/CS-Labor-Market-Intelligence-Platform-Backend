# Skills ingest plan (Revelio / WRDS)

## April 2026 taxonomy change

Revelio expanded the skills taxonomy from ~3,000 to **30,000+** distinct skills.

| Legacy | Current (Apr 2026+) |
|--------|---------------------|
| `skill_mapped` | **`skill_k35000`** (most granular skill id) |
| — | Broader buckets via **`individual_user_skill_lookup`** joined on `skill_k35000` (e.g. up to `skill_k15`) |

**WRDS tables (workforce / profile grain):**

- `individual_user_skills` — one row per user/skill; use **`skill_k35000`** as `skill_id` in allowlists.
- `individual_user_skill_lookup` — maps `skill_k35000` to coarser `skill_k*` levels for rollups.

Example lookup join:

```sql
SELECT s.user_id, s.skill_k35000, l.skill_k15, l.skill_k50
FROM revelio.individual_user_skills AS s
LEFT JOIN revelio.individual_user_skill_lookup AS l
  ON s.skill_k35000 = l.skill_k35000;
```

Adjust `revelio` schema name to match your WRDS subscription (`revelio_individual`, etc.).

## What this repo uses skills for

| Product need | Grain | Source priority |
|--------------|-------|-----------------|
| CS skill demand, role↔skill associations, Skillset dashboard | **Job posting** | `postings_cosmos` skill columns or posting↔skill bridge (discover first) |
| Allowlist discovery, taxonomy QA | Profile | `individual_user_skills.skill_k35000` + lookup |
| Optional supply / workforce views | Profile | Same + `revelio_workforce_dynamics` |

`individual_user_skills` alone does **not** populate posting demand marts without a posting-level join path.

## WRDS scan result (2026-05-30)

Script: `python scripts/discover_wrds_posting_skills.py`

Across all **83** Revelio tables on this subscription (`revelio`, `revelio_job_postings`,
`revelio_individual`, `revelio_common`, `revelio_workforce_dynamics`, etc.):

| Finding | Detail |
|---------|--------|
| **No posting↔skill bridge** | No table named `*posting*skill*`, `*job*skill*`, or similar |
| **`postings_cosmos`** | Has `job_id`, roles, geo, salary — **no `skill_*` columns** |
| **`postings_cosmos_raw`** | Has `job_id`, `title_raw`, `description`, location — **skills only in free text** |
| **Profile skills** | `revelio.individual_user_skills` has `skill_k35000` (join key: `user_id`, not `job_id`) |
| **Lookup** | `revelio.individual_user_skill_lookup` — taxonomy only, no `job_id` |
| **Workforce dynamics** | `workforce_dynamics_geo` — roles/geo counts, **no skill dimension** on WRDS |

**Conclusion:** Structured posting-level `skill_k35000` is **not published on WRDS** for this
Job Postings product slice. COSMOS marketing mentions “skills required”; that enrichment is
either not loaded to WRDS, requires a separate Revelio deliverable, or must be derived from
`postings_cosmos_raw.description`.

### Practical options

1. **Ask WRDS / Revelio support** whether a `postings_*_skills` or Job Posting Dynamics
   with `skill_k*` exists outside the current schemas (or requires an add-on).
2. **Derive from text (Phase B)** — extract `postings_cosmos_raw` (`job_id`, `description`),
   map text to `skill_k35000` via Revelio tooling or an internal matcher; expensive, document lineage.
3. **Interim dashboard** — role + industry demand now; Skillset entry stays hidden until (1) or (2).
4. **Do not join** `individual_user_skills` to postings without a valid bridge — grain mismatch.

Reports: `metadata/taxonomy_reports/posting_skill_table_candidates_*.csv`,
`metadata/posting_skills_*.json`.

## Monthly cadence (same as roles)

1. Run WRDS discovery after each extract (or on the same schedule):
   ```bash
   python scripts/discover_wrds_skills.py
   ```
   Default run is fast (lookup table only). Avoids a full-table `COUNT` on
   `individual_user_skills`, which can look hung on WRDS. For sampled popularity:
   `python scripts/discover_wrds_skills.py --profile-counts --sample-percent 1`
2. Review reports under `data/raw_cs_snapshot/metadata/taxonomy_reports/`:
   - `skill_table_inventory_<run_id>.csv` — skill-related tables/columns on WRDS
   - `skill_k35000_frequency_<run_id>.csv` — top `skill_k35000` values (profile skills)
   - `postings_skill_columns_<run_id>.csv` — skill columns on `postings_cosmos`, if any
3. Update `config/cs_universe.yml` `skills:` with real **`skill_k35000`** keys (not display names).
4. When posting-level skills are confirmed, extend `wrds_extract.yml` + `build_duckdb.py`, then rebuild and `load_postgres.py`.

## Allowlist format (`cs_universe.yml`)

```yaml
skills:
  - skill_id: "<skill_k35000 value>"
    skill_name: "<human-readable label from lookup or WRDS>"
```

`skill_id` must match the WRDS key exactly (same rule as `role_k17000_v3` for roles).

## Product honesty until ingest is wired

- Treat `analytics.cs_skill_demand` and `role_skill_associations` as **non-production** while `skill_id = 'UNK'`.
- After keyword ingest is wired, label API responses `skills_status: inferred` (see `docs/skill_demand_methodology.md`).
- Do not ship the Skillset dashboard entry as live data until non-UNK rows are loaded.

## Implementation phases

| Phase | Work |
|-------|------|
| **A (now)** | Monthly discovery script + allowlist maintenance from `skill_k35000_frequency_*` |
| **B** | Confirm posting-level skill source; add extract + DuckDB staging |
| **C** | Rebuild marts, reload Postgres, enable Skillset UI |
| **D (later)** | Skill-level trajectory entities (`entity_type = 'skill'`) |

Role trajectory ML (Phase 2) does **not** depend on Phase B.
