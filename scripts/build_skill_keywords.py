"""
Build config/skill_keywords.yml from Revelio WRDS skill taxonomy.

Uses individual_user_skill_lookup (~33k rows) for skill_k35000 ids, labels,
and skill_k15 rollups. Optional profile popularity via TABLESAMPLE on
individual_user_skills.

Example:
  python scripts/build_skill_keywords.py
  python scripts/build_skill_keywords.py --limit 200 --profile-counts
  python scripts/build_skill_keywords.py --dry-run
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import wrds
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.backend_env import load_backend_env

load_backend_env()

DEFAULT_OUTPUT = ROOT / "config" / "skill_keywords.yml"
DEFAULT_LOOKUP_LIBRARY = "revelio"
DEFAULT_LOOKUP_TABLE = "individual_user_skill_lookup"
DEFAULT_SKILLS_LIBRARY = "revelio"
DEFAULT_SKILLS_TABLE = "individual_user_skills"

# Revelio skill_k15 buckets most relevant to CS posting keyword matching.
DEFAULT_SKILL_K15_BUCKETS = (
    "Software Development",
    "Data Analytics",
    "IT and Security",
    "Digital Content Creation",
    "Engineering Systems",
)

# High-signal CS skills to always include when present in lookup (seed list).
DEFAULT_SEED_SKILLS = (
    "Python",
    "Java",
    "JavaScript",
    "TypeScript",
    "SQL",
    "R",
    "C",
    "C++",
    "C#",
    "Go",
    "Rust",
    "Scala",
    "Kotlin",
    "Swift",
    "Ruby",
    "PHP",
    "HTML",
    "CSS",
    "React",
    "Angular",
    "Vue.js",
    "Node.js",
    "Spring Boot",
    "Django",
    "Flask",
    "FastAPI",
    ".NET",
    "Amazon Web Services",
    "AWS",
    "Azure",
    "Google Cloud Platform",
    "GCP",
    "Kubernetes",
    "Docker",
    "Terraform",
    "Linux",
    "Git",
    "CI/CD",
    "DevOps",
    "Agile",
    "Scrum",
    "Machine Learning",
    "Deep Learning",
    "TensorFlow",
    "PyTorch",
    "Scikit-Learn",
    "Pandas",
    "NumPy",
    "Spark",
    "Hadoop",
    "PostgreSQL",
    "MySQL",
    "MongoDB",
    "Redis",
    "Elasticsearch",
    "Kafka",
    "Snowflake",
    "Databricks",
    "Tableau",
    "Power BI",
    "Jenkins",
    "GitHub Actions",
    "Ansible",
    "Prometheus",
    "Grafana",
    "Cybersecurity",
    "Penetration Testing",
    "Network Security",
    "OAuth",
    "REST API",
    "GraphQL",
    "Microservices",
    "Object-Oriented Programming",
    "Data Structures",
    "Algorithms",
)

# Extra posting-text aliases not derivable from the canonical skill label alone.
EXTRA_KEYWORD_ALIASES: dict[str, list[str]] = {
    "JavaScript": ["js", "ecmascript", "es6"],
    "TypeScript": ["ts"],
    "Amazon Web Services": ["aws"],
    "Google Cloud Platform": ["gcp", "google cloud"],
    "Azure": ["microsoft azure", "ms azure"],
    "Kubernetes": ["k8s"],
    "PostgreSQL": ["postgres", "psql"],
    "MongoDB": ["mongo"],
    "Machine Learning": ["ml"],
    "Deep Learning": ["dl"],
    "Scikit-Learn": ["sklearn", "scikit learn"],
    "CI/CD": ["cicd", "ci cd", "continuous integration", "continuous delivery"],
    "DevOps": ["dev ops"],
    "REST API": ["rest", "restful", "rest apis"],
    "GraphQL": ["gql"],
    "Object-Oriented Programming": ["oop", "object oriented"],
    "Penetration Testing": ["pentest", "pen testing"],
    "Power BI": ["powerbi"],
    "GitHub Actions": ["github action"],
    "FastAPI": ["fast api"],
    "Node.js": ["nodejs", "node js"],
    "Vue.js": ["vuejs", "vue js"],
    "C++": ["cpp", "c plus plus"],
    "C#": ["csharp", "c sharp"],
    ".NET": ["dotnet", "dot net", "asp.net", "aspnet"],
}


def _log(msg: str) -> None:
    print(msg, flush=True)


def _normalize_token(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def _derive_keywords(skill_id: str) -> list[str]:
    """Build lowercase keyword variants for posting text matching."""
    keywords: set[str] = set()
    base = skill_id.strip()
    if not base:
        return []

    keywords.add(_normalize_token(base))

    # Strip parenthetical qualifiers: "Python (Programming Language)" -> python
    no_paren = re.sub(r"\s*\([^)]*\)", "", base).strip()
    if no_paren:
        keywords.add(_normalize_token(no_paren))

    # Hyphen / space / dot variants
    for variant in {
        base.replace("-", " "),
        base.replace(" ", "-"),
        base.replace(".", ""),
        base.replace(".", " "),
    }:
        keywords.add(_normalize_token(variant))

    for alias in EXTRA_KEYWORD_ALIASES.get(skill_id, []):
        keywords.add(_normalize_token(alias))

    # Drop very short tokens unless explicitly seeded (e.g. "R", "C", "Go")
    min_len = 1 if skill_id in {"R", "C", "Go"} else 2
    return sorted(k for k in keywords if len(k) >= min_len)


def _fetch_lookup(
    db: wrds.Connection,
    library: str,
    table: str,
    skill_k15_buckets: tuple[str, ...],
) -> pd.DataFrame:
    buckets_sql = ", ".join(f"'{b.replace(chr(39), chr(39)+chr(39))}'" for b in skill_k15_buckets)
    sql = f"""
        SELECT
            skill_k35000 AS skill_id,
            skill_k15,
            skill_k50,
            skill_k150,
            skill_k500
        FROM {library}.{table}
        WHERE skill_k35000 IS NOT NULL
          AND skill_k15 IN ({buckets_sql})
        ORDER BY skill_k15, skill_k35000
    """
    _log(f"Fetching lookup rows from {library}.{table} ...")
    df = db.raw_sql(sql)
    _log(f"  {len(df)} rows in selected skill_k15 buckets")
    return df


def _fetch_profile_counts(
    db: wrds.Connection,
    skills_library: str,
    skills_table: str,
    lookup_library: str,
    lookup_table: str,
    skill_k15_buckets: tuple[str, ...],
    sample_percent: float,
    limit: int,
) -> pd.DataFrame:
    buckets_sql = ", ".join(f"'{b.replace(chr(39), chr(39)+chr(39))}'" for b in skill_k15_buckets)
    pct = max(0.01, min(float(sample_percent), 100.0))

    # WRDS may expose individual_user_skills as a view (TABLESAMPLE unsupported).
    sample_sql = f"""
        SELECT
            s.skill_k35000 AS skill_id,
            l.skill_k15,
            COUNT(*) AS profile_rows
        FROM (
            SELECT skill_k35000
            FROM {skills_library}.{skills_table} TABLESAMPLE SYSTEM ({pct})
            WHERE skill_k35000 IS NOT NULL
        ) AS s
        JOIN {lookup_library}.{lookup_table} AS l
          ON s.skill_k35000 = l.skill_k35000
        WHERE l.skill_k15 IN ({buckets_sql})
        GROUP BY 1, 2
        ORDER BY 3 DESC
        LIMIT {int(limit)}
    """
    join_sql = f"""
        SELECT
            s.skill_k35000 AS skill_id,
            l.skill_k15,
            COUNT(*) AS profile_rows
        FROM {skills_library}.{skills_table} AS s
        JOIN {lookup_library}.{lookup_table} AS l
          ON s.skill_k35000 = l.skill_k35000
        WHERE s.skill_k35000 IS NOT NULL
          AND l.skill_k15 IN ({buckets_sql})
        GROUP BY 1, 2
        ORDER BY 3 DESC
        LIMIT {int(limit)}
    """
    _log(
        f"Fetching profile popularity (TABLESAMPLE {pct}% on "
        f"{skills_library}.{skills_table}) ..."
    )
    try:
        return db.raw_sql(sample_sql)
    except Exception as exc:
        if "TABLESAMPLE" not in str(exc):
            raise
        _log(
            "  TABLESAMPLE not supported on this WRDS object; "
            f"falling back to full join count (slow): {exc}"
        )
        _log(f"  Running join count on {skills_library}.{skills_table} ...")
        return db.raw_sql(join_sql)


def _select_skills(
    lookup: pd.DataFrame,
    profile_counts: pd.DataFrame | None,
    seed_skills: set[str],
    limit: int,
    *,
    fill_from_lookup: bool,
) -> pd.DataFrame:
    if lookup.empty:
        return lookup

    work = lookup.drop_duplicates(subset=["skill_id"]).copy()
    work["is_seed"] = work["skill_id"].isin(seed_skills)

    if profile_counts is not None and not profile_counts.empty:
        pop = profile_counts.drop_duplicates(subset=["skill_id"]).set_index("skill_id")["profile_rows"]
        work["profile_rows"] = work["skill_id"].map(pop).fillna(0).astype(int)
    else:
        work["profile_rows"] = 0

    seeds = work[work["is_seed"]].sort_values(["skill_k15", "skill_id"])
    if not fill_from_lookup and work["profile_rows"].max() == 0:
        # Default: canonical seed list resolved against lookup only (no acronym noise).
        return seeds.reset_index(drop=True)

    work = work.sort_values(
        ["is_seed", "profile_rows", "skill_id"],
        ascending=[False, False, True],
    )
    seeds = work[work["is_seed"]]
    remainder = work[~work["is_seed"]].head(max(0, limit - len(seeds)))
    selected = pd.concat([seeds, remainder], ignore_index=True)
    selected = selected.drop_duplicates(subset=["skill_id"]).head(limit)
    return selected.sort_values(["skill_k15", "skill_id"]).reset_index(drop=True)


def _to_yaml_records(selected: pd.DataFrame) -> list[dict]:
    records: list[dict] = []
    for row in selected.itertuples(index=False):
        skill_id = str(row.skill_id)
        entry: dict = {
            "skill_id": skill_id,
            "skill_name": skill_id,
            "skill_k15": str(row.skill_k15),
            "keywords": _derive_keywords(skill_id),
        }
        if hasattr(row, "skill_k50") and pd.notna(row.skill_k50):
            entry["skill_k50"] = str(row.skill_k50)
        if hasattr(row, "profile_rows") and int(getattr(row, "profile_rows", 0)) > 0:
            entry["profile_rows_sample"] = int(row.profile_rows)
        records.append(entry)
    return records


def build_skill_keywords(
    *,
    output_path: Path,
    skill_k15_buckets: tuple[str, ...],
    seed_skills: tuple[str, ...],
    limit: int,
    profile_counts: bool,
    sample_percent: float,
    lookup_library: str,
    lookup_table: str,
    skills_library: str,
    skills_table: str,
    fill_from_lookup: bool,
    dry_run: bool,
) -> dict:
    username = os.getenv("WRDS_USERNAME", "")
    password = os.getenv("WRDS_PASSWORD")
    if not username:
        raise ValueError("Missing WRDS_USERNAME (set in .env)")

    seed_set = set(seed_skills)
    db = wrds.Connection(wrds_username=username, wrds_password=password)
    try:
        lookup = _fetch_lookup(db, lookup_library, lookup_table, skill_k15_buckets)
        pop_df = None
        if profile_counts:
            pop_df = _fetch_profile_counts(
                db,
                skills_library,
                skills_table,
                lookup_library,
                lookup_table,
                skill_k15_buckets,
                sample_percent,
                limit=max(limit * 3, 500),
            )
        selected = _select_skills(
            lookup, pop_df, seed_set, limit, fill_from_lookup=fill_from_lookup
        )
    finally:
        db.close()

    payload = {
        "version": datetime.now(timezone.utc).strftime("%Y.%m"),
        "description": (
            "Keyword -> skill_k35000 mapping for inferred posting skill demand. "
            "Generated from Revelio individual_user_skill_lookup via "
            "scripts/build_skill_keywords.py."
        ),
        "source": {
            "lookup_table": f"{lookup_library}.{lookup_table}",
            "skill_id_column": "skill_k35000",
            "skill_k15_buckets": list(skill_k15_buckets),
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "profile_counts_used": profile_counts,
        },
        "skills": _to_yaml_records(selected),
    }

    if dry_run:
        _log(f"Dry run: would write {len(payload['skills'])} skills to {output_path}")
        for skill in payload["skills"][:5]:
            _log(f"  {skill['skill_id']} ({skill['skill_k15']}): {skill['keywords'][:4]}")
        return payload

    output_path.parent.mkdir(parents=True, exist_ok=True)
    header = (
        "# Auto-generated by scripts/build_skill_keywords.py — edit seeds/aliases and re-run.\n"
        "# skill_id must match Revelio skill_k35000 exactly.\n"
    )
    output_path.write_text(
        header + yaml.safe_dump(payload, sort_keys=False, allow_unicode=True, width=100),
        encoding="utf-8",
    )
    _log(f"Wrote {len(payload['skills'])} skills to {output_path}")
    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build config/skill_keywords.yml from WRDS Revelio skill lookup."
    )
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--limit", type=int, default=150, help="Max skills to emit (default 150).")
    parser.add_argument(
        "--skill-k15",
        action="append",
        dest="skill_k15_buckets",
        help="skill_k15 bucket to include (repeatable). Defaults to CS-relevant buckets.",
    )
    parser.add_argument(
        "--profile-counts",
        action="store_true",
        help="Rank by profile skill frequency (TABLESAMPLE on individual_user_skills; slow).",
    )
    parser.add_argument("--sample-percent", type=float, default=1.0)
    parser.add_argument("--lookup-library", default=DEFAULT_LOOKUP_LIBRARY)
    parser.add_argument("--lookup-table", default=DEFAULT_LOOKUP_TABLE)
    parser.add_argument("--skills-library", default=DEFAULT_SKILLS_LIBRARY)
    parser.add_argument("--skills-table", default=DEFAULT_SKILLS_TABLE)
    parser.add_argument(
        "--fill-from-lookup",
        action="store_true",
        help="Without --profile-counts, pad to --limit from lookup (not recommended).",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    buckets = tuple(args.skill_k15_buckets) if args.skill_k15_buckets else DEFAULT_SKILL_K15_BUCKETS
    build_skill_keywords(
        output_path=Path(args.output),
        skill_k15_buckets=buckets,
        seed_skills=DEFAULT_SEED_SKILLS,
        limit=args.limit,
        profile_counts=args.profile_counts,
        sample_percent=args.sample_percent,
        lookup_library=args.lookup_library,
        lookup_table=args.lookup_table,
        skills_library=args.skills_library,
        skills_table=args.skills_table,
        fill_from_lookup=args.fill_from_lookup,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    main()
