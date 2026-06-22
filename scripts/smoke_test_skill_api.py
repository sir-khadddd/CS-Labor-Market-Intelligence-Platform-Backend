"""Smoke tests for inferred skill API behavior (Postgres-backed, no httpx required)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg

from api.skill_config import SKILLS_METHOD, SKILLS_STATUS, skill_filter_clause
from config.postgres import get_postgres_dsn


def _check(name: str, ok: bool, detail: str = "") -> None:
    status = "PASS" if ok else "FAIL"
    msg = f"[{status}] {name}"
    if detail:
        msg += f" — {detail}"
    print(msg)
    if not ok:
        raise SystemExit(1)


def main() -> None:
    dsn = get_postgres_dsn()
    skill_filter = skill_filter_clause()

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(DISTINCT skill_id) FROM analytics.cs_skill_demand WHERE {skill_filter}"
            )
            distinct_skills = cur.fetchone()[0]
            _check("distinct inferred skills", distinct_skills > 100, f"{distinct_skills} skills")

            cur.execute(
                f"SELECT COUNT(*) FROM analytics.cs_skill_demand WHERE skill_id IN ('UNK', 'UNKNOWN')"
            )
            unk_rows = cur.fetchone()[0]
            _check("no UNK rows in mart", unk_rows == 0, f"{unk_rows} UNK rows")

            cur.execute(
                f"SELECT COUNT(*) FROM analytics.cs_skill_demand WHERE {skill_filter}"
            )
            total = cur.fetchone()[0]
            _check("skill demand rows", total > 100_000, f"{total:,} rows")

            cur.execute(
                f"""
                SELECT skill_id, skill_name, skill_posting_count
                FROM analytics.cs_skill_demand
                WHERE role_id = %s AND {skill_filter}
                ORDER BY skill_posting_count DESC
                LIMIT 5
                """,
                ["SOFTWARE ENGINEERING"],
            )
            top = cur.fetchall()
            _check("software engineering top skills", len(top) == 5, str(top[0][0] if top else "empty"))

            cur.execute(
                f"SELECT COUNT(*) FROM analytics.role_skill_associations WHERE {skill_filter}"
            )
            assoc = cur.fetchone()[0]
            _check("role_skill_associations", assoc > 1000, f"{assoc:,} rows")

    _check("skills_status constant", SKILLS_STATUS == "inferred")
    _check("skills_method constant", SKILLS_METHOD == "keyword_posting_text")

    # Optional HTTP check if server is up
    try:
        import httpx

        response = httpx.get("http://127.0.0.1:8000/api/v1/info", timeout=2.0)
        if response.status_code == 200:
            payload = response.json()
            _check("HTTP /api/v1/info", payload.get("skills_status") == "inferred")
        else:
            print("[SKIP] HTTP /api/v1/info — server not responding on :8000")
    except Exception:
        print("[SKIP] HTTP checks — start uvicorn api.main:app to test live routes")

    print("All smoke checks passed.")


if __name__ == "__main__":
    main()
