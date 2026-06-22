"""Skill demand API metadata and query filters."""

from __future__ import annotations

SKILLS_STATUS = "inferred"
SKILLS_METHOD = "keyword_posting_text"
SKILLS_SOURCE = "postings_cosmos_raw.description"

# Placeholder skill ids excluded from API responses unless include_unknown=true.
PLACEHOLDER_SKILL_IDS = ("UNK", "UNKNOWN")


def skill_filter_clause(
    *,
    column: str = "skill_id",
    include_unknown: bool = False,
) -> str:
    """SQL fragment excluding placeholder skill ids."""
    if include_unknown:
        return "TRUE"
    placeholders = ", ".join(f"'{value}'" for value in PLACEHOLDER_SKILL_IDS)
    return f"{column} NOT IN ({placeholders})"
