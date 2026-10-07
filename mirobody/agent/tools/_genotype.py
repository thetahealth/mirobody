"""What the genetics and pharmacogenomics tools read alike: the person's active
genotype upload, and how an answer cites it. Underscore-prefixed so the tool
loader never publishes anything in here.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

#: One active upload per person; a new upload supersedes the last
#: (`collect/files/services/genetic_store.py`).
_ACTIVE_SET = (
    "SELECT id, vendor, format_id, build_declared, build_detected, n_rows, n_called, "
    "sex_inferred, normalizer_version, site_table_version "
    "FROM th_genotype_set WHERE user_id = :user_id AND status = 'active' LIMIT 1"
)


async def fetch_rows(execute: Any, sql: str, params: Mapping[str, Any]) -> list[dict[str, Any]]:
    """`sql`'s rows as dicts, through `execute` (a test's fake) or the database."""
    if execute is None:
        from mirobody.utils import execute_query

        execute = execute_query
    return [dict(row) for row in await execute(sql, dict(params)) or []]


async def active_set(execute: Any, user_id: str) -> dict[str, Any] | None:
    """The person's active genotype upload, or None when they have none."""
    found = await fetch_rows(execute, _ACTIVE_SET, {"user_id": user_id})
    return found[0] if found else None


def source_note(genotype_set: Mapping[str, Any]) -> str:
    """The upload an answer was read from, as its notes cite it."""
    return (
        f"source: genotype set {genotype_set['id']}, vendor={genotype_set.get('vendor') or genotype_set['format_id']}, "
        f"declared build={genotype_set.get('build_declared') or 'unknown'}, "
        f"detected build={genotype_set['build_detected']}; "
        f"normalizer={genotype_set['normalizer_version']}, sites={genotype_set['site_table_version']}"
    )


def in_clause(prefix: str, values: Sequence[str]) -> tuple[str, dict[str, Any]]:
    """The placeholders of an `IN (...)` list over `values`, and their parameters."""
    params = {f"{prefix}_{i}": value for i, value in enumerate(values)}
    return ", ".join(f":{key}" for key in params), params
