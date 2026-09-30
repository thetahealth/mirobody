"""Model settings saved from the first-run page, for a deployment set up in the
browser rather than in `.env`.

Only the names that choose where health data goes are accepted: each vendor's
key and the two local-model addresses. They are applied to this process's
environment, which is where every model entry reads them, so nothing else has
to know where a value came from. A name the environment already held when the
process started wins over a saved row: `.env` stays the authority for whoever
uses it, and the page only fills what it left empty.
"""

from __future__ import annotations

import logging
import os
import time

from mirobody.utils.config.llm import KEYS_URL

logger = logging.getLogger(__name__)

ENDPOINT_NAMES = ("LOCAL_BASE_URL", "LOCAL_OCR_BASE_URL")

#: Names the environment held before any saved row was applied; filled on the
#: first `apply()`, after the config files have put their keys into os.environ.
_from_environment: frozenset[str] | None = None
_applied_at = 0.0


def allowed_names() -> frozenset[str]:
    return frozenset(KEYS_URL) | frozenset(ENDPOINT_NAMES)


def set_in_environment() -> frozenset[str]:
    """The allowed names `.env` (or a config file) set; the page shows them as
    fixed rather than offering to replace them."""
    if _from_environment is None:
        return frozenset(n for n in allowed_names() if os.environ.get(n))
    return _from_environment


async def load() -> dict[str, str]:
    from mirobody.utils import execute_query

    try:
        rows = await execute_query("SELECT name, decrypt_content(value) AS value FROM th_setting", log_sql=False) or []
    except Exception as e:
        logger.warning("settings not read: error_type=%s", type(e).__name__)
        return {}
    allowed = allowed_names()
    return {str(r["name"]): str(r["value"] or "") for r in rows if r["name"] in allowed}


async def apply() -> int:
    """Put the saved values into os.environ; returns how many were applied."""
    global _from_environment, _applied_at
    if _from_environment is None:
        _from_environment = frozenset(n for n in allowed_names() if os.environ.get(n))
    saved = await load()
    applied = 0
    for name in allowed_names() - _from_environment:
        value = saved.get(name, "")
        if value:
            applied += os.environ.get(name) != value
            os.environ[name] = value
        else:
            os.environ.pop(name, None)
    _applied_at = time.monotonic()
    return applied


async def refresh(every_sec: float = 30.0) -> int:
    """`apply()` at most once per `every_sec`: the worker calls it between tasks."""
    if time.monotonic() - _applied_at < every_sec:
        return 0
    return await apply()


async def save(values: dict[str, str]) -> None:
    """Replace the saved values for these names (an empty value removes one)."""
    from mirobody.utils import execute_query

    unknown = set(values) - allowed_names()
    if unknown:
        raise ValueError(f"not a model setting: {sorted(unknown)}")
    for name, value in values.items():
        if value:
            await execute_query(
                "INSERT INTO th_setting (name, value) VALUES (:name, encrypt_content(:value))"
                " ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
                {"name": name, "value": value}, log_sql=False)
        else:
            await execute_query("DELETE FROM th_setting WHERE name = :name", {"name": name}, log_sql=False)
    await apply()


__all__ = ["ENDPOINT_NAMES", "allowed_names", "apply", "load", "refresh", "save", "set_in_environment"]
