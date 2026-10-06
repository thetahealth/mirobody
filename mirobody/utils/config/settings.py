"""Model settings saved from the first-run page, for a deployment set up in the
browser rather than in `.env`.

Only the names that choose where health data goes, and which model reads it,
are accepted: each vendor's key, the two local-model addresses, and the
variables an entry names to replace its model (`model_env`). They are applied to this process's
environment, which is where every model entry reads them, so nothing else has
to know where a value came from. A name the environment or a config file
already answered when the process started (a key under any of its aliases
too) wins over a saved row: `.env` stays the authority for whoever uses it,
and the page only fills what it left empty.
"""

from __future__ import annotations

import logging
import os
import time

from mirobody.utils.config.llm import KEYS_URL

logger = logging.getLogger(__name__)

ENDPOINT_NAMES = ("LOCAL_BASE_URL", "LOCAL_OCR_BASE_URL")
#: What `encrypt_content` prefixes. `decrypt_content` answers with the
#: ciphertext itself when the connection's key is not the one a row was saved
#: under, and a "key" of `gAAAA...` would pass every presence check.
_CIPHER_PREFIX = "gAAAA"

#: Names the environment held before any saved row was applied; filled on the
#: first `apply()`, after the config files have put their keys into os.environ.
_from_environment: frozenset[str] | None = None
_applied_at = 0.0


def allowed_names() -> frozenset[str]:
    """Each vendor's key, the two local addresses, and every variable a
    `MODELS` entry lets replace its model (`model_env`)."""
    from mirobody.utils.config.llm import model_env_names

    return frozenset(KEYS_URL) | frozenset(ENDPOINT_NAMES) | model_env_names()


def _set_now() -> frozenset[str]:
    """The allowed names that hold a value now, counting a key's aliases
    (`GEMINI_API_KEY` is `GOOGLE_API_KEY`) and the config files, the same
    reading every entry gets: a page value for a name `.env` already answers
    through an alias would have replaced it."""
    from mirobody.utils.config.llm import endpoint_value, read_api_key

    return frozenset(n for n in allowed_names()
                     if (read_api_key(n) if n.endswith("_API_KEY") else endpoint_value(n)))


def set_in_environment() -> frozenset[str]:
    """The allowed names `.env` (or a config file) set; the page shows them as
    fixed rather than offering to replace them."""
    return _set_now() if _from_environment is None else _from_environment


async def load() -> dict[str, str] | None:
    """The saved values, or None when they could not be read: the caller then
    keeps what it applied last, rather than taking a failed read for "nothing
    saved" and dropping the model. A value that did not decrypt is left out."""
    from mirobody.utils import execute_query

    try:
        rows = await execute_query("SELECT name, decrypt_content(value) AS value FROM th_setting", log_sql=False) or []
    except Exception as e:
        logger.warning("settings not read: error_type=%s", type(e).__name__)
        return None
    allowed = allowed_names()
    saved, undecrypted = {}, 0
    for r in rows:
        if r["name"] not in allowed:
            continue
        value = str(r["value"] or "")
        if value.startswith(_CIPHER_PREFIX):
            undecrypted += 1
            continue
        saved[str(r["name"])] = value
    if undecrypted:
        logger.warning("settings not decrypted under this PG_ENCRYPTION_KEY: count=%d", undecrypted)
    return saved


async def apply() -> int:
    """Put the saved values into os.environ; returns how many were applied."""
    global _from_environment, _applied_at
    if _from_environment is None:
        _from_environment = _set_now()
    saved = await load()
    _applied_at = time.monotonic()
    if saved is None:
        return 0
    applied = 0
    for name in allowed_names() - _from_environment:
        value = saved.get(name, "")
        if value:
            applied += os.environ.get(name) != value
            os.environ[name] = value
        else:
            os.environ.pop(name, None)
    return applied


async def refresh(every_sec: float = 30.0) -> int:
    """`apply()` at most once per `every_sec`: the worker calls it between tasks."""
    if time.monotonic() - _applied_at < every_sec:
        return 0
    return await apply()


async def save(values: dict[str, str]) -> None:
    """Replace the saved values for these names (an empty value removes one),
    all or none: a key saved beside a local address it was meant to replace
    would leave the two choices half applied."""
    from mirobody.utils.db import transaction

    unknown = set(values) - allowed_names()
    if unknown:
        raise ValueError(f"not a model setting: {sorted(unknown)}")
    async with transaction() as tx:
        for name, value in values.items():
            if value:
                await tx.execute(
                    "INSERT INTO th_setting (name, value) VALUES (:name, encrypt_content(:value))"
                    " ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, updated_at = now()",
                    {"name": name, "value": value})
            else:
                await tx.execute("DELETE FROM th_setting WHERE name = :name", {"name": name})
    await apply()


__all__ = ["ENDPOINT_NAMES", "allowed_names", "apply", "load", "refresh", "save", "set_in_environment"]
