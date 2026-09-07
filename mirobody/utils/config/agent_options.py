"""Parsing for the agent's config keys: ``PROMPTS`` and ``PROVIDERS``.

These two are the only parts of `Config` that know anything about the agent,
and the only parts that do real work rather than read a key: one resolves
prompt-template *references* into template *text* (from the working directory
or from the installed package), the other normalises a provider table that YAML
may legitimately express three different ways.

They are plain functions, taking the reader they need (`get`, i.e. any
`Config`) rather than the whole object, so a test can pass a dict-backed stub.
`Config.get_agent_settings` is the caching around them.
"""

from __future__ import annotations

import importlib.resources
import json
import logging
import os
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class _Reader(Protocol):
    """The slice of `Config` these functions actually use."""

    def get(self, key: str, default: Any = None) -> Any: ...


#-----------------------------------------------------------------------------


def _package_root():
    """The installed `mirobody` package as a traversable, or None.

    Prompts ship inside the wheel, so a deployment that runs from site-packages
    (rather than a source checkout) resolves them through this rather than
    through the filesystem.
    """
    try:
        return importlib.resources.files("mirobody")
    except Exception:
        return None


def _as_list(raw: Any) -> list:
    """Coerce a config value that may arrive as a JSON string into a list.

    YAML gives a real list; an environment variable can only give a string, so
    a JSON-encoded list is accepted too, and a bare string is a one-element
    list.
    """
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception as e:
            logger.error(str(e), exc_info=True)
        else:
            if isinstance(parsed, list):
                return parsed
        return [raw]
    return raw if isinstance(raw, list) else []


def load_prompt_templates(cfg: _Reader, key: str = "PROMPTS") -> dict[str, str]:
    """Resolve ``PROMPTS`` entries into ``{name: template_text}``.

    Each entry is a path, optionally ``path@name`` to override the key (the
    default key is the basename without ``.jinja``). A path is looked up on the
    filesystem first, then inside the installed package. If neither resolves,
    the *path itself* is kept as the value — the historical behaviour, which
    lets a deployment pass a literal prompt string where a path is expected.
    Entries that resolve to no name at all are numbered ``Prompt_1``, ``…_2``.
    """
    templates: dict[str, str] = {}
    entries = _as_list(cfg.get(key))
    if not entries:
        return templates

    dist = _package_root()
    unnamed = 0

    for entry in entries:
        if not isinstance(entry, str):
            continue

        file_path, explicit_key = entry, ""
        if "@" in entry:
            parts = entry.rsplit("@", 1)
            if len(parts) == 2:
                file_path, explicit_key = parts[0].strip(), parts[1].strip()

        key, value = "", file_path

        if os.path.isfile(file_path):
            try:
                with open(file_path) as f:
                    value = f.read()
                key = explicit_key or _default_key(file_path)
            except Exception as e:
                logger.warning(str(e), exc_info=True)
                value = file_path

        elif dist and dist.is_dir():
            try:
                value = dist.joinpath(file_path).read_text(encoding="utf-8")
                key = explicit_key or _default_key(file_path)
            except Exception as e:
                logger.warning(str(e), exc_info=True)
                value = file_path

        if not key:
            unnamed += 1
            key = f"Prompt_{unnamed}"

        templates[key] = value

    return templates


def _default_key(file_path: str) -> str:
    return os.path.basename(file_path).removesuffix(".jinja").strip()


def parse_providers(cfg: _Reader, key: str = "PROVIDERS") -> dict[str, dict]:
    """Normalise ``PROVIDERS`` into ``{provider_name: settings}``.

    Three accepted shapes, because all three appear in the wild:
      * a mapping of name → settings (what config.yaml uses), taken as-is;
      * a list of settings dicts each carrying a ``provider`` key, which names
        it and is then removed from the settings;
      * a JSON string of either, for environment-variable configuration.
    """
    raw = cfg.get(key)
    if not raw:
        return {}

    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception as e:
            logger.error(str(e), exc_info=True)
        else:
            if isinstance(parsed, (dict, list)):
                raw = parsed

    if isinstance(raw, dict):
        return raw

    providers: dict[str, dict] = {}
    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict) or "provider" not in item:
                continue
            name = item["provider"]
            if not isinstance(name, str) or not name.strip():
                continue
            item = {k: v for k, v in item.items() if k != "provider"}
            providers[name.strip()] = item

    return providers
