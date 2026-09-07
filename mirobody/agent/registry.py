"""The one agent, and how a deployment replaces it.

mirobody ships exactly one agent — `MirobodyAgent`, on the deepagents harness
— and does not switch between agents at request time. What this module
provides is REPLACEMENT, from two places: an installed distribution that
declares a `mirobody.agents` entry point (a `pip install`ed harness) is looked
at first, then the `AGENT_DIRS` directories in order; the first class that
defines `generate_response` becomes the agent for the whole process. A
deployment that wants its own harness ships it as a plugin or points
`AGENT_DIRS` at its own directory (an overlay replaces the list, it does not
append to it) and never touches this package; everything else — the MCP
tools, the chat endpoints, the wire format — is the same.

The contract a replacement has to meet is the two methods on `AbstractAgent`.
The per-agent config keys that used to be suffixed with the agent's name
(`PROVIDERS_<NAME>`, `PROMPTS_<NAME>`, ...) are plain `PROVIDERS`, `PROMPTS`,
`ALLOWED_TOOLS`, `DISALLOWED_TOOLS`: one agent, one set of keys.

This module holds the process-wide state the chat layer reads — the agent
class and its LLM clients — and nothing else. It does not import LangChain.
"""

from __future__ import annotations

import inspect
import logging
import os
from collections.abc import AsyncGenerator
from types import ModuleType
from typing import Any

from ..utils import Config, global_config
from ..utils.plugin_dirs import GROUP_AGENTS, entry_point_modules, import_plugin_module, resolve_plugin_dir

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------


class AbstractAgent:
    """What a replacement agent has to provide.

    `generate_response` is called once per turn with the kwargs
    `ChatProtocolAdapter._prepare_agent_kwargs` builds (`user_id`, `session_id`,
    `messages`, `provider`, `prompt_name`, `file_list`, ...) and yields chunk
    dicts `{"type": ..., "content": ...}` — see `agent/README.md` for the types.
    `load_llm_clients` is optional: given the `PROVIDERS` table it returns
    `{provider_name: client}`; an agent that needs no model returns `{}`.
    """

    def __init__(self, **kwargs):
        pass

    async def generate_response(self, *args: Any, **kwargs: Any) -> AsyncGenerator[dict[str, Any], None]: ...

    @classmethod
    def load_llm_clients(cls, llm_client_config: dict[str, Any]) -> dict[str, Any]:
        return {}


#-----------------------------------------------------------------------------

_agent_class: type | None = None
_llm_clients: dict[str, Any] = {}


def _agent_classes_in(module: ModuleType, module_name: str) -> list[type]:
    """Classes DEFINED in `module` (not imported into it) with a `generate_response`."""
    try:
        classes = inspect.getmembers(module, predicate=inspect.isclass)
    except Exception:
        logger.warning("could not inspect an agent module: module_kind=%s", type(module).__name__)
        return []
    found = []
    for _, klass in classes:
        if inspect.isabstract(klass) or klass.__module__ != module_name or klass is AbstractAgent:
            continue
        if callable(getattr(klass, "generate_response", None)):
            found.append(klass)
    return found


def load_agent(dirs: list[str], config: Config | None = None) -> type | None:
    """Installed `mirobody.agents` plugins first, then `dirs` in order; the
    first agent class found becomes THE agent.

    Every further candidate is logged by name and ignored — there is no second
    slot. Loads the agent's LLM clients from the `PROVIDERS` table when the
    class offers `load_llm_clients`. Returns the class, or None when no
    directory yielded one (the chat endpoints then answer "no agent").
    """
    global _agent_class, _llm_clients

    candidates: list[type] = []
    for module in entry_point_modules(GROUP_AGENTS):
        candidates.extend(_agent_classes_in(module, module.__name__))
    for dir in dirs or []:
        if not dir:
            continue
        target_directory, module_name_prefix = resolve_plugin_dir(dir)
        if not target_directory:
            logger.warning("an AGENT_DIRS entry is not a directory")
            continue
        try:
            entries = sorted(os.scandir(target_directory), key=lambda e: e.name)
        except Exception:
            logger.warning("could not scan an AGENT_DIRS directory")
            continue
        for entry in entries:
            if entry.is_dir() or not entry.name.lower().endswith(".py") \
                    or entry.name.startswith("_") or entry.name.startswith("test_"):
                continue
            try:
                module_name, module = import_plugin_module(target_directory, module_name_prefix, entry.name)
            except Exception as e:
                logger.warning("could not import an agent module: error_type=%s", type(e).__name__)
                continue
            candidates.extend(_agent_classes_in(module, module_name))

    if not candidates:
        logger.warning("no agent class found in AGENT_DIRS: dir_count=%d", len(dirs or []))
        _agent_class, _llm_clients = None, {}
        return None

    klass, extra = candidates[0], candidates[1:]
    if extra:
        logger.warning(
            "one agent only: using agent_class=%s, ignored_count=%d",
            klass.__name__, len(extra),
        )

    clients: dict[str, Any] = {}
    loader = getattr(klass, "load_llm_clients", None)
    cfg = config or global_config()
    if callable(loader) and cfg:
        providers = (cfg.get_agent_settings() or {}).get("providers") or {}
        try:
            clients = loader(providers) or {}
        except Exception as e:
            logger.error("agent LLM clients failed to load: agent_class=%s error_type=%s", klass.__name__, type(e).__name__)
            clients = {}

    _agent_class, _llm_clients = klass, clients
    logger.info("agent loaded: agent_class=%s provider_count=%d", klass.__name__, len(clients))
    return klass


#-----------------------------------------------------------------------------


def agent_class() -> type | None:
    return _agent_class


def agent_name() -> str:
    """The class name without its `Agent` suffix — what `th_messages.agent` records."""
    if _agent_class is None:
        return ""
    return _agent_class.__name__.removesuffix("Agent") or _agent_class.__name__


def new_agent(**kwargs) -> AbstractAgent | None:
    """A fresh agent instance for one turn, built on the `PROVIDERS` /
    `PROMPTS` / `ALLOWED_TOOLS` / `DISALLOWED_TOOLS` settings plus `kwargs`."""
    if _agent_class is None:
        return None
    options: dict[str, Any] = {}
    cfg = global_config()
    if cfg:
        settings = cfg.get_agent_settings()
        if isinstance(settings, dict):
            options.update(settings)
    options.update(kwargs)
    return _agent_class(**options)


def llm_client(provider: str) -> Any | None:
    return _llm_clients.get(provider) if provider else None


def llm_client_names() -> list[str]:
    return list(_llm_clients)


def available_models() -> list[str]:
    """The `/api/models` list: provider names whose key resolves RIGHT NOW.

    Unfiltered, this listed every configured provider — five models on a
    zero-key deployment — so the picker offered choices that could only fail
    at chat time. A provider appears only when its client was loaded at
    startup AND its `api_key` reference resolves non-empty (recomputed per
    call: removing a key hides its model on the next request; adding one still
    needs a restart, because the client itself is built at boot).
    """
    if not _llm_clients:
        return []
    from ..utils.config import safe_read_cfg

    cfg = global_config()
    providers = (cfg.get_agent_settings() or {}).get("providers") or {} if cfg else {}
    names = []
    for name in _llm_clients:
        key_name = (providers.get(name) or {}).get("api_key", "")
        if key_name and not safe_read_cfg(key_name):
            continue
        names.append(name)
    return sorted(names)
