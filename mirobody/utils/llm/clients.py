"""One cached OpenAI-compatible client per resolved route.

Every utility surface (vision, text, structured extraction) ends in a
`RouteSpec`: an entry from `config.llm.yaml` with its key and endpoint
resolved, and this is the only place that turns one into an SDK client. A
call site that constructs `AsyncOpenAI(base_url=...)` itself is how issue #52
happened (vision hardcoded openrouter.ai while everything else followed
`OPENROUTER_BASE_URL`), so there is one constructor, keyed on the endpoint
and the key name.
"""

import logging

from openai import AsyncOpenAI, OpenAI

from mirobody.utils.config.llm import RouteSpec

logger = logging.getLogger(__name__)


class AIClientManager:
    """AI client manager: one cached async client per (endpoint, key)."""

    def __init__(self):
        self._async_clients: dict[tuple, AsyncOpenAI] = {}

    def for_spec(self, spec: RouteSpec) -> AsyncOpenAI:
        """The cached async client for a resolved route.

        Raises `ValueError` when the route's key is absent: the OpenAI SDK
        would accept an empty key and fail with a 401 at request time, one
        layer further from the person who can fix it.
        """
        key = spec.key
        if spec.api_key_env and not key:
            raise ValueError(f"{spec.alias}: {spec.api_key_env} is not set")
        # The key's value, not only its name: the first-run page can replace a
        # key in a running process, and the old client would keep the old one.
        cache_key = (spec.base_url, spec.api_key_env, key, spec.timeout, spec.max_retries)
        if cache_key not in self._async_clients:
            self._async_clients[cache_key] = AsyncOpenAI(api_key=key or "-", base_url=spec.base_url or None, **_limits(spec))
        return self._async_clients[cache_key]

    @staticmethod
    def sync_for_spec(spec: RouteSpec) -> OpenAI:
        """The sync twin, for a future sync call site: the alternative it
        replaces is an inline `OpenAI(base_url="https://...")`."""
        key = spec.key
        if spec.api_key_env and not key:
            raise ValueError(f"{spec.alias}: {spec.api_key_env} is not set")
        return OpenAI(api_key=key or "-", base_url=spec.base_url or None, **_limits(spec))


def _limits(spec: RouteSpec) -> dict:
    """An entry's `timeout` / `max_retries`, when it declares them. A local
    server that needs 20 minutes for a long extraction was cut at the SDK's
    600 s and asked twice more, from scratch: 1,801 s for nothing (1.5.4)."""
    limits: dict = {}
    if spec.timeout is not None:
        limits["timeout"] = spec.timeout
    if spec.max_retries is not None:
        limits["max_retries"] = spec.max_retries
    return limits


client_manager = AIClientManager()
