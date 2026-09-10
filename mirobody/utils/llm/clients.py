"""One cached OpenAI-compatible client per resolved route.

Every utility surface (vision, text, structured extraction) ends in a
`RouteSpec` — an entry from `config.llm.yaml` with its key and endpoint
resolved — and this is the only place that turns one into an SDK client. A
call site that constructs `AsyncOpenAI(base_url=...)` itself is how issue #52
happened (vision hardcoded openrouter.ai while everything else followed
`OPENROUTER_BASE_URL`), so there is one constructor, keyed on the endpoint
and the key name.
"""

import logging

from openai import AsyncOpenAI, OpenAI

from ..config.llm import RouteSpec

logger = logging.getLogger(__name__)


class AIClientManager:
    """AI client manager — one cached async client per (endpoint, key)."""

    def __init__(self):
        self._async_clients: dict[tuple[str, str], AsyncOpenAI] = {}

    def for_spec(self, spec: RouteSpec) -> AsyncOpenAI:
        """The cached async client for a resolved route.

        Raises `ValueError` when the route's key is absent: the OpenAI SDK
        would accept an empty key and fail with a 401 at request time, one
        layer further from the person who can fix it.
        """
        key = spec.key
        if spec.api_key_env and not key:
            raise ValueError(f"{spec.alias}: {spec.api_key_env} is not set")
        cache_key = (spec.base_url, spec.api_key_env)
        if cache_key not in self._async_clients:
            self._async_clients[cache_key] = AsyncOpenAI(api_key=key or "-", base_url=spec.base_url or None)
        return self._async_clients[cache_key]

    @staticmethod
    def sync_for_spec(spec: RouteSpec) -> OpenAI:
        """The sync twin, for a future sync call site — the alternative it
        replaces is an inline `OpenAI(base_url="https://...")`."""
        key = spec.key
        if spec.api_key_env and not key:
            raise ValueError(f"{spec.alias}: {spec.api_key_env} is not set")
        return OpenAI(api_key=key or "-", base_url=spec.base_url or None)


client_manager = AIClientManager()
