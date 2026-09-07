"""An agent from a plugin — the REPLACEMENT slot (`mirobody/agent/registry.py`).

Two methods are the whole contract. This one answers without a model so the
replacement path can be exercised with no key; a real harness would build a
model client in `load_llm_clients` from the `PROVIDERS` table it is handed.
"""

from __future__ import annotations

from typing import Any


class EchoAgent:
    def __init__(self, **kwargs):
        pass

    @classmethod
    def load_llm_clients(cls, providers: dict[str, Any]) -> dict[str, Any]:
        return {}

    async def generate_response(self, user_id: str, messages: list[dict], **kwargs):
        last = messages[-1]["content"] if messages else ""
        yield {"type": "reply", "content": f"You said: {last}"}
