"""Lightweight embedding API — provider-agnostic, config-driven.

Usage::

    from mirobody.utils.embedding import text_embedding

    vectors = await text_embedding(["hello", "world"])                # default: gemini
    vectors = await text_embedding(["hello", "world"], provider="qwen")
"""

from __future__ import annotations

import asyncio
import logging
import os

import aiohttp

log = logging.getLogger(__name__)

# ── Retry settings ───────────────────────────────────────────────────

_EMB_MAX_RETRIES = 3
_EMB_RETRY_BACKOFF = (1, 2, 4)  # seconds

# ── Provider registry ────────────────────────────────────────────────
#
# Each factory returns (llm, url, batch_limit, make_body, parse).
# ``text_embedding()`` is completely provider-agnostic.

_EMB_PROVIDERS: dict[str, callable] = {}


def _emb_provider(name: str):
    """Decorator that registers an embedding provider factory."""
    def _register(fn):
        _EMB_PROVIDERS[name] = fn
        return fn
    return _register


@_emb_provider("gemini")
def _gemini():
    from mirobody.utils.config import global_config
    from mirobody.utils.config.llm import LLMProvider

    use_vertex = os.environ.get("GOOGLE_GENAI_USE_VERTEXAI", "0").lower() in ("true", "1")
    llm = global_config().get_llm(LLMProvider.VERTEX_AI if use_vertex else LLMProvider.GEMINI)
    model = "gemini-embedding-001"
    model_ref = f"publishers/google/models/{model}" if use_vertex else f"models/{model}"
    return (
        llm,
        f"{model_ref}:batchEmbedContents",
        100,
        lambda chunk: {"requests": [
            {"model": model_ref, "content": {"parts": [{"text": t}]}, "output_dimensionality": 1024}
            for t in chunk
        ]},
        lambda data: [item["values"] for item in data["embeddings"]],
    )


@_emb_provider("qwen")
def _qwen():
    from mirobody.utils.config import global_config
    from mirobody.utils.config.llm import LLMProvider

    return (
        global_config().get_llm(LLMProvider.DASHSCOPE),
        "embeddings",
        10,
        lambda chunk: {"model": "text-embedding-v4", "input": chunk, "dimensions": 1024},
        lambda data: [item["embedding"] for item in data["data"]],
    )


# ── Public API ───────────────────────────────────────────────────────

async def text_embedding(texts: list[str], provider: str = "gemini") -> list[list[float] | None]:
    """Compute 1024-dim embeddings via *provider*.

    Supported providers: ``"gemini"`` (auto Vertex AI), ``"qwen"``.
    Long input lists are chunked per provider batch limit.
    Invalid entries (non-str / blank) yield ``None`` at the same index.
    """
    if isinstance(texts, str):
        texts = [texts]

    # Sanitise: keep positional correspondence, only embed valid texts.
    valid_indices: list[int] = []
    clean_texts: list[str] = []
    for i, t in enumerate(texts):
        if isinstance(t, str) and (s := t.strip()):
            valid_indices.append(i)
            clean_texts.append(s)

    results: list[list[float] | None] = [None] * len(texts)
    if not clean_texts:
        return results

    factory = _EMB_PROVIDERS.get(provider)
    if not factory:
        raise ValueError(f"unknown embedding provider: {provider!r} (available: {', '.join(_EMB_PROVIDERS)})")
    llm, url, batch_limit, make_body, parse = factory()

    # Dedup before hitting the API; map back by text at the end.
    unique_texts: list[str] = list(dict.fromkeys(clean_texts))

    embedded: list[list[float]] = []
    async with llm.get_aiohttp_session(timeout=aiohttp.ClientTimeout(total=30)) as session:
        for i in range(0, len(unique_texts), batch_limit):
            body = make_body(unique_texts[i : i + batch_limit])
            for attempt in range(_EMB_MAX_RETRIES):
                async with session.post(url, json=body) as resp:
                    if resp.status == 200:
                        embedded.extend(parse(await resp.json()))
                        break
                    resp_body = await resp.text()
                    if resp.status in (429, 503) and attempt < _EMB_MAX_RETRIES - 1:
                        wait = _EMB_RETRY_BACKOFF[attempt]
                        log.warning(f"{provider} embedding API {resp.status}, retry in {wait}s (attempt {attempt + 1})")
                        await asyncio.sleep(wait)
                        continue
                    raise RuntimeError(f"{provider} embedding API error: {resp.status}, {resp_body}")

    if len(embedded) != len(unique_texts):
        raise RuntimeError(
            f"{provider} returned {len(embedded)} embeddings for {len(unique_texts)} unique texts"
        )

    text_to_embedding = dict(zip(unique_texts, embedded))
    for idx, text in zip(valid_indices, clean_texts):
        results[idx] = text_to_embedding[text]
    return results
