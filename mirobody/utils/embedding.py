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
from typing import Literal

import aiohttp

log = logging.getLogger(__name__)

# ── Retry settings ───────────────────────────────────────────────────

_EMB_MAX_RETRIES = 3
_EMB_RETRY_BACKOFF = (1, 2, 4)  # seconds
_EMB_RETRY_STATUSES = (408, 429, 502, 503, 504)

# ── Provider registry ────────────────────────────────────────────────
#
# Each factory returns (llm, url, batch_limit, max_concurrency, make_body, parse).
# max_concurrency=1: sequential, fail-fast. >1: asyncio.gather + Semaphore fan-out.

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

    if use_vertex:
        # Vertex :predict accepts one input per request for this model;
        # text_embedding() fans chunks out concurrently to mask round-trip latency.
        model_ref = f"publishers/google/models/{model}"
        return (
            llm,
            f"{model_ref}:predict",
            1,
            10,  # max_concurrency
            lambda chunk: {
                "instances"  : [{"content": chunk[0]}],
                "parameters" : {"outputDimensionality": 1024},
            },
            lambda data: [item["embeddings"]["values"] for item in data["predictions"]],
        )

    model_ref = f"models/{model}"
    return (
        llm,
        f"{model_ref}:batchEmbedContents",
        100,
        1,  # max_concurrency
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
        1,  # max_concurrency
        lambda chunk: {"model": "text-embedding-v4", "input": chunk, "dimensions": 1024},
        lambda data: [item["embedding"] for item in data["data"]],
    )


# ── Public API ───────────────────────────────────────────────────────

# Snapshot of provider names registered above. Callers that need to validate
# untrusted provider inputs (e.g. before interpolating into a SQL column name)
# should check against this instead of hardcoding their own allowlist.
EMBEDDING_PROVIDERS: frozenset[str] = frozenset(_EMB_PROVIDERS)


async def text_embedding(
    texts: list[str],
    provider: Literal["gemini", "qwen"] | None = None,
) -> list[list[float] | None]:
    """Compute 1024-dim embeddings via *provider*.

    Supported providers: ``"gemini"`` (auto Vertex AI), ``"qwen"``.
    When *provider* is ``None``, reads config key ``EMBEDDING_PROVIDER`` (default: ``"gemini"``).
    Long input lists are chunked per provider batch limit.
    Invalid entries (non-str / blank) yield ``None`` at the same index.
    """
    if provider is None:
        from .config import safe_read_cfg
        provider = safe_read_cfg("EMBEDDING_PROVIDER", "gemini")

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
    llm, url, batch_limit, max_concurrency, make_body, parse = factory()

    unique_texts: list[str] = list(dict.fromkeys(clean_texts))

    embedded: list[list[float]] = []
    async with llm.get_aiohttp_session(timeout=aiohttp.ClientTimeout(total=30)) as session:
        async def _post_with_retry(body: dict) -> list[list[float]]:
            for attempt in range(_EMB_MAX_RETRIES):
                try:
                    async with session.post(url, json=body) as resp:
                        if resp.status == 200:
                            return parse(await resp.json())
                        resp_body = (await resp.text())[:500]
                        if resp.status in _EMB_RETRY_STATUSES and attempt < _EMB_MAX_RETRIES - 1:
                            wait = _EMB_RETRY_BACKOFF[attempt]
                            log.warning(f"{provider} embedding API {resp.status}, retry in {wait}s (attempt {attempt + 1})")
                            await asyncio.sleep(wait)
                            continue
                        raise RuntimeError(f"{provider} embedding API error: {resp.status}, {resp_body}")
                except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                    if attempt < _EMB_MAX_RETRIES - 1:
                        wait = _EMB_RETRY_BACKOFF[attempt]
                        log.warning(f"{provider} embedding network error: {e!r}, retry in {wait}s (attempt {attempt + 1})")
                        await asyncio.sleep(wait)
                        continue
                    raise
            raise RuntimeError(f"{provider} embedding API: exhausted retries")

        chunks = [unique_texts[i : i + batch_limit] for i in range(0, len(unique_texts), batch_limit)]
        if max_concurrency > 1:
            sem = asyncio.Semaphore(max_concurrency)

            async def _bounded(chunk: list[str]) -> list[list[float]]:
                async with sem:
                    return await _post_with_retry(make_body(chunk))

            chunk_results = await asyncio.gather(*[_bounded(c) for c in chunks])
            embedded = [v for r in chunk_results for v in r]
        else:
            for c in chunks:
                embedded.extend(await _post_with_retry(make_body(c)))

    if len(embedded) != len(unique_texts):
        raise RuntimeError(
            f"{provider} returned {len(embedded)} embeddings for {len(unique_texts)} unique texts"
        )

    text_to_embedding = dict(zip(unique_texts, embedded))
    for idx, text in zip(valid_indices, clean_texts):
        results[idx] = text_to_embedding[text]
    return results
