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
import sqlite3
import threading
from pathlib import Path
from typing import Literal

import aiohttp

log = logging.getLogger(__name__)

# ── Retry settings ───────────────────────────────────────────────────

_EMB_MAX_RETRIES = 3
_EMB_RETRY_BACKOFF = (1, 2, 4)  # seconds
_EMB_RETRY_STATUSES = (408, 429, 502, 503, 504)

# ── Disk cache (opt-in) ──────────────────────────────────────────────
#
# Single-process sqlite at ``~/.cache/mirobody/text_embedding.sqlite``
# keyed on ``(provider, text)``. Vector stored as raw float32 bytes
# (4 KB per 1024-dim row). WAL mode for safe concurrent reads from the
# async event loop; writes go through a threading.Lock since sqlite3
# isn't async-safe for write transactions.
#
# Opt-in (``text_embedding(..., cache=True)``) — bulk index builds
# (`fhir/embeddings/ref.py` embeds ~700K unique concepts that won't
# re-occur) would bloat the cache to multi-GB with zero hit rate.
# Query-side callers (resolve, benchmarks, search) opt in.
#
# To invalidate: ``rm ~/.cache/mirobody/text_embedding.sqlite``.
# Model-version changes aren't auto-detected — clear the cache when
# the provider's underlying model version is bumped.

_CACHE_PATH = Path.home() / ".cache" / "mirobody" / "text_embedding.sqlite"
_CACHE_PARAM_LIMIT = 500  # sqlite3 SQLITE_MAX_VARIABLE_NUMBER conservatively
_cache_conn: sqlite3.Connection | None = None
_cache_lock = threading.Lock()


def _get_embedding_cache() -> sqlite3.Connection:
    global _cache_conn
    with _cache_lock:
        if _cache_conn is None:
            _CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(_CACHE_PATH), check_same_thread=False)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS embeddings (
                    provider TEXT NOT NULL,
                    text     TEXT NOT NULL,
                    vector   BLOB NOT NULL,
                    PRIMARY KEY (provider, text)
                )
            """)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.commit()
            _cache_conn = conn
        return _cache_conn


def _cache_lookup(provider: str, texts: list[str]) -> dict[str, list[float]]:
    """Bulk-fetch cached vectors for ``texts``. Missing keys absent from
    returned dict — caller embeds those via the API."""
    import numpy as np

    conn = _get_embedding_cache()
    hits: dict[str, list[float]] = {}
    for i in range(0, len(texts), _CACHE_PARAM_LIMIT):
        chunk = texts[i : i + _CACHE_PARAM_LIMIT]
        placeholders = ",".join("?" * len(chunk))
        rows = conn.execute(
            f"SELECT text, vector FROM embeddings "
            f"WHERE provider = ? AND text IN ({placeholders})",
            [provider, *chunk],
        ).fetchall()
        for text, blob in rows:
            hits[text] = np.frombuffer(blob, dtype=np.float32).tolist()
    return hits


def _cache_store(provider: str, items: dict[str, list[float]]) -> None:
    """Persist freshly-embedded ``(text, vector)`` pairs."""
    if not items:
        return
    import numpy as np

    rows = [
        (provider, text, np.asarray(vec, dtype=np.float32).tobytes())
        for text, vec in items.items()
    ]
    conn = _get_embedding_cache()
    with _cache_lock:
        conn.executemany(
            "INSERT OR REPLACE INTO embeddings (provider, text, vector) "
            "VALUES (?, ?, ?)",
            rows,
        )
        conn.commit()

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
    *,
    cache: bool = False,
) -> list[list[float] | None]:
    """Compute 1024-dim embeddings via *provider*.

    Supported providers: ``"gemini"`` (auto Vertex AI), ``"qwen"``.
    When *provider* is ``None``, reads config key ``EMBEDDING_PROVIDER`` (default: ``"gemini"``).
    Long input lists are chunked per provider batch limit.
    Invalid entries (non-str / blank) yield ``None`` at the same index.

    Set ``cache=True`` to read/write the disk cache at
    ``~/.cache/mirobody/text_embedding.sqlite``. Off by default because
    bulk index builds (~700K one-time unique texts) would bloat the
    cache for no recurring benefit; query-side callers (resolve,
    benchmarks, search) should opt in. The cache key is
    ``(provider, text)``; identical inputs across runs return without
    hitting the API.
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

    unique_texts: list[str] = list(dict.fromkeys(clean_texts))

    # Disk cache hits short-circuit the API call. ``api_texts`` is the
    # subset that actually needs an embedding API roundtrip.
    text_to_embedding: dict[str, list[float]] = {}
    if cache:
        text_to_embedding = _cache_lookup(provider, unique_texts)
    api_texts: list[str] = [t for t in unique_texts if t not in text_to_embedding]

    if api_texts:
        factory = _EMB_PROVIDERS.get(provider)
        if not factory:
            raise ValueError(f"unknown embedding provider: {provider!r} (available: {', '.join(_EMB_PROVIDERS)})")
        llm, url, batch_limit, max_concurrency, make_body, parse = factory()

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

            chunks = [api_texts[i : i + batch_limit] for i in range(0, len(api_texts), batch_limit)]
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

        if len(embedded) != len(api_texts):
            raise RuntimeError(
                f"{provider} returned {len(embedded)} embeddings for {len(api_texts)} unique texts"
            )

        for t, vec in zip(api_texts, embedded):
            text_to_embedding[t] = vec

        if cache:
            _cache_store(provider, {t: text_to_embedding[t] for t in api_texts})

    for idx, text in zip(valid_indices, clean_texts):
        results[idx] = text_to_embedding[text]
    return results
