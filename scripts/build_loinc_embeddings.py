"""Build the LOINC-only embedding matrix — the semantic tier's downloadable half.

    python scripts/build_loinc_embeddings.py --out /tmp/fhir_loinc_embeddings_qwen.npy

**Why LOINC-only.** The full corpus matrix is 677,643 rows × 1024 dims × fp16 =
1.39 GB, because it covers six vocabularies (SNOMED CT, LOINC, RxNorm, CVX, DCM,
THETA). Indicator resolution ranks LOINC rows and nothing else, so 108,248 rows
— **221 MB** — is the whole of what it needs. The other 1.17 GB is drugs,
conditions and imaging codes the resolver never scores.

It also settles a licensing question: the LOINC licence permits redistribution
with its copyright notice reproduced (see `res/fhir_loinc_bundle.NOTICE`), while
SNOMED CT requires every downstream recipient to hold their own Affiliate
Licence (`res/fhir_snomed_ct_bundle.NOTICE`). A LOINC-only artifact is
distributable; a full-corpus one drags that obligation onto every user.

**Why this script exists rather than a matrix somebody once built.** The qwen
matrix that was sitting in `~/Desktop/personal/FHIRs/qwen/` is a valid,
unit-normalized, corpus-aligned 1024-dim matrix built from a model that had to
be identified by experiment:

    cosine(stored_row, <model>(that row's own name)), same row, should be ~1.0

    DashScope text-embedding-v3      -0.018      (text_type document AND query)
    DashScope text-embedding-v4      -0.030
    qwen3-embedding-4b               ~0.00
    qwen3-embedding-8b              +0.64        <- the family
      ... and on a rank test against all 108,248 LOINC rows, a row's own name
      retrieved that row at rank 1, 1, 3, 18, 21 — against a cosine floor of
      0.30 mean / 0.45 p99. Real retrieval, not noise.

So it is Qwen3-Embedding-8B, but not the checkpoint or serving configuration
reachable now: 0.64 is a related space, not the same one, and at 0.64 the search
degrades silently rather than failing. `空腹血糖` came back as *"Widespread
delusions [DI-PAD]"* through the mismatched pair.

The fix is not to hunt the original weights. It is to build the matrix with the
exact model the queries will use, so self-cosine is 1.0 by construction — and to
pick a model that is open-source and reachable, which Qwen3-Embedding-8B is.

Output: structured `.npy`, dtype `[('fhir_id','i8'),('emb','f2',(1024,))]`, one
row per ACTIVE LOINC row of the shipped bundle, in corpus order.
"""

from __future__ import annotations

import argparse
import json
import asyncio
import csv
import gzip
import io
import os
import sys
import time

import numpy as np

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DIM = 1024

#: Provider → (endpoint, model, inputs per request, config key holding the key).
#:
#: `openrouter` is the default, for three reasons: the model is the open-weights
#: Qwen3-Embedding-8B (anyone can self-host it and get the same vectors), a
#: mirobody deployment already needs an OPENROUTER_API_KEY for the agent, and it
#: costs about $0.01 per million tokens — the whole 96k-row build is under two
#: cents. Note the endpoint is NOT in OpenRouter's /models listing, which covers
#: chat models only; /embeddings works regardless.
# Model ids are imported from `mirobody.utils.embedding.EMBEDDING_MODEL_IDS` in
# main() — the same table the runtime query side reads — so this script cannot
# drift onto a model the deployed `text_embedding()` does not call.
PROVIDERS: dict[str, tuple[str, str | None, int, str]] = {
    "openrouter": (
        "https://openrouter.ai/api/v1/embeddings",
        None,                    # filled from EMBEDDING_MODEL_IDS["openrouter"]
        256,
        "OPENROUTER_API_KEY",
    ),
    "qwen": (
        "https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings",
        None,                    # filled from EMBEDDING_MODEL_IDS["qwen"]
        10,                      # DashScope caps a request at 10 inputs
        "DASHSCOPE_API_KEY",
    ),
}
CONCURRENCY = 8


async def _embed_all(texts: list[str], provider: str, key: str) -> list[list[float] | None]:
    import aiohttp

    url, model, batch, _ = PROVIDERS[provider]
    out: list[list[float] | None] = [None] * len(texts)
    sem = asyncio.Semaphore(CONCURRENCY)
    done = 0
    started = time.time()

    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=120),
        headers={"Authorization": f"Bearer {key}"},
    ) as session:

        async def one(start: int, chunk: list[str]) -> None:
            nonlocal done
            body = {"model": model, "input": chunk, "dimensions": DIM}
            async with sem:
                for attempt in range(5):
                    try:
                        async with session.post(url, json=body) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                for j, item in enumerate(data["data"]):
                                    out[start + j] = item["embedding"]
                                break
                            # 429 / 5xx are the retryable ones; back off and try again.
                            if resp.status in (429, 500, 502, 503, 504):
                                await asyncio.sleep(1.5 * (attempt + 1))
                                continue
                            raise RuntimeError(f"HTTP {resp.status}: {(await resp.text())[:200]}")
                    except (TimeoutError, asyncio.TimeoutError):
                        await asyncio.sleep(1.5 * (attempt + 1))
            done += len(chunk)
            if done % 5000 < batch:
                rate = done / max(time.time() - started, 1e-9)
                eta = (len(texts) - done) / max(rate, 1e-9)
                print(f"  {done:,}/{len(texts):,}  {rate:.0f}/s  eta {eta/60:.1f} min", flush=True)

        await asyncio.gather(*(
            one(i, texts[i:i + batch]) for i in range(0, len(texts), batch)
        ))
    return out


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--provider", default="openrouter", choices=sorted(PROVIDERS))
    ap.add_argument("--limit", type=int, default=0, help="First N LOINC rows only (a pilot run)")
    args = ap.parse_args()

    from mirobody.indicator.fhir.common import SYSTEM_TO_CODE, code_to_fhir_id
    from mirobody.indicator.fhir.embeddings.bundle import read_member
    from mirobody.utils import Config
    from mirobody.utils.embedding import EMBEDDING_MODEL_IDS

    url0, _, batch0, key0 = PROVIDERS[args.provider]
    PROVIDERS[args.provider] = (url0, EMBEDDING_MODEL_IDS[args.provider], batch0, key0)

    config = await Config.init(yaml_filenames=["config.yaml", "config.local.yaml"])
    key_name = PROVIDERS[args.provider][3]
    key = config.get(key_name)
    if not key:
        print(f"{key_name} is not set", file=sys.stderr)
        return 1

    res = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "mirobody", "res")
    axis_raw = read_member("loinc_axis.csv", bundle_path=os.path.join(res, "fhir_loinc_bundle.tar.gz"))
    axis = list(csv.DictReader(io.StringIO(axis_raw.decode("utf-8"))))

    # The corpus row order is what every row-indexed sidecar assumes, and it is
    # meta order — not axis order. Take the names from meta and keep only the
    # rows whose packed id says LOINC.
    with gzip.open(os.path.join(res, "fhir_meta.csv.gz"), "rt", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader)
        names = [row[0] for row in reader]

    by_name = {r["LONG_COMMON_NAME"]: r["LOINC_NUM"] for r in axis}
    rows: list[tuple[int, str, str]] = []       # (corpus_row, loinc_code, name)
    for i, name in enumerate(names):
        code = by_name.get(name)
        if code:
            rows.append((i, code, name))
    if args.limit:
        rows = rows[: args.limit]

    url, model, batch, _ = PROVIDERS[args.provider]
    print(f"LOINC rows to embed: {len(rows):,} via {model} "
          f"(dim {DIM}, fp16 -> {len(rows)*DIM*2/1e6:.0f} MB)", flush=True)

    vectors = await _embed_all([r[2] for r in rows], args.provider, key)
    missing = sum(1 for v in vectors if v is None)
    if missing:
        print(f"WARNING: {missing:,} rows came back empty", file=sys.stderr)

    out = np.zeros(len(rows), dtype=[("fhir_id", "<i8"), ("emb", "<f2", (DIM,))])
    loinc_sys = SYSTEM_TO_CODE["LOINC"]
    for k, ((_, code, _), vec) in enumerate(zip(rows, vectors)):
        out["fhir_id"][k] = code_to_fhir_id(loinc_sys, code)
        if vec is not None:
            # MRL: Qwen3-Embedding's first DIM dimensions are the meaningful
            # prefix, so a longer vector is truncated rather than projected.
            v = np.asarray(vec, dtype=np.float32)[:DIM]
            norm = float(np.linalg.norm(v))
            # Stored unit-normalized: the recall step takes a plain dot product.
            out["emb"][k] = (v / norm if norm > 0 else v).astype(np.float16)

    np.save(args.out, out)

    # Sidecar identity stamp. Vectors are only comparable within one
    # (provider, model) pair, and a mismatched matrix does not error — it
    # returns confident nonsense ("空腹血糖" once answered as "Widespread
    # delusions" off a matrix from a different serving config). SemanticIndex
    # refuses to load a matrix whose stamp disagrees with the configured
    # EMBEDDING_PROVIDER, and warns when the stamp is missing.
    meta_path = f"{args.out}.meta.json"
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump({
            "provider": args.provider,
            "model": model,
            "dim": DIM,
            "rows": len(out),
        }, f, indent=2)
    print(f"wrote {args.out}  {os.path.getsize(args.out)/1e6:.0f} MB  {len(out):,} rows", flush=True)
    print(f"wrote {meta_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
