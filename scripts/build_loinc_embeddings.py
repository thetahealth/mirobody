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
unit-normalized, corpus-aligned 1024-dim matrix — and it is unusable, because no
DashScope model reachable today produces query vectors in its space:

    cosine(stored_row, text-embedding-v3(that row's own name)) = -0.018
    cosine(stored_row, text-embedding-v4(that row's own name)) = -0.030
    (text_type=document and text_type=query both, so that is not the cause)

Cosine against a row's *own text* should be ~1.0. At ~0 the search is noise, and
it fails silently and confidently: `空腹血糖` came back as *"Widespread delusions
[DI-PAD]"*. A matrix whose query model cannot be reproduced is worse than no
matrix, so the artifact has to be something anyone can regenerate — which is
this script plus a key.

Output: structured `.npy`, dtype `[('fhir_id','i8'),('emb','f2',(1024,))]`, one
row per ACTIVE LOINC row of the shipped bundle, in corpus order.
"""

from __future__ import annotations

import argparse
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
BATCH = 10          # DashScope text-embedding-v4 caps a request at 10 inputs
CONCURRENCY = 16    # the provider's own client serializes; this script does not


async def _embed_all(texts: list[str], provider: str, key: str) -> list[list[float] | None]:
    import aiohttp

    url = "https://dashscope.aliyuncs.com/compatible-mode/v1/embeddings"
    model = "text-embedding-v4"
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
            if done % 5000 < BATCH:
                rate = done / max(time.time() - started, 1e-9)
                eta = (len(texts) - done) / max(rate, 1e-9)
                print(f"  {done:,}/{len(texts):,}  {rate:.0f}/s  eta {eta/60:.1f} min", flush=True)

        await asyncio.gather(*(
            one(i, texts[i:i + BATCH]) for i in range(0, len(texts), BATCH)
        ))
    return out


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--provider", default="qwen", choices=["qwen"])
    ap.add_argument("--limit", type=int, default=0, help="First N LOINC rows only (a pilot run)")
    args = ap.parse_args()

    from mirobody.indicator.fhir.common import SYSTEM_TO_CODE, code_to_fhir_id
    from mirobody.indicator.fhir.embeddings.bundle import read_member
    from mirobody.utils import Config

    config = await Config.init(yaml_filenames=["config.yaml", "config.local.yaml"])
    key = config.get("DASHSCOPE_API_KEY")
    if not key:
        print("DASHSCOPE_API_KEY is not set", file=sys.stderr)
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

    print(f"LOINC rows to embed: {len(rows):,}  (dim {DIM}, fp16 -> {len(rows)*DIM*2/1e6:.0f} MB)", flush=True)

    vectors = await _embed_all([r[2] for r in rows], args.provider, key)
    missing = sum(1 for v in vectors if v is None)
    if missing:
        print(f"WARNING: {missing:,} rows came back empty", file=sys.stderr)

    out = np.zeros(len(rows), dtype=[("fhir_id", "<i8"), ("emb", "<f2", (DIM,))])
    loinc_sys = SYSTEM_TO_CODE["LOINC"]
    for k, ((_, code, _), vec) in enumerate(zip(rows, vectors)):
        out["fhir_id"][k] = code_to_fhir_id(loinc_sys, code)
        if vec is not None:
            v = np.asarray(vec, dtype=np.float32)
            norm = float(np.linalg.norm(v))
            # Stored unit-normalized: the recall step takes a plain dot product.
            out["emb"][k] = (v / norm if norm > 0 else v).astype(np.float16)

    np.save(args.out, out)
    print(f"wrote {args.out}  {os.path.getsize(args.out)/1e6:.0f} MB  {len(out):,} rows", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
