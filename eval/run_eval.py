"""Score every resolver tier and every fusion of them, per stratum.

    python eval/run_eval.py --no-embed                # lexical tiers, offline, ~1s
    python eval/run_eval.py --matrix <path.npy>       # + the embedding tiers
    python eval/run_eval.py --testset <cases.jsonl>   # grade your own distribution

**This runner is the shared asset; the cases are not.** The maintained test set
is not in this repository and neither are its results — see `.gitignore` for
why. What ships is the harness: the four metrics, the analyte-level grading
rule, and the strata, so a downstream team can score the same resolver against
their own inputs without those inputs ever coming here. Point `--testset` at a
private file; nothing is read from this directory that you do not pass in.

Each case is one JSON object per line::

    {"term": "血红蛋白", "code": "718-7", "stratum": "covered",
     "value": "13.5", "unit": "g/dL"}          # value/unit optional

**What the numbers mean here**, because "accuracy" hides the whole tradeoff:

    coverage   answered / total          how much of the input gets a code
    precision  correct / answered        of what it answers, how much is right
    recall     correct / total           coverage x precision
    wrong-rate wrong / total             the clinically load-bearing one

A tier that never abstains has coverage 1.0 by construction and buys it entirely
out of precision. A tier that abstains often has high precision and leaves the
work undone. Neither number alone can rank them, which is why the fusion configs
below are scored on all four.

**Wrong-rate is not just 1 - precision.** An abstention is not a wrong answer:
an uncoded reading stays visible and fixable, while a confidently miscoded one
silently joins the wrong series. That asymmetry is why a tier is allowed to buy
precision with coverage but not the reverse.

Grading is by ANALYTE (LOINC COMPONENT head), not exact code: this engine and
any given curated seed disagree on a large minority of pairs, and most of those
are the same COMPONENT under a different specimen or method. Grading them as
failures measures agreement with one team's specimen conventions rather than
correctness. Exact-code agreement is reported alongside, never instead.

`stratum` is free text — whatever you put there becomes a reported section, so
the strata are yours to choose. The ones worth having separate the failure
modes: what already works (a regression guard), what currently MISSES but has
ground truth (where recall actually lives), and the cases where the unit or the
value's kind should pick a different code for the same analyte.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import os
import pathlib
import sys
import time
from collections import defaultdict

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

DEFAULT_TESTSET = REPO / "eval" / "testset.jsonl"
RESULTS = REPO / "eval" / "results"

# The embedding matrix is multi-GB and ships on a volume, never in git, so
# there is no defensible default path — it used to be one maintainer's Desktop,
# which meant `run_eval.py` with no flags only ran on one machine.
# MIROBODY_EVAL_MATRIX keeps that machine's convenience without hardcoding it.
DEFAULT_MATRIX = os.environ.get("MIROBODY_EVAL_MATRIX", "")


def _analyte_table() -> dict[str, str]:
    from mirobody.indicator.fhir.embeddings.bundle import read_member

    raw = read_member(
        "loinc_axis.csv", bundle_path=str(REPO / "mirobody" / "res" / "fhir_loinc_bundle.tar.gz")
    )
    out = {}
    for row in csv.DictReader(io.StringIO(raw.decode("utf-8"))):
        out[row["LOINC_NUM"]] = (row["COMPONENT"] or "").split("^")[0].strip().lower()
    return out


class Scorer:
    """One config's tally, kept per stratum as well as overall."""

    def __init__(self, name: str):
        self.name = name
        self.rows: list[dict] = []

    def add(self, case: dict, code: str | None, analyte_of: dict[str, str]) -> None:
        refuse = case["expect_code"] is None
        answered = bool(code)
        if refuse:
            correct = not answered
            exact = correct
        else:
            correct = answered and analyte_of.get(code, "") == case["expect_analyte"]
            exact = answered and code == case["expect_code"]
        self.rows.append({
            "stratum": case["stratum"], "term": case["term"], "got": code,
            "expect": case["expect_code"], "answered": answered,
            "correct": correct, "exact": exact,
        })

    def tally(self, stratum: str | None = None) -> dict:
        rows = [r for r in self.rows if stratum is None or r["stratum"] == stratum]
        n = len(rows)
        if not n:
            return {}
        answered = sum(r["answered"] for r in rows)
        correct = sum(r["correct"] for r in rows)
        exact = sum(r["exact"] for r in rows)
        wrong = sum(1 for r in rows if r["answered"] and not r["correct"])
        # On a stratum where the right move is to say nothing, "correct" means
        # "did not answer", so correct/answered is not a precision — it can
        # exceed 1, which is how this bug announced itself (a printed 8.000).
        # Those strata get coverage and wrong-rate, which is all that is
        # meaningful: every answer is a wrong answer.
        refusal_stratum = all(r["expect"] is None for r in rows)
        return {
            "n": n,
            "coverage": round(answered / n, 4),
            "precision": (
                None if refusal_stratum or not answered else round(correct / answered, 4)
            ),
            "recall": round(correct / n, 4),
            "wrong_rate": round(wrong / n, 4),
            "exact_code": round(exact / n, 4),
            "abstained": n - answered,
            "wrong": wrong,
        }


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--testset",
        type=pathlib.Path,
        default=DEFAULT_TESTSET,
        help="JSONL cases to score (default: eval/testset.jsonl). The runner is "
             "the shared asset; the cases need not be — point this at a private "
             "set to grade the same resolver against your own distribution.",
    )
    ap.add_argument("--matrix", default=DEFAULT_MATRIX,
                    help="embedding matrix .npy; or set MIROBODY_EVAL_MATRIX")
    ap.add_argument("--no-embed", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    testset = args.testset
    if not testset.is_file():
        print(f"no test set at {testset} — pass --testset <path>", file=sys.stderr)
        return 2
    if not args.no_embed and not args.matrix:
        print("no embedding matrix: pass --matrix <path>, set MIROBODY_EVAL_MATRIX, "
              "or use --no-embed for the lexical tiers only", file=sys.stderr)
        return 2

    cases = [json.loads(l) for l in testset.open(encoding="utf-8")]
    if args.limit:
        by_stratum: dict[str, list[dict]] = defaultdict(list)
        for c in cases:
            by_stratum[c["stratum"]].append(c)
        cases = [c for group in by_stratum.values() for c in group[: args.limit]]
    print(f"{len(cases):,} cases from {testset}", flush=True)

    analyte_of = _analyte_table()

    from mirobody.engine import get_resolver, resolve, resolve_reading

    resolver = get_resolver()

    # ── tier decisions, computed once each ───────────────────────────────────
    t0 = time.time()
    lex = [resolve(c["term"]) for c in cases]
    lex_unit = [resolve_reading(c["term"], c["value"], c["unit"]) for c in cases]
    print(f"lexical tiers: {time.time()-t0:.1f}s", flush=True)

    sem_plain: list[str | None] = [None] * len(cases)
    sem_gated: list[str | None] = [None] * len(cases)
    matrix_used = None
    if not args.no_embed:
        from mirobody.indicator.semantic import SemanticIndex
        from mirobody.utils import Config

        await Config.init(yaml_filenames=["config.yaml", "config.local.yaml"])
        from mirobody.utils.embedding import text_embedding

        index = SemanticIndex(args.matrix)
        matrix_used = args.matrix
        t0 = time.time()
        vectors = await text_embedding([c["term"] for c in cases], provider="openrouter", cache=True)
        print(f"embedded {len(cases):,} terms in {time.time()-t0:.1f}s", flush=True)

        usable = [(i, v) for i, v in enumerate(vectors) if v]
        gates = [
            SemanticIndex.gate_for(cases[i]["value"], cases[i]["unit"], cases[i]["term"])
            for i, _ in usable
        ]
        plain = index.search_vectors([v for _, v in usable], top_k=1)
        gated = index.search_vectors([v for _, v in usable], top_k=1, gates=gates)
        for (i, _), p, g in zip(usable, plain, gated):
            sem_plain[i] = p[0].loinc if p else None
            sem_gated[i] = g[0].loinc if g else None

    # ── configs, all derived from the decisions above ────────────────────────
    configs: dict[str, list[str | None]] = {
        "T2 lexical": [r.loinc or None for r in lex],
        "T2+T3 lexical+unit-variant": [r.loinc or None for r in lex_unit],
    }
    if not args.no_embed:
        configs["T4 embedding (ungated)"] = sem_plain
        configs["T4 embedding + axis gates"] = sem_gated
        # A fallback that fires on EVERY non-answer, including the deliberate
        # refusals. Kept as a config because it is what the first version did,
        # and the eval is what caught it: the embedding tier answered all nine
        # refusals and got all nine wrong.
        configs["T2+T3 -> T4 gated, no refusal guard"] = [
            (r.loinc or None) or s for r, s in zip(lex_unit, sem_gated)
        ]
        # What `resolve_with_semantic_fallback` actually does: a refusal is a
        # decision and stays a refusal; only genuine misses go on to the second
        # tier.
        configs["T2+T3 -> T4 gated  [PROPOSED]"] = [
            (r.loinc or None) or (None if r.method == "refused" else s)
            for r, s in zip(lex_unit, sem_gated)
        ]

    scorers = {}
    for name, codes in configs.items():
        sc = Scorer(name)
        for case, code in zip(cases, codes):
            sc.add(case, code, analyte_of)
        scorers[name] = sc

    # This repo's own strata first, so its reports keep their reading order,
    # then anything else the test set names, in first-seen order. It used to be
    # this list and nothing else, which meant an injected `--testset` got the
    # OVERALL table and silently lost every section of its own — the exact
    # thing `--testset` exists to provide.
    _CANONICAL = ["covered", "frontier", "frontier+unit", "shaped", "unit",
                  "qualitative", "zh-hant", "refuse", "ood"]
    seen = list(dict.fromkeys(c["stratum"] for c in cases))
    strata = [st for st in _CANONICAL if st in seen] + [st for st in seen if st not in _CANONICAL]
    report = {
        "when": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "cases": len(cases),
        "matrix": matrix_used,
        "overall": {n: s.tally() for n, s in scorers.items()},
        "by_stratum": {
            n: {st: s.tally(st) for st in strata if s.tally(st)} for n, s in scorers.items()
        },
    }
    RESULTS.mkdir(parents=True, exist_ok=True)
    out = RESULTS / f"{time.strftime('%Y%m%d-%H%M%S')}.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    def table(title: str, get) -> None:
        print(f"\n{title}")
        print(f"  {'config':34} {'cov':>7} {'prec':>7} {'recall':>7} {'wrong':>7} {'exact':>7}")
        for name in configs:
            t = get(scorers[name])
            if not t:
                continue
            prec = "  —  " if t["precision"] is None else f"{t['precision']:.3f}"
            print(f"  {name:34} {t['coverage']:>7.3f} {prec:>7} "
                  f"{t['recall']:>7.3f} {t['wrong_rate']:>7.3f} {t['exact_code']:>7.3f}")

    table(f"OVERALL  (n={len(cases):,})", lambda s: s.tally())
    for st in strata:
        n = sum(1 for c in cases if c["stratum"] == st)
        if n:
            table(f"{st.upper()}  (n={n:,})", lambda s, st=st: s.tally(st))

    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
