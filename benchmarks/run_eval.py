"""Score every resolver tier and every fusion of them, per scope and per stratum.

    python -m benchmarks.run_eval                         # lexical tiers, offline, ~1s
    python benchmarks/run_eval.py --testset <cases.jsonl> # grade your own distribution

Runs from a checkout with `mirobody` installed (`pip install -e .`). It is a
benchmark runner, not a test (nothing here asserts) and not library code
(nothing imports it), which is why it lives beside `tests/` and `examples/`
rather than inside the package — the same place starlette, deepagents and
langchain keep theirs.

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

`scope` is the other axis, and it is DERIVED rather than written: a stratum
says how hard a case is, a scope says what kind of term it is. It comes off
the shipped bundle's answer about the expected code (see `scope_of`), so a
private test set gets the same sections without being edited, and a
regression reads as "the everyday panels got worse" instead of as a number
that moved. `checkup` is the section the release gate names.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import pathlib
import sys
import time
from collections import defaultdict
from functools import lru_cache

REPO = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

DEFAULT_TESTSET = REPO / "eval" / "testset.jsonl"
RESULTS = REPO / "eval" / "results"

def _analyte_table() -> dict[str, str]:
    """code -> its analyte, for "right analyte, different variant" credit.

    Read off the axis table the resolver itself answers from. It used to parse
    `loinc_axis.csv`, a whole second copy of the table that the 1.5.0 cut stopped
    shipping; `AXIS_ANALYTE` is the same COMPONENT head, folded at build time.
    """
    from mirobody._bundle import AXIS_ANALYTE, AXIS_CODE, load_axis

    axis, _order_code, _order_name = load_axis()
    out = {}
    for row in range(len(axis)):
        out[axis.field(row, AXIS_CODE)] = axis.field(row, AXIS_ANALYTE)
    return out


#: LOINC CLASS families an ordinary checkup prints. The `*.ATOM` rows are the
#: vital signs; the rest are the panels a 体检 or an annual physical runs. A
#: code outside these is still clinical, just not what a first-time user is
#: holding, which is the distinction `scope` exists to make.
CHECKUP_CLASSES = frozenset({
    "CHEM", "HEM/BC", "UA", "COAG", "SERO", "PANEL.VITALS",
    "BP.ATOM", "BDYWGT.ATOM", "BDYHGT.ATOM", "HRTRATE.ATOM", "RESP.ATOM", "BDYTMP.ATOM",
})

#: Reported in this order. `symptom` is reserved for the ICPC-3 axis.
SCOPES = ["checkup", "clinical-extended", "device", "out", "deprecated", "symptom", "refuse"]


def _scope_table():
    """`(class_of, skipped)` for deriving a case's scope from the bundle."""
    from mirobody._bundle import AXIS_CLASS, AXIS_CODE, read_member
    from mirobody.bundle import load_axis

    axis, by_code, _by_name = load_axis()
    class_of = {axis.field(i, AXIS_CODE): axis.field(i, AXIS_CLASS) for i in range(len(by_code))}
    skipped = set((read_member("loinc_skip.txt") or b"").decode().split())
    return class_of, skipped


def scope_of(case: dict, class_of: dict[str, str], skipped: set[str]) -> str:
    """The scope a case is reported under.

    A case may carry its own `scope`; otherwise it is derived, so a private
    test set gets the same sections without being edited. The derivation is
    the bundle's own answer about the expected code and nothing else:

      refuse             the case expects no code
      device             the term is a name in the device catalogue
      out                the code is ACTIVE in 2.83 and the cut drops it
      deprecated         the code is not ACTIVE in 2.83
      checkup            in the cut, in a CHECKUP_CLASSES family
      clinical-extended  in the cut, anywhere else
    """
    if case.get("scope"):
        return str(case["scope"])
    code = case.get("expect_code")
    if not code:
        return "refuse"
    if _is_device(case.get("term", "")):
        return "device"
    family = class_of.get(code)
    if family is None:
        return "out" if code in skipped else "deprecated"
    return "checkup" if family in CHECKUP_CLASSES else "clinical-extended"


@lru_cache(maxsize=1)
def _device_names() -> frozenset[str]:
    try:
        from mirobody.kernel import metrics
    except Exception:
        return frozenset()
    return frozenset(n.lower() for n in metrics.METRICS)


def _is_device(term: str) -> bool:
    return term.strip().lower() in _device_names()


class Scorer:
    """One config's tally, kept per stratum and per scope as well as overall."""

    def __init__(self, name: str):
        self.name = name
        self.rows: list[dict] = []

    def add(self, case: dict, code: str | None, analyte_of: dict[str, str], scope: str = "") -> None:
        refuse = case["expect_code"] is None
        answered = bool(code)
        if refuse:
            correct = not answered
            exact = correct
        else:
            correct = answered and analyte_of.get(code, "") == case["expect_analyte"]
            exact = answered and code == case["expect_code"]
        self.rows.append({
            "stratum": case["stratum"], "scope": scope, "term": case["term"], "got": code,
            "expect": case["expect_code"], "answered": answered,
            "correct": correct, "exact": exact,
        })

    def tally(self, stratum: str | None = None, scope: str | None = None) -> dict:
        rows = [
            r for r in self.rows
            if (stratum is None or r["stratum"] == stratum)
            and (scope is None or r["scope"] == scope)
        ]
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
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    testset = args.testset
    if not testset.is_file():
        print(f"no test set at {testset} — pass --testset <path>", file=sys.stderr)
        return 2

    cases = [json.loads(l) for l in testset.open(encoding="utf-8")]
    if args.limit:
        by_stratum: dict[str, list[dict]] = defaultdict(list)
        for c in cases:
            by_stratum[c["stratum"]].append(c)
        cases = [c for group in by_stratum.values() for c in group[: args.limit]]
    print(f"{len(cases):,} cases from {testset}", flush=True)

    analyte_of = _analyte_table()
    class_of, skipped = _scope_table()
    scopes = [scope_of(c, class_of, skipped) for c in cases]

    from mirobody.engine import resolve, resolve_reading

    # ── tier decisions, computed once each ───────────────────────────────────
    t0 = time.time()
    lex = [resolve(c["term"]) for c in cases]
    lex_unit = [resolve_reading(c["term"], c["value"], c["unit"]) for c in cases]
    print(f"lexical tiers: {time.time()-t0:.1f}s", flush=True)

    # ── configs, all derived from the decisions above ────────────────────────
    configs: dict[str, list[str | None]] = {
        "T2 lexical": [r.loinc or None for r in lex],
        "T2+T3 lexical+unit-variant": [r.loinc or None for r in lex_unit],
    }
    scorers = {}
    for name, codes in configs.items():
        sc = Scorer(name)
        for case, code, scope in zip(cases, codes, scopes, strict=True):
            sc.add(case, code, analyte_of, scope)
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
        "overall": {n: s.tally() for n, s in scorers.items()},
        "by_stratum": {
            n: {st: s.tally(st) for st in strata if s.tally(st)} for n, s in scorers.items()
        },
        "by_scope": {
            n: {sc: s.tally(scope=sc) for sc in SCOPES if s.tally(scope=sc)}
            for n, s in scorers.items()
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
    # Scope first: it is what the release gate reads, and `checkup` is the
    # section a first-time user's terms land in.
    for sc in SCOPES:
        n = scopes.count(sc)
        if n:
            table(f"scope {sc}  (n={n:,})", lambda s, sc=sc: s.tally(scope=sc))
    for st in strata:
        n = sum(1 for c in cases if c["stratum"] == st)
        if n:
            table(f"{st.upper()}  (n={n:,})", lambda s, st=st: s.tally(st))

    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
