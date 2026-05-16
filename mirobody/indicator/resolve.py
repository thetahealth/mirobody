"""Resolve subcommand: free-text term → standard medical codes.

Routes to :func:`mirobody.indicator.fhir.resolve.pipeline.resolve_many`,
the minimal cosine + LOINC analyte-digit family rerank + deprecated
demote pipeline. The full ``FhirAdapter`` stack (axis / specificity /
alias / dose / tag gates) is intentionally NOT used here — see
``benchmarks/run_resolve.py`` for the same algorithm wrapped in CSV
IO. Callers needing the legacy adapter path import ``FhirAdapter``
directly.
"""

from __future__ import annotations

import json
import logging
import sys
from argparse import Namespace
from dataclasses import asdict
from pathlib import Path

log = logging.getLogger(__name__)


def _parse_term_value(s: str) -> tuple[str, str | None]:
    """Split an input record into ``(term, value)``.

    Syntax: ``term=value``. Splits on the first ``=`` only, so
    ``glucose=150 mg/dL`` → ``("glucose", "150 mg/dL")`` and
    ``"a=b=c"`` → ``("a", "b=c")``. ``glucose`` (no separator) and
    ``glucose=`` (empty rhs) both yield ``value=None`` — the pipeline
    then skips the SCALE_TYP rerank for that term.
    """
    if "=" not in s:
        return s, None
    term, _, value = s.partition("=")
    return term, value or None


async def cmd_resolve(args: Namespace) -> None:
    """Subcommand: resolve — map terms to standard medical codes.

    Each input record is ``term`` or ``term=value``. When a value is
    given, the resolver classifies it (``qn`` / ``ord`` / ``nom`` /
    ``nar``); the picker then prefers same-analyte LOINC rows with a
    compatible SCALE_TYP —
    disambiguates analytes that carry both quantitative and qualitative
    variants (``glucose=150 mg/dL`` → Qn; ``glucose=++`` → Ord).

    Single term, no --output  → pretty JSON list on stdout (legacy shape).
    Otherwise                 → JSON Lines (one ``{"term":..., "value":...,
                                "results":[...]}`` per line; ``value`` omitted
                                when not supplied). With --output, results
                                append to the file and the run is resumable:
                                records already present (matched on the
                                ``(term, value)`` pair, so the same term with
                                different values runs as separate jobs) are
                                skipped on re-run. tqdm prints to stderr, with
                                the bar's "completed" count seeded from the
                                existing output so overall progress reflects
                                the full job.
    """
    from .fhir.resolve.pipeline import resolve_many
    from mirobody.utils import safe_read_cfg

    # Resolve terms source: positional XOR --input. Build aligned
    # ``terms`` and ``values`` lists; both branches honor ``term=value``
    # syntax so positional and --input behave identically.
    if args.input:
        if args.terms:
            log.error("cannot pass both positional terms and --input")
            sys.exit(2)
        with open(args.input, encoding="utf-8") as f:
            raw = [line.rstrip("\n") for line in f if line.strip()]
    else:
        if not args.terms:
            log.error("must pass terms positionally or via --input")
            sys.exit(2)
        raw = list(args.terms)

    parsed = [_parse_term_value(r) for r in raw]
    # Drop records whose term portion is blank (e.g. ``=value``).
    parsed = [(t, v) for t, v in parsed if t.strip()]
    terms = [t for t, _ in parsed]
    values: list[str | None] = [v for _, v in parsed]
    has_any_value = any(v is not None for v in values)

    bundle_dir = safe_read_cfg("FHIR_INDICATORS_DIR")

    # Legacy shape preserved for ad-hoc single-term lookups.
    if len(terms) == 1 and not args.output:
        batch_results = await resolve_many(
            terms, top_k=args.top_k, systems=args.systems,
            values=values if has_any_value else None,
            bundle_dir=bundle_dir,
        )
        results = batch_results[0]
        print(json.dumps([asdict(r) for r in results], ensure_ascii=False, indent=2))
        return

    # Resume: scan existing output for already-completed (term, value) pairs.
    # Falls back to term-only matching for legacy output files written
    # before --value support landed.
    done: set[tuple[str, str | None]] = set()
    if args.output and Path(args.output).exists():
        with open(args.output, encoding="utf-8") as f:
            for line in f:
                try:
                    rec = json.loads(line)
                    done.add((rec["term"], rec.get("value")))
                except (json.JSONDecodeError, KeyError, TypeError):
                    continue
        if done:
            log.info(f"resume: {len(done)} records already in {args.output}")

    remaining_idx = [
        i for i, (t, v) in enumerate(zip(terms, values)) if (t, v) not in done
    ]
    if not remaining_idx:
        log.info(f"all {len(terms)} records already resolved, nothing to do")
        return

    out_fp = (
        open(args.output, "a", encoding="utf-8") if args.output else sys.stdout
    )

    from tqdm import tqdm
    chunk = 256
    pbar = tqdm(
        total=len(terms), initial=len(done), desc="resolve", unit="term",
    )
    try:
        for i in range(0, len(remaining_idx), chunk):
            batch_idx = remaining_idx[i : i + chunk]
            batch_terms = [terms[j] for j in batch_idx]
            batch_values = [values[j] for j in batch_idx]
            batch_results = await resolve_many(
                batch_terms, top_k=args.top_k, systems=args.systems,
                values=batch_values if has_any_value else None,
                bundle_dir=bundle_dir,
            )
            for term, value, results in zip(batch_terms, batch_values, batch_results):
                rec: dict = {"term": term}
                if value is not None:
                    rec["value"] = value
                rec["results"] = [asdict(r) for r in results]
                out_fp.write(json.dumps(rec, ensure_ascii=False) + "\n")
            out_fp.flush()
            pbar.update(len(batch_idx))
    finally:
        pbar.close()
        if out_fp is not sys.stdout:
            out_fp.close()
