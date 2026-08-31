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
from functools import lru_cache
from pathlib import Path

log = logging.getLogger(__name__)


@lru_cache(maxsize=4)
def _load_alias_pairs(bundle_dir: str | None) -> tuple[tuple[str, str], ...]:
    """Load CJK alias key -> target pairs from ``res/aliases_src/``.

    Used by the axes-vs-legacy merger to confirm an axes COMPONENT pick when an
    alias key from the query maps to the same English term.

    Returns a tuple of ``(cjk_key, en_target_lower)`` pairs across zh/ja/ko.
    Tuple form so the result is hashable for ``lru_cache``. Empty tuple when
    the files are missing — caller falls through to score-based arbitration.

    These used to be read as ``aliases/{lang}.tsv`` members inside the bundle,
    byte-identical copies of the loose files; the copies had drifted and are
    gone. Curated rows come first here, same as everywhere else.
    """
    import os

    from mirobody._bundle import ALIAS_SRC_DIR, alias_source_files

    src_dir = os.path.join(bundle_dir, "aliases_src") if bundle_dir else ALIAS_SRC_DIR
    if bundle_dir:
        paths = [
            os.path.join(src_dir, fn)
            for fn in sorted(os.listdir(src_dir), key=lambda n: (0 if "_curated" in n else 1, n))
            if fn.endswith(".tsv")
        ] if os.path.isdir(src_dir) else []
    else:
        paths = alias_source_files(include_overrides=False)

    pairs: list[tuple[str, str]] = []
    for path in paths:
        # `zh.tsv` and `zh_curated.tsv` both belong to zh.
        lang = os.path.basename(path).split(".")[0].removesuffix("_curated")
        if lang not in ("zh", "ja", "ko"):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                for line in f:
                    parts = line.split("\t", 1)
                    if len(parts) == 2:
                        pairs.append((parts[0], parts[1].strip().lower()))
        except OSError:
            return ()
    return tuple(pairs)


def _axes_component_alias_locked(
    query_text: str,
    axes_component_name: str,
    bundle_dir: str | None,
) -> bool:
    """Return True when at least one alias key appearing in *query_text*
    has a target that **exactly equals** *axes_component_name*
    (case-insensitive, whitespace-trimmed). Confirms axes anchored on
    the precise analyte the alias bridge maps to.

    Substring (either direction) is intentionally NOT used — partial
    matches like ``维生素 → Vitamin`` would falsely lock axes for any
    COMPONENT that happens to contain ``Vitamin`` as a substring
    (e.g. ``Vitamin A/Retinol binding protein``, ``3-epi-25-Hydroxy-
    vitamin D3 ratio``, ``Glucose standard deviation``). Exact-match
    keeps the lock narrow: only when the alias bridge promises a
    1:1 mapping to the COMPONENT name.

    Used by :func:`_merge_axes_and_legacy` to override its default
    ``max(score)`` arbitration when axes is alias-locked and legacy
    disagrees on the analyte family.
    """
    if not query_text or not axes_component_name:
        return False
    pairs = _load_alias_pairs(bundle_dir)
    if not pairs:
        return False
    needle = axes_component_name.strip().lower()
    if not needle:
        return False
    for key, val_lower in pairs:
        if not key:
            continue
        if key in query_text and val_lower == needle:
            return True
    return False


_PICK_ORIENTATION_RE = __import__("re").compile(
    # Capture ``head/tail`` where head ends before the slash and tail
    # ends before the first downstream qualifier marker.
    #
    # Tail terminators (any of):
    #   - ``\s+\[`` — bracket qualifier (``[Mass Ratio]`` / ``[Molar ratio]``)
    #   - ``\s+by\s+`` — method (``by Automated count``)
    #   - ``\s+in\s+[A-Z]`` — specimen (``in Serum``); the uppercase
    #     anchor distinguishes specimen ``in Serum`` from analyte-name
    #     particles like ``Cholesterol in HDL`` which keep ``in`` followed
    #     by an uppercase abbreviation that's part of the analyte — but
    #     ``HDL`` here is part of head/tail, NOT a specimen — see the
    #     greedy stop on the FIRST specimen-style marker.
    #   - end of string
    r"^(?P<head>[^/\[\n]+?)\s*/\s*"
    r"(?P<tail>.+?)"
    r"(?:\s+\[|\s+by\s+|\s+in\s+(?:Ser|Plas|Bld|Bloo|Urin|Stoo|CSF|Body|"
    r"Spec|Tiss|Amni|Vag|Sali|Sweat|Hair|Nail|Pleu|Peri|Asci|Sper|Synov|"
    r"Wound|Eye|Ear|Naso|Oral|Skin|Tear|Vit)|$)",
)


def _parse_pick_orientation(lcn: str) -> tuple[str, str] | None:
    """Parse a LOINC LongCommonName to extract the ``(head, tail)`` of
    an ``X/Y`` pair. Returns ``None`` if no pair is present.

    Tail extends until a qualifier marker (``[...]``, ``by ...``, or
    ``in <Specimen>``) — analyte-internal ``in`` particles (``Cholesterol
    in HDL``) survive on the correct side because the specimen prefix
    is matched non-greedily against a closed set of specimen tokens.
    """
    if not lcn or "/" not in lcn:
        return None
    m = _PICK_ORIENTATION_RE.match(lcn)
    if not m:
        return None
    head = m.group("head").strip()
    tail = m.group("tail").strip()
    if not head or not tail:
        return None
    return (head, tail)


def _orientation_matches(
    orient: tuple[str, str],
    q_head_lower: list[str],
    q_tail_lower: list[str],
) -> bool:
    """True when the pick's head contains any q_head token AND its tail
    contains any q_tail token (case-insensitive substring). Used to
    detect correct-vs-reversed orientation alignment between the query
    and a LOINC pick."""
    rh_l = orient[0].lower()
    rt_l = orient[1].lower()
    return (
        any(t in rh_l for t in q_head_lower)
        and any(t in rt_l for t in q_tail_lower)
    )


def _merge_axes_and_legacy(
    axes_result: dict,
    legacy_top: object | None,
    *,
    query_text: str | None = None,
    bundle_dir: str | None = None,
) -> dict:
    """Pick winner between axes pipeline and legacy ``resolve_many``.

    Three-stage arbitration (in order):

      1. **Alias-locked axes COMPONENT** — when an alias key appearing
         in *query_text* maps to a target matching the axes COMPONENT
         pick name AND axes top LCN contains that target, the COMPONENT
         is alias-confirmed and axes wins regardless of score. Catches
         ``β-葡萄糖醛酸苷酶`` where axes correctly anchors on
         ``Beta glucuronidase`` (LOINC 13962-6 family) but legacy's
         cosine is dragged onto a wrong-analyte high-cosine outlier
         (``Glucose in Stool`` 48036-8) because the embedder sees
         ``粪便`` + ``葡萄糖`` together.

      2. **Close-tie → legacy** — when ``|axes_score - legacy_score|
         < 0.005``, prefer legacy. The v2 pipeline has more rerank
         layers (specificity / family / digit / scale / system-tier),
         so on near-ties it's the more reliable signal. Catches
         ``肺吸虫 IgG`` where axes picks ``Ascaris lumbricoides``
         (wrong genus, no alias support) at 0.7410 and legacy picks
         ``Paragonimus sp Ab`` (correct) at 0.7408.

      3. **Score arbitration** — default ``max(axes_score,
         legacy_score)``. Both scores live in the same query ×
         LongCommonName cosine space.

    **Empty-legacy rule**: when legacy returns no LOINC result, the
    merged pick is also empty — legacy's emptiness is a meaningful
    "no confident match" signal (it has 10+ rerank layers / specificity
    masks / hpv-ag-dna gates that *deliberately* refuse to pick when
    they detect a type-number / family / assay mismatch). Auto-filling
    with the axes pipeline's best-guess routinely surfaces wrong
    LOINC codes — e.g. HPV-23 / HPV-46 / HPV-83 / HPV-8 where LOINC
    has no specific code, axes-pipeline picks a different-numbered
    HPV variant by closest cosine.

    Returns dict with ``chosen`` (winner code/name/score or None) +
    individual pipeline picks.
    """
    axes_picks = axes_result.get("loinc") or []
    axes_top = axes_picks[0] if axes_picks else None  # (code, name, rescore)

    legacy_loinc = None
    if legacy_top is not None:
        legacy_loinc = (
            legacy_top.code, legacy_top.name, float(legacy_top.score),
        )

    # Legacy empty → merged empty. Respect legacy's "no confident match".
    if legacy_loinc is None:
        return {
            "winner": None,
            "chosen": None,
            "axes_pipeline": axes_top,
            "legacy_pipeline": None,
        }

    axes_score = axes_top[2] if axes_top else -1.0
    legacy_score = legacy_loinc[2]

    # Rule 1: axes COMPONENT alias-locked → trust axes.
    if axes_top and query_text:
        axes_component = axes_result.get("axes", {}).get("COMPONENT")
        axes_component_name = (
            axes_component.name
            if axes_component is not None and hasattr(axes_component, "name")
            else (axes_component.get("name", "") if axes_component else "")
        )
        if (
            axes_component_name
            and axes_component_name.lower() in axes_top[1].lower()
            and _axes_component_alias_locked(
                query_text, axes_component_name, bundle_dir,
            )
            # Only override score when the analytes disagree — if both
            # pipelines already picked the same analyte family, the
            # rescore tells us which specimen/method variant wins and
            # we should respect it.
            and axes_component_name.lower() not in legacy_loinc[1].lower()
        ):
            return {
                "winner": "axes",
                "chosen": axes_top,
                "axes_pipeline": axes_top,
                "legacy_pipeline": legacy_loinc,
            }

    # Rule 1.5: ratio orientation — when the query carries an ``X/Y``
    # glyph pair bridgeable through the short-form overlay AND the
    # legacy pick is correct-orientation AND the axes pick is NOT
    # correct-orientation (either reversed, or a non-ratio single-
    # analyte / panel row), trust legacy regardless of score.
    #
    # Catches ``谷草/谷丙`` where legacy promotes AST/ALT 1916-6 via the
    # ``_apply_ratio_orientation`` finalize step, but axes (which has
    # no orientation-aware lookup) lands on ALT/AST 16325-3 or a
    # single-component ALT row at higher cosine. The score gap would
    # otherwise hand the lead to axes under Rule 3 arbitration.
    #
    # Silent when either query side can't bridge (alternative-style
    # slashes ``白色念珠菌/都柏林念珠菌`` / ``卵巢/睾丸`` /
    # ``小麦/麸质蛋白组反应性与自身免疫·...``), preserving cosine-
    # driven picks for non-ratio uses of the slash.
    if axes_top and query_text:
        from mirobody.indicator.fhir.resolve.pipeline import (
            _RATIO_PAIR_QUERY_RE, _ratio_side_tokens,
        )
        m_pair = _RATIO_PAIR_QUERY_RE.search(query_text)
        if m_pair:
            q_head_toks, h_ok = _ratio_side_tokens(m_pair.group("head"))
            q_tail_toks, t_ok = _ratio_side_tokens(m_pair.group("tail"))
            if h_ok and t_ok and q_head_toks and q_tail_toks:
                qh_l = [t.lower() for t in q_head_toks]
                qt_l = [t.lower() for t in q_tail_toks]
                legacy_o = _parse_pick_orientation(legacy_loinc[1])
                axes_o = _parse_pick_orientation(axes_top[1])
                legacy_correct = (
                    legacy_o is not None
                    and _orientation_matches(legacy_o, qh_l, qt_l)
                )
                axes_correct = (
                    axes_o is not None
                    and _orientation_matches(axes_o, qh_l, qt_l)
                )
                if legacy_correct and not axes_correct:
                    return {
                        "winner": "legacy",
                        "chosen": legacy_loinc,
                        "axes_pipeline": axes_top,
                        "legacy_pipeline": legacy_loinc,
                    }

    # Rule 2: close-tie → legacy. Threshold 0.001 is narrow on purpose —
    # at this scale the two pipelines genuinely share a top candidate
    # and legacy's deeper rerank stack should arbitrate (see ``肺吸虫
    # IgG`` where axes 0.7410 / legacy 0.7408 disagree on genus but
    # legacy hit the right one via alias-free cosine + analyte-concept
    # filter). A wider threshold (e.g. 0.005) catches ``用力肺活量``
    # where axes correctly anchors on FVC at 0.7593 and legacy lands
    # on a sibling ``FVC percent change`` at 0.7545 — the 0.005-tie
    # would falsely hand the lead to legacy. Keep narrow.
    if axes_top and abs(axes_score - legacy_score) < 0.001:
        return {
            "winner": "legacy",
            "chosen": legacy_loinc,
            "axes_pipeline": axes_top,
            "legacy_pipeline": legacy_loinc,
        }

    # Rule 2.5: count-alt corroboration → legacy. The axes pipeline
    # emits ``loinc_alt`` ONLY when the COMPONENT count-rerank disagrees
    # with the score-rerank (different anchor → different candidate
    # set); it is empty otherwise. So when legacy's pick appears in the
    # count-alt set but NOT in the score-primary set, the count signal
    # is corroborating legacy and the score-primary COMPONENT is the
    # embedding artifact — defer to legacy regardless of the cosine gap.
    #
    # Catches ``血小板分布宽度`` where the COMPONENT vocab ranks the
    # superset string ``Platelet component distribution width`` (an
    # aggregometry parameter, cos 0.8877) above the exact ``Platelet
    # distribution width``, locking the candidate set to 76137-9
    # ``[Mass/volume] by calculation`` (rescore 0.7899) and out-scoring
    # legacy's correct 51631-0 ``[Ratio]`` 0.7730 under Rule 3. The
    # count-alt recovers ``Platelet distribution width`` → loinc_alt
    # carries 51631-0, so this rule hands the lead back to legacy.
    #
    # Safe against the Rule-2 ``用力肺活量`` (FVC) case: there the
    # COMPONENT pick is stable, no count-disagreement fires, loinc_alt
    # is empty, and this rule is a no-op.
    if axes_top:
        loinc_alt = axes_result.get("loinc_alt") or []
        alt_codes = {str(c) for c, _n, _s in loinc_alt}
        primary_codes = {str(c) for c, _n, _s in axes_picks}
        legacy_code = str(legacy_loinc[0])
        if legacy_code in alt_codes and legacy_code not in primary_codes:
            return {
                "winner": "legacy",
                "chosen": legacy_loinc,
                "axes_pipeline": axes_top,
                "legacy_pipeline": legacy_loinc,
            }

    # Rule 3: score arbitration.
    if axes_top and axes_score >= legacy_score:
        winner = "axes"
        chosen = axes_top
    else:
        winner = "legacy"
        chosen = legacy_loinc

    return {
        "winner": winner,
        "chosen": chosen,
        "axes_pipeline": axes_top,
        "legacy_pipeline": legacy_loinc,
    }


def _axis_to_dict(c) -> dict:
    """Serialize an AxisCode to dict with score rounded to 4 decimals."""
    d = asdict(c)
    if "score" in d:
        d["score"] = round(float(d["score"]), 4)
    return d


def _resolve_result_to_dict(r) -> dict:
    """Serialize a ResolveResult to dict with all ``score`` fields
    (top-level + any nested per-axis picks) rounded to 4 decimals."""
    d = asdict(r)
    if "score" in d:
        d["score"] = round(float(d["score"]), 4)
    axes = d.get("axes")
    if isinstance(axes, dict):
        for k, v in list(axes.items()):
            if isinstance(v, dict) and "score" in v:
                v["score"] = round(float(v["score"]), 4)
    return d


def _format_axes_rec(term: str, result: dict) -> dict:
    """Shape ``resolve_axes_many`` output into the JSON record.

    Primary picks (``axes`` / ``loinc``) are score-top-1 anchored. When
    a count-rerank tiebreaker disagrees with the score winner on at
    least one axis, the alt picks (``axes_alt`` / ``loinc_alt``) are
    emitted alongside so callers can compare both signals.

    All ``score`` floats are rounded to 4 decimals — full ``float32``
    precision would just add JSON noise without changing rankings (the
    cosine differences that matter live in the third decimal).
    """
    axes = result.get("axes") or {}
    loinc = result.get("loinc") or []
    # ``score`` here is query × LongCommonName cosine over
    # fhir_embeddings.npy — same space as legacy ``resolve_many``
    # output's score, so consumers can A/B compare the two pipelines.
    rec: dict = {
        "term": term,
        "axes": {a: _axis_to_dict(c) for a, c in axes.items()},
        "loinc": [
            {"code": c, "name": n, "score": round(float(s), 4)} for c, n, s in loinc
        ],
    }
    axes_alt = result.get("axes_alt") or {}
    if axes_alt:
        rec["axes_alt"] = {a: _axis_to_dict(c) for a, c in axes_alt.items()}
    loinc_alt = result.get("loinc_alt") or []
    if loinc_alt:
        rec["loinc_alt"] = [
            {"code": c, "name": n, "score": round(float(s), 4)}
            for c, n, s in loinc_alt
        ]
    return rec


async def _emit_axes_only(terms, values, args, bundle_dir, resolve_axes_many) -> None:
    """``resolve --axes`` mode — runs both the new vocab-based pipeline
    and the legacy ``resolve_many`` LOINC ranker, then arbitrates by
    max(score) since both scores live in the same query × LongCommonName
    cosine space. The chosen LOINC code is the one most confident in
    that shared metric; both individual pipeline picks are also
    surfaced for inspection.

    When *values* carries a parseable unit (``"350 mg/24h"``), the new
    pipeline's PROPERTY axis is unit-driven (overrides the embedder
    pick) — the rescore against full LongCommonName naturally reflects
    this through the LOINC filter chain narrowing.

    Single term + no --output → pretty JSON dict on stdout.
    Otherwise → JSON Lines ``{"term": ..., "axes": {...},
    "loinc_merged": {...}, "loinc_axes": [...], "loinc_legacy": [...]}``;
    resume keyed on ``(term, value)``.
    """
    from .fhir.resolve.pipeline import resolve_many

    async def _run_both(batch_terms, batch_values):
        has_any = any(v is not None for v in batch_values)
        axes_task = resolve_axes_many(
            batch_terms, values=batch_values, bundle_dir=bundle_dir,
        )
        legacy_task = resolve_many(
            batch_terms, top_k=1, systems=["LOINC"],
            values=batch_values if has_any else None,
            bundle_dir=bundle_dir,
        )
        import asyncio as _aio
        return await _aio.gather(axes_task, legacy_task)

    def _build_rec(term, axes_result, legacy_result_list):
        legacy_top = legacy_result_list[0] if legacy_result_list else None
        merged = _merge_axes_and_legacy(
            axes_result, legacy_top,
            query_text=term, bundle_dir=bundle_dir,
        )
        rec = _format_axes_rec(term, axes_result)
        rec["loinc_merged"] = (
            {"code": merged["chosen"][0],
             "name": merged["chosen"][1],
             "score": round(float(merged["chosen"][2]), 4),
             "winner": merged["winner"]}
            if merged["chosen"] else None
        )
        # Rename: surface both pipeline picks explicitly. The original
        # ``loinc`` field (axes pipeline result) stays for back-compat.
        rec["loinc_legacy"] = (
            [{"code": merged["legacy_pipeline"][0],
              "name": merged["legacy_pipeline"][1],
              "score": round(float(merged["legacy_pipeline"][2]), 4)}]
            if merged["legacy_pipeline"] else []
        )
        return rec

    if len(terms) == 1 and not args.output:
        axes_results, legacy_results = await _run_both(
            [terms[0]], [values[0]],
        )
        rec = _build_rec(terms[0], axes_results[0], legacy_results[0])
        rec.pop("term", None)  # legacy single-term shape omits the echo
        print(json.dumps(rec, ensure_ascii=False, indent=2))
        return

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
    pbar = tqdm(total=len(terms), initial=len(done), desc="resolve-axes", unit="term")
    try:
        for i in range(0, len(remaining_idx), chunk):
            batch_idx = remaining_idx[i : i + chunk]
            batch_terms = [terms[j] for j in batch_idx]
            batch_values = [values[j] for j in batch_idx]
            axes_results, legacy_results = await _run_both(
                batch_terms, batch_values,
            )
            for term, value, axes_r, legacy_r in zip(
                batch_terms, batch_values, axes_results, legacy_results,
            ):
                rec = _build_rec(term, axes_r, legacy_r)
                if value is not None:
                    rec["value"] = value
                out_fp.write(json.dumps(rec, ensure_ascii=False) + "\n")
            out_fp.flush()
            pbar.update(len(batch_idx))
    finally:
        pbar.close()
        if out_fp is not sys.stdout:
            out_fp.close()


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
    from .fhir.embeddings.axes import resolve_axes_many
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

    # ── --axes mode: independent of the LOINC resolve pipeline ───────
    # Embeds each query once and cosines against the per-axis vocab
    # matrices (``mirobody/res/loinc_axes/<AXIS>.npy``), returning the
    # top-1 part_name + score per axis. No LOINC top-1, no centroid
    # gate, no SNOMED fallback — just direct multilingual axis matching.
    if args.axes:
        await _emit_axes_only(terms, values, args, bundle_dir, resolve_axes_many)
        return

    # Legacy shape preserved for ad-hoc single-term lookups.
    if len(terms) == 1 and not args.output:
        batch_results = await resolve_many(
            terms, top_k=args.top_k, systems=args.systems,
            values=values if has_any_value else None,
            bundle_dir=bundle_dir,
            emit_axes=args.axes,
        )
        results = batch_results[0]
        print(json.dumps(
            [_resolve_result_to_dict(r) for r in results],
            ensure_ascii=False, indent=2,
        ))
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
                emit_axes=args.axes,
            )
            for term, value, results in zip(batch_terms, batch_values, batch_results):
                rec: dict = {"term": term}
                if value is not None:
                    rec["value"] = value
                rec["results"] = [_resolve_result_to_dict(r) for r in results]
                out_fp.write(json.dumps(rec, ensure_ascii=False) + "\n")
            out_fp.flush()
            pbar.update(len(batch_idx))
    finally:
        pbar.close()
        if out_fp is not sys.stdout:
            out_fp.close()
