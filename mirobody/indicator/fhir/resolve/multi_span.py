"""Candidate anchor-span generation for query-side multi-framing embed.

A typical resolve query is a layered string:

    ``血液专项检查|全血粘度测定·红细胞电泳时间|RCE``
     ^^^^^^^^^^^ ^^^^^^^^^^^^^ ^^^^^^^^^^^^ ^^^
     source         parent       child      abbrev

Embedded as one string, cosine is dominated by the densest semantic
mass — usually the parent term in parent·child hierarchies. ``红细胞
电泳时间`` (RBC electrophoresis time) gets pulled toward LOINC
``Viscosity of Blood`` because the parent ``全血粘度测定`` is a strong
LOINC anchor while the child is a weak one.

Instead of trying to define "the right span" up front, this module
returns up to four candidate framings of the same query. The caller
(:mod:`.pipeline`) embeds each, then reduces per-LOINC-row similarity
to ``max`` across spans — so whichever framing has the strongest
match for a given LOINC concept wins that row independently. Net
effect: parent and child concepts both get a fair shot at the picker,
without having to pre-decide which framing is authoritative.

Spans generated (deduplicated, full always first):

  1. Full query (always).
  2. Last hierarchical segment (``·・``-split) of the longest
     ``|``-separated chunk. The "child" in parent·child constructions.

We deliberately do NOT include:

  * **First segment (parent)** as its own span. Under ``max``-pool,
    the parent span acts as a per-row cosine floor for every query
    that shares it — ``全血粘度测定 → 4690-4`` cosine is ~0.85, which
    beats any child-specific Plasma/Hematocrit match in a different
    family. The full-query span already carries the parent text;
    we just don't let it dominate.

  * **Indicator-only chunk** (``|``-split). Counterintuitively, this
    span HURTS more than helps: stripping the source prefix removes
    generic context (``血液专项检查``/``Lab tests``) that anchors the
    blood-vs-plasma distinction, making the parent term relatively
    more dominant inside the indicator label. Empirically flipped
    ``全血粘度测定·血浆粘度`` from correctly resolving 4691-2
    (Viscosity of Plasma) to 4690-4 (Viscosity of Blood).

The minimal full+last pairing is the smallest reduction that
actually helps the cases this module exists to fix without
introducing regressions.

Spans shorter than :data:`_MIN_SPAN_LEN` are dropped (a bare digit
or single letter doesn't anchor anything in embedding space). Order
is documentation only; the consumer's ``max`` reduction is
order-independent.

Cost: 1-2 embedding calls per query, batched together with all other
queries in the chunk. Disk cache (``text_embedding(..., cache=True)``)
deduplicates repeated spans across queries — many parent·child rows
in one batch share the child suffix (rarer than parent sharing, but
still helps). End-to-end overhead is a small fraction of total
resolve time (matmul against ~700k corpus rows dominates).
"""

from __future__ import annotations

import re


# Hierarchical separators used inside indicator labels. Only the
# explicit CJK middle dots — ``·`` (U+00B7) and ``・`` (U+30FB) — are
# treated as parent·child separators. ASCII and fullwidth commas
# (``,`` ``，``) are deliberately EXCLUDED here even though
# :data:`.category._INDICATOR_SEPARATOR_RE` includes them: the
# section-header detector only checks the last segment for an exact
# whitelist match, so splitting on comma in chemistry notation (``1,25-
# (OH)2-D`` or ``D-二聚体，ELISA``) is harmless there but actively
# destructive here, where the last segment becomes a span that drives
# embedding cosine via max-pool. Empirically: 0 indicators in the
# corpus use comma as a hierarchy separator; 1 uses it for a position
# prefix (``1,25-``); 16 use ``，`` for method/list separators inside
# an analyte name. Splitting either would surface a misleading span.
_HIERARCHY_SEP_RE = re.compile(r"[·・]")


# Field separator between source / indicator / abbrev. Matches
# ``run_resolve.py``'s default join character; the resolve CLI surfaces
# it verbatim when the upstream catalog uses it.
_FIELD_SEP_RE = re.compile(r"\|")


# Minimum span length (characters, after strip). Spans of one character
# (``5``, ``D``, ``A``) embed too generically — they amplify noise,
# not signal. Two characters is the smallest unit that reliably maps
# to a specific concept across CJK + Latin scripts.
_MIN_SPAN_LEN: int = 2


# Single-character Han element names are complete, specific analytes
# (铬 Chromium, 钙 Calcium, 硒 Selenium, …) yet embed too generically in
# isolation to clear _MIN_SPAN_LEN — and as a ``·``-child they are easily
# buried by a parent that is itself a valid LOINC analyte: ``碳水化合物
# 代谢·铬`` resolves to ``Carbohydrates [Identifier] in Urine`` because
# the parent ``碳水化合物`` (Carbohydrate) dominates the only surviving
# (full-string) span. Emitting the canonical English element name as a
# child span gives the element a strong, parent-independent framing that
# wins max-pool. This is deliberately NOT a lexicon alias: the shipped
# alias bonus path ignores 1-char candidates, and the substring-based
# query augment would fire ``钙``→Calcium inside ``降钙素`` (Calcitonin),
# ``铁`` inside ``铁蛋白`` (Ferritin), ``钠`` inside ``利钠肽`` (BNP).
# Here the char is already the isolated ``·``-split child, so no
# compound-word collision is possible.
_CN_ELEMENT_EN: dict[str, str] = {
    "铬": "Chromium", "钙": "Calcium", "铁": "Iron", "锌": "Zinc",
    "硒": "Selenium", "碘": "Iodine", "磷": "Phosphorus", "钾": "Potassium",
    "钠": "Sodium", "镁": "Magnesium", "铜": "Copper", "锰": "Manganese",
    "钴": "Cobalt", "钼": "Molybdenum", "汞": "Mercury", "铅": "Lead",
    "砷": "Arsenic", "镉": "Cadmium", "铝": "Aluminum", "镍": "Nickel",
    "锂": "Lithium", "钒": "Vanadium", "锶": "Strontium", "钡": "Barium",
    "铊": "Thallium", "铍": "Beryllium", "锑": "Antimony", "铋": "Bismuth",
    "铂": "Platinum", "硅": "Silicon", "硼": "Boron", "氟": "Fluoride",
    "溴": "Bromide", "银": "Silver", "金": "Gold", "钛": "Titanium",
    "钨": "Tungsten", "镓": "Gallium", "锗": "Germanium", "铷": "Rubidium",
    "铯": "Cesium", "碲": "Tellurium",
}


def generate_anchor_spans(query: str) -> list[str]:
    """Return candidate anchor spans for *query*, full string first.

    Always includes the full query (after strip) when non-empty. When
    the query's longest ``|``-separated chunk contains a hierarchical
    separator (``·・``), additionally appends the last segment of
    that chunk (the "child"). Both parent (first segment) and the
    bare indicator chunk are deliberately excluded — see module
    docstring for why each one causes max-pool regressions.

    Spans of fewer than :data:`_MIN_SPAN_LEN` characters or that
    exactly equal an already-yielded span are dropped. Empty input
    returns ``[]``.

    The order is documentation only — the pipeline's ``max``
    reduction over per-row span cosine is order-independent.
    """
    if not query:
        return []
    query = query.strip()
    if not query:
        return []

    spans: list[str] = [query]
    seen: set[str] = {query}

    def _add(span: str) -> None:
        span = span.strip()
        if len(span) >= _MIN_SPAN_LEN and span not in seen:
            spans.append(span)
            seen.add(span)

    # Identify the indicator chunk for hierarchical split: longest
    # piece after ``|``-split (the indicator label, sans source-prefix
    # and abbreviation). When no ``|`` is present, the whole query is
    # the indicator chunk. The chunk itself is NOT added as a span —
    # stripping the source prefix paradoxically amplifies parent
    # dominance for cases where the source phrase carries useful
    # blood/plasma/etc. context.
    if "|" in query:
        chunks = [c.strip() for c in _FIELD_SEP_RE.split(query) if c.strip()]
        indicator_chunk = max(chunks, key=len) if chunks else query
    else:
        indicator_chunk = query

    # Hierarchical split inside the indicator chunk: child (last
    # segment) only.
    if _HIERARCHY_SEP_RE.search(indicator_chunk):
        parts = [
            p.strip()
            for p in _HIERARCHY_SEP_RE.split(indicator_chunk)
            if p.strip()
        ]
        if len(parts) >= 2:
            child = parts[-1]
            _add(child)
            # Bridge a single-char element child to its English name. A
            # bare ``Chromium`` span embeds too generically to beat a
            # competing parent analyte; appending the element name to the
            # FULL query keeps the lab-test context that anchors the
            # specimen/measurement while injecting the element signal that
            # the buried 1-char child could not carry on its own.
            en = _CN_ELEMENT_EN.get(child)
            if en is not None:
                _add(f"{query} {en}")

    return spans
