"""The engine's front door — parse a health document, resolve to standard codes.

Zero infrastructure: no PostgreSQL, no Redis, no server. ``resolve`` needs no
credentials at all (pure offline lookup against the shipped data bundles);
``parse`` needs exactly one LLM key (any of the providers
:func:`mirobody.utils.llm.unified_file_extract` auto-detects).

    from mirobody.engine import resolve, parse_file

    resolve("血红蛋白").loinc          # -> "718-7", offline
    await parse_file("labs.pdf")       # -> readings + resolutions, one LLM call

This is the deliberate small door into the first two engine stages (① Collect,
② Standardize) — the same machinery the full platform uses, minus its persistence:

* **Lexical resolution** rides the shipped LOINC bundle: the 921k-entry
  multilingual alias index (``loinc_alias_index.npz``), the 677k-name corpus
  sidecar (``fhir_meta.csv.gz``), the per-row commonness prior
  (``loinc_rank_bonus.npy``), and the LOINC axis table for the final
  name → LOINC_NUM hop. All official build artifacts, loaded read-only —
  plus ``res/resolver_overrides.tsv``, the hand-written corrections for terms
  the index gets wrong or misses (see that file's header, and
  ``test_engine_coverage.py`` for what it is measured against).
* **What this deliberately is NOT**: the full v2 semantic pipeline
  (:func:`mirobody.indicator.fhir.resolve.pipeline.resolve_many`), which adds
  embedding recall + family rerank but requires the multi-GB embedding matrix
  that does not ship in git. When a term misses here, the honest answer is
  ``unresolved`` — never a guess.
* **Unit normalization** via :mod:`mirobody.indicator.fhir.units` (offline).

The candidate picker is a lite heuristic (commonness prior, then a small
preference for the plain Serum/Plasma/Blood variants over cord/capillary
specials). It exists to give ONE good default answer; ``candidates`` carries
the count so callers know when a term was ambiguous.
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import logging
import os
import re
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Optional

from .indicator.lexical import split_trailing_parenthetical, surface_variants

logger = logging.getLogger(__name__)

_RES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "res")
_BUNDLE = os.path.join(_RES_DIR, "fhir_loinc_bundle.tar.gz")
_META = os.path.join(_RES_DIR, "fhir_meta.csv.gz")
_ALIAS_SRC_DIR = os.path.join(_RES_DIR, "aliases_src")
_OVERRIDES = os.path.join(_RES_DIR, "resolver_overrides.tsv")

# Sentinel target in resolver_overrides.tsv meaning "deliberately unresolved".
_BLOCK_SENTINEL = "!unresolved"

# Prefer the everyday specimen variants when the commonness prior ties.
_PLAIN_SPECIMEN = re.compile(r"in (Serum or Plasma|Blood)\b", re.I)
_SPECIAL_SPECIMEN = re.compile(r"\b(cord|capillary|venous|arterial|dialysis)\b", re.I)
# "Fasting plasma glucose FPG" -> "Fasting plasma glucose" (trailing acronym).
_TRAILING_ACRONYM = re.compile(r"\s+[A-Z][A-Z0-9-]{1,7}$")


@dataclass(frozen=True)
class Resolution:
    """One resolved indicator name."""

    term: str                       # the input, as given
    canonical: str = ""             # canonical long name from the corpus
    loinc: str = ""                 # LOINC_NUM when the canonical name is LOINC
    candidates: int = 0             # how many corpus rows matched the alias
    resolved: bool = False

    @property
    def code(self) -> str:
        return self.loinc


@dataclass(frozen=True)
class Reading:
    """One extracted observation from a document."""

    name: str
    value: str = ""
    unit: str = ""
    reference_range: str = ""
    resolution: Optional[Resolution] = None


class OfflineResolver:
    """Lexical indicator-name resolution against the shipped bundles.

    Construction cost is a few seconds (rebuilding the 921k-alias dict);
    use :func:`get_resolver` for the cached singleton.
    """

    def __init__(self) -> None:
        import numpy as np

        from .indicator.fhir.embeddings.alias import _normalize
        from .indicator.fhir.embeddings.bundle import read_member

        self._normalize = _normalize

        raw = read_member("loinc_alias_index.npz", bundle_path=_BUNDLE)
        if raw is None:
            raise RuntimeError(
                f"loinc_alias_index.npz not found in {_BUNDLE} — run `git lfs pull` "
                "to fetch the data bundles (see README prerequisites)"
            )
        with np.load(io.BytesIO(raw), allow_pickle=True) as z:
            aliases, offsets, rows = z["aliases"], z["offsets"], z["rows"]
        self._alias: dict[str, "np.ndarray"] = {
            str(a): rows[offsets[i]:offsets[i + 1]] for i, a in enumerate(aliases)
        }

        self._names: list[str] = []
        with gzip.open(_META, "rt", encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                self._names.append(row[0])

        rank_raw = read_member("loinc_rank_bonus.npy", bundle_path=_BUNDLE)
        self._rank = (
            np.load(io.BytesIO(rank_raw))
            if rank_raw is not None
            else np.zeros(len(self._names), dtype=np.float32)
        )

        # name -> LOINC_NUM, from the axis table inside the bundle.
        self._loinc_by_name: dict[str, str] = {}
        axis_raw = read_member("loinc_axis.csv", bundle_path=_BUNDLE)
        if axis_raw is not None:
            for row in csv.DictReader(io.StringIO(axis_raw.decode("utf-8"))):
                self._loinc_by_name[_normalize(row["LONG_COMMON_NAME"])] = row["LOINC_NUM"]

        # term -> a target the index resolves. Two sources, in precedence order:
        #
        #   1. res/resolver_overrides.tsv — this resolver's own corrections,
        #      where the target is guaranteed to be an index key.
        #   2. res/aliases_src/*.tsv — the bundle build's curated inputs,
        #      reused here as a broad fallback (~48k foreign-language terms).
        #      Their targets are descriptive phrases meant for the index BUILD,
        #      so they resolve only sometimes; that is why (1) exists.
        self._src: dict[str, str] = {}
        for path in self._alias_source_files():
            with open(path, encoding="utf-8") as f:
                for line in f:
                    if line.startswith("#"):
                        continue
                    parts = line.rstrip("\n").split("\t")
                    if len(parts) == 2 and parts[0] and parts[1]:
                        self._src.setdefault(_normalize(parts[0]), parts[1])

        logger.info(
            "OfflineResolver ready: %d aliases, %d corpus names, %d LOINC axis rows",
            len(self._alias), len(self._names), len(self._loinc_by_name),
        )

    @staticmethod
    def _alias_source_files() -> list[str]:
        """Alias TSVs in precedence order; earlier files win (setdefault).

        Overrides first, then the bundle-build inputs — and within those, the
        hand-written `*_curated.tsv` ahead of the machine-derived `<lang>.tsv`.
        Plain sorted order put zh.tsv before zh_curated.tsv, which silently
        discarded curated corrections: adding the right row changed nothing.
        """
        files = [_OVERRIDES] if os.path.isfile(_OVERRIDES) else []
        if os.path.isdir(_ALIAS_SRC_DIR):
            names = [fn for fn in os.listdir(_ALIAS_SRC_DIR) if fn.endswith(".tsv")]
            files += [
                os.path.join(_ALIAS_SRC_DIR, fn)
                for fn in sorted(names, key=lambda n: (0 if "_curated" in n else 1, n))
            ]
        return files

    # -- lookup ----------------------------------------------------------------

    def _candidate_keys(self, term: str) -> list[str]:
        """The lookup keys to try, most-specific first.

        The alias-table hop goes FIRST, ahead of the raw index lookup. A row in
        ``self._src`` is a statement about what one term *means* — one term,
        one concept, written by a person. A hit in the big alias index is a bag
        of every corpus row sharing a surface string, which ``_pick`` then
        guesses among by commonness. When both are available the written
        statement wins, because it is the one carrying human intent.

        This ordering is not cosmetic. ``血红蛋白`` matches 301 rows in the index,
        of which the commonness prior likes *Hemoglobin A1c* best — so the
        index-first order answered "hemoglobin" with the code for a completely
        different test, silently and confidently. The alias table says plainly
        ``血红蛋白 -> Hemoglobin``. See test_engine_coverage.py, which exists
        largely to keep this class of near-miss from coming back.

        Both hops are then repeated for each surface variant from
        :func:`mirobody.indicator.lexical.surface_variants`. The bundle's own
        normalizer is NFKC + casefold and nothing more — it has to be, it folded
        the index keys at build time — so a full-width ``ＦＢＧ``, an en-dashed
        ``LDL–C`` and a snake_case ``fasting_glucose`` each sat one invisible
        codepoint away from a key that already exists. (snake_case is not a
        corner case: it is the convention the platform API documents in every
        ``POST /data`` example.) Variants come LAST, after the term as written
        has missed, so they can only turn a miss into a hit — never overrule an
        answer that was already correct.
        """
        keys: list[str] = []
        for surface in surface_variants(term):
            for key in self._keys_for(self._normalize(surface)):
                if key not in keys:
                    keys.append(key)
        return keys

    def _is_blocked(self, term: str) -> bool:
        """True when ANY surface variant of the term is a deliberate
        non-answer (target ``!unresolved`` in resolver_overrides.tsv).

        Every variant, not only the term as written: variant lookup is a new
        route into the index, so a block that knew one spelling would simply be
        walked around. Measured — ``blood_pressure`` skipped a block written for
        ``blood pressure``, tokenized to it anyway, hit those 183 rows and came
        back 8462-4 (diastolic): the exact bug the sentinel exists to prevent,
        reached through the back door.
        """
        return any(
            self._src.get(self._normalize(surface)) == _BLOCK_SENTINEL
            for surface in surface_variants(term)
        )

    def _keys_for(self, norm: str) -> list[str]:
        """Alias-table hop then the raw key, for one already-normalized term."""
        keys: list[str] = []
        eng = self._src.get(norm)
        if eng:
            keys.append(self._normalize(eng))
            # "Fasting plasma glucose FPG" -> "fasting plasma glucose"
            stripped = _TRAILING_ACRONYM.sub("", eng).strip()
            if stripped and stripped != eng:
                keys.append(self._normalize(stripped))
        keys.append(norm)
        return keys

    def _pick(self, rows) -> int:
        """Best corpus row: commonness prior, nudged toward plain specimens."""
        best_row, best_score = int(rows[0]), float("-inf")
        for r in rows:
            r = int(r)
            name = self._names[r]
            score = float(self._rank[r])
            if _PLAIN_SPECIMEN.search(name):
                score += 0.25
            if _SPECIAL_SPECIMEN.search(name):
                score -= 0.25
            if score > best_score:
                best_row, best_score = r, score
        return best_row

    def resolve(self, term: str) -> Resolution:
        """Resolve one free-text indicator name. Never raises on a miss."""
        term = (term or "").strip()
        if not term:
            return Resolution(term=term)

        # Deliberate non-answers (target `!unresolved` in the overrides file).
        # Some terms name a PANEL, not one observation: "blood pressure" matched
        # 183 index rows and the commonness prior returned the DIASTOLIC code —
        # so a systolic reading filed under that answer lands in the wrong
        # series entirely. A confident wrong code is worse than an honest miss,
        # so these resolve to nothing and the caller has to ask which
        # measurement was meant.
        #
        # Both spellings are checked, because underscore flattening in
        # `_candidate_keys` reaches the index under the spaced form: with only
        # the as-written check, `blood_pressure` skipped the block, flattened to
        # `blood pressure`, hit those 183 rows and came back 8462-4 —
        # re-creating the very bug this guard was written for, through the back
        # door.
        if self._is_blocked(term):
            return Resolution(term=term)

        hit = self._lookup(term)
        if hit is not None:
            return hit

        # "名称(缩写)" — the shape a lab report prints more often than not. On
        # the hosted platform's production data, 147 of 868 distinct indicator
        # names are this shape and 70 of them carried no code at all.
        #
        # Which half is the answer is NOT decidable by position, so it is not
        # guessed. ``空腹血糖(GLU)`` means the stem; ``血糖(HbA1c)`` means the
        # parenthetical, and answering that one with glucose would file an HbA1c
        # reading into the glucose series. So both halves are resolved, and when
        # they disagree the term stays unresolved — the same trade the
        # ``!unresolved`` sentinel makes, applied to a shape rather than a word.
        stem, inside = split_trailing_parenthetical(term)
        if not stem and not inside:
            return Resolution(term=term)
        if (stem and self._is_blocked(stem)) or (inside and self._is_blocked(inside)):
            return Resolution(term=term)

        stem_hit = self._lookup(stem) if stem else None
        inside_hit = self._lookup(inside) if inside else None
        if stem_hit and inside_hit and stem_hit.loinc != inside_hit.loinc:
            return Resolution(term=term)

        chosen = stem_hit or inside_hit
        if chosen is None:
            return Resolution(term=term)
        return Resolution(
            term=term,
            canonical=chosen.canonical,
            loinc=chosen.loinc,
            candidates=chosen.candidates,
            resolved=True,
        )

    def _lookup(self, term: str) -> Resolution | None:
        """First candidate key that hits the alias index, or None on a miss."""
        for key in self._candidate_keys(term):
            rows = self._alias.get(key)
            if rows is None or not len(rows):
                continue
            row = self._pick(rows)
            name = self._names[row]
            return Resolution(
                term=term,
                canonical=name,
                loinc=self._loinc_by_name.get(self._normalize(name), ""),
                candidates=int(len(rows)),
                resolved=True,
            )
        return None


@lru_cache(maxsize=1)
def get_resolver() -> OfflineResolver:
    return OfflineResolver()


def resolve(term: str) -> Resolution:
    """Module-level convenience: offline-resolve one indicator name."""
    return get_resolver().resolve(term)


# ── parse: document -> readings (one LLM call) ────────────────────────────────

_EXTRACT_PROMPT = """You are a medical lab-report extraction engine.
Extract EVERY health indicator measurement from this document.
Return ONLY a JSON array, no prose. Each element:
{"name": "<indicator name exactly as printed>", "value": "<numeric or textual result>", "unit": "<unit as printed, empty if none>", "reference_range": "<as printed, empty if none>"}
Rules: keep the original language of names; do not translate; do not invent
values; skip section headers and non-measurements."""

_MIME = {
    ".pdf": "application/pdf",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".txt": "text/plain",
    ".csv": "text/csv",
}


async def parse_file(path: str, *, resolve_names: bool = True) -> list[Reading]:
    """Parse a lab report / health document into readings, optionally resolving
    each indicator name to its canonical LOINC identity (offline).

    One LLM call via :func:`mirobody.utils.llm.unified_file_extract` (provider
    auto-selected from whichever API key is present). Raises RuntimeError with
    a plain message when no provider key is configured.
    """
    # The provider auto-detection reads keys through the config system (which
    # also loads .env); standalone callers — the CLI, a bare library user —
    # haven't initialized it. Init is idempotent and works with zero yaml files.
    from .utils import Config
    from .utils.llm import unified_file_extract

    await Config.init()

    ext = os.path.splitext(path)[1].lower()
    if ext in (".txt", ".csv", ".md", ".tsv"):
        # Plain-text documents skip the vision path entirely: read the text and
        # ask for the extraction directly (unified_file_extract is built for
        # binary/vision inputs and returns nothing useful for text uploads).
        from .utils.llm import async_get_text_completion

        with open(path, encoding="utf-8", errors="replace") as f:
            document = f.read()
        raw = await async_get_text_completion(
            [
                {"role": "system", "content": _EXTRACT_PROMPT},
                {"role": "user", "content": document},
            ]
        ) or ""
    else:
        content_type = _MIME.get(ext, "application/pdf")
        raw = await unified_file_extract(path, _EXTRACT_PROMPT, content_type=content_type, json_mode=True)

    text = raw.strip()
    if text.startswith("```"):
        text = re.sub(r"^```[a-zA-Z]*\n?|```$", "", text).strip()
    try:
        items = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"extraction returned non-JSON output: {text[:200]}") from exc
    if not isinstance(items, list):
        raise RuntimeError(f"extraction returned {type(items).__name__}, expected a JSON array")

    resolver = get_resolver() if resolve_names else None
    readings: list[Reading] = []
    for it in items:
        if not isinstance(it, dict) or not it.get("name"):
            continue
        name = str(it["name"]).strip()
        readings.append(
            Reading(
                name=name,
                value=str(it.get("value") or "").strip(),
                unit=str(it.get("unit") or "").strip(),
                reference_range=str(it.get("reference_range") or "").strip(),
                resolution=resolver.resolve(name) if resolver else None,
            )
        )
    return readings
