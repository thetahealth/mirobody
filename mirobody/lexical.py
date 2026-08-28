"""Surface algebra for indicator names: NFKC-lite folding + a CJK-aware tokenizer.

Pure Python, no dependencies, no data files — importable anywhere in the engine.

**Two normalizers live here, and the split is load-bearing.**
:func:`index_fold` is the BUNDLE's normalizer: the keys in
``loinc_alias_index.npz`` were folded with it at build time, so changing it
would silently stop those keys matching. It is therefore frozen by the
artifact, and it is minimal — NFKC + casefold, and nothing else.
:func:`normalize` is the LOOKUP side, free to be as thorough as the input
deserves; it derives *additional* candidate surfaces that are then looked up
with :func:`index_fold`.

They used to sit in different packages — ``index_fold`` was a private
``_normalize`` inside ``indicator/fhir/embeddings/alias.py``, i.e. inside the
bundle-BUILD tooling, imported from there by ``engine.py``. One function that
the build and the runtime must agree on exactly is precisely the function that
must have one home, and that home has to be on the runtime side, because the
build tooling does not ship.

What that thoroughness buys, measured on real report text:

* ``ＦＢＧ`` (full-width), ``LDL–C`` (en-dash), ``mg/m²`` (superscript),
  ``空腹　血糖`` (ideographic space) all fold to the plain form. Before this,
  ``LDL–C`` missed while ``LDL-C`` resolved — one invisible codepoint apart.
* ``fasting_glucose`` tokenizes to ``fasting glucose``. Every ``POST /data``
  example in the platform docs names indicators in snake_case, and every one of
  them missed while its spaced form resolved.

A run of CJK is deliberately ONE token: it keeps ``血糖`` and ``空腹血糖``
distinct surfaces rather than making one a substring of the other.

The folds are deliberately conservative, and they are pinned rather than tuned:
``test_lexical.py`` golden-locks every one of them, because a fold that looks
harmless in isolation changes which surface a term collides with.
"""

from __future__ import annotations

import re
import unicodedata

__all__ = [
    "TRAILING_PARENTHETICAL",
    "index_fold",
    "normalize",
    "split_trailing_parenthetical",
    "surface_variants",
    "word_tokens",
]

# NFKC-lite: the 1:1 codepoint folds that actually occur in clinical surfaces.
_SUPERSCRIPT = {
    "⁰": "0", "¹": "1", "²": "2", "³": "3", "⁴": "4",
    "⁵": "5", "⁶": "6", "⁷": "7", "⁸": "8", "⁹": "9",
}

# Hyphen / dash / minus variants → ASCII "-". IUPAC chemical names use U+2010,
# and a report exported from Word carries en-dashes; both look identical on
# screen to the ASCII form the index was built with.
_DASHES = {"‐", "‑", "‒", "–", "—", "―", "−", "⁃"}

_ASCII_WS = frozenset(" \t\n\r\f\v")

# A trailing parenthetical, in any of the bracket pairs a lab report uses.
# Full-width（）is what a Chinese report prints; 【】appears in some templates.
TRAILING_PARENTHETICAL = re.compile(r"[（(\[【]([^)）\]】]*)[)）\]】]\s*$")


def index_fold(s: str) -> str:
    """NFKC normalize + casefold. CJK passes through unchanged.

    **The bundle's own key fold — do not "improve" it.** Every key in
    ``loinc_alias_index.npz`` was written through this exact function, so any
    change here stops those keys matching and the resolver silently loses
    recall. The build pass that mints the index
    (``indicator/fhir/embeddings/alias.py``) imports it from here rather than
    keeping a second copy, which is what makes "the build and the runtime fold
    identically" a fact instead of a convention.

    ``casefold`` (not ``lower``) handles ß / İ correctly; CJK is unaffected.
    """
    if not s:
        return ""
    return unicodedata.normalize("NFKC", s).strip().casefold()


def _fold_cp(ch: str) -> str:
    if ch in _SUPERSCRIPT:
        return _SUPERSCRIPT[ch]
    if ch == "　":  # ideographic space
        return " "
    if ch in _DASHES:
        return "-"
    o = ord(ch)
    if 0xFF01 <= o <= 0xFF5E:  # full-width ASCII
        return chr(o - 0xFEE0)
    return ch


def normalize(text: str) -> str:
    """NFKC-lite fold + ASCII casefold + whitespace collapse/trim.

    ASCII ``A-Z`` only, not a full Unicode casefold: CJK and Greek pass through
    untouched, which is what keeps ``β2-微球蛋白`` intact.
    """
    out: list[str] = []
    pending_space = False
    wrote_any = False
    for raw in text or "":
        ch = _fold_cp(raw)
        if ch in _ASCII_WS:
            if wrote_any:
                pending_space = True
            continue
        if "A" <= ch <= "Z":
            ch = ch.lower()
        if pending_space:
            out.append(" ")
            pending_space = False
        out.append(ch)
        wrote_any = True
    return "".join(out)


def _is_ascii_alnum(ch: str) -> bool:
    return ("a" <= ch <= "z") or ("0" <= ch <= "9")


def word_tokens(text: str) -> list[str]:
    """Split ``normalize(text)`` into word tokens.

    A token is a maximal run of ASCII ``[a-z0-9]`` **or** a maximal run of
    non-ASCII codepoints. Every ASCII non-alphanumeric byte — space, slash,
    bracket, comma, and crucially ``_`` and ``-`` — is a separator. No stopword
    or number filtering: the matcher needs ``hpv 16`` and ``vitamin d``.
    """
    out: list[str] = []
    cur: list[str] = []
    cur_nonascii = False
    for ch in normalize(text):
        if _is_ascii_alnum(ch):
            if cur and cur_nonascii:
                out.append("".join(cur))
                cur = []
            cur.append(ch)
            cur_nonascii = False
        elif ord(ch) >= 0x80:
            if cur and not cur_nonascii:
                out.append("".join(cur))
                cur = []
            cur.append(ch)
            cur_nonascii = True
        else:
            if cur:
                out.append("".join(cur))
                cur = []
    if cur:
        out.append("".join(cur))
    return out


# "Total Cholesterol-TC" -> "Total Cholesterol". The analyte and its
# abbreviation joined by a hyphen, which is what `mirobody parse` emits: on the
# shipped demo report, 12 of 12 extracted names carried this shape and 0 of 12
# resolved. `engine._TRAILING_ACRONYM` already handles the space-separated form
# ("Fasting plasma glucose FPG") but only on alias-table VALUES, never on the
# incoming term.
#
# Bounded on both sides so it strips a suffix and not a word: at least three
# characters before the hyphen, at most seven after, and the tail must start
# with a letter or digit. "High-Density Lipoprotein" is untouched (the hyphen is
# not final), and so is "25-Hydroxyvitamin D3".
_TRAILING_HYPHEN_ABBREV = re.compile(r"(?<=\w{3})-([A-Za-z][A-Za-z0-9]{0,6}|[0-9][A-Za-z0-9]{0,6})$")


def surface_variants(term: str) -> list[str]:
    """The spellings of ``term`` worth trying, most faithful first.

    Never more than five, and the first is always the term as written, so a
    caller that stops at the first hit keeps today's answer for today's inputs.
    Every entry after the first can only turn a miss into a hit.

    The last is the zh-Hant → zh-Hans fold. The alias lexicon build already
    mirrors Simplified keys to Traditional in the BUNDLE, but
    ``res/resolver_overrides.tsv`` is a runtime file that gets no such
    expansion — and it holds the hand-curated everyday panel terms. Measured
    before this: of eight common indicators whose Traditional spelling differs,
    two resolved and six returned nothing, with no rule distinguishing them.
    Folding the query is the symmetric half of what the build does to the
    corpus. See :mod:`mirobody.zh_fold` for why folding is a script
    transform and never a translation.
    """
    from .zh_fold import fold_to_hans

    out: list[str] = []
    candidates = (
        term,
        normalize(term),
        " ".join(word_tokens(term)),
        _TRAILING_HYPHEN_ABBREV.sub("", term or "").strip(),
        fold_to_hans(term or ""),
    )
    for candidate in candidates:
        candidate = (candidate or "").strip()
        if candidate and candidate not in out:
            out.append(candidate)
    return out


def split_trailing_parenthetical(term: str) -> tuple[str, str]:
    """``"空腹血糖(GLU)"`` → ``("空腹血糖", "GLU")``; ``("", "")`` when there is none.

    A lab report writes the analyte and its abbreviation together far more often
    than not: on the hosted platform's production data, 147 of 868 distinct
    indicator names were this shape, and 70 of those carried no code at all.
    Both halves are returned because neither is reliably the answer — see
    ``OfflineResolver.resolve`` for the rule that decides between them.
    """
    match = TRAILING_PARENTHETICAL.search(term or "")
    if not match:
        return "", ""
    inside = match.group(1).strip()
    # The parenthetical must look like a NAME, not a unit. `中性粒细胞(%)` is a
    # differential percentage whose stem answers the ABSOLUTE-count code
    # (751-8) while the value is a fraction — stripping it would turn an honest
    # miss into a confidently wrong answer, which is the failure this project
    # scores worst. Requiring a letter keeps `(ALT)` and `(10*9/L)` (which does
    # name a unit, but is at least a token the index can be asked about) while
    # rejecting `(%)`, `(+)` and `(-)`.
    if not any(ch.isalpha() for ch in inside):
        return "", ""
    return TRAILING_PARENTHETICAL.sub("", term).strip(), inside
