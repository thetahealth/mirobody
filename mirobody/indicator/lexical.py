"""Surface algebra for indicator names: NFKC-lite folding + a CJK-aware tokenizer.

Pure Python, no dependencies, no data files — importable anywhere in the engine.

**Why this is separate from ``fhir/embeddings/alias.py::_normalize``.** That one
is the BUNDLE's normalizer: the keys in ``loinc_alias_index.npz`` were folded
with it at build time, so changing it would silently stop those keys matching.
It is therefore frozen by the artifact, and it is minimal — NFKC + casefold, and
nothing else. This module is the LOOKUP side, free to be as thorough as the
input deserves, and it is used to derive *additional* candidate surfaces that
are then looked up with the bundle's own normalizer.

What that thoroughness buys, measured on real report text:

* ``ＦＢＧ`` (full-width), ``LDL–C`` (en-dash), ``mg/m²`` (superscript),
  ``空腹　血糖`` (ideographic space) all fold to the plain form. Before this,
  ``LDL–C`` missed while ``LDL-C`` resolved — one invisible codepoint apart.
* ``fasting_glucose`` tokenizes to ``fasting glucose``. Every ``POST /data``
  example in the platform docs names indicators in snake_case, and every one of
  them missed while its spaced form resolved.

A run of CJK is deliberately ONE token: it keeps ``血糖`` and ``空腹血糖``
distinct surfaces rather than making one a substring of the other.

Ported from the same C++ ``src/indicator`` engine (``normalize.cpp`` /
``word.cpp``) that the hosted platform ported into
``mirovital/domains/health_records/indicators/lexical.py``; kept faithful so the
two implementations answer alike. Golden-locked by ``test_lexical.py``.
"""

from __future__ import annotations

import re

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


def surface_variants(term: str) -> list[str]:
    """The spellings of ``term`` worth trying, most faithful first.

    Never more than three, and the first is always the term as written, so a
    caller that stops at the first hit keeps today's answer for today's inputs.
    """
    out: list[str] = []
    for candidate in (term, normalize(term), " ".join(word_tokens(term))):
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
    return TRAILING_PARENTHETICAL.sub("", term).strip(), match.group(1).strip()
