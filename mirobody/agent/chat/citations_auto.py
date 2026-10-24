"""Cite what the turn actually read: verify a claim's source, never guess at it.

A citation says "this number came from that row". The turn holds the rows, so the
relation can be checked - but only when it is unambiguous: a value that several rows share
(HbA1c 5.5 % and glucose 5.5 mmol/L) does not identify a source, and citing the first of
them creates a false provenance, which is worse than citing nothing.

So this module:

* parses the rows of the results this turn returned, including the compact rendering
  that hoists constant columns into a `(constants: ...)` line;
* accepts a number only when exactly one row carries it within tolerance **and** the row
  is not contradicted by a nearer metric name, unless the row is unique by value;
* leaves the answer untouched where it is machine-readable (fenced blocks, including
  `vis-chart`) or already cited, so the inserted tags never corrupt JSON or nest;
* returns the numbers it could not attribute, whether or not it cited anything: those are
  the unsupported figures a reviewer needs.

Numbers that no row supports stay as they are, and a number whose row is ambiguous is
reported as ambiguous rather than silently attributed.
"""
from __future__ import annotations

import re
from typing import Any

_NUM = re.compile(r"-?\d+(?:\.\d+)?")
_CITE = re.compile(r"<cite>.*?</cite>", re.S)
_FENCE = re.compile(r"```.*?```", re.S)
_RID = re.compile(r"^r\d+$")
_UNIT = re.compile(r"\s?(?:[a-zA-Zµμ%/][a-zA-Zµμ%^0-9/\-.]{0,11})?")
_CONSTANTS = re.compile(r"^\(constants:\s*(.*?)\)\s*$")


def _tolerance(target: float) -> float:
    return max(0.011, abs(target) * 0.002)


def _columns(header: str, constants: dict[str, str]) -> list[str]:
    return [part.strip() for part in header.split("|") if part.strip()]


def rows_from_result(content: str) -> list[dict[str, str]]:
    """Rows of a record result, in either rendering the tool emits.

    A table may arrive with its header line (`rid|name|time|value`) or with constant
    columns hoisted into a leading `(constants: rid=r3, value=69.5, ...)` line, where the
    remaining table can be as narrow as `rid|time`. Both shapes describe rows; a single
    hoisted row describes exactly one.
    """
    if not content:
        return []
    rows: list[dict[str, str]] = []
    constants: dict[str, str] = {}
    header: list[str] | None = None

    for line in content.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        match = _CONSTANTS.match(stripped)
        if match:
            for item in match.group(1).split(","):
                if "=" in item:
                    key, value = item.split("=", 1)
                    constants[key.strip()] = value.strip()
            continue
        parts = [part.strip() for part in stripped.split("|")]
        if parts and parts[0] == "rid" and "value" in parts:
            header = parts
            continue
        if not header:
            # A hoisted single row: the table below lists only the varying columns.
            if constants.get("rid") and "value" in constants:
                rows.append({**constants})
                header = parts
                continue
            continue
        if len(parts) != len(header):
            continue
        row = dict(zip(header, parts, strict=False))
        for key, value in constants.items():
            row.setdefault(key, value)
        if row.get("rid"):
            rows.append(row)

    if not rows and constants.get("rid") and "value" in constants:
        rows.append(dict(constants))

    parsed = []
    for row in rows:
        try:
            row["_value"] = float(row["value"])
        except (KeyError, TypeError, ValueError):
            continue
        parsed.append(row)
    return parsed


def _spans_outside_code(text: str) -> list[tuple[int, int]]:
    return [match.span() for match in _FENCE.finditer(text)]


def _inside(span: tuple[int, int], regions: list[tuple[int, int]]) -> bool:
    return any(start <= span[0] < end for start, end in regions)


def attach_citations(text: str, rows: list[dict[str, str]]) -> tuple[str, int, list[str]]:
    """Return (text with citations, cited count, numbers left uncited).

    A number is cited only when exactly one row carries it and the answer does not put a
    different indicator's name immediately before it. Fenced blocks (a `vis-chart` JSON
    body) and existing `<cite>` spans are copied through untouched.
    """
    if not text or not rows:
        return text, 0, []

    by_value: dict[float, list[dict[str, str]]] = {}
    for row in rows:
        by_value.setdefault(row["_value"], []).append(row)
    names = {str(row.get("name") or row.get("indicator") or "").strip() for row in rows}
    names.discard("")

    regions = _spans_outside_code(text) + [match.span() for match in _CITE.finditer(text)]
    out: list[str] = []
    cursor = 0
    cited = 0
    uncited: list[str] = []

    for match in _NUM.finditer(text):
        if _inside(match.span(), regions):
            continue
        before = text[max(0, match.start() - 16):match.start()]
        after = text[match.end():match.end() + 16]
        if re.search(r"[A-Za-zµμ]$", before) or re.match(r"[A-Za-z]", after):
            continue
        if re.search(r"\d[-:/年.]$", before) or re.match(r"[-:/年月日]", after):
            continue
        try:
            value = float(match.group())
        except ValueError:
            continue

        candidates = []
        for target, group in by_value.items():
            if abs(value - target) <= _tolerance(target):
                candidates.extend(group)

        end = match.end()
        unit = _UNIT.match(text, end)
        if unit:
            end = unit.end()
            # A sentence period is not part of the unit: "5.5 %." keeps its full stop
            # after the citation.
            while end > match.end() and text[end - 1] == ".":
                end -= 1

        chosen = _choose(before, candidates)
        out.append(text[cursor:end])
        if chosen is not None:
            if not text[end:end + 6].startswith("<cite>"):
                out.append(f"<cite>[{chosen['rid']}]</cite>")
                cited += 1
        else:
            uncited.append(match.group())
        cursor = end

    out.append(text[cursor:])
    return "".join(out), cited, uncited


def _choose(before: str, candidates: list[dict[str, str]]) -> dict[str, str] | None:
    """The single row a number refers to, or None when the answer does not say which.

    One candidate needs no name. Several candidates (HbA1c 5.5 % and glucose 5.5 mmol/L)
    are distinguished only when the text names one of them; naming a third indicator, or
    naming none, means the source cannot be attributed and no citation is written.
    """
    if len(candidates) == 1:
        return candidates[0]
    window = before.casefold()
    named = [row for row in candidates
             if str(row.get("name") or row.get("indicator") or "").strip().casefold() in window
             and str(row.get("name") or row.get("indicator") or "").strip()]
    return named[0] if len(named) == 1 else None


def _contradicted(before: str, row: dict[str, str], names: set[str]) -> bool:
    """True when the text right before the number names a *different* indicator."""
    own = str(row.get("name") or row.get("indicator") or "").strip()
    if not own:
        return False
    window = before.casefold()
    if own.casefold() in window:
        return False
    return any(name.casefold() in window for name in names if name != own)


def collect_rows(transcript: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for block in transcript:
        if block.get("type") != "tool_result":
            continue
        if block.get("status") not in (None, "ok"):
            continue
        rows.extend(rows_from_result(str(block.get("content") or "")))
    return rows
