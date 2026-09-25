"""The UCUM specification's own unit table, shipped verbatim, and a check on ours.

This package's unit tables are written by hand: `tokens.py` maps printed
spellings to UCUM expressions, `families.py` maps an expression to its LOINC
PROPERTY, `convert.py` does dimensional analysis over UCUM atoms. The UCUM
License does not let us ship an edited copy of the specification, so the
check runs the other way: `res/ucum/ucum-essence.xml` is Regenstrief's own
machine-readable table, unmodified (its digest is pinned here), and this
module reads it to say which atoms an expression uses (`atoms_of`), which of
those UCUM does not define (`undefined_atoms`) and what a unit is worth in
UCUM's base units (`magnitude`). The gates hold our tables to those answers.
"""

from __future__ import annotations

import hashlib
import re
import xml.etree.ElementTree as ET
from functools import cache
from importlib import resources

#: The release this package ships, and the file's SHA-256 as published at the
#: ucum-org/ucum tag `v2.2`. A different digest means the file was edited.
UCUM_VERSION = "2.2"
UCUM_ESSENCE_SHA256 = "dfccea1b5dc284245ebae97edd1dc03c45864da4e87df55bc9851797b4fd0b61"

_NS = "{http://unitsofmeasure.org/ucum-essence}"
_POWER_OF_TEN = re.compile(r"^(10[*^])([+-]?\d+)$")
_EXPONENT = re.compile(r"^(.*?[^\d+-])([+-]?\d+)$")


def essence_bytes() -> bytes:
    return resources.files("mirobody").joinpath("res", "ucum", "ucum-essence.xml").read_bytes()


def essence_digest() -> str:
    return hashlib.sha256(essence_bytes()).hexdigest()


@cache
def _table() -> tuple[dict[str, float], dict[str, str], dict[str, dict]]:
    """(prefix -> factor, base unit -> dimension, unit -> definition). An
    edited file is refused: a check against a modified specification checks
    nothing, and shipping one is what the License forbids."""
    raw = essence_bytes()
    if hashlib.sha256(raw).hexdigest() != UCUM_ESSENCE_SHA256:
        raise ValueError("res/ucum/ucum-essence.xml is not the published UCUM 2.2 file")
    root = ET.fromstring(raw)
    prefixes = {p.get("Code"): float(p.find(f"{_NS}value").get("value")) for p in root.iter(f"{_NS}prefix")}
    bases = {b.get("Code"): b.get("dim") for b in root.iter(f"{_NS}base-unit")}
    units = {}
    for u in root.iter(f"{_NS}unit"):
        value = u.find(f"{_NS}value")
        units[u.get("Code")] = {
            "metric": u.get("isMetric") == "yes",
            "special": u.get("isSpecial") == "yes",
            "arbitrary": u.get("isArbitrary") == "yes",
            "value": float(value.get("value")) if value.get("value") else None,
            "unit": value.get("Unit") or "",
        }
    return prefixes, bases, units


def version() -> str:
    return ET.fromstring(essence_bytes()).get("version") or ""


# --- expressions -------------------------------------------------------------


def _symbols(expr: str) -> list[tuple[str, int]]:
    """An expression as `(symbol, exponent)` pairs, a divisor carrying a
    negative exponent. Annotations (`{beats}`) are dropped: UCUM gives them no
    meaning. A bare integer is a factor and comes back as a symbol too."""
    out: list[tuple[str, int]] = []
    pos = 0

    def term(sign: int) -> None:
        nonlocal pos
        op = 1
        if expr[pos:pos + 1] == "/":
            op, pos = -1, pos + 1
        while True:
            component(sign * op)
            if expr[pos:pos + 1] in (".", "/"):
                op = 1 if expr[pos] == "." else -1
                pos += 1
                continue
            return

    def component(sign: int) -> None:
        nonlocal pos
        if expr[pos:pos + 1] == "(":
            pos += 1
            start = len(out)
            term(1)
            if expr[pos:pos + 1] != ")":
                raise ValueError(f"unbalanced parenthesis in {expr!r}")
            pos += 1
            for i in range(start, len(out)):
                out[i] = (out[i][0], out[i][1] * sign)
            return
        depth, begin = 0, pos
        while pos < len(expr):
            ch = expr[pos]
            if ch in "[{":
                depth += 1
            elif ch in "]}":
                depth -= 1
            elif depth == 0 and ch in "./()":
                break
            pos += 1
        symbol = re.sub(r"\{[^}]*\}", "", expr[begin:pos])
        if not symbol:
            return
        m = _POWER_OF_TEN.match(symbol) or _EXPONENT.match(symbol)
        if m and not symbol.isdigit():
            out.append((m.group(1), int(m.group(2)) * sign))
        else:
            out.append((symbol, sign))

    term(1)
    if pos != len(expr):
        raise ValueError(f"cannot read {expr!r} past position {pos}")
    return out


def _split(atom: str) -> tuple[float, str] | None:
    """`atom` as (prefix factor, unit code), or None when UCUM defines neither
    it nor a metric unit under a prefix."""
    prefixes, bases, units = _table()
    if atom in bases or atom in units:
        return 1.0, atom
    for p in sorted(prefixes, key=len, reverse=True):
        rest = atom[len(p):]
        if atom.startswith(p) and rest and (rest in bases or units.get(rest, {}).get("metric")):
            return prefixes[p], rest
    return None


def atoms_of(expr: str) -> list[str]:
    """The unit symbols an expression uses, factors left out."""
    return [s for s, _ in _symbols(expr) if not s.isdigit()]


def undefined_atoms(expr: str) -> list[str]:
    """The symbols in `expr` UCUM does not define. Empty for a valid unit."""
    try:
        return [a for a in atoms_of(expr) if _split(a) is None]
    except ValueError:
        return [expr]


@cache
def magnitude(expr: str) -> tuple[float, tuple[tuple[str, int], ...]] | None:
    """`expr` in UCUM's base units: (factor, ((dimension, power), ...)). None
    for a special unit (a function, such as Celsius) or an arbitrary one
    (`[IU]`), which have no factor to compare."""
    _, bases, units = _table()
    factor, dims = 1.0, {}
    for symbol, power in _symbols(expr):
        if symbol.isdigit():
            factor *= float(symbol) ** power
            continue
        split = _split(symbol)
        if split is None:
            return None
        prefix, code = split
        if code in bases:
            f, d = 1.0, ((bases[code], 1),)
        else:
            u = units[code]
            if u["special"] or u["arbitrary"] or u["value"] is None:
                return None
            inner = magnitude(u["unit"])
            if inner is None:
                return None
            f, d = u["value"] * inner[0], inner[1]
        factor *= (prefix * f) ** power
        for dim, p in d:
            dims[dim] = dims.get(dim, 0) + p * power
    return factor, tuple(sorted((k, v) for k, v in dims.items() if v))


__all__ = [
    "UCUM_ESSENCE_SHA256",
    "UCUM_VERSION",
    "atoms_of",
    "essence_bytes",
    "essence_digest",
    "magnitude",
    "undefined_atoms",
    "version",
]
