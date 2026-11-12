"""What a comma inside a printed number means, decided in one place.

`250,000 /uL` is a platelet count and `1,25 mmol/L` a Russian or Estonian
calcium: the same character is a thousands separator in one report and a
decimal mark in the other. The value parser read every comma as a decimal
mark and stored the platelet count as 250, while the check of a value against
the document's text read `250,000` as thousands and found it printed, so the
wrong number was also marked as matching the report. The value parser
(`normalize`, `standardize.parse`) and that check read commas through this
module now. `kernel.citations` keeps a stdlib-only copy of the single-comma
cases by design; `mirobody/tests/test_printed_commas.py` holds it to this
rule.

The rule, by the shape of the digits:

* Two or more commas, in groups of three (`1,250,000`) or in the lakh
  grouping Indian reports print (`2,50,000`): a grouping. A decimal mark
  appears once.
* One comma before a decimal point (`1,250.5`): a grouping.
* One comma with one to three digits before it, not starting with 0, and
  exactly three after it (`1,250`, `12,500`, `250,000`): a grouping. Both
  conventions can print this shape. Of the 583 numbers printed this way in
  `benchmarks/`, `demo/`, `docs/` and the tests on 2026-11-12, none is a
  decimal comma: they are counts (steps, tokens, bytes, a 1,650 kg weight)
  and a few lists of two readings; the decimal commas the repository does
  print have one or two digits after the mark (`5,6 mmol/L`, `6,1`). The
  price is a European value with exactly three decimals and a whole part,
  such as a urine specific gravity printed `1,015`.
* Any other single comma (`1,25`, `0,125`, `1234,5`): a decimal mark. A
  grouping never starts with 0 and has at most three digits before its
  first comma.

Anything else (`72,66,65`, `1,25.5`) has no reading as one number.
"""

from __future__ import annotations

import re

__all__ = ["DIGITS", "fold", "plain"]

#: One printed number without its sign: digit runs joined by commas, then an
#: optional decimal point and fraction. A regex fragment for callers that put
#: their own sign, comparator or exponent around it; `plain` reads a match.
DIGITS = r"[0-9]+(?:,[0-9]+)*(?:\.[0-9]+)?"

# Not after a digit or a point, so a fraction (`1.250,5`) is never read as the
# start of a number of its own. After a comma is allowed: `Glucose,95,99`.
_COMMA_NUMBER = re.compile(rf"(?<![0-9.]){DIGITS}")


def _grouped(head: str, groups: list[str]) -> bool:
    if head[0] == "0":
        return False
    if len(head) <= 3 and all(len(g) == 3 for g in groups):
        return True
    # Lakh grouping: the last three digits, then pairs.
    return (len(groups) > 1 and len(head) <= 2 and len(groups[-1]) == 3
            and all(len(g) == 2 for g in groups[:-1]))


def plain(printed: str) -> str | None:
    """`printed`, a match of `DIGITS`, as a plain decimal: `"250,000"` is
    `"250000"`, `"1,25"` is `"1.25"`, `"1,250.5"` is `"1250.5"`. `None` when
    the commas fit no reading as one number (`"72,66,65"`)."""
    if "," not in printed:
        return printed
    whole, dot, fraction = printed.partition(".")
    head, *groups = whole.split(",")
    if _grouped(head, groups):
        return head + "".join(groups) + dot + fraction
    if len(groups) == 1 and not dot:
        return f"{head}.{groups[0]}"
    return None


def fold(text: str, *, separators: bool = False) -> str:
    """`text` with every comma-bearing number written as `plain` reads it;
    one with no reading is left as printed. With `separators`, a comma read
    as a decimal mark is left as printed too: in running text and CSV a comma
    between digits may also separate two numbers (`2026-08-01,72`), so a
    caller asking whether a number is printed anywhere reads the text both
    ways. A grouping is never left: `250,000` does not print 250."""

    def one(m: re.Match[str]) -> str:
        printed = m.group()
        reading = plain(printed)
        if reading is None or (separators and reading != printed.replace(",", "")):
            return printed
        return reading

    return _COMMA_NUMBER.sub(one, text) if "," in text else text
