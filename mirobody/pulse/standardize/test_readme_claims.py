"""The indicator/unit README has to agree with the catalogue it describes.

Every claim in `pulse/standardize/README.md` is checkable, so it should be checked. It
had drifted in three ways at once, all of which a provider developer would hit
on their first attempt:

* the enumeration listed lowercase members — `StandardIndicator.heart_rate`
  raises `AttributeError`; the members are uppercase;
* three stored units were wrong (`HEART_RATE` was documented as `bpm` but is
  stored as `count/min`, `VISCERAL_FAT` as `level` but is `%`, `BMI` as `kg/m²`
  but is `count`);
* the usage example opened `from ..core import ...`, a relative import that
  cannot work from a user's own module.

A wrong unit in the document that tells providers what unit to send is not a
typo — it is the failure mode this whole layer exists to prevent.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from . import StandardIndicator

README = Path(__file__).with_name("README.md")
TEXT = README.read_text(encoding="utf-8")

# `| `StandardIndicator.X` | `unit` | name |`
ROWS = re.findall(r"^\| `StandardIndicator\.([A-Z_0-9]+)` \| `([^`]*)` \|", TEXT, re.M)


def test_the_table_is_not_empty():
    assert len(ROWS) >= 15, f"expected the generated indicator table, parsed {len(ROWS)} rows"


@pytest.mark.parametrize("member,unit", ROWS, ids=[m for m, _ in ROWS])
def test_documented_member_and_unit_match_the_catalogue(member: str, unit: str):
    assert hasattr(StandardIndicator, member), (
        f"README documents StandardIndicator.{member}, which does not exist"
    )
    actual = getattr(StandardIndicator, member).value.standard_unit
    assert actual == unit, (
        f"README says {member} is stored in {unit!r}; the catalogue says {actual!r}. "
        "A provider following the README would send the wrong unit."
    )


def test_no_lowercase_member_references_survive():
    """`StandardIndicator.heart_rate` would raise AttributeError for a reader."""
    bad = re.findall(r"StandardIndicator\.([a-z][a-z_0-9]*)", TEXT)
    assert not bad, f"README references non-existent lowercase members: {sorted(set(bad))}"


def test_examples_use_importable_paths():
    """A relative import in a user-facing example cannot work for a user."""
    assert "from ..core import" not in TEXT, (
        "README shows a relative import; readers cannot run it from their own module"
    )
    for m in re.findall(r"^from ([\w.]+) import", TEXT, re.M):
        assert not m.startswith("."), f"relative import in a documented example: {m}"
