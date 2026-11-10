"""Official enzyme names remain usable beyond the former 50-character cap."""

import pytest

from mirobody.engine import resolve, resolve_reading


@pytest.mark.parametrize(("name", "code"), [
    ("Lactate dehydrogenase [Enzymatic activity/volume] in Serum or Plasma by Pyruvate to lactate reaction", "14805-6"),
    ("Alanine aminotransferase [Enzymatic activity/volume] in Serum or Plasma", "1742-6"),
    ("Aspartate aminotransferase [Enzymatic activity/volume] in Serum or Plasma", "1920-8"),
])
def test_official_long_names_keep_their_code_with_and_without_a_reading(name, code):
    assert resolve(name).loinc == code
    assert resolve_reading(name, "1", "U/L").loinc == code
