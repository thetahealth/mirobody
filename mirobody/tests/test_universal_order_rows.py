"""Universal-order rows retain the unit and six axes from LOINC 2.83.

The manifest is generated from source fields independently of the shipped
axis table. A missing row, unsupported unit or changed axis fails explicitly.
"""

from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

import pytest

from mirobody.engine import get_resolver, resolve_reading
from mirobody.kernel import metrics

MANIFEST = Path(__file__).resolve().parents[2] / "standardize_build" / "universal_order_manifest.tsv"

EXPECTED_PROPERTY = {
    "mg/dL": {"MCnc"}, "g/dL": {"MCnc"}, "mg/L": {"MCnc"}, "ug/mL": {"MCnc"},
    "ng/mL": {"MCnc"}, "pg/mL": {"MCnc"}, "g/L": {"MCnc"}, "ug/dL": {"MCnc"},
    "ng/dL": {"MCnc"}, "ug/L": {"MCnc"},
    "mol/L": {"SCnc"}, "mmol/L": {"SCnc"}, "umol/L": {"SCnc"}, "nmol/L": {"SCnc"}, "pmol/L": {"SCnc"},
    "U/L": {"CCnc"}, "[IU]/L": {"CCnc", "ACnc"}, "[IU]/mL": {"ACnc"},
    "mU/L": {"CCnc"}, "U/mL": {"CCnc"}, "%": {"MFr", "VFr"},
}


def _manifest():
    with MANIFEST.open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def test_the_manifest_has_substantial_nonempty_coverage():
    assert len(_manifest()) >= 200


def test_each_row_keeps_its_code_unit_and_internal_lookup():
    for expected in _manifest():
        row = metrics.MEMBERS[expected["member"]]
        assert row.loinc == expected["loinc"], row.member
        assert row.unit_ucum == expected["unit_ucum"] and row.unit_ucum, row.member
        assert metrics.METRICS[row.name] == row, row.member


def test_each_display_and_numeric_reading_answers_its_own_code():
    resolver = get_resolver()
    for expected in _manifest():
        row = metrics.MEMBERS[expected["member"]]
        assert resolver.resolve(row.display).loinc == row.loinc, row.member
        assert resolve_reading(row.display, "1", row.unit_ucum).loinc == row.loinc, row.member


def test_each_unit_matches_its_code_property():
    """The unit a row carries must be able to express its code's PROPERTY.

    EXPECTED_PROPERTY names the units the first batch used. The later batches
    take their units from LOINC's own EXAMPLE_UCUM_UNITS, so a unit the table
    does not name falls back to the repository's mapping
    (mirobody.units.families), which is the same authority the generator reads
    and which test_unit_families_vs_loinc.py guards against the LOINC release.
    """
    from mirobody.units import unit_families

    resolver = get_resolver()
    for expected in _manifest():
        unit = expected["unit_ucum"]
        axes = resolver.axes_of(expected["loinc"])
        assert axes is not None, expected["member"]
        families = EXPECTED_PROPERTY.get(unit) or unit_families(unit)
        assert families, (expected["member"], unit)
        assert axes[1] in families, (expected["member"], unit, axes[1], sorted(families))


def test_all_six_axes_match_the_source_release():
    resolver = get_resolver()
    for expected in _manifest():
        axes = resolver.axes_of(expected["loinc"])
        wanted = tuple(expected[key] for key in ("component", "property", "scale", "system", "method", "time"))
        assert axes is not None and axes[:6] == wanted, expected["member"]


def test_timing_selects_a_usable_shape_and_aggregation_method():
    for expected in _manifest():
        row = metrics.MEMBERS[expected["member"]]
        if expected["time"] == "Pt":
            assert (row.state_class, row.aggregation_policy, row.data_type) == ("instant", "last", "series")
            assert row.aggregation_methods == ("last",)
        else:
            assert expected["time"] == "24H", row.member
            assert (row.state_class, row.aggregation_policy, row.data_type) == ("provider_daily", "provider_value", "summary")
            assert row.aggregation_methods == ()


def test_no_generated_member_or_code_is_used_twice():
    rows = _manifest()
    for key in ("member", "loinc"):
        assert max(Counter(row[key] for row in rows).values()) == 1
    counts = Counter(row.loinc for row in metrics.ROWS if row.loinc)
    assert all(counts[row["loinc"]] == 1 for row in rows)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
