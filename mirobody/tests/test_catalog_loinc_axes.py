"""Catalogue identities checked against all six LOINC axes.

The explicit expectations describe the new mappings, not every row in a lab
category: urine, CSF and other specimens are legitimate independent tests.
Raw ROWS retain conflicting duplicate names that a first-wins index hides.
"""

from __future__ import annotations

from mirobody.engine import get_resolver
from mirobody.kernel import metrics
from mirobody.units import unit_families

# LOINC 2.83: code, full component, property, scale, specimen, method, time.
EXPECTED_AXES = {
    "ALANINE_AMINOTRANSFERASE": ("1742-6", "alanine aminotransferase", "CCnc", "Qn", "Ser/Plas", "", "Pt"),
    "ASPARTATE_AMINOTRANSFERASE": ("1920-8", "aspartate aminotransferase", "CCnc", "Qn", "Ser/Plas", "", "Pt"),
    "ALKALINE_PHOSPHATASE": ("6768-6", "alkaline phosphatase", "CCnc", "Qn", "Ser/Plas", "", "Pt"),
    "GAMMA_GLUTAMYL_TRANSFERASE": ("2324-2", "gamma glutamyl transferase", "CCnc", "Qn", "Ser/Plas", "", "Pt"),
    "LACTATE_DEHYDROGENASE": ("14804-9", "lactate dehydrogenase", "CCnc", "Qn", "Ser/Plas", "Reaction: lactate to pyruvate", "Pt"),
    "UREA_NITROGEN": ("3094-0", "urea nitrogen", "MCnc", "Qn", "Ser/Plas", "", "Pt"),
    "CREATININE": ("2160-0", "creatinine", "MCnc", "Qn", "Ser/Plas", "", "Pt"),
    "URATE": ("3084-1", "urate", "MCnc", "Qn", "Ser/Plas", "", "Pt"),
    "BILIRUBIN": ("1975-2", "bilirubin", "MCnc", "Qn", "Ser/Plas", "", "Pt"),
    "ALBUMIN": ("1751-7", "albumin", "MCnc", "Qn", "Ser/Plas", "", "Pt"),
    "PROTEIN": ("2885-2", "protein", "MCnc", "Qn", "Ser/Plas", "", "Pt"),
    "SODIUM": ("2951-2", "sodium", "SCnc", "Qn", "Ser/Plas", "", "Pt"),
    "POTASSIUM": ("2823-3", "potassium", "SCnc", "Qn", "Ser/Plas", "", "Pt"),
    "CHLORIDE": ("2075-0", "chloride", "SCnc", "Qn", "Ser/Plas", "", "Pt"),
    "BICARBONATE": ("1959-6", "bicarbonate", "SCnc", "Qn", "Bld", "", "Pt"),
    "CALCIUM": ("17861-6", "calcium", "MCnc", "Qn", "Ser/Plas", "", "Pt"),
    "PHOSPHATE": ("2777-1", "phosphate", "MCnc", "Qn", "Ser/Plas", "", "Pt"),
    "GLOBULIN": ("10834-0", "globulin", "MCnc", "Qn", "Ser", "Calculated", "Pt"),
    "CYCLING_DISTANCE": ("93818-3", "cycling distance", "Len", "Qn", "^Patient", "", "XXX"),
    "BODY_FAT_MASS": ("73708-0", "body fat mass", "Mass", "Qn", "^Patient", "Calculated", "Pt"),
    "BODY_MUSCLE_MASS": ("73964-9", "body muscle mass", "Mass", "Qn", "^Patient", "Calculated", "Pt"),
}


def test_every_code_is_reachable():
    resolver = get_resolver()
    for metric in metrics.ROWS:
        if metric.loinc:
            assert resolver.axes_of(metric.loinc) is not None, metric.member
            assert metric.confidence, metric.member


def test_new_mappings_match_all_six_axes():
    resolver = get_resolver()
    for member, (code, *expected) in EXPECTED_AXES.items():
        metric = metrics.MEMBERS[member]
        assert metric.loinc == code, member
        axes = resolver.axes_of(code)
        assert axes is not None, member
        assert axes[:6] == tuple(expected), member


def test_lab_units_match_their_property():
    resolver = get_resolver()
    for metric in metrics.ROWS:
        if not metric.loinc or metric.category not in {"Medical Metrics", "Metabolic Metrics"}:
            continue
        if metric.member in {"APNEA_HYPOPNEA_INDEX", "OBSTRUCTIVE_APNEA_INDEX", "PULSE_WAVE_VELOCITY"}:
            continue
        families = unit_families(metric.unit_ucum)
        assert families, (metric.member, metric.unit_ucum)
        assert resolver.axes_of(metric.loinc)[1] in families, metric.member


def test_serum_sodium_has_its_own_lookup_identity():
    serum = metrics.MEMBERS["SODIUM"]
    dietary = metrics.MEMBERS["DIETARY_SODIUM"]
    assert serum.name != dietary.name
    assert metrics.METRICS[serum.name] is serum
    assert serum.canonical == ("loinc", "2951-2")
    assert serum.unit_ucum == "mmol/L"
    assert dietary.name == "sodium" and dietary.unit_ucum == "mg"
    assert metrics.mapping_for("synthetic", serum.name).metric is serum


def test_instantaneous_walking_speed_keeps_its_device_identity():
    walking = metrics.MEMBERS["WALKING_SPEED"]
    assert walking.state_class == "instant"
    assert walking.loinc == ""
    assert walking.canonical == ("mirobody-device", "walkingSpeeds")


def test_conflicting_names_are_rejected_before_first_wins_lookup(monkeypatch):
    import pytest

    rows = metrics._read_tsv("catalog/metrics.tsv")
    duplicate = {**rows[0], "member": "SYNTHETIC_COLLISION", "unit_ucum": "synthetic-conflict"}
    monkeypatch.setattr(metrics, "_read_tsv", lambda _: [*rows, duplicate])
    with pytest.raises(ValueError, match="name"):
        metrics._load()
