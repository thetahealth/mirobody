"""Golden vectors for unit conversion — including the ones it must REFUSE.

Offline, no bundle, no key. Half of these are negative cases on purpose: the
value of this module is as much in what it declines as in what it converts.
"""

from __future__ import annotations

import pytest

from mirobody.indicator.fhir.units.convert import (
    MOLAR_MASS,
    conversion_factor,
    convert_value,
    convertible,
    partition_units,
    scale,
)


# ── T1: same dimension, no domain knowledge ─────────────────────────────────

@pytest.mark.parametrize(
    "value, src, dst, expected",
    [
        (1.0, "g/L", "mg/dL", 100.0),
        (100.0, "mg/dL", "g/L", 1.0),
        (1.0, "mg/L", "ug/L", 1000.0),
        (1.0, "m", "cm", 100.0),
        (2.5, "cm", "mm", 25.0),
        (1.0, "kg", "g", 1000.0),
        (1.0, "[lb_av]", "g", 453.59237),
        (5.0, "10*9/L", "10*6/L", 5000.0),
        # 1 x 10^9 per litre = 10^9 / 10^6 uL = 1000 per uL.
        (1.0, "10*9/L", "/uL", 1_000.0),
        # U and [IU] are 1:1 in LOINC usage — and land in DIFFERENT
        # unit_family() buckets (CCnc vs ACnc), which is exactly why family is
        # not the convertibility test.
        (42.0, "U/L", "[IU]/L", 42.0),
    ],
)
def test_t1_same_dimension(value, src, dst, expected):
    got = convert_value(value, src, dst)
    assert got == pytest.approx(expected, rel=1e-9)


def test_t1_roundtrip_is_lossless_enough():
    there = convert_value(5.6, "mmol/L", "umol/L")
    back = convert_value(there, "umol/L", "mmol/L")
    assert back == pytest.approx(5.6, rel=1e-12)


# ── T1 refusals: the traps a family-based test walks into ───────────────────

def test_bmi_does_not_convert_to_a_concentration():
    """`kg/m2` and `mg/dL` are both PROPERTY family MCnc.

    A "same family ⇒ convertible" rule turns a BMI of 24 into a mass
    concentration. The dimension signature (L^-2·M vs M·V^-1) refuses it.
    """
    from mirobody.indicator.fhir.units import unit_family

    assert unit_family("kg/m2") == unit_family("mg/dL") == "MCnc"   # the trap
    assert not convertible("kg/m2", "mg/dL")
    assert convert_value(24.0, "kg/m2", "mg/dL") is None


@pytest.mark.parametrize("unit", ["%", "mm[Hg]"])
def test_atomic_units_equal_only_themselves(unit):
    """Unparseable is not an error — it means "equal only to itself"."""
    assert scale(unit) is None
    assert convertible(unit, unit)          # identity still holds
    assert not convertible(unit, "mg/dL")


def test_percent_and_absolute_count_do_not_convert():
    """Needs that draw's own WBC total — a derived calculation, not a unit
    conversion, and the two are different LOINC codes to begin with."""
    assert not convertible("%", "10*9/L")


def test_equivalents_are_refused_rather_than_guessed():
    """mEq↔mmol differs by valence (1 mmol Ca2+ = 2 mEq); a 1:1 conversion is
    silently off by a factor of two, so `eq` is not in the base table."""
    assert not convertible("meq/L", "mmol/L")


def test_unknown_noise_does_not_compose_a_factor():
    assert scale("mgL") is None
    assert scale("") is None
    assert scale(None) is None


# ── T2: the molar-mass bridge ───────────────────────────────────────────────

@pytest.mark.parametrize(
    "code, value, src, dst, expected",
    [
        # 1 mmol/L glucose = 18.016 mg/dL
        ("1558-6", 5.6, "mmol/L", "mg/dL", 100.89),
        ("1558-6", 100.0, "mg/dL", "mmol/L", 5.5506),
        # 1 mmol/L cholesterol = 38.665 mg/dL
        ("2093-3", 5.0, "mmol/L", "mg/dL", 193.325),
        # 1 mg/dL creatinine = 88.4 umol/L
        ("2160-0", 1.0, "mg/dL", "umol/L", 88.402),
        # 1 mg/dL urate = 59.48 umol/L
        ("3084-1", 1.0, "mg/dL", "umol/L", 59.485),
        # 1 mg/dL total bilirubin = 17.10 umol/L
        ("1975-2", 1.0, "mg/dL", "umol/L", 17.104),
    ],
)
def test_t2_golden_vectors(code, value, src, dst, expected):
    got = convert_value(value, src, dst, loinc_code=code)
    assert got == pytest.approx(expected, rel=1e-3)


def test_bun_and_urea_never_share_a_bridge():
    """BUN is reported as nitrogen, urea as the whole molecule — 2.14x apart.

    Sharing one constant would be wrong by more than a factor of two, so they
    hold separate rows and the difference is visible in the table itself.
    """
    bun = MOLAR_MASS["3094-0"][0]
    urea = MOLAR_MASS["6299-2"][0]
    assert urea / bun == pytest.approx(2.143, rel=1e-2)
    assert convert_value(1.0, "mg/dL", "mmol/L", loinc_code="3094-0") != \
           convert_value(1.0, "mg/dL", "mmol/L", loinc_code="6299-2")


def test_triglyceride_uses_the_conventional_mass():
    """Not a determinate molecule: triolein ≈ 885.4 by industry convention."""
    mass, basis, _ = MOLAR_MASS["2571-8"]
    assert mass == pytest.approx(885.40)
    assert "convention" in basis.lower()
    assert convert_value(1.0, "mmol/L", "mg/dL", loinc_code="2571-8") == pytest.approx(88.54, rel=1e-3)


def test_no_bridge_means_no_conversion_not_a_guess():
    """A code absent from the table degrades to "separate series", safely."""
    assert "718-7" not in MOLAR_MASS               # hemoglobin
    assert not convertible("mg/dL", "mmol/L", loinc_code="718-7")
    assert not convertible("mg/dL", "mmol/L")      # and with no code at all


def test_bridge_does_not_cross_codes():
    """Glucose's constant must not rescue a cholesterol conversion."""
    glucose = conversion_factor("mmol/L", "mg/dL", loinc_code="1558-6")
    cholesterol = conversion_factor("mmol/L", "mg/dL", loinc_code="2093-3")
    assert glucose != cholesterol


# ── partitioning ────────────────────────────────────────────────────────────

def test_partition_merges_across_the_bridge_when_there_is_one():
    classes = partition_units(["mmol/L", "mg/dL", "g/L"], loinc_code="1558-6")
    assert len(classes) == 1


def test_partition_keeps_them_apart_without_a_bridge():
    classes = partition_units(["mmol/L", "mg/dL"], loinc_code="")
    assert len(classes) == 2


def test_partition_isolates_the_unconvertible():
    """The real shape of a neutrophil series: a percentage and a count."""
    classes = partition_units(["%", "10*9/L", "/uL"])
    assert sorted(len(c) for c in classes) == [1, 2]      # % alone; the counts together


def test_every_bridge_row_carries_its_basis_and_a_note():
    for code, (mass, basis, note) in MOLAR_MASS.items():
        assert mass > 0, code
        assert basis.strip(), code
        assert note.strip(), code
