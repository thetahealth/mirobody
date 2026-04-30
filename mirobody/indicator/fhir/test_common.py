"""Unit tests for common.py code/fhir_id helpers."""

from __future__ import annotations

import pytest

from .common import (
    SYSTEMS,
    SYSTEM_TO_CODE,
    _CODE_BITS,
    _CODE_MASK,
    code_to_fhir_id,
    code_to_int,
    fhir_id_to_code,
    int_to_code,
)


# ─── code_to_int / int_to_code ───────────────────────────────────────

class TestCodeToInt:
    def test_snomed_numeric_round_trip(self) -> None:
        for code in ("391073007", "1167364002", "73211009", "0"):
            n = code_to_int(code, "SNOMED_CT")
            assert int_to_code(n, "SNOMED_CT") == code

    def test_snomed_extension_sctid_round_trip(self) -> None:
        # International-extension SCTIDs are 18 digits and routinely
        # exceed 2^59 — they must still round-trip via the 60-bit budget.
        for code in ("999480561000087100", "577777871000119101"):
            n = code_to_int(code, "SNOMED_CT")
            assert n < (1 << _CODE_BITS), "must fit 60-bit budget"
            assert int_to_code(n, "SNOMED_CT") == code

    def test_loinc_dash_round_trip(self) -> None:
        for code in ("4548-4", "8867-4", "8480-6", "61008-9"):
            n = code_to_int(code, "LOINC")
            assert int_to_code(n, "LOINC") == code

    def test_rxnorm_numeric_round_trip(self) -> None:
        for code in ("8896", "1234567", "1"):
            n = code_to_int(code, "RXNORM")
            assert int_to_code(n, "RXNORM") == code

    def test_cvx_numeric_round_trip(self) -> None:
        for code in ("140", "207"):
            n = code_to_int(code, "CVX")
            assert int_to_code(n, "CVX") == code

    def test_dcm_always_hashed(self) -> None:
        # DCM uniformly hashes — even numeric codes lose round-trip via
        # fhir_id alone (recoverable through the meta sidecar).
        for code in ("1234", "ABC123"):
            n = code_to_int(code, "DCM")
            with pytest.raises(NotImplementedError):
                int_to_code(n, "DCM")

    def test_theta_always_hashed(self) -> None:
        for code in ("custom_glucose_metric", "daily_stats_omega3Max"):
            n = code_to_int(code, "THETA")
            with pytest.raises(NotImplementedError):
                int_to_code(n, "THETA")

    def test_no_system_numeric(self) -> None:
        # Some call sites pass no system for SNOMED etc.
        n = code_to_int("391073007")
        assert int_to_code(n) == "391073007"

    def test_hash_deterministic(self) -> None:
        a = code_to_int("ABC123", "DCM")
        b = code_to_int("ABC123", "DCM")
        assert a == b

    def test_hash_distinct(self) -> None:
        a = code_to_int("ABC123", "DCM")
        b = code_to_int("XYZ789", "DCM")
        assert a != b

    def test_hash_fits_code_budget(self) -> None:
        # blake2b output >> 4 must fit in 60 bits so packing won't
        # collide with the system bits.
        for code, sys_name in [("ABC", "DCM"), ("custom", "THETA")]:
            n = code_to_int(code, sys_name)
            assert n < (1 << _CODE_BITS)


# ─── code_to_fhir_id / fhir_id_to_code ───────────────────────────────

class TestCodeToFhirId:
    def test_numeric_round_trip_all_systems(self) -> None:
        cases = [
            ("SNOMED_CT", "391073007"),
            ("SNOMED_CT", "999480561000087100"),  # extension SCTID
            ("LOINC", "4548-4"),
            ("RXNORM", "8896"),
            ("CVX", "140"),
        ]
        for sys_name, code in cases:
            fid = code_to_fhir_id(sys_name, code)
            assert fid > 0, "fhir_id must be positive"
            assert fid >> _CODE_BITS == SYSTEM_TO_CODE[sys_name]
            sys_back, code_back = fhir_id_to_code(fid)
            assert (sys_back, code_back) == (sys_name, code)

    def test_hashed_round_trip_raises(self) -> None:
        for sys_name, code in [("DCM", "1234"), ("DCM", "ABC123"),
                               ("THETA", "weird_metric")]:
            fid = code_to_fhir_id(sys_name, code)
            assert fid > 0
            assert fid >> _CODE_BITS == SYSTEM_TO_CODE[sys_name]
            with pytest.raises(NotImplementedError):
                fhir_id_to_code(fid)

    def test_system_int_form_accepted(self) -> None:
        # code_index.csv.gz stores system as enum int — passing the int
        # directly must produce the same fhir_id as passing the name.
        for sys_name in SYSTEMS:
            sys_int = SYSTEM_TO_CODE[sys_name]
            fid_a = code_to_fhir_id(sys_name, "1")
            fid_b = code_to_fhir_id(sys_int, "1")
            assert fid_a == fid_b

    def test_distinct_systems_distinct_ids(self) -> None:
        # Same code string under different vocabs must pack to distinct
        # fhir_ids (the system bits matter).
        a = code_to_fhir_id("SNOMED_CT", "1")
        b = code_to_fhir_id("LOINC", "11")
        # LOINC "11" → 11 (no dash to strip), system differs from SNOMED.
        assert a != b

    def test_packed_code_within_mask(self) -> None:
        # Decoded code field must equal what code_to_int produced.
        for sys_name, code in [("SNOMED_CT", "391073007"), ("DCM", "ABC")]:
            fid = code_to_fhir_id(sys_name, code)
            assert fid & _CODE_MASK == code_to_int(code, sys_name)
