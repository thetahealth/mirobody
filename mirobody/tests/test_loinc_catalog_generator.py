"""Synthetic releases exercise generation before any rows reach the catalogue."""

from __future__ import annotations

import csv

import pytest

from mirobody.kernel import metrics


@pytest.fixture
def generate(tmp_path):
    from standardize_build.loinc_catalog_rows import main

    def run(records, existing=None):
        root = tmp_path / "release"
        table = root / "LoincTable"
        orders = root / "AccessoryFiles" / "LoincUniversalLabOrdersValueSet"
        table.mkdir(parents=True, exist_ok=True)
        orders.mkdir(parents=True, exist_ok=True)
        defaults = {"STATUS": "ACTIVE", "CLASS": "CHEM", "COMPONENT": "Creatinine",
                    "PROPERTY": "MCnc", "SCALE_TYP": "Qn", "TIME_ASPCT": "Pt", "SYSTEM": "Body fld",
                    "METHOD_TYP": "", "LONG_COMMON_NAME": "Creatinine [Mass/volume] in Body fluid",
                    "SHORTNAME": "Creat Fld-mCnc", "EXAMPLE_UCUM_UNITS": "mg/dL"}
        records = [{**defaults, **r} for r in records]
        with (table / "Loinc.csv").open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["LOINC_NUM", *defaults])
            writer.writeheader()
            writer.writerows(records)
        (orders / "LoincUniversalLabOrdersValueSet.csv").write_text(
            "LOINC_NUM\n" + "".join(r["LOINC_NUM"] + "\n" for r in records))
        base = metrics._read_tsv("catalog/metrics.tsv")
        header = list(base[0])
        catalogue = tmp_path / "catalogue.tsv"
        with catalogue.open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=header, delimiter="\t")
            writer.writeheader()
            writer.writerows(existing if existing is not None else base[:1])
        out = tmp_path / "proposed.tsv"
        assert main(["--loinc", str(root), "--catalog", str(catalogue), "--out", str(out)]) == 0
        with out.open() as fh:
            return list(csv.DictReader(fh, fieldnames=header, delimiter="\t"))
    return run


@pytest.mark.parametrize("unit", ["", "not-a-unit", "mmol/L"])
def test_missing_unrecognized_and_incompatible_units_are_not_emitted(generate, unit):
    assert generate([{"LOINC_NUM": "12190-5", "EXAMPLE_UCUM_UNITS": unit}]) == []


def test_existing_serum_member_does_not_hide_other_specimens(generate):
    existing = [r for r in metrics._read_tsv("catalog/metrics.tsv") if r["member"] == "CREATININE"]
    [row] = generate([{"LOINC_NUM": "12190-5"}], existing)
    assert row["member"] == "CREATININE_BODY_FLD"
    assert row["name"] != existing[0]["name"]


def test_a_known_internal_name_is_not_reused_for_another_member(generate):
    existing = [{**metrics._read_tsv("catalog/metrics.tsv")[0], "name": "creatinineBodyFld"}]
    assert generate([{"LOINC_NUM": "12190-5"}], existing) == []


def test_point_and_daily_specimens_keep_their_identity_and_shape(generate):
    rows = generate([
        {"LOINC_NUM": "12190-5"},
        {"LOINC_NUM": "20624-3", "SYSTEM": "Urine", "TIME_ASPCT": "24H",
         "LONG_COMMON_NAME": "Creatinine [Mass/volume] in 24 hour Urine", "SHORTNAME": "Creat 24h Ur-mCnc"},
    ])
    by_code = {r["loinc"]: r for r in rows}
    assert by_code["12190-5"]["state_class"] == "instant"
    daily = by_code["20624-3"]
    assert daily["member"] == "CREATININE_URINE"
    assert daily["state_class"] == "provider_daily" and daily["aggregation_policy"] == "provider_value"
    assert daily["data_type"] == "summary" and daily["aggregation_methods"] == ""
    base = metrics._read_tsv("catalog/metrics.tsv")[:1]
    assert generate([{"LOINC_NUM": "12190-5"}], [*base, *rows]) == []
