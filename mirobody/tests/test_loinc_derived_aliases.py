"""The names generated from LOINC must keep answering their own code.

`standardize_build/loinc_aliases.py` derives alias rows from a LOINC release —
the official designations of the codes the catalogue ships, for the ones the
index did not answer yet. This test pins the result without needing the release
present: the manifest the generator writes names each alias and the code it must
answer, and every one of those codes has to be a code the catalogue carries.

Two properties matter:

  the alias answers its own code
      a row that answers a different code is worse than no row: the resolver
      would report an identity the product does not use.

  the code is in the catalogue
      an alias pointing at a code no indicator carries would resolve into
      nothing a tool can query.
"""

from __future__ import annotations

import csv
from pathlib import Path

import pytest

from mirobody.engine import get_resolver

MANIFEST = Path(__file__).resolve().parents[2] / "standardize_build" / "loinc_alias_manifest.tsv"
CATALOGUE = Path(__file__).resolve().parents[1] / "res" / "catalog" / "metrics.tsv"
SEPARATOR = "\t"


def _manifest() -> list[tuple[str, str]]:
    rows = []
    for line in MANIFEST.read_text(encoding="utf-8").splitlines():
        if not line.strip() or line.startswith("#"):
            continue
        term, code = line.split(SEPARATOR)
        rows.append((term, code))
    return rows


def _catalogue_codes() -> set[str]:
    with CATALOGUE.open(encoding="utf-8") as fh:
        return {(row.get("loinc") or "").strip()
                for row in csv.DictReader(fh, delimiter="\t") if (row.get("loinc") or "").strip()}


def test_the_manifest_is_not_empty():
    """An empty manifest would make the rest of this file vacuous.

    The floor is what the generator found on the catalogue as shipped; it grows
    when more codes are added, never shrinks.
    """
    assert len(_manifest()) >= 8, _manifest()


def test_each_alias_answers_its_own_code():
    resolver = get_resolver()
    wrong = []
    for term, code in _manifest():
        got = resolver.resolve(term)
        if not got.resolved or got.loinc != code:
            wrong.append((term, code, got.loinc or "-"))
    assert not wrong, f"alias does not answer its code: {wrong}"


def test_each_alias_points_at_a_catalogue_code():
    codes = _catalogue_codes()
    orphans = [(term, code) for term, code in _manifest() if code not in codes]
    assert not orphans, f"alias points at a code no indicator carries: {orphans}"


def test_aliases_are_two_column_rows():
    """The overrides file is `term TAB target`; a third column is rejected by
    test_engine_coverage.py, so catch it here too with a clearer message."""
    overrides = CATALOGUE.parents[1] / "loinc" / "resolver_overrides.tsv"
    bad = []
    for number, line in enumerate(overrides.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip() or line.startswith("#"):
            continue
        if line.count("\t") != 1:
            bad.append((number, line[:60]))
    assert not bad, f"rows that are not term<TAB>target: {bad[:5]}"


@pytest.fixture
def synthetic_release(tmp_path):
    def write(records):
        from standardize_build.loinc_aliases import ALIAS_FIELDS

        directory = tmp_path / "release" / "LoincTable"
        directory.mkdir(parents=True, exist_ok=True)
        with (directory / "Loinc.csv").open("w", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=["LOINC_NUM", "STATUS", "CLASS", *ALIAS_FIELDS])
            writer.writeheader()
            for record in records:
                writer.writerow({"STATUS": "ACTIVE", "CLASS": "CHEM", **record})
        catalogue = tmp_path / "catalogue.tsv"
        catalogue.write_text("loinc\tdisplay\n" + "".join(f"{r['LOINC_NUM']}\tSynthetic\n" for r in records))
        return directory.parent, catalogue, tmp_path / "aliases.tsv", tmp_path / "manifest.tsv"
    return write


def _generate(paths):
    from standardize_build.loinc_aliases import main

    release, catalogue, out, manifest = paths
    assert main(["--loinc", str(release), "--catalog", str(catalogue), "--out", str(out),
                 "--manifest", str(manifest)]) == 0
    return [tuple(line.split("\t")) for line in out.read_text().splitlines() if not line.startswith("#")]


def test_one_code_repeated_across_fields_is_not_ambiguous(synthetic_release):
    paths = synthetic_release([{"LOINC_NUM": "2339-0", "COMPONENT": "Glucose",
                                "LONG_COMMON_NAME": "Synthetic glucose designation",
                                "CONSUMER_NAME": "Synthetic glucose designation"}])
    assert _generate(paths) == [("Synthetic glucose designation", "Glucose")]


def test_folded_names_claimed_by_different_codes_are_refused(synthetic_release):
    paths = synthetic_release([
        {"LOINC_NUM": "2339-0", "COMPONENT": "Glucose", "CONSUMER_NAME": "Synthetic Shared Name"},
        {"LOINC_NUM": "2160-0", "COMPONENT": "Creatinine", "CONSUMER_NAME": "synthetic shared name"},
    ])
    assert _generate(paths) == []


def test_generator_preserves_existing_hits_and_deliberate_refusals(synthetic_release):
    paths = synthetic_release([{"LOINC_NUM": "2339-0", "COMPONENT": "Glucose",
                                "LONG_COMMON_NAME": "Creatinine", "CONSUMER_NAME": "HRV (RMSSD)"}])
    assert _generate(paths) == []


def test_generated_targets_are_raw_index_keys_and_reruns_are_empty(synthetic_release, monkeypatch):
    from mirobody.lexical import index_fold

    resolver = get_resolver()
    monkeypatch.setitem(resolver._src, index_fold("Synthetic reference alias"), "Glucose")
    paths = synthetic_release([{"LOINC_NUM": "2339-0", "COMPONENT": "Synthetic reference alias",
                                "SHORTNAME": "Glucose", "LONG_COMMON_NAME": "Synthetic official designation"}])
    rows = _generate(paths)
    assert rows == [("Synthetic official designation", "Glucose")]
    for term, target in rows:
        monkeypatch.setitem(resolver._src, index_fold(term), target)
        assert resolver.resolve(term).loinc == "2339-0"
    assert _generate(paths) == []


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__]))
