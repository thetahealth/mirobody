"""Offline CPIC evidence lookup with explicit limits on what can be inferred."""

from __future__ import annotations

import gzip
import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from pathlib import Path
from typing import Literal

BUNDLED_VERSION = "v1.60.0"
BUNDLED_FILE = "cpic-v1.60.0.json.gz"
BUNDLED_SHA256 = "47af59c170dc2e498a81237392f3ead4291856aa1f3805b60e57b7b3a4affd7f"
EXPECTED_COLUMNS = {
    "drug": ("drugid", "name"),
    "guideline": ("id", "name", "url"),
    "pair": ("genesymbol", "drugid", "guidelineid", "usedforrecommendation", "cpiclevel", "removed"),
    "allele_definition": ("id", "version", "genesymbol", "name", "structuralvariation"),
    "allele_location_value": ("alleledefinitionid", "locationid", "variantallele", "version"),
    "sequence_location": ("id", "version", "chromosomelocation", "genesymbol", "dbsnpid"),
    "gene_result": ("id", "genesymbol", "result", "version"),
    "gene_result_lookup": ("id", "phenotypeid"),
    "gene_result_diplotype": ("id", "functionphenotypeid", "diplotype"),
}
ROW_WIDTHS = {name: len(columns) for name, columns in EXPECTED_COLUMNS.items()}


@dataclass(frozen=True)
class ArrayCoverage:
    gene: str
    cpic_version: str
    status: Literal["not_determined"]
    required_sites: int
    called_sites: int
    missing_sites: tuple[str, ...]
    no_call_sites: tuple[str, ...]
    unresolved_sites: tuple[str, ...]
    unmapped_allele_definitions: int
    structural_allele_definitions: int
    reason: str


@dataclass(frozen=True)
class PhenotypeLookup:
    gene: str
    cpic_version: str
    status: Literal["known", "not_determined"]
    diplotype: str | None
    phenotype: str | None
    reason: str


@dataclass(frozen=True)
class RareVariantGate:
    status: Literal["needs_confirmation", "not_assessed"]
    reason: str


@dataclass(frozen=True)
class PhenotypeChange:
    status: Literal["comparable", "not_determined"]
    from_version: str
    to_version: str
    previous: str | None
    current: str | None
    changed: bool | None


@dataclass(frozen=True)
class DrugGenePair:
    drug: str
    gene: str
    cpic_level: str
    guideline_name: str
    guideline_url: str


class CpicKnowledge:
    """A pinned CPIC extract; no patient data or network calls are retained."""

    def __init__(self, payload: dict) -> None:
        if payload.get("format") != "mirobody-cpic-extract-1":
            raise ValueError("unsupported CPIC extract format")
        version = payload.get("version")
        if not isinstance(version, str) or not version.startswith("v"):
            raise ValueError("missing CPIC version")
        tables = payload.get("tables")
        if not isinstance(tables, dict):
            raise ValueError("missing CPIC tables")
        if payload.get("columns") != {name: list(columns) for name, columns in EXPECTED_COLUMNS.items()}:
            raise ValueError("unsupported CPIC extract columns")
        if set(tables) != set(ROW_WIDTHS) or any(not isinstance(tables[t], list) for t in ROW_WIDTHS):
            raise ValueError("incomplete CPIC tables")
        counts = payload.get("counts")
        if not isinstance(counts, dict) or any(
            counts.get(table) != len(rows)
            or any(not isinstance(row, list) or len(row) != ROW_WIDTHS[table] for row in rows)
            for table, rows in tables.items()
        ):
            raise ValueError("malformed CPIC extract rows")
        self.version = version
        self.source_sha256 = payload["source_sha256"]
        self._definitions: dict[str, list[list[str | None]]] = {}
        for row in tables["allele_definition"]:
            self._definitions.setdefault(str(row[2]), []).append(row)
        locations = {row[0]: row for row in tables["sequence_location"]}
        self._allele_locations: dict[str, list[list[str | None]]] = {}
        for row in tables["allele_location_value"]:
            location = locations.get(row[1])
            if location is None:
                raise ValueError("broken CPIC location reference")
            self._allele_locations.setdefault(str(row[0]), []).append(location)
        results = {row[0]: row for row in tables["gene_result"]}
        lookups = {row[0]: row for row in tables["gene_result_lookup"]}
        self._diplotypes: dict[tuple[str, str], set[str]] = {}
        for row in tables["gene_result_diplotype"]:
            lookup = lookups.get(row[1])
            result = results.get(lookup[1]) if lookup is not None else None
            if result is None:
                raise ValueError("broken CPIC phenotype reference")
            key = (str(result[1]), str(row[2]))
            self._diplotypes.setdefault(key, set()).add(str(result[2]))
        drugs = {row[0]: row[1] for row in tables["drug"]}
        guidelines = {row[0]: row for row in tables["guideline"]}
        self._pairs: list[DrugGenePair] = []
        for row in tables["pair"]:
            gene, drug_id, guideline_id, used, level, removed = row
            if used != "t" or removed != "f" or level not in {"A", "B"}:
                continue
            drug = drugs.get(drug_id)
            guideline = guidelines.get(guideline_id)
            if drug is None or guideline is None:
                raise ValueError("broken CPIC drug or guideline reference")
            self._pairs.append(DrugGenePair(str(drug), str(gene), str(level),
                                            str(guideline[1]), str(guideline[2])))

    def drug_pairs(self, *, drugs: tuple[str, ...] = (), genes: tuple[str, ...] = ()) -> tuple[DrugGenePair, ...]:
        """Return current A/B gene-drug links, without a patient recommendation."""
        selected_drugs = {name.casefold() for name in drugs}
        selected_genes = {name.upper() for name in genes}
        return tuple(pair for pair in self._pairs
                     if (not selected_drugs or pair.drug.casefold() in selected_drugs)
                     and (not selected_genes or pair.gene in selected_genes))

    def array_coverage(self, gene: str, calls: Mapping[str, str]) -> ArrayCoverage:
        """Audit defining sites; array rows alone never establish a diplotype."""
        gene = gene.upper()
        definitions = self._definitions.get(gene, [])
        sites: set[str] = set()
        unmapped = 0
        structural = 0
        for definition in definitions:
            if definition[4] == "t":
                structural += 1
            locations = self._allele_locations.get(str(definition[0]), [])
            rsids = {str(location[4]) for location in locations if location[4]}
            sites.update(rsids)
            if not locations or len(rsids) < len(locations):
                unmapped += 1
        missing = tuple(sorted(site for site in sites if site not in calls))
        no_call = tuple(sorted(site for site in sites if calls.get(site) == "no_call"))
        unresolved = tuple(sorted(
            site for site in sites
            if site in calls and calls[site] not in {"called", "no_call"}
        ))
        called = len(sites) - len(missing) - len(no_call) - len(unresolved)
        reason = (
            "CPIC has no allele definitions for this gene"
            if not definitions else
            "Array calls do not establish phase, copy number, or exclusion of untyped alleles"
        )
        return ArrayCoverage(
            gene, self.version, "not_determined", len(sites), called,
            missing, no_call, unresolved, unmapped, structural, reason,
        )

    def lookup_reported_diplotype(
        self, gene: str, diplotype: str, *, externally_confirmed: bool = False,
    ) -> PhenotypeLookup:
        """Map an externally confirmed diplotype, never infer one from array calls."""
        gene = gene.upper()
        if not externally_confirmed:
            return PhenotypeLookup(
                gene, self.version, "not_determined", None, None,
                "Diplotype requires independent laboratory confirmation",
            )
        phenotypes = self._diplotypes.get((gene, diplotype), set())
        if len(phenotypes) != 1:
            return PhenotypeLookup(
                gene, self.version, "not_determined", diplotype, None,
                "No unique CPIC phenotype for this exact diplotype",
            )
        phenotype = next(iter(phenotypes))
        if any(term in phenotype.casefold() for term in ("indeterminate", "unknown", "uncertain")):
            return PhenotypeLookup(
                gene, self.version, "not_determined", diplotype, None,
                "CPIC classifies this diplotype as indeterminate",
            )
        return PhenotypeLookup(
            gene, self.version, "known", diplotype, phenotype,
            "Exact mapping from externally confirmed diplotype in CPIC extract",
        )


def rare_variant_gate(classification: str) -> RareVariantGate:
    """A pathogenic label from an array cannot become a disease-risk claim."""
    if classification.strip().casefold().replace("-", "_").replace(" ", "_") in {"pathogenic", "likely_pathogenic"}:
        return RareVariantGate(
            "needs_confirmation", "Confirm this array finding with a clinical-grade assay",
        )
    return RareVariantGate("not_assessed", "No disease-risk assessment is available")


def compare_reported_diplotype(
    old: CpicKnowledge,
    new: CpicKnowledge,
    gene: str,
    diplotype: str,
    *,
    externally_confirmed: bool = False,
) -> PhenotypeChange:
    """Expose a pinned-version change only when both mappings are determinate."""
    previous = old.lookup_reported_diplotype(
        gene, diplotype, externally_confirmed=externally_confirmed,
    )
    current = new.lookup_reported_diplotype(
        gene, diplotype, externally_confirmed=externally_confirmed,
    )
    if previous.status != "known" or current.status != "known":
        return PhenotypeChange(
            "not_determined", old.version, new.version,
            previous.phenotype, current.phenotype, None,
        )
    return PhenotypeChange(
        "comparable", old.version, new.version,
        previous.phenotype, current.phenotype,
        previous.phenotype != current.phenotype,
    )


@lru_cache(maxsize=4)
def load_cpic(version: str = "bundled", directory: str | Path | None = None) -> CpicKnowledge:
    """Load a bundled or explicitly pinned local extract, without fetching data."""
    if version == "bundled":
        version = BUNDLED_VERSION
    if not version.startswith("v") or not all(part.isdigit() for part in version[1:].split(".")) or len(version[1:].split(".")) != 3:
        raise ValueError("CPIC version must be an exact vX.Y.Z release")
    filename = BUNDLED_FILE if version == BUNDLED_VERSION else f"cpic-{version}.json.gz"
    if directory is None:
        data = resources.files("mirobody").joinpath("res", "genomics", filename).read_bytes()
        if version == BUNDLED_VERSION and hashlib.sha256(data).hexdigest() != BUNDLED_SHA256:
            raise ValueError("bundled CPIC extract checksum mismatch")
    else:
        data = (Path(directory) / filename).read_bytes()
    payload = json.loads(gzip.decompress(data))
    if payload.get("version") != version:
        raise ValueError("CPIC extract version mismatch")
    return CpicKnowledge(payload)
