"""Normalize a raw genotype call against a public, versioned site record.

The raw spelling is retained by the caller. A site or strand that cannot be
verified produces ``unresolved``; it never becomes a reference genotype.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

NORMALIZER_VERSION = "1.5.2.1"
_COMPLEMENT = str.maketrans("ACGT", "TGCA")


@dataclass(frozen=True)
class NormalizedCall:
    rsid: str
    chrom: str
    pos37: int | None
    pos38: int | None
    ref: str | None
    alt: str | None
    gene: str | None
    gt: str | None
    call_status: str
    zygosity: str | None
    strand_check: str | None
    matched_build: str | None


def _alleles(value: str) -> tuple[str, ...]:
    text = value.upper().replace("/", "").replace("|", "")
    return tuple(text) if text and all(letter in "ACGTID" for letter in text) else ()


def normalize(
    *, rsid: str, chrom: str, position: int, genotype: str,
    strand: str = "plus", vcf_gt: str | None = None,
    vcf_ref: str | None = None, vcf_alt: str | None = None,
    site: Mapping[str, object] | None = None, sex: str = "unknown",
) -> NormalizedCall:
    """Return a call whose GT is set only when its reference is defensible."""
    canonical = str(site.get("rsid") or rsid) if site else rsid
    site_chrom = str(site.get("chrom") or chrom) if site else chrom
    pos37 = _positive(site.get("pos37")) if site else None
    pos38 = _positive(site.get("pos38")) if site else None
    ref = str(site.get("ref") or "").upper() if site else (vcf_ref or "").upper()
    alt = str(site.get("alt") or "").upper() if site else (vcf_alt or "").upper()
    gene_name = str(site.get("gene") or "") if site else ""
    # dbSNP may list several overlapping genes. A single HGNC query must not
    # silently pick one of them; wait for a one-to-many site index instead.
    gene = gene_name if gene_name and "," not in gene_name and len(gene_name) <= 32 else None
    matched_build = (
        "GRCh37" if pos37 == position and site_chrom == chrom else
        "GRCh38" if pos38 == position and site_chrom == chrom else None
    )
    basis = {
        "rsid": canonical, "chrom": site_chrom if matched_build else chrom,
        "pos37": pos37 if matched_build else None,
        "pos38": pos38 if matched_build else None,
        "ref": ref or None, "alt": alt or None, "gene": gene,
        "matched_build": matched_build,
    }
    if sex == "female" and chrom == "Y" and genotype == "--":
        return NormalizedCall(**basis, gt=None, call_status="not_applicable", zygosity=None, strand_check=None)
    if genotype == "--" or vcf_gt and "." in vcf_gt.replace("|", "/").split("/"):
        return NormalizedCall(**basis, gt=None, call_status="no_call", zygosity=None, strand_check=None)
    if strand == "top":
        return NormalizedCall(**basis, gt=None, call_status="unresolved", zygosity=None,
                              strand_check="top_unresolved")
    if site and not matched_build:
        return NormalizedCall(**basis, gt=None, call_status="unresolved", zygosity=None,
                              strand_check="coordinate_conflict")
    if vcf_gt is not None and ref and alt in {"", "."}:
        parts = vcf_gt.replace("|", "/").split("/")
        if all(part == "0" for part in parts):
            return NormalizedCall(**basis, gt=vcf_gt, call_status="called",
                                  zygosity=_zygosity(tuple(0 for _ in parts)), strand_check="vcf_plus")
    if not ref or not alt or alt == ".":
        return NormalizedCall(**basis, gt=None, call_status="unresolved", zygosity=None,
                              strand_check="no_reference")

    alts = alt.split(",")
    reference_alleles = [ref, *alts]
    if vcf_gt is not None:
        # A VCF already defines its own GT relative to REF/ALT. Preserve
        # phasing, but refuse if the public site disagrees with the VCF.
        if site and (ref != (vcf_ref or "").upper() or alt != (vcf_alt or "").upper()):
            return NormalizedCall(**basis, gt=None, call_status="unresolved", zygosity=None,
                                  strand_check="reference_conflict")
        parts = vcf_gt.replace("|", "/").split("/")
        if not all(part.isdigit() and int(part) < len(reference_alleles) for part in parts):
            return NormalizedCall(**basis, gt=None, call_status="unresolved", zygosity=None,
                                  strand_check="invalid_gt")
        indices = tuple(int(part) for part in parts)
        zygosity = _zygosity(indices)
        return NormalizedCall(**basis, gt=vcf_gt, call_status="called", zygosity=zygosity,
                              strand_check="vcf_plus")

    bases = _alleles(genotype)
    if not bases:
        return NormalizedCall(**basis, gt=None, call_status="unresolved", zygosity=None,
                              strand_check="unsupported_alleles")
    if "I" in bases or "D" in bases:
        if len(alts) != 1 or len(ref) == len(alts[0]):
            return NormalizedCall(**basis, gt=None, call_status="unresolved", zygosity=None,
                                  strand_check="indel_undefined")
        insertion = 1 if len(alts[0]) > len(ref) else 0
        mapping = {"I": insertion, "D": 1 - insertion}
        indices = tuple(mapping[base] for base in bases)
        check = "indel_defined"
    else:
        if any(len(allele) != 1 for allele in reference_alleles):
            return NormalizedCall(**basis, gt=None, call_status="unresolved", zygosity=None,
                                  strand_check="indel_undefined")
        mapping = {allele: i for i, allele in enumerate(reference_alleles)}
        check = "plus"
        if not all(base in mapping for base in bases):
            flipped = tuple(base.translate(_COMPLEMENT) for base in bases)
            if not all(base in mapping for base in flipped):
                return NormalizedCall(**basis, gt=None, call_status="unresolved", zygosity=None,
                                      strand_check="allele_conflict")
            bases = flipped
            check = "flipped"
        elif {ref, *alts} in ({"A", "T"}, {"C", "G"}):
            check = "palindromic_trusted"
        indices = tuple(mapping[base] for base in bases)
    if chrom == "MT" or sex == "male" and chrom in {"X", "Y"}:
        if len(set(indices)) != 1:
            return NormalizedCall(**basis, gt=None, call_status="unresolved", zygosity=None,
                                  strand_check="haploid_conflict")
        indices = (indices[0],)
    gt = "/".join(str(i) for i in sorted(indices))
    return NormalizedCall(**basis, gt=gt, call_status="called", zygosity=_zygosity(indices), strand_check=check)


def _positive(value: object) -> int | None:
    number = int(value) if value is not None else 0
    return number if number > 0 else None


def _zygosity(indices: tuple[int, ...]) -> str:
    if len(indices) == 1:
        return "hemizygous"
    return "homozygous" if len(set(indices)) == 1 else "heterozygous"


def infer_sex(*, x_total: int, x_heterozygous: int, y_called: int) -> str:
    """Call only with enough X markers to avoid inferring from a tiny panel."""
    if x_total < 1000:
        return "unknown"
    ratio = x_heterozygous / x_total
    if ratio >= 0.05 and y_called == 0:
        return "female"
    if ratio < 0.01 and y_called >= 10:
        return "male"
    return "unknown"
