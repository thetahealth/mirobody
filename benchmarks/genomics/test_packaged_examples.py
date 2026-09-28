"""The public sample files in a wheel agree with one canonical genotype truth."""

from __future__ import annotations

import hashlib
import json
import unittest
from importlib.resources import as_file, files

from mirobody.collect.files.services import genotype_format
from mirobody.translate.genotype import normalize
from mirobody.translate.genotype_sites import SiteCatalog

EXAMPLES = files("mirobody.testing").joinpath("genomics")
ARRAY_FILES = (
    "hg00096-wegene.txt", "hg00096-23andme.txt", "hg00096-ancestry.txt",
    "hg00096-myheritage.csv", "hg00096-ftdna.csv",
)
VCF37_FILES = (
    "hg00096-grch37.vcf", "hg00096-grch37.vcf.gz",
    "hg00096-grch37.vcf.bgz", "hg00096-grch37-vcf-sidecars.zip",
)
VCF38_FILES = ("hg00096-grch38.vcf",)


def _read_records(name: str):
    with as_file(EXAMPLES.joinpath(name)) as path:
        with path.open("rb") as stream:
            fmt = genotype_format.sniff_stream(stream)
        if fmt is None:
            raise AssertionError(f"packaged genotype example was not recognized: {name}")
        with genotype_format.open_lines(path) as lines:
            return fmt, list(genotype_format.records(lines, fmt))


class PackagedGenomicsExamplesTests(unittest.TestCase):
    def test_manifest_pins_every_small_public_file(self) -> None:
        manifest = json.loads(EXAMPLES.joinpath("manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(manifest["schema"], "mirobody-public-genotype-examples-1")
        names = {entry["name"] for entry in manifest["files"]}
        self.assertEqual(names, set(ARRAY_FILES + VCF37_FILES + VCF38_FILES + (
            "hg00097-x-grch37.vcf", "pgp4220-nocall-23andme.txt", "canonical.json",
        )))
        for entry in manifest["files"]:
            with self.subTest(name=entry["name"]):
                payload = EXAMPLES.joinpath(entry["name"]).read_bytes()
                self.assertEqual(len(payload), entry["bytes"])
                self.assertEqual(hashlib.sha256(payload).hexdigest(), entry["sha256"])
                self.assertLess(len(payload), 16 * 1024)

    def test_vendor_and_vcf_renderings_share_thirteen_canonical_calls(self) -> None:
        canonical = json.loads(EXAMPLES.joinpath("canonical.json").read_text(encoding="utf-8"))
        expected = {call["rsid"]: call for call in canonical["hg00096"]}
        self.assertEqual(len(expected), 13)
        self.assertEqual(len({call["gene"] for call in expected.values()}), 12)
        with SiteCatalog() as catalog:
            for name in ARRAY_FILES + VCF37_FILES + VCF38_FILES:
                with self.subTest(name=name):
                    fmt, records = _read_records(name)
                    is_vcf = name in VCF37_FILES + VCF38_FILES
                    self.assertEqual(fmt.shape, "vcf" if is_vcf else
                                     "alleles" if name.endswith(("ancestry.txt", "ftdna.csv"))
                                     else "genotype")
                    self.assertEqual(fmt.build, "GRCh38" if name in VCF38_FILES else "GRCh37")
                    self.assertEqual(len(records), 16 if name in VCF37_FILES else 13)
                    seen = set()
                    for row in records:
                        if row.rsid not in expected:
                            self.assertEqual(row.rsid, ".")
                            continue
                        self.assertNotIn(row.rsid, seen)
                        seen.add(row.rsid)
                        site = catalog.lookup(row.rsid, row.chromosome, row.position)
                        self.assertIsNotNone(site)
                        call = normalize(
                            rsid=row.rsid, chrom=row.chromosome, position=row.position,
                            genotype=row.genotype_raw, strand=row.strand,
                            vcf_gt=row.gt, vcf_ref=row.ref,
                            vcf_alt=",".join(row.alt) if row.alt else None, site=site,
                        )
                        truth = expected[row.rsid]
                        self.assertEqual(
                            (call.rsid, call.gene, call.chrom, call.pos37, call.pos38,
                             call.ref, call.alt, call.gt, call.call_status, call.zygosity),
                            (truth["rsid"], truth["gene"], truth["chrom"], truth["pos37"],
                             truth["pos38"], truth["ref"], truth["alt"],
                             truth["vcf_gt" if is_vcf else "array_gt"],
                             "called", truth["zygosity"]),
                        )
                    self.assertEqual(seen, set(expected))

    def test_public_no_call_and_second_person_remain_separate(self) -> None:
        canonical = json.loads(EXAMPLES.joinpath("canonical.json").read_text(encoding="utf-8"))
        fmt, rows = _read_records("pgp4220-nocall-23andme.txt")
        self.assertEqual((fmt.shape, len(rows)), ("genotype", 1))
        row = rows[0]
        with SiteCatalog() as catalog:
            site = catalog.lookup(row.rsid, row.chromosome, row.position)
        call = normalize(rsid=row.rsid, chrom=row.chromosome, position=row.position,
                         genotype=row.genotype_raw, site=site)
        self.assertEqual((call.rsid, call.call_status, call.gt),
                         (canonical["pgp4220_no_call"]["rsid"], "no_call", None))
        fmt, rows = _read_records("hg00097-x-grch37.vcf")
        self.assertEqual((fmt.shape, fmt.samples, len(rows)), ("vcf", ("HG00097",), 1))
        row = rows[0]
        female = normalize(rsid=row.rsid, chrom=row.chromosome, position=row.position,
                           genotype=row.genotype_raw, vcf_gt=row.gt, vcf_ref=row.ref,
                           vcf_alt=",".join(row.alt), sex="female")
        self.assertEqual((female.call_status, female.gt, female.zygosity),
                         ("called", canonical["public_x"]["HG00097"][0]["gt"], "heterozygous"))


if __name__ == "__main__":
    unittest.main()
