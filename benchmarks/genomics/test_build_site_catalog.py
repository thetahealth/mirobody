"""Offline checks using NCBI's published dbSNP b157 rs268 example."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
SCRIPT = HERE / "build_site_catalog.py"
MANIFEST = HERE / "sample-b157.json"
NCBI_SAMPLE_URL = "https://ftp.ncbi.nlm.nih.gov/snp/archive/b157/JSON/refsnp-sample.json.bz2"
CANDIDATE = HERE.parents[1] / "mirobody/res/genomics/genotype_sites.sqlite3"
CANDIDATE_SHA256 = "557fed0f94ff613a091be21a877ebc2b4f7dd2e5019ebfcb6d636afd84083b60"


def _run(manifest: Path, output: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--manifest", str(manifest), "--output", str(output)],
        capture_output=True, text=True, check=False,
    )


class SiteCatalogBuildTests(unittest.TestCase):
    def test_shipped_public_candidate_has_measured_scope(self) -> None:
        self.assertEqual(hashlib.sha256(CANDIDATE.read_bytes()).hexdigest(), CANDIDATE_SHA256)
        with sqlite3.connect(CANDIDATE) as db:
            self.assertEqual(db.execute("SELECT value FROM metadata WHERE key='version'").fetchone(),
                             ("dbsnp-b155-common-pgx-candidate",))
            self.assertEqual(db.execute("SELECT COUNT(*) FROM sites").fetchone(), (489,))
            self.assertEqual(
                db.execute("SELECT pos37,pos38,ref,alt,gene FROM sites WHERE rsid='rs4244285'").fetchone(),
                (96541616, 94781859, "G", "A,C,T", "CYP2C19"),
            )
            stats = json.loads(db.execute("SELECT value FROM metadata WHERE key='stats_json'").fetchone()[0])
            self.assertEqual(stats["marker_coverage_by_source"]["pgp_23andme_v5"],
                             {"covered": 262, "total": 625705})
            self.assertEqual(stats["marker_coverage_by_source"]["pgp_ancestry_v2"],
                             {"covered": 340, "total": 677436})

    def test_official_json_build_is_reproducible_and_readable(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            first = Path(directory) / "first.sqlite3"
            second = Path(directory) / "second.sqlite3"
            for output in (first, second):
                result = _run(MANIFEST, output)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(json.loads(result.stdout)["sites"], 1)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            with sqlite3.connect(first) as db:
                row = db.execute("SELECT * FROM sites WHERE rsid='rs268'").fetchone()
                self.assertEqual(row, ("rs268", "8", 19813529, 19956018, "A", "G", "LPL"))
                self.assertEqual(
                    db.execute("SELECT rsid FROM merged WHERE old_rsid='rs17850737'").fetchone(),
                    ("rs268",),
                )
                self.assertEqual(
                    db.execute("SELECT value FROM metadata WHERE key='version'").fetchone(),
                    ("dbsnp-b157-sample",),
                )

    def test_vcf_reader_accepts_official_sample_derived_rows_with_info_rs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = {}
            for build, chrom, pos in ((37, "NC_000008.10", 19813529), (38, "NC_000008.11", 19956018)):
                path = root / f"b157-sample-{build}.vcf"
                path.write_text(
                    "##fileformat=VCFv4.2\n#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n"
                    f"{chrom}\t{pos}\t.\tA\tG\t.\t.\tRS=268;GENEINFO=LPL:4023\n",
                    encoding="utf-8",
                )
                sources[f"vcf{build}"] = {
                    "path": str(path), "url": NCBI_SAMPLE_URL, "license": "NCBI-PD",
                    "derived_from": "VCF rendering of NCBI b157 rs268 RefSNP JSON example",
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                }
            manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
            del manifest["refsnp_json"]
            manifest["markers"][0]["path"] = str((HERE / "fixtures/markers.tsv").resolve())
            manifest.update(sources)
            path = root / "manifest.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")
            output = root / "sites.sqlite3"
            result = _run(path, output)
            self.assertEqual(result.returncode, 0, result.stderr)
            with sqlite3.connect(output) as db:
                self.assertEqual(
                    db.execute("SELECT rsid,chrom,pos37,pos38,ref,alt,gene FROM sites").fetchone(),
                    ("rs268", "8", 19813529, 19956018, "A", "G", "LPL"),
                )

    def test_source_hash_mismatch_refuses_output(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
            for entry in [*manifest["markers"], *manifest["refsnp_json"]]:
                entry["path"] = str((HERE / entry["path"]).resolve())
            manifest["refsnp_json"][0]["sha256"] = "0" * 64
            path = root / "manifest.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")
            output = root / "refused.sqlite3"
            result = _run(path, output)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("source hash mismatch", result.stderr)
            self.assertFalse(output.exists())

    def test_public_old_rsid_marker_resolves_to_current_site(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = root / "markers.tsv"
            marker.write_text("rsid\nrs17850737\n", encoding="utf-8")
            manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
            manifest["markers"][0].update({
                "path": str(marker),
                "url": "https://api.ncbi.nlm.nih.gov/variation/v0/refsnp/17850737",
                "derived_from": "refsnp_id of the NCBI merged RefSNP API example",
                "sha256": hashlib.sha256(marker.read_bytes()).hexdigest(),
            })
            manifest["refsnp_json"][0]["path"] = str((HERE / "fixtures/refsnp-sample.json.bz2").resolve())
            manifest["merged"] = {
                "path": str((HERE / "fixtures/merged-rs17850737.json").resolve()),
                "url": "https://api.ncbi.nlm.nih.gov/variation/v0/refsnp/17850737",
                "sha256": "42d16ceff2184ffc87c2e90d0bba36abc366accbb59c4e90045009aca090a27b",
                "license": "NCBI-PD",
            }
            path = root / "manifest.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")
            result = _run(path, root / "sites.sqlite3")
            self.assertEqual(result.returncode, 0, result.stderr)
            counts = json.loads(result.stdout)
            self.assertEqual(counts["marker_covered"], 1)
            with sqlite3.connect(root / "sites.sqlite3") as db:
                self.assertEqual(db.execute("SELECT rsid FROM merged WHERE old_rsid='rs17850737'").fetchone(), ("rs268",))

    def test_public_common_candidate_accepts_partial_marker_sources(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            marker = root / "markers.tsv"
            marker.write_text("rsid\nrs4244285\nrs268\n", encoding="utf-8")
            converter = root / "bigBedToBed"
            converter.write_text(
                f"#!{sys.executable}\n"
                "import sys\n"
                "position = 96541615 if 'hg19' in sys.argv[1] else 94781858\n"
                "print('\\t'.join(map(str, ['chr10', position, position + 1, 'rs4244285', "
                "'G', '3', 'A,C,T,', '0', '31', '', '', '', '1819', 'snv', '', '', ''])))\n",
                encoding="utf-8",
            )
            converter.chmod(0o755)
            manifest = {
                "release": "dbsnp-b155-common", "scope": "candidate",
                "markers": [{
                    "name": "public_example", "path": str(marker),
                    "url": "https://github.com/cpicpgx/cpic-data/releases/tag/v1.60.0",
                    "derived_from": "rs4244285 from CPIC and rs268 from the NCBI b157 sample",
                    "license": "CC0-1.0", "sha256": hashlib.sha256(marker.read_bytes()).hexdigest(),
                }],
            }
            genes = root / "genes.tsv"
            genes.write_text("rsid\tgene\nrs4244285\tCYP2C19\n", encoding="utf-8")
            manifest["gene_annotations"] = {
                "path": str(genes), "url": "https://github.com/cpicpgx/cpic-data/releases/tag/v1.60.0",
                "license": "CC0-1.0", "sha256": hashlib.sha256(genes.read_bytes()).hexdigest(),
            }
            for build, assembly in ((37, "hg19"), (38, "hg38")):
                source = root / f"dbSnp155Common-{assembly}.bb"
                source.write_bytes(b"test converter fixture")
                manifest[f"ucsc{build}"] = {
                    "path": str(source),
                    "url": f"https://hgdownload.soe.ucsc.edu/gbdb/{assembly}/snp/dbSnp155Common.bb",
                    "license": "UCSC-Public-Data", "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                }
            manifest_path = root / "manifest.json"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            output = root / "candidate.sqlite3"
            result = subprocess.run(
                [sys.executable, str(SCRIPT), "--manifest", str(manifest_path),
                 "--output", str(output), "--big-bed-to-bed", str(converter)],
                capture_output=True, text=True, check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            counts = json.loads(result.stdout)
            self.assertEqual(counts["marker_coverage_by_source"]["public_example"],
                             {"total": 2, "covered": 1})
            with sqlite3.connect(output) as db:
                self.assertEqual(db.execute("SELECT rsid,pos37,pos38,ref,alt,gene FROM sites").fetchone(),
                                 ("rs4244285", 96541616, 94781859, "G", "A,C,T", "CYP2C19"))
                self.assertEqual(db.execute("SELECT value FROM metadata WHERE key='version'").fetchone(),
                                 ("dbsnp-b155-common-candidate",))


if __name__ == "__main__":
    unittest.main()
