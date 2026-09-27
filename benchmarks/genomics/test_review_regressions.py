"""Regressions from the 1.5.2 public-data review."""

from __future__ import annotations

import gzip
import io
import csv
import unittest
import zipfile
from pathlib import Path

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from mirobody.agent.chat.file import _detect_file_scene
from mirobody.agent.middleware.genotype_row_guard import GenotypeRowGuardMiddleware
from mirobody.agent.tools.genetic_service import TOOL_NAME
from mirobody.kernel import tools
from mirobody.translate.genotype import normalize, pseudoautosomal_status


PUBLIC_VCF = Path(__file__).parent / "fixtures/public-hg00096.vcf"
PUBLIC_X = Path(__file__).parent / "fixtures/public-1000g-x.tsv"


class ChatClassificationTests(unittest.TestCase):
    def test_public_vcf_plain_gzip_zip_are_genetic(self) -> None:
        # Padding contains no extra calls; the only genotype rows are the
        # published 1000 Genomes HG00096 CYP2C19 calls in the fixture.
        public = PUBLIC_VCF.read_bytes() + b"##source=1000Genomes HG00096 public truth\n" * 500
        archive = io.BytesIO()
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_STORED) as output:
            output.writestr("public.vcf", public)
        cases = (
            ("public.vcf", "text/vcf", public),
            ("public.vcf.gz", "application/gzip", gzip.compress(public, compresslevel=0)),
            ("public.zip", "application/zip", archive.getvalue()),
        )
        for name, mime, data in cases:
            with self.subTest(name=name):
                self.assertGreater(len(data), 16 * 1024)
                self.assertEqual(_detect_file_scene({
                    "file_name": name, "content_type": mime, "content_bytes": data,
                }), "genetic")

    def test_classification_is_per_file(self) -> None:
        files = (
            {"file_name": "public.vcf", "content_type": "text/vcf", "content_bytes": PUBLIC_VCF.read_bytes()},
            {"file_name": "notes.txt", "content_type": "text/plain", "content_bytes": b"non-genetic notes"},
        )
        self.assertEqual([_detect_file_scene(file) for file in files], ["genetic", "report"])


class ParCoordinateTests(unittest.TestCase):
    def test_public_grc_and_ensembl_par_boundaries(self) -> None:
        self.assertTrue(pseudoautosomal_status("X", 60001, None))
        self.assertTrue(pseudoautosomal_status("Y", 2649520, None))
        self.assertTrue(pseudoautosomal_status("X", None, 2781479))
        self.assertTrue(pseudoautosomal_status("Y", None, 56887903))
        self.assertFalse(pseudoautosomal_status("X", 2699521, None))
        self.assertFalse(pseudoautosomal_status("Y", None, 2781480))
        self.assertIsNone(pseudoautosomal_status("X", None, None))

    def test_public_1000g_x_calls_respect_par_and_inferred_sex(self) -> None:
        with PUBLIC_X.open() as source:
            rows = list(csv.DictReader((line for line in source if not line.startswith("#")), delimiter="\t"))
        calls = {}
        for row in rows:
            pos = int(row["pos37"])
            site = {"rsid": f"X:{pos}:{row['ref']}:{row['alt']}", "chrom": "X",
                    "pos37": pos, "ref": row["ref"], "alt": row["alt"]}
            calls[pos] = normalize(
                rsid=site["rsid"], chrom="X", position=pos, genotype="", vcf_gt=row["gt"],
                vcf_ref=row["ref"], vcf_alt=row["alt"], site=site, sex="male",
            )
        self.assertEqual((calls[60052].call_status, calls[60052].gt), ("called", "0|1"))
        self.assertEqual((calls[60026].call_status, calls[60026].gt), ("called", "0|0"))
        self.assertEqual((calls[3000679].call_status, calls[3000679].strand_check),
                         ("unresolved", "haploid_conflict"))
        self.assertEqual((calls[3000166].call_status, calls[3000166].zygosity),
                         ("called", "hemizygous"))


class GenotypeRowGuardTests(unittest.TestCase):
    def test_checkpoint_replay_requires_a_new_bounded_tool_result(self) -> None:
        # rs4244285=AG comes from the pinned public HG00096 VCF above.
        envelope = tools.Envelope(tools.STATUS_OK,
                                  data=[{"rsid": "rs4244285", "genotype": "AG"}],
                                  meta=tools.Meta(row_count=1))
        previous = ToolMessage(name=TOOL_NAME, tool_call_id="previous",
                               content="rs4244285 | AG", artifact=envelope)
        current = ToolMessage(name=TOOL_NAME, tool_call_id="current",
                              content="rs4244285 | AG", artifact=envelope)
        guard = GenotypeRowGuardMiddleware()
        messages = [HumanMessage(content="My public call?"), previous,
                    AIMessage(content="Your rs4244285 genotype is AG."),
                    HumanMessage(content="What is it now?"), current, current]
        guard._record(current)
        guarded, visible, returned, redacted, answers = guard._guard_messages(messages)
        self.assertEqual((visible, returned, redacted, answers), (1, 1, 2, 1))
        self.assertNotIn("AG", guarded[1].content + guarded[2].content + guarded[5].content)
        self.assertIsNone(guarded[1].artifact)
        self.assertEqual(guarded[4].content, current.content)
