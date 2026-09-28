"""Optional full public PGP format gate; raw participant files stay external."""

from __future__ import annotations

import hashlib
import json
import os
import unittest
from pathlib import Path

from mirobody.collect.files.services.genotype_format import sniff_stream


class PublicPgpClassificationTests(unittest.TestCase):
    def test_all_public_originals_and_html_report(self) -> None:
        directory = Path(os.environ.get("MIROBODY_PUBLIC_PGP_DIR", ""))
        manifest_path = directory / "MANIFEST.json"
        if not manifest_path.is_file():
            self.skipTest("set MIROBODY_PUBLIC_PGP_DIR to external Harvard PGP corpus")
        entries = json.loads(manifest_path.read_text(encoding="utf-8"))
        recognized = rejected = 0
        for entry in entries:
            if entry.get("status") != "ok":
                continue
            source = directory / entry["file"]
            if not source.is_file():
                self.fail(f"missing public PGP source id={entry['id']}")
            digest = hashlib.sha256()
            with source.open("rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
            self.assertEqual(digest.hexdigest(), entry["sha256"], entry["id"])
            with source.open("rb") as stream:
                detected = sniff_stream(stream)
            if source.suffix == ".html":
                self.assertIsNone(detected, entry["id"])
                rejected += 1
            else:
                self.assertIsNotNone(detected, entry["id"])
                recognized += 1
        self.assertEqual((recognized, rejected), (17, 1))


if __name__ == "__main__":
    unittest.main()
