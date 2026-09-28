"""Offline CPIC fetch check using a pinned public release dump."""

from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from mirobody.translate.cpic_extract import fetch_extract
from mirobody.translate.pgx import load_cpic


PUBLIC_DUMP = Path("internal/genomics/corpus/reference/cpic_db_dump-v1.59.1.sql.gz")
PUBLIC_SHA256 = "b4d7a473821d82ba78a58ee79f28fb5a4702919b968eeefe64c7c5191e1652bd"


class CpicFetchTests(unittest.TestCase):
    @unittest.skipUnless(PUBLIC_DUMP.exists(), "pinned public CPIC dump is not installed")
    def test_pinned_fetch_switch_and_failed_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as root:
            directory = Path(root)
            mirror = directory / "mirror"
            mirror.mkdir()
            source = mirror / PUBLIC_DUMP.name
            source.write_bytes(PUBLIC_DUMP.read_bytes())
            installed_dir = directory / "installed"
            version, digest, installed = fetch_extract("v1.59.1", installed_dir, mirror.as_uri())
            self.assertEqual((version, digest), ("v1.59.1", PUBLIC_SHA256))
            original = installed.read_bytes()
            self.assertEqual(load_cpic("v1.59.1", installed_dir).version, "v1.59.1")
            self.assertEqual(load_cpic("latest", installed_dir).version, "v1.60.0")
            with patch.dict(os.environ, {"CPIC_VERSION": "v1.59.1", "CPIC_DIR": str(installed_dir)}):
                self.assertEqual(load_cpic().version, "v1.59.1")
            self.assertEqual(load_cpic().version, "v1.60.0")
            fetch_extract("v1.59.1", installed_dir, mirror.as_uri())
            source.write_bytes(source.read_bytes()[:100])
            with self.assertRaises((EOFError, OSError, ValueError)):
                fetch_extract("v1.59.1", installed_dir, mirror.as_uri())
            self.assertEqual(installed.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
