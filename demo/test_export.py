"""Public invariants of the synthetic walkthrough, independent of a server."""

from collections import Counter
import hashlib
import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("demo_export_under_test", ROOT / "demo/export.py")
export = importlib.util.module_from_spec(spec)
spec.loader.exec_module(export)


class ExportContractTest(unittest.TestCase):
    def test_initial_record_keeps_upload_samples_out(self):
        fixture = export.build_fixture()
        self.assertEqual([len(m["readings"]) for m in fixture["members"]], [814, 1208])
        self.assertEqual(fixture["period"], {"start": "2025-09-15", "end": "2026-09-14"})
        seeded = {"you": {"you_lab_2025-11.md"},
                  "mom": {"mom_lab_2025-05.md", "mom_lab_2025-11.md"}}
        for member in fixture["members"]:
            names = {r["fileId"] for r in member["readings"] if r["fileId"]}
            self.assertEqual(names, seeded[member["id"]])
            # Every document a seeded reading cites ships as a seed file.
            self.assertEqual(names, {f["filename"] for f in fixture["files"]
                                     if f["owner"] == member["id"] and f["stage"] == "seed"})
        self.assertEqual(fixture["sharing"], [{"owner": "mom", "viewer": "you", "access": "view"}])

    def test_samples_keep_the_source_dates_readings_and_bytes(self):
        fixture = export.build_fixture()
        counts = Counter()
        for file in fixture["files"]:
            self.assertEqual(file["sha256"], hashlib.sha256(
                (ROOT / "demo" / file["stage"] / file["filename"]).read_bytes()).hexdigest())
            counts[file["stage"]] += 1
        self.assertEqual(counts, {"seed": 3, "upload": 4})
        uploads = {f["filename"]: f for f in fixture["files"] if f["stage"] == "upload"}
        self.assertEqual(sorted(len(f["readings"]) for f in uploads.values()), [4, 5, 9, 9])
        csv = uploads["you_lipid_panel_2026-08.csv"]
        self.assertEqual(csv["collected"], "2026-08-04")
        cholesterol = next(r for r in csv["readings"] if r["indicator"] == "Cholesterol, Total")
        self.assertEqual((cholesterol["value"], cholesterol["unit"]), ("4.38", "mmol/L"))

    def test_repeat_exports_are_identical(self):
        self.assertEqual(export.build_fixture(), export.build_fixture())


if __name__ == "__main__":
    unittest.main()
