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
spec = importlib.util.spec_from_file_location("demo_generate_under_test", ROOT / "demo/generate.py")
generate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generate)


class ExportContractTest(unittest.TestCase):
    def test_initial_record_keeps_upload_samples_out(self):
        fixture = export.build_fixture()
        self.assertEqual([len(m["readings"]) for m in fixture["members"]], [814, 1208])
        self.assertEqual(fixture["period"], {"start": "2025-05-14", "end": "2026-09-14"})
        seeded = {"you": {"you_lab_2025-11.md"}, "mom": {"mom_lab_2025-05.md", "mom_lab_2025-11.md"}}
        for member in fixture["members"]:
            names = {r["fileId"] for r in member["readings"] if r["fileId"]}
            self.assertEqual(names, seeded[member["id"]])
            self.assertEqual(names, {f["filename"] for f in fixture["files"]
                                     if f["stage"] == "seed" and f["owner"] == member["id"]})
        self.assertEqual(fixture["sharing"], [{"owner": "mom", "viewer": "you", "access": "view"}])

    def test_samples_keep_the_source_dates_readings_and_bytes(self):
        fixture = export.build_fixture()
        counts = Counter()
        for file in fixture["files"]:
            self.assertEqual(file["sha256"], hashlib.sha256(
                (ROOT / "demo" / file["stage"] / file["filename"]).read_bytes()).hexdigest())
            counts[file["stage"]] += 1
        self.assertEqual(counts, {"seed": 3, "upload": 7})
        uploads = {f["filename"]: f for f in fixture["files"] if f["stage"] == "upload"}
        self.assertEqual(set(uploads), {p.name for p in (ROOT / "demo/upload").iterdir()})
        self.assertEqual(sorted(len(f["readings"]) for f in uploads.values()), [4, 5, 9, 9, 16, 16, 16])
        csv = uploads["you_lipid_panel_2026-08.csv"]
        self.assertEqual(csv["collected"], "2026-08-04")
        cholesterol = next(r for r in csv["readings"] if r["indicator"] == "Cholesterol, Total")
        self.assertEqual((cholesterol["value"], cholesterol["unit"]), ("4.38", "mmol/L"))

    def test_repeat_exports_are_identical(self):
        self.assertEqual(export.build_fixture(), export.build_fixture())

    def test_three_years_print_one_test_three_ways(self):
        """`upload/you_*_zh_*` and `you_lab_results_en_*`: three places, three
        names for LDL cholesterol, two units. Each file is checked against its
        panel the way it can be without a model: the PDF by its text layer's
        bytes, the workbook by its cells. The photo has no text to compare."""
        history = generate.HISTORY
        ldl = [next(r for r in panel["rows"] if "LDL" in r[0] or "低密度" in r[0]) for panel in history.values()]
        self.assertEqual([(r[0], r[1], r[2]) for r in ldl], [
            ("LDL-C", "3.62", "mmol/L"),
            ("低密度脂蛋白胆固醇", "128", "mg/dL"),
            ("LDL Cholesterol", "112", "mg/dL"),
        ])
        self.assertEqual(sorted(p["collected"][:4] for p in history.values()), ["2023", "2024", "2025"])

        pdf = "you_lab_report_zh_2023-03.pdf"
        self.assertEqual((ROOT / "demo/upload" / pdf).read_bytes(), generate._pdf_zh(history[pdf]))

        book = "you_lab_results_en_2025-03.xlsx"
        from openpyxl import load_workbook
        cells = list(load_workbook(ROOT / "demo/upload" / book).worksheets[0].iter_rows(values_only=True))
        self.assertEqual(cells[0][2:5], ("Test", "Result", "Units"))
        self.assertEqual([(row[2], str(row[3]), row[4]) for row in cells[1:]],
                         [(r[0], r[1], r[2]) for r in history[book]["rows"]])


if __name__ == "__main__":
    unittest.main()
