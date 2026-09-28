"""Small, tracked examples of the public indicator and complaint vocabulary APIs."""

from __future__ import annotations

import json
import unittest
from pathlib import Path

from mirobody.engine import standardize_reading
from mirobody.translate.icpc3 import resolve_condition, resolve_symptom
from mirobody.translate.terminology import standardize_complaint

CASES = json.loads(Path(__file__).with_name("cases.json").read_text(encoding="utf-8"))


class HealthRecordBenchmarkTests(unittest.TestCase):
    def test_indicators_keep_loinc_and_ucum_separate(self) -> None:
        self.assertEqual(CASES["schema"], "mirobody-small-health-record-benchmark-1")
        for case in CASES["indicators"]:
            with self.subTest(name=case["name"]):
                result = standardize_reading(case["name"], case["value"], case["unit"])
                self.assertEqual(result["code"]["text"], case["name"])
                codes = result["code"].get("coding", [])
                self.assertEqual(codes[0]["code"] if codes else None, case["loinc"])
                self.assertEqual(result["valueQuantity"]["code"], case["ucum"])

    def test_complaints_keep_original_words_and_selected_axis(self) -> None:
        for case in CASES["complaints"]:
            with self.subTest(kind=case["kind"], name=case["name"]):
                resolver = resolve_symptom if case["kind"] == "symptom" else resolve_condition
                result = resolver(case["name"])
                self.assertEqual((result.code, result.outcome, result.reason, result.series_id),
                                 (case["code"], case["outcome"], case["reason"], case["series"]))
                if result.code:
                    self.assertEqual(result.code_system,
                                     "http://terminology.hl7.org/CodeSystem/ICPC-3")
                observation = standardize_complaint(case["name"], case["kind"])
                self.assertEqual(observation["code"]["text"], case["name"])
                codings = observation["code"].get("coding", [])
                self.assertEqual(codings[0]["code"] if codings else None, case["code"])


if __name__ == "__main__":
    unittest.main()
