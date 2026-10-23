"""Frozen development-set provenance and fail-closed source verification."""

from __future__ import annotations

import hashlib
import json
from datetime import date, timedelta

from common import HERE, Corpus, read_jsonl, sha256

SUITE_ID = "harness100-20261023b"
GENERATOR_COMMIT = "9723605"
AS_OF = "2026-09-02T00:00:00+00:00"
RECIPE = "python -m mirobody_gen build --seed 7 --people 8 --render --clinical --handwriting --verify --out <corpus>"


def digest_json(value: object) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def record_identity(plan: dict) -> str:
    """Question edits and machine-specific audit logs do not change a loaded record."""
    return digest_json({"source_sha256": plan["provenance"]["sha256"], "qa_people": plan["qa_people"]})


def medication_versions(plan: dict) -> list[dict]:
    """Each actual dose change closes the preceding interval, inclusively.

    The product stores one schedule per plan. Splitting the generator's
    explicit change events preserves historical doses without fabricating
    intake events or overwriting an earlier schedule with a future dose.
    """
    versions = [{"date": plan["start"], "strength": plan["strength"],
                 "dose_text_en": plan["dose_text_en"], "dose_text_zh": plan["dose_text_zh"]},
                *sorted(plan["changes"], key=lambda x: x["date"])]
    out = []
    for i, version in enumerate(versions):
        end = ((date.fromisoformat(versions[i + 1]["date"]) - timedelta(days=1)).isoformat()
               if i + 1 < len(versions) else plan["end"])
        out.append({**plan, **version, "version_id": f"{plan['plan_id']}:{i}", "start": version["date"], "end": end})
    return out


def corpus_manifest(corpus: Corpus, people: dict, *, generator_commit: str) -> dict:
    paths = {p.name for p in corpus.dir.glob("*.jsonl")} | {"generation.json"}
    audit_paths = {"verification.json"} | {
        p.relative_to(corpus.dir).as_posix() for p in (corpus.dir / "verification").glob("*.log")}
    for person, cfg in people.items():
        paths.update(cfg["documents"])
        paths.update(corpus.devices.get(person, {}).get("files", []))
        if cfg["genomics"]:
            paths.add(corpus.genomics[person]["file"])
    return {
        "suite_id": SUITE_ID, "split": "development_regression", "independent_test_set": False,
        "as_of": AS_OF, "generator_commit": generator_commit, "recipe": RECIPE,
        "gold_source": "generator fields and independent Python arithmetic; never product output",
        "selection": "deterministic semantic predicates; 8 people, selected documents only",
        "audit": json.loads((corpus.dir / "verification.json").read_text(encoding="utf-8")),
        "audit_sha256": {rel: sha256(corpus.path(rel)) for rel in sorted(audit_paths)},
        "sha256": {rel: sha256(corpus.path(rel)) for rel in sorted(paths)},
    }


def validate_corpus(corpus: Corpus, plan: dict) -> None:
    provenance = plan.get("provenance") or {}
    expected = provenance.get("sha256")
    if not expected or provenance.get("suite_id") != SUITE_ID:
        raise ValueError("Missing or incompatible corpus provenance; regenerate cases and plan together")
    failures = [rel for rel, value in expected.items()
                if not corpus.path(rel).is_file() or sha256(corpus.path(rel)) != value]
    if failures:
        raise ValueError(f"Corpus hash mismatch in {len(failures)} file(s): {', '.join(failures[:5])}")


def validate_cases(cases: list[dict], plan: dict) -> None:
    if len(cases) != 100 or len({c["id"] for c in cases}) != 100:
        raise ValueError("The full suite must contain exactly 100 unique cases")
    if digest_json(cases) != plan.get("cases_sha256"):
        raise ValueError("Case manifest hash mismatch; regenerate cases and plan together")
    seen: dict[str, dict] = {}
    for case in cases:
        parent = case.get("depends_on")
        if parent and (parent not in seen or seen[parent]["person"] != case["person"]
                       or seen[parent]["lang"] != case["lang"]):
            raise ValueError(f"Invalid conversation dependency in {case['id']}")
        seen[case["id"]] = case
        if not case.get("facts") or not case.get("evidence"):
            raise ValueError(f"Missing independent truth in {case['id']}")


def select_cases(cases: list[dict], *, wanted: list[str] | None = None, smoke: bool = False) -> list[dict]:
    by_id = {c["id"]: c for c in cases}
    selected = set(wanted or (c["id"] for c in cases if not smoke or c.get("smoke")))
    unknown = selected - by_id.keys()
    if unknown:
        raise ValueError(f"Unknown case ids: {', '.join(sorted(unknown))}")
    for case_id in list(selected):
        while by_id[case_id].get("depends_on"):
            case_id = by_id[case_id]["depends_on"]
            selected.add(case_id)
    return [c for c in cases if c["id"] in selected]


def saved_manifest(qa: dict) -> list[dict]:
    """Read the question version that produced an archived or current run."""
    cases = qa.get("case_manifest")
    if cases:
        if digest_json(cases) != qa.get("cases_sha256"):
            raise ValueError("Saved case manifest failed its own hash check")
    else:
        cases = read_jsonl(HERE / "results" / "2026-10-10" / "cases.jsonl")
    unknown = {r["id"] for r in qa.get("runs", [])} - {c["id"] for c in cases}
    if unknown:
        raise ValueError("Saved run has no matching frozen question manifest")
    return cases


def comparison_identity(qa: dict) -> tuple:
    cases = saved_manifest(qa)
    selection = tuple(sorted(qa.get("selected_ids", []))) if qa.get("case_manifest") else ()
    return digest_json(cases), selection
