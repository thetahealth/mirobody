"""Export the self-hosted demo as a deterministic, database-free UI fixture.

The hosted walkthrough must use the same people, readings and source files as
the local seed. Export facts from the seed and upload generator rather than
maintaining another patient story. Nothing here reads deployment configuration.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _reading(row: dict) -> dict:
    return {
        "indicator": row["indicator"],
        "value": row["value"],
        "unit": json.loads(row["fhir_mapping_info"])["unit"],
        "date": row["start_time"],
        "fileId": row["source_table_id"].rsplit("/", 1)[-1] or None,
        "referenceRange": row["reference_range"],
        "flag": row["flag"],
    }


def build_fixture() -> dict:
    """Return only the two public synthetic accounts and their sample files."""
    seed = _load("demo_seed_export", ROOT / "mirobody/server/demo.py")
    uploads = _load("demo_upload_export", ROOT / "demo/generate.py")
    members = []
    files = []
    for key in ("you", "mom"):
        email = f"{key}@mirobody.ai"
        readings = [_reading(row) for row in seed._member_series(key, email)]
        members.append({"id": key, "email": email, "readings": readings})
        # Every seeded panel, not `profile["lab"]` alone: mom has an older
        # one, and its readings named a file this list did not carry.
        for collected, filename, panel in seed.panels_for(email):
            files.append(_file(key, filename, "seed", collected, panel))

    for name, panel in uploads.PANELS.items():
        extension = ".pdf" if name.startswith("you_") else ".jpg"
        files.append(_file(name.split("_", 1)[0], name + extension, "upload",
                           panel["collected"], panel["rows"]))
    files.append(_file("you", "you_lipid_panel_2026-08.csv", "upload",
                       uploads.CSV_PANEL["collected"], uploads.CSV_PANEL["rows"]))
    files.append(_file("mom", "mom_clinic_visit_2026-07.xlsx", "upload",
                       uploads.XLSX_PANEL["collected"], uploads.XLSX_PANEL["rows"]))
    for filename, panel in uploads.HISTORY.items():
        files.append(_file("you", filename, "upload", panel["collected"], panel["rows"]))
    first = min(row["date"][:10] for member in members for row in member["readings"])
    fixture = {
        "schemaVersion": 1,
        "asOf": seed._LAST_DAY.isoformat(),
        "period": {"start": first, "end": seed._LAST_DAY.isoformat()},
        "members": members,
        "sharing": [{"owner": "mom", "viewer": "you", "access": "view"}],
        "files": files,
    }
    encoded = json.dumps(fixture, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    fixture["dataDigest"] = hashlib.sha256(encoded.encode()).hexdigest()
    fixture["sourceCommit"] = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True,
    ).strip()
    return fixture


def _file(owner: str, filename: str, stage: str, collected: str, rows) -> dict:
    path = ROOT / "demo" / stage / filename
    contents = path.read_bytes()
    observations = []
    for row in rows:
        indicator, value, unit, *tail = row
        observations.append({
            "indicator": indicator,
            "value": str(value),
            "unit": unit,
            "referenceRange": tail[0] if tail else "",
            "flag": tail[1] if len(tail) > 1 else "",
        })
    return {
        "id": filename, "owner": owner, "stage": stage, "collected": collected,
        "filename": filename, "bytes": len(contents),
        "sha256": hashlib.sha256(contents).hexdigest(), "readings": observations,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="Write JSON here instead of stdout")
    args = parser.parse_args()
    sys.dont_write_bytecode = True
    encoded = json.dumps(build_fixture(), ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    if args.output:
        args.output.write_text(encoded, encoding="utf-8")
    else:
        print(encoded, end="")


if __name__ == "__main__":
    main()
