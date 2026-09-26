"""The device vocabulary as one file a client without Python can load.

A phone codes a health-store batch before any server sees it, and it has to
code it the way this package would: the same catalogue name, the same LOINC
code, the same refusal where the crosswalk has none. Porting the TSVs by hand
is how two vocabularies start. This module writes what the phone needs, from
the same files the server reads:

    metrics     the catalogue (``res/metrics.tsv``), every member, with the
                ``(system, code)`` identity `Metric.canonical` gives it
    labels      the shipped display labels (``res/labels/<locale>.tsv``)
    codes       the crosswalk base table, one row per LOINC code
    vendors     every vendor field read, coded or declined, with its source
    unmappable  the metrics no LOINC code fits, and why

The file names its own versions (``terminology_version``, ``loinc_release``)
and carries a ``digest``: sha256 over the canonical JSON of everything else
(keys sorted, no whitespace, UTF-8). The same release gives the same bytes on
any machine, so the digest is what a coded reading can cite as the vocabulary
that coded it. To check one, drop ``digest`` and hash `canonical_json` of the
rest.

Pure, like the seam it serves: no clock, no database, stdlib only.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
from importlib import resources
from typing import Any

from mirobody.kernel import metrics

from . import devices

#: The shape of this file. Raise it by one when a consumer written against the
#: previous shape would misread the new one; adding a key does not count.
FORMAT = "mirobody.device-bundle"
FORMAT_VERSION = 1


def _labels() -> dict[str, dict[str, dict[str, str]]]:
    out: dict[str, dict[str, dict[str, str]]] = {}
    root = resources.files("mirobody").joinpath("res", "labels")
    for entry in sorted(root.iterdir(), key=lambda e: e.name):
        if not entry.name.endswith(".tsv"):
            continue
        rows = csv.DictReader(io.StringIO(entry.read_text(encoding="utf-8")), delimiter="\t")
        out[entry.name[:-4]] = {
            r["key"]: {"kind": r["kind"], "label": r.get("label") or "", "description": r.get("description") or ""}
            for r in rows
        }
    return out


def _metric(m: metrics.Metric) -> dict[str, Any]:
    system, code = m.canonical
    return {
        "member": m.member,
        "name": m.name,
        # A name declared under several members keeps its first as the identity.
        "primary": metrics.METRICS[m.name] is m,
        "display": m.display,
        "category": m.category,
        "standard_unit": m.standard_unit,
        "unit_ucum": m.unit_ucum,
        "data_type": m.data_type,
        "state_class": m.state_class,
        "aggregation_policy": m.aggregation_policy,
        "loinc": m.loinc,
        "confidence": m.confidence,
        "system": system,
        "code": code,
        "window": m.window,
        "panel": m.panel,
        "aggregation_methods": list(m.aggregation_methods),
        "derived": list(m.derived),
        "description": m.description,
        "since": m.since,
    }


def _vendor(vendor: str) -> dict[str, Any]:
    source = devices.SOURCES[vendor]
    return {
        "name": source.name,
        "urls": list(source.urls),
        "note": source.note,
        "fields": [
            {
                "type": f.type,
                "field": f.field,
                "key": f.key,
                "metric": f.metric,
                "loinc": f.loinc,
                "confidence": f.confidence,
                "note": f.note,
            }
            for f in devices.vendor_fields(vendor)
        ],
    }


def canonical_json(value: Any) -> bytes:
    """The one serialization the digest is taken over."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def build() -> dict[str, Any]:
    """The bundle as a dict, ``digest`` included."""
    body: dict[str, Any] = {
        "format": FORMAT,
        "format_version": FORMAT_VERSION,
        "terminology_version": metrics.TERMINOLOGY_VERSION,
        "loinc_release": devices.LOINC_RELEASE,
        "retrieved": devices.RETRIEVED,
        "systems": {"loinc": metrics.SYSTEM_LOINC, "device": metrics.SYSTEM_DEVICE},
        "metrics": [_metric(m) for m in metrics.ROWS],
        "labels": _labels(),
        "codes": [
            {
                "loinc": c.loinc,
                "name": c.name,
                "axes": c.axes,
                "metric": c.metric,
                "confidence": c.confidence,
                "fields": dict(c.fields),
                "note": c.note,
            }
            for c in devices.base_table()
        ],
        "vendors": {v: _vendor(v) for v in devices.VENDORS},
        "unmappable": [
            {"reason": u.reason, "metric": u.metric, "vendors": u.vendors, "detail": u.detail, "disposition": u.disposition}
            for u in devices.unmappable()
        ],
    }
    body["digest"] = "sha256:" + hashlib.sha256(canonical_json(body)).hexdigest()
    return body


def dumps() -> bytes:
    """The bundle file: `canonical_json` of `build`, newline-terminated."""
    return canonical_json(build()) + b"\n"


def verify(bundle: dict[str, Any]) -> bool:
    """Whether ``bundle`` still hashes to the digest it carries."""
    rest = {k: v for k, v in bundle.items() if k != "digest"}
    return bundle.get("digest") == "sha256:" + hashlib.sha256(canonical_json(rest)).hexdigest()


__all__ = ["FORMAT", "FORMAT_VERSION", "build", "canonical_json", "dumps", "verify"]
