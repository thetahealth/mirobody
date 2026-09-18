"""The device crosswalk: which LOINC code a wearable's field means, and which
fields mean nothing LOINC has a code for.

Thirteen vendors' public data types were read field by field against the
LOINC 2.83 axis table (COMPONENT, PROPERTY, TIME, SYSTEM, SCALE, METHOD) on
2026-09-09. No vendor API carries a LOINC code of its own; every code here is
a human judgement over those six axes, recorded with its confidence:

    confident    the six axes agree with the vendor's own definition
    unverified   semantically close, and a person must confirm before a
                 reading is filed under it

The tables live in ``res/crosswalks/`` and are the public form of that work:

    loinc_device_base.tsv   one row per LOINC code, one column per vendor
    <vendor>.tsv            one row per vendor field, with its code or its
                            reason for having none
    unmappable.tsv          the metrics no LOINC code fits, by reason

Everything here is stdlib and reads only those files. The catalogue in
``res/metrics.tsv`` carries the confident codes as each metric's identity;
this module is where the evidence behind them, and the unverified ones,
can be looked up. `SOURCES` names the vendor document each table was read
from, so a row can be checked against its origin.
"""

from __future__ import annotations

import csv
import io
from collections.abc import Iterable
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources

CONFIDENT = "confident"
UNVERIFIED = "unverified"
CONFIDENCES = frozenset({CONFIDENT, UNVERIFIED})

#: The date every vendor document was read.
RETRIEVED = "2026-09-09"
#: The LOINC release the axes were checked against.
LOINC_RELEASE = "2.83"


@dataclass(frozen=True)
class Source:
    """Where one vendor's field names came from."""

    vendor: str
    name: str
    urls: tuple[str, ...]
    note: str = ""


SOURCES: dict[str, Source] = {
    "apple": Source(
        "apple", "Apple HealthKit",
        (
            "https://developer.apple.com/tutorials/data/documentation/healthkit/hkquantitytypeidentifier.json",
            "https://developer.apple.com/tutorials/data/documentation/healthkit/hkcategorytypeidentifier.json",
            "https://developer.apple.com/tutorials/data/documentation/healthkit/hkcategoryvaluesleepanalysis.json",
        ),
        "Apple's own JSON endpoints behind the HealthKit reference pages.",
    ),
    "health_connect": Source(
        "health_connect", "Google Health Connect",
        ("https://developer.android.com/reference/androidx/health/connect/client/records/package-summary",),
        "The androidx Record classes; 42 of them.",
    ),
    "huawei": Source(
        "huawei", "Huawei Health Kit",
        (
            "https://developer.huawei.com/consumer/cn/doc/HMSCore-Guides/overview-0000001177423513",
            "https://developer.huawei.com/consumer/cn/doc/HMSCore-Guides/health-sampling-data-0000001131423778",
        ),
        "Data-type overview and the field-level sampling, daily-activity and health-record pages.",
    ),
    "honor": Source(
        "honor", "Honor Health Kit",
        ("https://developer.honor.com/",),
        "Fitness > Health service (kitId 11005); SDK com.hihonor.mcs:fitness-health:1.0.0.302, "
        "Maven https://developer.hihonor.com/repo. Cloud side is read-only.",
    ),
    "samsung": Source(
        "samsung", "Samsung Health Data SDK",
        ("https://developer.samsung.com/health/data/api-reference",),
        "26 data types.",
    ),
    "fitbit": Source(
        "fitbit", "Fitbit Web API",
        ("https://dev.fitbit.com/build/reference/web-api/",),
        "Per-endpoint response tables; Fitbit publishes no OpenAPI document.",
    ),
    "whoop": Source(
        "whoop", "WHOOP API v2",
        ("https://api.prod.whoop.com/developer/doc/openapi.json",),
        "The official OpenAPI document.",
    ),
    "oura": Source(
        "oura", "Oura API v2",
        ("https://cloud.ouraring.com/v2/static/json/openapi-1.37.json",),
        "The official OpenAPI document, version 1.37.",
    ),
    "garmin": Source(
        "garmin", "Garmin Health API",
        ("https://developer.garmin.com/gc-developer-program/health-api/",),
        "Only the metric families are public; field-level schemas need developer approval.",
    ),
    "vivo": Source(
        "vivo", "vivo BlueOS health API",
        (
            "https://developers-watch.vivo.com.cn/api/health/health/",
            "https://developers.vivo.com/doc/d/4ea8ba1ec4cd44bd8bdaca9f3fecf795",
        ),
        "17 on-watch DATA_TYPES, plus the cloud interface that exposes workout records only.",
    ),
    "xiaomi": Source(
        "xiaomi", "Xiaomi Health cloud",
        ("https://dev.mi.com/",),
        "Public data types under the com.xiaomi.micloud.fit.* namespace; a clone of the Google Fit naming.",
    ),
    "zepp": Source(
        "zepp", "Zepp OS Sensor module",
        ("https://docs.huami.com/",),
        "On-device sensor classes, plus the shared-data list Zepp publishes through Xiaomi's third-party notice.",
    ),
    "oppo": Source(
        "oppo", "OPPO theme engine health data",
        (
            "https://open.oppomobile.com/documentation/page/info?id=13624",
            "https://open.oppomobile.com/wiki/new-doc/index.json",
        ),
        "Four values exposed to watch-face scripts; OPPO publishes no typed health data API.",
    ),
}

VENDORS: tuple[str, ...] = tuple(SOURCES)

#: The nine reasons a metric has no LOINC code, in `unmappable.tsv`.
REASONS: tuple[str, ...] = (
    "ALGO_MISMATCH",
    "VENDOR_SCORE",
    "NO_CONCEPT",
    "UNIT_INCOMPARABLE",
    "RELATIVE",
    "NOT_MEASUREMENT",
    "QUALITY_META",
    "NOT_PERSON",
    "OUT_OF_SCOPE",
)


@dataclass(frozen=True)
class DeviceCode:
    """One row of the base table: a LOINC code and every vendor field that means it."""

    loinc: str
    name: str
    axes: str
    metric: str
    confidence: str
    fields: dict[str, str]
    note: str = ""


@dataclass(frozen=True)
class VendorField:
    """One vendor field: its catalogue metric and its code, or why it has none."""

    vendor: str
    type: str
    field: str
    metric: str
    loinc: str
    confidence: str
    note: str = ""

    @property
    def key(self) -> str:
        return f"{self.type}:{self.field}" if self.field else self.type


@dataclass(frozen=True)
class Unmappable:
    reason: str
    metric: str
    vendors: str
    detail: str
    disposition: str


def _rows(name: str) -> list[dict[str, str]]:
    text = resources.files("mirobody").joinpath("res", "crosswalks", name).read_text(encoding="utf-8")
    body = "".join(line for line in io.StringIO(text) if not line.startswith("#"))
    return [{k: (v or "").strip() for k, v in row.items()} for row in csv.DictReader(io.StringIO(body), delimiter="\t")]


@lru_cache(maxsize=1)
def base_table() -> tuple[DeviceCode, ...]:
    """Every coded device quantity, one row per LOINC code."""
    out = []
    for r in _rows("loinc_device_base.tsv"):
        fields = {v: r.get(v, "") for v in VENDORS if r.get(v, "") not in ("", "-")}
        out.append(DeviceCode(r["loinc"], r["name"], r["axes"], r["metric"], r["confidence"], fields, r.get("note", "")))
    return tuple(out)


def by_code(loinc: str) -> DeviceCode | None:
    for row in base_table():
        if row.loinc == loinc:
            return row
    return None


def codes_for_metric(metric: str) -> tuple[DeviceCode, ...]:
    """The base-table rows naming a catalogue metric; usually one, two when
    the code depends on the measurement site."""
    return tuple(row for row in base_table() if row.metric == metric)


@lru_cache(maxsize=32)
def vendor_fields(vendor: str) -> tuple[VendorField, ...]:
    """Every field read for one vendor, coded or declined."""
    if vendor not in SOURCES:
        raise KeyError(f"unknown vendor {vendor!r}; one of {', '.join(VENDORS)}")
    return tuple(
        VendorField(vendor, r["type"], r.get("field", ""), r["metric"], r["loinc"], r["confidence"], r.get("note", ""))
        for r in _rows(f"{vendor}.tsv")
    )


def lookup(vendor: str, type: str, field: str = "") -> VendorField | None:
    """The row for one vendor field, or `None` when the field was not read."""
    for row in vendor_fields(vendor):
        if row.type == type and row.field == field:
            return row
    return None


@lru_cache(maxsize=1)
def unmappable() -> tuple[Unmappable, ...]:
    return tuple(
        Unmappable(r["reason"], r["metric"], r["vendors"], r["detail"], r["disposition"])
        for r in _rows("unmappable.tsv")
    )


def summary(vendors: Iterable[str] = VENDORS) -> dict[str, dict[str, int]]:
    """Per vendor: fields read, fields with a code, and confident codes."""
    out: dict[str, dict[str, int]] = {}
    for vendor in vendors:
        rows = vendor_fields(vendor)
        out[vendor] = {
            "fields": len(rows),
            "coded": sum(1 for r in rows if r.loinc),
            "confident": sum(1 for r in rows if r.confidence == CONFIDENT),
        }
    return out


__all__ = [
    "CONFIDENCES",
    "CONFIDENT",
    "LOINC_RELEASE",
    "REASONS",
    "RETRIEVED",
    "SOURCES",
    "UNVERIFIED",
    "VENDORS",
    "DeviceCode",
    "Source",
    "Unmappable",
    "VendorField",
    "base_table",
    "by_code",
    "codes_for_metric",
    "lookup",
    "summary",
    "unmappable",
    "vendor_fields",
]
