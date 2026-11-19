"""Regression tests for reading storage-unit series in their LOINC unit (#296).

Device decoders and crosswalks store sleep durations in milliseconds (`ms`), the
unit `res/catalog/metrics.tsv` declares for `dailyTotalSleepTime`,
`dailySleepDuration` and the sleep-stage metrics. Left in `ms` on the read side,
`24,533,700 ms` reached a 2B local model as `unit=ms` and was read as 24.5 hours.

These tests exercise the full pure pipeline from real vendor decoder payloads,
legacy device rows and mixed lab panels through `observations.prepare` +
`observations.coding_for`, `collect.query`'s unit selection and row builders,
`kernel.query.trend_rows` + `presentable`, `_render.render_compact`, and
`citations_auto.attach_citations`.
"""

from __future__ import annotations

import json
import statistics
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from mirobody import standardize, units
from mirobody.agent.chat.citations_auto import attach_citations, rows_from_result
from mirobody.agent.tools._render import render_compact, render_rest
from mirobody.collect import observations
from mirobody.collect.query import (
    SeriesUnit,
    _bucket_row,
    _catalog_row,
    _latest_row,
    _like,
    _ranges_differ,
    _reading_row,
    _record_row,
    _series_unit,
    _stats_row,
    export_row,
)
from mirobody.kernel import metrics, query, tools
from mirobody.kernel.decoders import garmin
from mirobody.utils.req_ctx import set_req_ctx

GARMIN_SAMPLES = Path(garmin.__file__).resolve().parent / "samples" / "garmin"
NOW = datetime(2026, 10, 1, tzinfo=UTC)


def _store_drafts(
    drafts: list[observations.Draft],
    provenance: observations.Provenance,
    *,
    user_tz: str = "UTC",
    start_id: int = 1,
) -> list[dict[str, Any]]:
    """Run the real write-side preparation and coding over `drafts`."""
    out: list[dict[str, Any]] = []
    for offset, draft in enumerate(drafts):
        prepared = observations.prepare(draft, provenance, "1", user_tz, now=NOW)
        coding = observations.coding_for(prepared, {})
        dt_local = prepared["observed_start"].astimezone(standardize.zone_for(prepared["tz"]))
        out.append({
            **prepared,
            "id": start_id + offset,
            "outcome": coding.outcome,
            "series_id": coding.series_id,
            "code_system": coding.code_system,
            "code": coding.code,
            "display": coding.display,
            "reason": coding.reason,
            "value_canonical": coding.value_canonical,
            "unit_canonical": coding.unit_canonical,
            "elected": False,
            "reported": prepared["kind"] in ("symptom", "condition", "note"),
            "period_start": prepared["observed_start"],
            "local_time": dt_local.strftime("%Y-%m-%d %H:%M:%S"),
            "file_key": prepared["source_ref"][9:] if prepared["source_kind"] == "file" else "",
            "file_name": "",
        })
    return out


class _MemoryHealthQuery:
    """In-memory `query.HealthQuery` mirroring `PostgresHealthQuery`'s exact
    unit selection (`_series_unit`), `_VALUE_STD` conversion, and row builders."""

    def __init__(self, rows: list[dict[str, Any]], *, tz_name: str = "UTC") -> None:
        self._rows = list(rows)
        self._tz = tz_name

    def tz(self, subject_id: str) -> str:
        return self._tz

    def on_read(self, subject_id: str) -> None:
        return None

    def series_units(self) -> dict[str, SeriesUnit]:
        by_series_ucum: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for r in self._rows:
            if r.get("value_num") is not None and r.get("unit_ucum"):
                by_series_ucum.setdefault((r["series_id"], r["unit_ucum"]), []).append(r)
        grouped: dict[str, list[dict[str, Any]]] = {}
        for (sid, ucum), bucket in by_series_ucum.items():
            ordered = sorted(bucket, key=lambda x: (x["observed_start"], x["id"]), reverse=True)
            loinc_rows = [x for x in ordered if x.get("code_system") == standardize.LOINC_SYSTEM]
            bases = [x["unit_canonical"] for x in ordered if x.get("unit_canonical")]
            grouped.setdefault(sid, []).append({
                "series_id": sid,
                "unit_ucum": ucum,
                "base": max(bases) if bases else None,
                "n": len(ordered),
                "latest": ordered[0]["observed_start"],
                "label": ordered[0].get("unit_text") or "",
                "code_system": ordered[0].get("code_system") or "",
                "code": ordered[0].get("code") or "",
                "display": ordered[0].get("display") or "",
                "loinc": loinc_rows[0]["code"] if loinc_rows else None,
                "device_only": all(x.get("modality") == observations.MODALITY_DEVICE for x in ordered),
            })
        return {sid: _series_unit(groups) for sid, groups in grouped.items()}

    @staticmethod
    def _value_std(r: dict[str, Any], su: SeriesUnit | None) -> float | None:
        val = r.get("value_num")
        if val is None:
            return None
        ucum = r.get("unit_ucum") or ""
        if su is None or ucum in ("", su.ucum):
            return float(val)
        if su.factor is not None and r.get("unit_canonical") == su.base and r.get("value_canonical") is not None:
            return float(r["value_canonical"]) / su.factor
        return None

    def _select(self, sel: query.Selection, window: query.Window | None) -> list[dict[str, Any]]:
        targets = {t.lower() for t in (*sel.indicators, *sel.keywords) if t}
        out: list[dict[str, Any]] = []
        for r in self._rows:
            if window and window.start and str(r["local_date"]) < window.start:
                continue
            if window and window.end and str(r["local_date"]) > window.end:
                continue
            if targets:
                candidates = {
                    str(r.get("series_id") or "").lower(),
                    str(r.get("code") or "").lower(),
                    str(r.get("display") or "").lower(),
                    str(r.get("name_text") or "").lower(),
                }
                if not (targets & candidates):
                    continue
            out.append(r)
        return out

    def catalog(self, subject_id: str, window: query.Window | None) -> list[dict[str, Any]]:
        unit_of = self.series_units()
        by_sid: dict[str, list[dict[str, Any]]] = {}
        for r in self._select(query.Selection(), window):
            by_sid.setdefault(r["series_id"], []).append(r)
        out: list[dict[str, Any]] = []
        for sid, group in sorted(by_sid.items()):
            ordered = sorted(group, key=lambda x: (x["observed_start"], x["id"]), reverse=True)
            top = ordered[0]
            dates = [str(x["local_date"]) for x in ordered]
            out.append(_catalog_row({
                "series_id": sid,
                "standard": not sid.startswith("local:"),
                "code_system": top["code_system"],
                "code": top["code"],
                "display": top["display"] or top["name_text"],
                "name": top["name_text"],
                "kind": top["kind"],
                "count": len(ordered),
                "total": len(by_sid),
                "first_date": min(dates),
                "last_date": max(dates),
                "latest_value": top["value_text"],
                "unit": top["unit_text"],
                "latest_ucum": top["unit_ucum"],
                "latest_num": top["value_num"],
                "latest_comparator": top["comparator"],
                "latest_canonical": top["value_canonical"],
                "latest_base": top["unit_canonical"],
                "reason": top["reason"],
                "unverified": False,
                "reported": top["reported"],
            }, unit_of.get(sid)))
        return out

    def readings(
        self, subject_id: str, sel: query.Selection, window: query.Window, *, limit: int = 50
    ) -> list[dict[str, Any]]:
        unit_of = self.series_units()
        matched = sorted(self._select(sel, window), key=lambda x: (x["observed_start"], x["id"]), reverse=True)
        totals: dict[str, int] = {}
        for r in matched:
            totals[r["series_id"]] = totals.get(r["series_id"], 0) + 1
        seen: dict[str, int] = {}
        out: list[dict[str, Any]] = []
        for r in matched:
            sid = r["series_id"]
            if seen.get(sid, 0) >= limit:
                continue
            seen[sid] = seen.get(sid, 0) + 1
            out.append(_reading_row({**r, "local_date": str(r["local_date"]), "total": totals[sid]}, unit_of.get(sid)))
        return out

    def latest(self, subject_id: str, sel: query.Selection, window: query.Window) -> list[dict[str, Any]]:
        unit_of = self.series_units()
        matched = sorted(
            self._select(sel, window),
            key=lambda x: (x["local_date"], x["elected"], x["observed_start"], x["id"]),
            reverse=True,
        )
        seen: set[str] = set()
        out: list[dict[str, Any]] = []
        for r in matched:
            if r["series_id"] in seen:
                continue
            seen.add(r["series_id"])
            out.append(_latest_row({**r, "local_date": str(r["local_date"]), "total": 1}, unit_of.get(r["series_id"])))
        return out

    def buckets(
        self, subject_id: str, sel: query.Selection, window: query.Window, *, resolution: str, limit: int = 400
    ) -> list[dict[str, Any]]:
        assert resolution == "day"
        unit_of = self.series_units()
        by_day: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for r in self._select(sel, window):
            by_day.setdefault((r["series_id"], str(r["local_date"])), []).append(r)
        per_series_days: dict[str, int] = {}
        for sid, _day in by_day:
            per_series_days[sid] = per_series_days.get(sid, 0) + 1
        out: list[dict[str, Any]] = []
        for (sid, day_str), group in sorted(by_day.items(), key=lambda kv: (kv[0][0], kv[0][1]), reverse=True):
            su = unit_of.get(sid)
            ordered = sorted(group, key=lambda x: (x["elected"], x["observed_start"], x["id"]), reverse=True)
            top = ordered[0]
            vstd = self._value_std(top, su)
            if vstd is None:
                continue
            out.append(_bucket_row({
                "series_id": sid,
                "display": top["display"] or top["name_text"],
                "code_system": top["code_system"],
                "code": top["code"],
                "period": day_str,
                "avg": vstd,
                "min": vstd,
                "max": vstd,
                "n": 1,
                "unit": top["unit_text"],
                "total": per_series_days[sid],
                "elected": top["elected"],
                "unverified": False,
                "reported": top["reported"],
            }, su))
        return out[:limit]

    def ranges_differ(self, subject_id: str, sel: query.Selection, window: query.Window) -> dict[str, bool]:
        """`PostgresHealthQuery.ranges_differ` over these rows: the same
        `low|high|unit|code|system` variants its SQL aggregates, judged by
        the same `_ranges_differ`."""
        unit_of = self.series_units()
        variants: dict[str, set[str]] = {}
        for r in self._select(sel, window):
            if r.get("ref_low") is None and r.get("ref_high") is None:
                continue
            low = "" if r.get("ref_low") is None else str(r["ref_low"])
            high = "" if r.get("ref_high") is None else str(r["ref_high"])
            variants.setdefault(r["series_id"], set()).add(
                f"{low}|{high}|{r.get('unit_ucum') or ''}|{r.get('code') or ''}|{r.get('code_system') or ''}")
        return {sid: _ranges_differ(sorted(v), unit_of.get(sid)) for sid, v in variants.items()}

    def stats(
        self,
        subject_id: str,
        sel: query.Selection,
        window: query.Window,
        *,
        value_min: float | None = None,
        value_max: float | None = None,
    ) -> list[dict[str, Any]]:
        unit_of = self.series_units()
        by_sid: dict[str, list[dict[str, Any]]] = {}
        for r in self._select(sel, window):
            by_sid.setdefault(r["series_id"], []).append(r)
        out: list[dict[str, Any]] = []
        for sid, group in sorted(by_sid.items()):
            su = unit_of.get(sid)
            asc = sorted(group, key=lambda x: (x["observed_start"], x["id"]))
            desc = list(reversed(asc))
            vals = [v for r in asc if (v := self._value_std(r, su)) is not None]
            nums = [r for r in asc if r.get("value_num") is not None]
            banded = None
            banded_days = None
            if value_min is not None or value_max is not None:
                in_band = [
                    r for r in asc
                    if (v := self._value_std(r, su)) is not None
                    and (value_min is None or v >= value_min)
                    and (value_max is None or v <= value_max)
                ]
                banded = len(in_band)
                banded_days = len({str(r["local_date"]) for r in in_band})
            first_row, last_row = asc[0], desc[0]
            out.append(_stats_row({
                "series_id": sid,
                "display": last_row["display"] or last_row["name_text"],
                "code_system": last_row["code_system"],
                "code": last_row["code"],
                "count": len(asc),
                "numeric_count": len(nums),
                "unconverted": len(nums) - len(vals),
                "count_in_range": banded,
                "days": len({str(r["local_date"]) for r in asc}),
                "days_in_range": banded_days,
                "min": min(vals) if vals else None,
                "max": max(vals) if vals else None,
                "avg": statistics.fmean(vals) if vals else None,
                "median": statistics.median(vals) if vals else None,
                "stddev": statistics.stdev(vals) if len(vals) >= 2 else None,
                "sum": sum(vals) if vals else None,
                "first": first_row["value_text"],
                "first_std": self._value_std(first_row, su),
                "first_ucum": first_row["unit_ucum"],
                "first_date": str(first_row["local_date"]),
                "last": last_row["value_text"],
                "last_std": self._value_std(last_row, su),
                "last_ucum": last_row["unit_ucum"],
                "last_date": str(last_row["local_date"]),
                "unit": last_row["unit_text"],
                "mixed_units": len({r["unit_ucum"] for r in asc if r.get("unit_ucum")}) > 1,
                "unverified": False,
                "reported": last_row["reported"],
            }, su))
        return out

    def record_rows(self) -> list[dict[str, Any]]:
        unit_of = self.series_units()
        ordered = sorted(self._rows, key=lambda x: (x["observed_start"], x["id"]), reverse=True)
        return [_record_row({**r, "local_date": str(r["local_date"]), "total": len(ordered)},
                            unit_of.get(r["series_id"])) for r in ordered]


def _sixty_nights_of_device_sleep() -> list[dict[str, Any]]:
    """60 nights of `dailyTotalSleepTime` in `ms` matching #296's reproduction:
    first-third mean 25,192,100 ms, last-third mean 24,162,200 ms, overall mean
    24,533,700 ms, min 21,952,000 ms, max 27,839,500 ms, last day (2026-09-14)
    23,256,000 ms (6.46 h)."""
    first_20 = [27_839_500, 22_544_700, *([25_192_100] * 18)]
    middle_20 = [21_952_000, 26_541_600, *([24_246_800] * 18)]
    last_20 = [*([24_162_200] * 18), 25_068_400, 23_256_000]
    values_ms = first_20 + middle_20 + last_20
    assert len(values_ms) == 60
    assert statistics.fmean(values_ms) == 24_533_700.0
    start_day = date(2026, 7, 17)
    drafts = [
        observations.Draft(
            name_text="dailyTotalSleepTime",
            value_text=str(ms),
            unit_text="ms",
            observed_start=datetime.combine(start_day + timedelta(days=i), datetime.min.time(), tzinfo=UTC)
            + timedelta(hours=7),
        )
        for i, ms in enumerate(values_ms)
    ]
    prov = observations.Provenance("device-sensed", "device", "device:garmin", vendor="garmin")
    return _store_drafts(drafts, prov)


def test_sixty_nights_of_device_sleep_reach_the_model_in_hours_and_cite_cleanly():
    """Exact reproduction of #296: 60 nights stored in `ms` reach every view,
    compact tool rendering, REST payload, export row, and auto-citation pass in
    LOINC's `h`, never `24533700 ms`."""
    store = _MemoryHealthQuery(_sixty_nights_of_device_sleep())
    sel = query.Selection(indicators=("93832-4",))
    win = query.resolve_window("UTC", start="2026-07-17", end="2026-09-14", hours=24, now=NOW)

    with set_req_ctx({"language": "zh-CN"}):
        # 1. Trend view: hours instead of eight-digit milliseconds.
        daily = store.buckets("1", sel, win, resolution="day", limit=400)
        [trend] = query.presentable(query.trend_rows(daily))
        assert (trend["code"], trend["unit"], trend["n"]) == ("93832-4", "h", 60)
        assert trend["first_third_mean"] == pytest.approx(25_192_100 / 3_600_000, rel=1e-5)
        assert trend["last_third_mean"] == pytest.approx(24_162_200 / 3_600_000, rel=1e-5)
        assert trend["min"] == pytest.approx(21_952_000 / 3_600_000, rel=1e-5)
        assert trend["max"] == pytest.approx(27_839_500 / 3_600_000, rel=1e-5)

        trend_env = tools.Envelope(
            status="ok",
            data=[{**trend, "rid": "r1"}],
            meta=tools.Meta(view="trend", row_count=1),
        )
        trend_text = render_compact(trend_env)
        assert "unit=h" in trend_text
        assert "unit=ms" not in trend_text
        assert "25192100" not in trend_text and "24162200" not in trend_text

        # 2. Stats view (and value_min band in hours): 6.81492 h average, 6.4600 h last night.
        [stats] = query.presentable(store.stats("1", sel, win, value_min=7.0))
        assert (stats["code"], stats["unit"], stats["count"], stats["days"]) == ("93832-4", "h", 60, 60)
        assert stats["avg"] == pytest.approx(24_533_700 / 3_600_000, rel=1e-5)
        assert float(stats["last"]) == pytest.approx(6.46)
        assert stats["last_date"] == "2026-09-14"
        # Only night 0 (27,839,500 ms = 7.733 h) and night 21 (26,541,600 ms = 7.373 h) are >= 7.0 h.
        assert (stats["count_in_range"], stats["days_in_range"]) == (2, 2)
        assert stats["mixed_units"] is False

        stats_env = tools.Envelope(
            status="ok",
            data=[{**stats, "rid": "r2"}],
            meta=tools.Meta(view="stats", row_count=1),
        )
        stats_text = render_compact(stats_env)
        assert "unit=h" in stats_text and "6.81492" in stats_text
        assert "24533700" not in stats_text and "unit=ms" not in stats_text

        # 3. Auto-citations match the model's hours answer in Chinese (`睡眠持续时间`).
        cited_zh, n_zh, _ = attach_citations("过去两个月平均睡眠持续时间 6.81 小时。", rows_from_result(stats_text))
        assert n_zh == 1 and "小时<cite>[r2]</cite>" in cited_zh

    with set_req_ctx({"language": "en"}):
        daily_en = store.buckets("1", sel, win, resolution="day", limit=400)
        [trend_en] = query.presentable(query.trend_rows(daily_en))
        trend_en_text = render_compact(
            tools.Envelope(status="ok", data=[{**trend_en, "rid": "r1"}], meta=tools.Meta(view="trend", row_count=1))
        )
        cited_en, n_en, _ = attach_citations(
            "Sleep duration fell from 7.00 h to 6.71 h.", rows_from_result(trend_en_text)
        )
        assert n_en == 2 and cited_en.count("h<cite>[r1]</cite>") == 2

        # 4. Raw, latest, catalog, web REST, records and CSV export agree on `h`,
        #    while keeping the device's verbatim `ms` value in `printed` / `printed_*`.
        [latest] = store.latest("1", sel, win)
        assert latest["unit"] == "h" and float(latest["value"]) == pytest.approx(6.46)
        assert latest["converted"] is True
        assert (latest["printed"]["value"], latest["printed"]["unit"]) == ("23256000", "ms")

        latest_compact = render_compact(
            tools.Envelope(status="ok", data=[latest], meta=tools.Meta(view="latest", row_count=1))
        )
        assert "23256000" not in latest_compact and "unit=h" in latest_compact

        rest_payload = render_rest(
            tools.Envelope(status="ok", data=[latest], meta=tools.Meta(view="latest", row_count=1))
        )
        assert rest_payload["rows"][0]["unit"] == "h"
        assert rest_payload["rows"][0]["printed"]["value"] == "23256000"

        [cat] = store.catalog("1", win)
        assert (cat["code"], cat["unit"]) == ("93832-4", "h")
        assert float(cat["latest_value"]) == pytest.approx(6.46)

        rec = store.record_rows()[0]
        assert rec["unit"] == "h" and float(rec["value"]) == pytest.approx(6.46)
        exported = export_row(rec)
        assert (exported["unit"], exported["printed_value"], exported["printed_unit"]) == ("h", "23256000", "ms")
        assert float(exported["value"]) == pytest.approx(6.46)


def test_garmin_sleep_export_converts_every_coded_duration_to_its_loinc_unit():
    """Real Garmin `sleeps.json` payload decoded through `garmin.decode`:
    sleep duration and duration in bed answer in `h`; deep, light and REM stage
    durations answer in `min`; sleep efficiency stays `%`; uncoded awake time
    stays `ms`."""
    payload = json.loads((GARMIN_SAMPLES / "sleeps.json").read_text(encoding="utf-8"))
    facts = garmin.decode(payload["data_type"], payload["input"], payload["tz"])
    drafts = [
        observations.Draft(
            name_text=f.metric_key,
            value_text=str(f.value_num),
            unit_text=f.unit,
            tz=payload["tz"],
            observed_start=datetime.fromtimestamp(f.effective_start_ms / 1000, tz=UTC),
            observed_end=(
                datetime.fromtimestamp(f.effective_end_ms / 1000, tz=UTC) if f.effective_end_ms else None
            ),
        )
        for f in facts
        if f.value_num is not None
    ]
    prov = observations.Provenance("device-sensed", "device", "device:garmin", vendor="garmin")
    store = _MemoryHealthQuery(_store_drafts(drafts, prov, user_tz=payload["tz"]), tz_name=payload["tz"])
    win = query.Window(0, 0, payload["tz"])

    by_name: dict[str, dict[str, Any]] = {}
    for metric_name in (
        "dailyTotalSleepTime",
        "dailySleepDuration",
        "dailyDeepSleep",
        "dailyLightSleep",
        "dailyRemSleep",
        "dailySleepEfficiency",
        "dailyAwakeTime",
    ):
        [row] = query.presentable(store.stats("1", query.Selection(indicators=(metric_name,)), win))
        by_name[metric_name] = row

    # 25,200,000 ms asleep -> 7.0 h; 27,000,000 ms in bed -> 7.5 h (never 27 h).
    assert (by_name["dailyTotalSleepTime"]["code"], by_name["dailyTotalSleepTime"]["unit"]) == ("93832-4", "h")
    assert by_name["dailyTotalSleepTime"]["avg"] == pytest.approx(7.0)
    assert by_name["dailyTotalSleepTime"]["last"] == "7.00"

    assert (by_name["dailySleepDuration"]["code"], by_name["dailySleepDuration"]["unit"]) == ("103213-5", "h")
    assert by_name["dailySleepDuration"]["avg"] == pytest.approx(7.5)
    assert by_name["dailySleepDuration"]["last"] == "7.50"

    # Stage summaries: 7,200,000 ms deep (120 min), 12,600,000 ms light (210 min),
    # 5,400,000 ms REM (90 min), plus stage interval spans from `sleepLevelsMap`.
    assert (by_name["dailyDeepSleep"]["code"], by_name["dailyDeepSleep"]["unit"]) == ("93831-6", "min")
    assert by_name["dailyDeepSleep"]["avg"] == pytest.approx(120.0)
    assert by_name["dailyDeepSleep"]["last"] == "120"

    assert (by_name["dailyLightSleep"]["code"], by_name["dailyLightSleep"]["unit"]) == ("93830-8", "min")
    assert by_name["dailyLightSleep"]["avg"] == pytest.approx(210.0)
    assert by_name["dailyLightSleep"]["last"] == "210"

    assert (by_name["dailyRemSleep"]["code"], by_name["dailyRemSleep"]["unit"]) == ("93829-0", "min")
    assert by_name["dailyRemSleep"]["avg"] == pytest.approx(90.0)
    assert by_name["dailyRemSleep"]["last"] == "90.0"

    assert by_name["dailySleepEfficiency"]["unit"] == "%"
    assert by_name["dailySleepEfficiency"]["avg"] == pytest.approx(93.3333, rel=1e-4)

    # `dailyAwakeTime` is in `mirobody-device` (103210-1 vs 103215-0 is unverified
    # in the crosswalk), so its unit stays what the device wrote.
    assert by_name["dailyAwakeTime"]["series"] == "mirobody-device:dailyAwakeTime"
    assert by_name["dailyAwakeTime"]["unit"] == "ms"
    assert by_name["dailyAwakeTime"]["avg"] == pytest.approx(1_800_000.0)


def test_legacy_device_rows_and_mixed_demo_hours_unify_under_hours_label():
    """When a person has both device rows in `ms` (via `legacy_draft`) and demo
    or manual rows printed in `"hours"`, the series keeps the printed `"hours"`
    label, leaves `"hours"` rows untouched, and converts `ms` rows into hours."""
    legacy_rows = [
        {
            "user_id": "1",
            "indicator": "dailyTotalSleepTime",
            "value": str(25_200_000 + d * 360_000),
            "unit": "ms",
            "start_time": datetime(2026, 8, d + 1, 7, tzinfo=UTC),
            "end_time": datetime(2026, 8, d + 1, 7, tzinfo=UTC),
            "source": "oura",
            "source_table": "th_series_data",
            "timezone": "UTC",
        }
        for d in range(10)
    ]
    device_drafts = [observations.legacy_draft(r) for r in legacy_rows]
    device_prov = observations.legacy_provenance(legacy_rows[0])
    stored = _store_drafts(device_drafts, device_prov)

    demo_drafts = [
        observations.Draft(
            name_text="dailyTotalSleepTime",
            value_text="7.2",
            unit_text="hours",
            observed_start=datetime(2026, 8, 20, 7, tzinfo=UTC),
        ),
        observations.Draft(
            name_text="dailyTotalSleepTime",
            value_text="6.5",
            unit_text="hours",
            observed_start=datetime(2026, 8, 21, 7, tzinfo=UTC),
        ),
    ]
    demo_prov = observations.Provenance("device-sensed", "device", "demo:seed", vendor="demo")
    stored += _store_drafts(demo_drafts, demo_prov, start_id=100)

    store = _MemoryHealthQuery(stored)
    sel = query.Selection(indicators=("93832-4",))
    win = query.Window(0, 0, "UTC")

    [stats] = store.stats("1", sel, win)
    assert (stats["unit"], stats["mixed_units"], stats["count"]) == ("hours", True, 12)
    assert stats["last"] == "6.5"
    assert stats["first"] == "7.00"

    readings = store.readings("1", sel, win, limit=20)
    newest_demo = readings[0]
    oldest_device = readings[-1]
    assert (newest_demo["value"], newest_demo["unit"]) == ("6.5", "hours")
    assert "converted" not in newest_demo
    assert (oldest_device["value"], oldest_device["unit"], oldest_device["converted"]) == ("7.00", "hours", True)
    assert (oldest_device["printed"]["value"], oldest_device["printed"]["unit"]) == ("25200000", "ms")


@pytest.mark.parametrize(
    ("name", "source_kind", "vendor", "value_text", "unit_text", "expected_unit", "expected_avg"),
    [
        # Storage units on codes whose LOINC unit is human-scaled: convert.
        ("dailyTotalSleepTime", "device", "garmin", "24533.7", "s", "h", 24533.7 / 3600),
        ("dailySleepLatency", "device", "oura", "600000", "ms", "min", 10.0),
        ("waso", "device", "oura", "1800000", "ms", "min", 30.0),
        ("activeCalories", "device", "garmin", "418400", "J", "kcal", 100.0),
        # Clinical codes whose own LOINC `EXAMPLE_UCUM_UNITS` IS `ms` or `s`: keep!
        ("hrvSDNN", "device", "apple", "42", "ms", "ms", 42.0),
        ("coagulationPpp", "file", None, "31.2", "s", "s", 31.2),
        # Human-chosen units that differ from LOINC's example (`min` on 93832-4,
        # `kJ` on 41981-2, `m` on 103208-5): keep what the person/report printed!
        ("dailyTotalSleepTime", "device", "fitbit", "420", "min", "min", 420.0),
        ("activeCalories", "device", "whoop", "1500", "kJ", "kJ", 1500.0),
        ("dailyDistance", "device", "garmin", "5200", "m", "m", 5200.0),
        # Uncoded / device-namespace durations: keep `ms`.
        ("activeDuration", "device", "garmin", "900000", "ms", "ms", 900000.0),
        # A unit a report printed is the person's, not a storage convenience,
        # even where LOINC's example differs: an ECG prints QT and PR in `ms`
        # (8634-8 and 8625-6 declare `s`), a lab may print bleeding time in `s`
        # (11067-6 declares `min`). Before the device-only gate these answered
        # 0.400 s and 2.50 min.
        ("QT间期", "file", None, "400", "ms", "ms", 400.0),
        ("PR间期", "file", None, "160", "ms", "ms", 160.0),
        ("出血时间", "file", None, "150", "s", "s", 150.0),
    ],
)
def test_storage_units_convert_only_when_loinc_declares_a_human_unit(
    name: str,
    source_kind: str,
    vendor: str | None,
    value_text: str,
    unit_text: str,
    expected_unit: str,
    expected_avg: float,
):
    modality = "device-sensed" if source_kind == "device" else "lab-report"
    ref = f"device:{vendor}" if source_kind == "device" else "th_files:lab-1"
    prov = observations.Provenance(modality, source_kind, ref, vendor=vendor)
    draft = observations.Draft(
        name_text=name,
        value_text=value_text,
        unit_text=unit_text,
        observed_start=datetime(2026, 9, 1, 8, tzinfo=UTC),
    )
    store = _MemoryHealthQuery(_store_drafts([draft], prov))
    [stats] = store.stats("1", query.Selection(), query.Window(0, 0, "UTC"))
    assert stats["unit"] == expected_unit
    assert stats["avg"] == pytest.approx(expected_avg, rel=1e-4)


def test_every_loinc_coded_catalogue_metric_in_storage_unit_has_a_clean_display_unit():
    """Catalogue-wide invariant (#296 open question 2): inspect every metric in
    `res/catalog/metrics.tsv` whose `unit_ucum` is a raw storage unit (`ms`,
    `s`, `J`) and whose code is standard LOINC. Clinical codes that genuinely
    read in `ms` (`112429-6` HRV SDNN) or `s` (`14979-9` aPTT) stay in that
    unit; every sleep duration and stage metric resolves to `h` or `min`."""
    found: dict[str, str] = {}
    for row in metrics.ROWS:
        system, code = row.canonical
        if system != metrics.SYSTEM_LOINC or not code or row.unit_ucum not in {"ms", "s", "J"}:
            continue
        draft = observations.Draft(
            name_text=row.name,
            value_text="3600000" if row.unit_ucum == "ms" else "30",
            unit_text=row.unit_ucum,
            observed_start=datetime(2026, 9, 1, 8, tzinfo=UTC),
        )
        prov = observations.Provenance("device-sensed", "device", "device:audit", vendor="garmin")
        store = _MemoryHealthQuery(_store_drafts([draft], prov))
        [stats] = store.stats("1", query.Selection(), query.Window(0, 0, "UTC"))
        found[row.name] = stats["unit"]

    assert found == {
        "activatedClottingTimeBld": "s",
        "activatedClottingTimePpp": "s",
        "coagulationBld": "s",
        "coagulationBldcrrt": "s",
        "coagulationPpp": "s",
        "dailyDeepSleep": "min",
        "dailyLightSleep": "min",
        "dailyRemSleep": "min",
        "dailySleepDuration": "h",
        "dailySleepLatency": "min",
        "dailyTotalSleepTime": "h",
        "hrvSDNN": "ms",
        "sleepAnalysis_Asleep(Deep)": "min",
        "sleepAnalysis_Asleep(REM)": "min",
        "sleepAnalysis_Asleep(Unspecified)": "h",
        "sleepAnalysis_InBed": "h",
        "waso": "min",
    }


def test_like_preserves_lab_precision_without_padding_millisecond_zeros():
    """Trailing zeros after a decimal point (`4.90`) are significant; trailing
    scale zeros on a millisecond integer (`25200000`, `36000000`) are not."""
    assert _like(212 / 38.66976, "212") == "5.48"
    assert _like(4.90 * 38.66976, "4.90") == "189"
    assert _like(80 * 0.011312, "80") == "0.905"
    assert _like(200 / 38.66976, "<200") == "5.17"
    assert _like(7.0, "25200000", stored=True) == "7.00"
    assert _like(7.0, "25200000.0", stored=True) == "7.00"
    assert _like(10.0, "36000000", stored=True) == "10.0"
    assert _like(90.0, "5400000", stored=True) == "90.0"
    assert _like(120.0, "7200000", stored=True) == "120"
    assert _like(24_533_700 / 3_600_000, "24533700", stored=True) == "6.81492"


def test_like_keeps_a_printed_trailing_zero_outside_device_storage():
    """A printed ".0" is precision unless a decoder wrote it: a range printed
    "70.0-100.0 mg/dL", a weight "100.0 kg", or "1500.0" keep every digit when
    converted, as on main. Stripping it for every conversion was a regression
    the review of #298 measured: "100.0" became "100"."""
    assert _like(100.0, "100.0") == "100.0"
    assert _like(1.5, "1500.0") == "1.5000"
    assert _like(3.456, "100.0") == "3.456"
    assert _like(70.0 / 18.0, "70.0") == "3.89"
    assert _like(1.2, "1200") == "1.200"


def test_tool_schema_no_longer_tells_models_sleep_durations_are_in_ms():
    desc = query.TOOL_SCHEMA["properties"]["value_min"]["description"]
    assert "sleep durations are in ms" not in desc
    assert "call stats once without it to read that unit" in desc


def test_loinc_units_reads_bundle_and_repairs_sleep_nrat_defects():
    assert standardize.loinc_units("93832-4") == ("h",)
    assert standardize.loinc_units("93831-6") == ("min",)
    assert standardize.loinc_units("112429-6") == ("ms",)
    assert standardize.loinc_units("14979-9") == ("s",)
    assert standardize.loinc_units("41981-2") == ("kcal",)
    assert standardize.loinc_units("103213-5") == ("h",)
    assert standardize.loinc_units("103215-0") == ("min",)
    assert standardize.loinc_units("") == ()
    assert units.canonicalize(1.0, standardize.loinc_units("93832-4")[0]).unit == "s"


async def test_health_indicators_service_end_to_end_over_device_sleep():
    pytest.importorskip("dotenv")
    pytest.importorskip("ruamel.yaml")
    from mirobody.agent.tools.health_indicators_service import HealthIndicatorsService

    store = _MemoryHealthQuery(_sixty_nights_of_device_sleep())
    service = HealthIndicatorsService(store, outside_note=False)

    trend_env = await service.envelope(
        {"user_id": "1"}, indicators=["93832-4"], view="trend", start="2026-07-17", end="2026-09-14"
    )
    [trend] = trend_env.data
    assert (trend["unit"], trend["n"]) == ("h", 60)
    assert trend["first_third_mean"] == pytest.approx(6.99781, rel=1e-4)
    assert trend["last_third_mean"] == pytest.approx(6.71172, rel=1e-4)
    assert "unit=h" in render_compact(trend_env)

    stats_env = await service.envelope(
        {"user_id": "1"}, indicators=["93832-4"], view="stats", start="2026-07-17", end="2026-09-14", value_min=7.0
    )
    [stats] = stats_env.data
    assert (stats["unit"], stats["days_in_range"]) == ("h", 2)
    assert stats["avg"] == pytest.approx(6.81492, rel=1e-4)
