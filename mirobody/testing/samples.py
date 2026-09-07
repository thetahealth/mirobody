"""Run vendor sample payloads through a decoder and compare with the facts a
human worked out.

A sample document (``mirobody/kernel/vendors/samples/<vendor>/*.json``) is either
one case or a ``cases`` list; each case has ``data_type``, ``tz``, ``input``
and an ``expected`` block whose keys are checked here:

``count``          exact number of facts
``values``         ``{metric: value}`` — first fact per metric, approx equal
``series``         ``{metric: [values...]}`` in order
``intervals``      ``{metric: [[start_ms, end_ms], ...]}``
``absent``         metrics that must not appear
``start_ms``       ``{metric or metric[i]: ms}``
``window``         ``[start_ms, end_ms]`` every fact must span
``panel_shared``   metrics that must share one non-empty panel id

A consumer with its own decoder points :func:`run_samples` at its own sample
directory and its own ``decode``. Pure; stdlib only.
"""

from __future__ import annotations

import json
import math
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path

from ..kernel import metrics
from ..kernel.series import Fact

Decoder = Callable[..., Sequence[Fact]]


@dataclass(frozen=True)
class SampleFailure:
    sample: str
    message: str

    def __str__(self) -> str:
        return f"{self.sample}: {self.message}"


def load_cases(root: Path) -> list[tuple[str, str, Mapping]]:
    """``(vendor, case_id, case)`` for every case under ``root``."""
    out: list[tuple[str, str, Mapping]] = []
    for f in sorted(root.rglob("*.json")):
        doc = json.loads(f.read_text(encoding="utf-8"))
        cases = doc.get("cases") or [doc]
        for i, case in enumerate(cases):
            if "vendor" not in doc:
                raise ValueError(
                    f"{f}: a sample file needs a 'vendor' key naming the decoder it exercises. "
                    "A fixture that is not a decoder sample does not belong in this directory."
                )
            out.append((doc["vendor"], f"{doc.get('test_id', f.stem)}[{i}:{case['data_type']}]", case))
    return out


def _approx(a: float, b: float) -> bool:
    return math.isclose(a, b, rel_tol=1e-6, abs_tol=1e-6)


def check_case(
    vendor: str, case: Mapping, decode: Decoder, *, catalogue: Mapping[str, metrics.Metric] | None = None
) -> list[str]:
    """Every way the decoder's output disagrees with the case's expectations;
    empty means the case passes."""
    cat = metrics.METRICS if catalogue is None else catalogue
    kw = {"pulled_at_ms": case["pulled_at_ms"]} if "pulled_at_ms" in case else {}
    facts = list(decode(vendor, case["data_type"], case["input"], case["tz"], **kw))
    exp = case["expected"]
    problems: list[str] = []
    if len(facts) != exp["count"]:
        problems.append(f"count {len(facts)} != {exp['count']} ({[f.metric_key for f in facts]})")
    by: dict[str, list[Fact]] = {}
    for f in facts:
        by.setdefault(f.metric_key, []).append(f)
        m = cat.get(f.metric_key)
        if m is None:
            problems.append(f"{f.metric_key} is not a catalogue name")
        elif f.unit != m.standard_unit:
            problems.append(f"{f.metric_key} unit {f.unit!r} != catalogue {m.standard_unit!r}")
    for name, value in exp.get("values", {}).items():
        if name not in by:
            problems.append(f"missing {name}")
        elif by[name][0].value_num is None or not _approx(by[name][0].value_num, value):
            problems.append(f"{name} = {by[name][0].value_num}, expected {value}")
    for name, values in exp.get("series", {}).items():
        got = [f.value_num for f in by.get(name, [])]
        if got != values:
            problems.append(f"{name} series {got} != {values}")
    for name, spans in exp.get("intervals", {}).items():
        got = [[f.effective_start_ms, f.effective_end_ms] for f in by.get(name, [])]
        if got != spans:
            problems.append(f"{name} intervals {got} != {spans}")
    for name in exp.get("absent", []):
        if name in by:
            problems.append(f"{name} should be absent")
    for key, ms in exp.get("start_ms", {}).items():
        name, _, idx = key[:-1].partition("[") if "[" in key else (key, "", "0")
        try:
            got = by[name][int(idx or 0)].effective_start_ms
        except (KeyError, IndexError):
            problems.append(f"no fact for {key}")
            continue
        if got != ms:
            problems.append(f"{key} start {got} != {ms}")
    if "window" in exp:
        start, end = exp["window"]
        for f in facts:
            if (f.effective_start_ms, f.end_ms) != (start, end):
                problems.append(f"{f.metric_key} spans {(f.effective_start_ms, f.end_ms)} != {(start, end)}")
    if "panel_shared" in exp:
        panels = {by[n][0].panel_id for n in exp["panel_shared"] if n in by}
        if len(panels) != 1 or panels == {""}:
            problems.append(f"panel ids differ or are empty: {sorted(panels)}")
    return problems


def run_samples(
    root: Path, decode: Decoder, *, catalogue: Mapping[str, metrics.Metric] | None = None
) -> tuple[SampleFailure, ...]:
    out: list[SampleFailure] = []
    for vendor, case_id, case in load_cases(root):
        for msg in check_case(vendor, case, decode, catalogue=catalogue):
            out.append(SampleFailure(case_id, msg))
    return tuple(out)


def metrics_used(decoder_tables: Iterable[Mapping]) -> set[str]:
    """Every metric name a set of decoder tables can emit — to assert they
    all exist in the catalogue."""
    names: set[str] = set()
    for table in decoder_tables:
        for entries in table.values():
            if isinstance(entries, Mapping):
                for v in entries.values():
                    if isinstance(v, tuple) and v and isinstance(v[0], str):
                        names.add(v[0])
    return names


__all__ = ["SampleFailure", "check_case", "load_cases", "metrics_used", "run_samples"]
