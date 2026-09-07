"""The provider coverage matrix, generated from the decoders themselves.

"This platform supports Oura" and "this platform brings you your blood
pressure" are different claims, and a person choosing a device wants the
second. A hand-written support table answers the first and drifts into
answering the second wrongly, because nothing checks it.

`gen_coverage` builds the table from `connect.Coverage` objects — which are
themselves derived from each decoder's own mapping — so the documentation
cannot promise a metric the code does not produce. `stale` is the CI half:
it compares a committed table with a freshly generated one and returns the
diff, so a decoder that gains a metric fails the build until the page is
regenerated.

Pure; stdlib only, and it takes the coverages as an argument rather than
importing `vendors`, so a consumer can run it over its OWN connectors.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

MARKER_START = "<!-- coverage:start -->"
MARKER_END = "<!-- coverage:end -->"


def gen_coverage(coverages: Mapping[str, object], *, metrics: Sequence[str] = ()) -> str:
    """A Markdown table of provider × metric.

    With `metrics`, one column per named metric and a ✓ where the connector
    carries it — the shape a reader uses to answer "will switching keep my
    charts working". Without, one row per connector with its counts and its
    data types, which is what a long metric list degrades to gracefully.
    """
    names = sorted(coverages)
    if metrics:
        header = "| Metric | " + " | ".join(names) + " |"
        rule = "|---" * (len(names) + 1) + "|"
        rows = [
            "| `" + metric + "` | " + " | ".join("✓" if _covers(coverages[n], metric) else "—" for n in names) + " |"
            for metric in metrics
        ]
        return "\n".join([header, rule, *rows])

    header = "| Provider | Metrics | Data types |"
    rule = "|---|---|---|"
    rows = [
        f"| `{name}` | {len(_timeseries(coverages[name]))} | "
        + ", ".join(f"`{t}`" for t in sorted(_data_types(coverages[name])))
        + " |"
        for name in names
    ]
    return "\n".join([header, rule, *rows])


def embed(document: str, table: str) -> str:
    """`document` with the region between the markers replaced by `table`.

    Markers rather than a whole generated file, so the prose around the table
    — which is where the honest caveats live — is written by a person and
    survives regeneration.
    """
    start, end = document.find(MARKER_START), document.find(MARKER_END)
    if start == -1 or end == -1 or end < start:
        raise ValueError(f"document has no {MARKER_START} … {MARKER_END} region")
    return document[: start + len(MARKER_START)] + "\n" + table + "\n" + document[end:]


def stale(document: str, table: str) -> str:
    """`""` when the document's table is current, else the two versions.

    The CI check: a decoder that gains a metric should fail the build until
    the page is regenerated, because a coverage table nobody regenerates is
    the hand-written table this replaced.
    """
    start, end = document.find(MARKER_START), document.find(MARKER_END)
    if start == -1 or end == -1:
        return f"document has no {MARKER_START} … {MARKER_END} region"
    current = document[start + len(MARKER_START) : end].strip()
    return "" if current == table.strip() else f"--- committed\n{current}\n--- generated\n{table}"


def _timeseries(coverage: object) -> frozenset[str]:
    return frozenset(getattr(coverage, "timeseries", ()) or ())


def _data_types(coverage: object) -> frozenset[str]:
    return frozenset(getattr(coverage, "data_types", ()) or ())


def _covers(coverage: object, metric: str) -> bool:
    return metric in _timeseries(coverage)


__all__ = ["MARKER_END", "MARKER_START", "embed", "gen_coverage", "stale"]
