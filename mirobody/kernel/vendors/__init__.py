"""Vendor payload decoders — the part of a provider that is pure.

A provider integration is two very different things glued together: the
OAuth dance, token storage, rate limits and pull windows (IO, per
deployment) and the translation of a vendor's JSON into standardised facts
(a pure function of the payload). Three repositories carried the second
part as three near-copies inside their IO classes, each fixing bugs the
others still had. This package is the second part on its own:

    from mirobody.kernel import vendors
    facts = vendors.decode("garmin", "dailies", item, tz="Asia/Shanghai")

Each decoder module exposes ``DATA_TYPES``, ``decode(data_type, item, tz,
...) -> list[series.Fact]`` and a ``synthesize`` helper (``synthetic.py``)
that produces plausible payloads for demos and tests. ``samples/`` holds
public-documentation-shaped sample payloads with their expected facts; they
ship in the wheel so a consumer's ``mirobody.testing.FormatTestRunner`` can
run them against its own decoders, and ``test_vendors.py`` runs them here.

Adding a vendor: one module here (table + ``decode``), one sample set, one
entry in ``DECODERS`` — the IO half lives with whoever runs it.
"""

from __future__ import annotations

from types import ModuleType

from ..connect import Coverage
from ..series import Fact
from . import garmin, open_wearables, oura, whoop

DECODERS: dict[str, ModuleType] = {
    "garmin": garmin,
    "whoop": whoop,
    "oura": oura,
    # Not a device vendor: an ACCESS PLATFORM. Decoding its API is the seam
    # between "connect eight vendors and a phone SDK" and "make the data mean
    # one thing" — see open_wearables.py.
    "open_wearables": open_wearables,
}


def decode(vendor: str, data_type: str, item: dict, tz: str, **kw) -> list[Fact]:
    """Facts from one raw record of ``vendor``/``data_type``; ``[]`` for an
    unknown vendor. Keyword arguments (``pulled_at_ms``, ``source_record_id``,
    ``ingested_at_ms``) pass through to the decoder."""
    mod = DECODERS.get(vendor)
    if mod is None:
        return []
    return mod.decode(data_type, item, tz, **kw)


def data_types(vendor: str) -> tuple[str, ...]:
    mod = DECODERS.get(vendor)
    return tuple(mod.DATA_TYPES) if mod else ()


def metrics_of(vendor: str) -> frozenset[str]:
    """Every catalogue metric this vendor's decoder can emit, read from the
    decoder's own table. Not hand-written anywhere, so a documentation page
    built from it cannot promise data the code does not produce."""
    mod = DECODERS.get(vendor)
    return frozenset(mod.METRICS) if mod is not None else frozenset()


def coverage_of(vendor: str) -> Coverage:
    """The connector's :class:`connect.Coverage`: what it ACTUALLY carries.

    "This platform supports Oura" and "this platform brings you your blood
    pressure" are different claims, and a person choosing a device wants the
    second. Generated, so the two cannot come apart.
    """
    return Coverage(
        provider=vendor,
        timeseries=metrics_of(vendor),
        data_types=frozenset(data_types(vendor)),
        medications=False,   # no cloud wearable API exposes a medication list
        documents=False,
    )


def coverage_matrix() -> dict[str, Coverage]:
    """Every decoder's coverage, for `testing.gen_coverage`."""
    return {vendor: coverage_of(vendor) for vendor in sorted(DECODERS)}


__all__ = [
    "DECODERS", "decode", "data_types", "metrics_of", "coverage_of", "coverage_matrix",
    "garmin", "whoop", "oura", "open_wearables",
]
