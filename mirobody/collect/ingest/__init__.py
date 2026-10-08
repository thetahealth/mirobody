"""Where everything a source produced converges, and lands.

A Garmin pull, an Apple push and an uploaded lab report have nothing in common
until here. `models/requests.py` is the shape they all arrive in
(`StandardPulseData`: a metaInfo and a list of records), `services/
upload_health.py` writes it, and `services/repair_reconcile.py` handles a
client resending a window it has corrected.

Landing is ① Collect's last step, not ② Translate's first: what gets written is
what the source said, with its own unit and its own timestamp, so a row can
always be traced back. What the value MEANS is `mirobody.translate`.

`collect/observations.py` one level up is the single writer of
observations; this package is the request path that calls it, and the
writer of `series_data`, the device point buffer the aggregation reads.

Lazy (PEP 562), matching the rest of `collect/`: importing this must not pull
the server stack.
"""

from typing import TYPE_CHECKING

_EXPORTS = {
    "FormatDataContext": "models.requests",
    "FormatDataInput": "models.requests",
    "StandardPulseData": "models.requests",
    "StandardPulseMetaInfo": "models.requests",
    "StandardPulseRecord": "models.requests",
    "StandardHealthService": "services.upload_health",
}
__all__ = [*_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .models.requests import (
        FormatDataContext,
        FormatDataInput,
        StandardPulseData,
        StandardPulseMetaInfo,
        StandardPulseRecord,
    )
    from .services.upload_health import StandardHealthService


def __getattr__(name: str):
    import importlib

    where = _EXPORTS.get(name)
    if where is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = importlib.import_module(f".{where}", __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
