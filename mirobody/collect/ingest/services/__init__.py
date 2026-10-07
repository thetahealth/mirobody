"""Where every platform's `StandardPulseData` is written.

`StandardHealthService.process_standard_data()` writes a summary indicator as
an observation (`collect/observations.py`) and a series point into
`series_data`; a repair batch is then swept (`repair_reconcile.py`).
"""

from .upload_health import StandardHealthService

__all__ = [
    "StandardHealthService",
]
