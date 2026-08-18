"""
Core module

Provides common functionalities for all Platforms and Providers, including:
- Unified logging
- Common data models and enumerations
- Common database operations
- Webhook processing framework
- Unified background task scheduler
- Encapsulated push service

What a value *means* — the indicator catalogue, unit conversion, value-range
validation, fhir_id mapping — lives in `mirobody.pulse.standardize`, not here.
The downstream pipeline stages — `pulse.aggregate` (series → daily summaries),
`pulse.insight`, `pulse.monitor` — are top-level pulse packages too. All four
used to live inside this package, which made "core" a grab-bag: the pipeline
was invisible in the directory tree, and pure data modules imported alongside
the server infrastructure above. Core is now only the shared base every stage
stands on.
"""

from typing import TYPE_CHECKING

# Lazy (PEP 562), matching `mirobody/pulse/__init__.py`,
# `mirobody/agent/__init__.py` and `mirobody/pulse/providers/__init__.py`.
#
# Importing any submodule ran this __init__, which imported `.database` and
# pulled SQLAlchemy and FastAPI into the process. That made the INDICATOR
# CATALOGUE — pure data, no I/O — unusable without the server stack installed,
# which is the opposite of the engine's premise.
#
# Every existing `from mirobody.pulse.core import X` keeps working unchanged; each export
# simply pays its own import cost at first use.
_EXPORTS = {
    'CacheConfig'             : 'constants',
    'CommonConfig'            : 'constants',
    'DataType'                : 'constants',
    'DocStatus'               : 'constants',
    'DocType'                 : 'constants',
    'LinkType'                : 'constants',
    'ProcessAction'           : 'constants',
    'ProviderStatus'          : 'constants',
    'ResourceType'            : 'constants',
    'BaseDatabaseService'     : 'database',
    'CacheableDatabaseService': 'database',
    'HealthDataBatch'         : 'models',
    'LinkRequest'             : 'models',
    'ProcessingResult'        : 'models',
    'ProviderInfo'            : 'models',
    'ProviderMetrics'         : 'models',
    'StandardHealthData'      : 'models',
    'UserProvider'            : 'models',
    'WebhookEvent'            : 'models',
    'PushService'             : 'push_service',
    'push_service'            : 'push_service',
    'PullTask'                : 'scheduler',
    'Scheduler'               : 'scheduler',
    'scheduler'               : 'scheduler',
}
__all__ = [*_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .constants import CacheConfig, CommonConfig, DataType, DocStatus, DocType, LinkType, ProcessAction, ProviderStatus, ResourceType
    from .database import BaseDatabaseService, CacheableDatabaseService
    from .models import HealthDataBatch, LinkRequest, ProcessingResult, ProviderInfo, ProviderMetrics, StandardHealthData, UserProvider, WebhookEvent
    from .push_service import PushService, push_service
    from .scheduler import PullTask, Scheduler, scheduler


def __getattr__(name: str):
    import importlib

    where = _EXPORTS.get(name)
    if where is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = importlib.import_module(f".{where}", __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
