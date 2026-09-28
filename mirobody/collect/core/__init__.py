"""What every stage in this package stands on.

    database.py          the DB base classes
    push_service.py      delivering a push to a provider
    constants.py         LinkType, ProviderStatus, CacheConfig
    models.py            the provider contract's own types

Not here, deliberately: what a value MEANS (the indicator catalogue, unit
conversion, value ranges, fhir_id) is `mirobody.translate`, and the pipeline
stage that turns a series into daily summaries is `translate.aggregate`. Both
lived in here once, which is what made "core" a grab-bag: the pipeline was
invisible in the directory tree, and pure data modules sat beside the server
infrastructure. `PlatformUserService` left for the same reason, and went
further: identity is `mirobody/user/platform.py` now, because user creation
and authentication are not something a collection package should own.
"""

from typing import TYPE_CHECKING

# Lazy (PEP 562), matching `mirobody/collect/__init__.py`,
# `mirobody/agent/__init__.py` and `mirobody/collect/providers/__init__.py`.
# Importing any submodule ran this __init__, which imported `.database` and
# pulled SQLAlchemy and FastAPI into the process, making the indicator
# catalogue (pure data, no I/O) unusable without the server stack installed.
# Every `from mirobody.collect.core import X` keeps working; each export simply
# pays its own import cost at first use.
_EXPORTS = {
    'CacheConfig'             : 'constants',
    'LinkType'                : 'constants',
    'ProviderStatus'          : 'constants',
    'LinkRequest'             : 'models',
    'ProviderInfo'            : 'models',
    'UserProvider'            : 'models',
    'PushService'             : 'push_service',
    'push_service'            : 'push_service',
}
__all__ = [*_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .constants import CacheConfig, LinkType, ProviderStatus
    from .models import LinkRequest, ProviderInfo, UserProvider
    from .push_service import PushService, push_service


def __getattr__(name: str):
    import importlib

    where = _EXPORTS.get(name)
    if where is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = importlib.import_module(f".{where}", __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
