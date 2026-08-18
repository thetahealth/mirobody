"""
Provider platform Module

Provides the provider platform implementation with pluggable providers for direct
device/service integrations (Garmin, Whoop, PostgreSQL, etc.).

Architecture:
    ProviderPlatform (platform/platform.py)
        — manages provider lifecycle, pull scheduling, and data routing
    BasePullProvider (platform/base.py)
        — abstract base for all providers; subclasses implement
          create_provider(), info, format_data(), pull_from_vendor_api(),
          save_raw_data_to_db(), is_data_already_processed()
    Providers (mirobody_*/provider_*.py)
        — one directory per device/service, self-contained implementations

Provider loading:
    ProviderPlatform.load_providers() scans mirobody_*/provider_*.py, calls
    create_provider(config) on each, and registers successful instances.
    Each provider is self-contained and can be added/removed by simply
    adding/removing its directory.
"""

# Lazy (PEP 562), matching `mirobody/pulse/__init__.py` and
# `mirobody/agent/__init__.py`. Importing ProviderPlatform eagerly pulls in
# fastapi, sqlalchemy, psycopg, redis and aiohttp — so `from
# mirobody.pulse.providers.installed import installed_provider_slugs`, a filesystem
# scan with no imports of its own, was loading the entire server stack just by
# touching this package. Reading a directory listing should not cost a database
# driver.
from typing import TYPE_CHECKING

_EXPORTS = {
    "ProviderPlatform"     : "platform.platform",
    "BasePullProvider" : "platform.base",
    # These four are submodules, not attributes — imported by path below.
    "database_service"  : "platform.database_service",
    "normalize"         : "platform.normalize",
    "pull_task"         : "platform.pull_task",
    "startup"           : "platform.startup",
    "installed_provider_slugs": "installed",
}
__all__ = [*_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .installed import installed_provider_slugs
    from .platform import database_service, normalize, pull_task, startup
    from .platform.base import BasePullProvider
    from .platform.platform import ProviderPlatform


def __getattr__(name: str):
    import importlib

    where = _EXPORTS.get(name)
    if where is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = importlib.import_module(f".{where}", __name__)
    # `where` names either the module that defines `name`, or — for the four
    # utility submodules re-exported wholesale — the submodule itself.
    value = module if where.rsplit(".", 1)[-1] == name else getattr(module, name)
    globals()[name] = value
    return value

