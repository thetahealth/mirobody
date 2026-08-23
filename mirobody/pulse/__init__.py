"""① Collect — every way a reading gets into Mirobody, and what happens next.

Three source shapes, one convergence point, then meaning:

    providers/   devices and health platforms, pulled on a schedule
    apple/       Apple Health exports and CDA documents
    file_parser/ a file is a source too: lab PDFs, photos, CSV, genetic raw data
         ↓
    ingest/      all three converge on StandardPulseData → th_series_data
         ↓
    standardize/ what a value MEANS: indicator catalogue, units, ranges, fhir_id
    aggregate/   series → daily summaries and derived indicators

`core/` is what those stand on, not a stage: the provider contract types, the
scheduler, the DB base classes, the distributed lock. Sub-package sizes and
entry points are in README.md, ordered the same way — the directory listing
cannot show this order, since `aggregate/` sorts before `providers/`.

Providers are discovered by file scan, so deleting one takes it offline.

Exports resolve lazily (PEP 562). ``import mirobody.pulse`` is the ENGINE —
it must not eagerly construct the platform singletons, and it carries no HTTP
routers at all: those were platform assembly, not engine logic, and now live
where they always belonged — ``mirobody/server/routers/``. Every existing
``from mirobody.pulse import X`` keeps working unchanged; each export simply
pays its own import cost at first use.
"""

from typing import TYPE_CHECKING

# name -> submodule that defines it
_EXPORTS = {
    # Base classes and models
    "Platform": "base",
    "Provider": "base",
    "ProviderInfo": "base",
    "UserProvider": "base",
    "LinkRequest": "base",
    # Managers
    "PlatformManager": "manager",
    "platform_manager": "manager",
    # Setup functions
    "setup_platform_system": "setup",
    "setup_platform_system_async": "setup",
    "get_platform_manager": "setup",
    # Concrete implementations
    "ProviderPlatform": "providers",
    "BasePullProvider": "providers",
    # Apple Health implementations
    "AppleHealthPlatform": "apple",
    "AppleHealthProvider": "apple",
    "CDAProvider": "apple",
    # Note: Specific providers (GarminProvider, etc.) are auto-loaded
    # and can be imported from .providers if needed
}
__all__ = [*_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .apple import AppleHealthPlatform, AppleHealthProvider, CDAProvider
    from .base import LinkRequest, Platform, Provider, ProviderInfo, UserProvider
    from .manager import PlatformManager, platform_manager
    from .setup import get_platform_manager, setup_platform_system, setup_platform_system_async
    from .providers import BasePullProvider, ProviderPlatform


def __getattr__(name: str):
    import importlib

    if name in _EXPORTS:
        module = importlib.import_module(f".{_EXPORTS[name]}", __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
