"""Where a device or a health platform gets read.

One underscore-prefixed directory is the machinery; every other directory is
one integration:

    _platform/   the base class, the loader, credential storage, the pull task
                 factory, startup and normalize. Nobody outside implements
                 against these directly.
    mirobody_*/  a pulled integration: Garmin, Oura, WHOOP
    apple/       a pushed one. Apple posts to an endpoint instead of being
                 polled, and it registers its own platform, so it carries no
                 `mirobody_` prefix: that prefix is the loader's glob.

`BasePullProvider` is the contract. A subclass implements `create_provider()`,
`info`, `format_data()`, `pull_from_vendor_api()`, `save_raw_data_to_db()` and
`is_data_already_processed()`.

Loading: `ProviderPlatform.load_providers()` scans `mirobody_*/provider_*.py`,
calls `create_provider(config)` on each and registers what comes back, so a
pulled integration is added or taken offline by adding or removing its
directory. `installed.py` reads the same convention without importing anything.
"""

# Lazy (PEP 562), matching `mirobody/collect/__init__.py` and
# `mirobody/agent/__init__.py`. Importing ProviderPlatform eagerly pulls in
# fastapi, sqlalchemy, psycopg and aiohttp, so `from
# mirobody.collect.providers.installed import installed_provider_slugs`, a filesystem
# scan with no imports of its own, was loading the entire server stack just by
# touching this package. Reading a directory listing should not cost a database
# driver.
from typing import TYPE_CHECKING

_EXPORTS = {
    "ProviderPlatform"     : "_platform.platform",
    "BasePullProvider" : "_platform.base",
    # These four are submodules, not attributes: imported by path below.
    "database_service"  : "_platform.database_service",
    "normalize"         : "_platform.normalize",
    "pull_task"         : "_platform.pull_task",
    "startup"           : "_platform.startup",
    "installed_provider_slugs": "installed",
}
__all__ = [*_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .installed import installed_provider_slugs
    from ._platform import database_service, normalize, pull_task, startup
    from ._platform.base import BasePullProvider
    from ._platform.platform import ProviderPlatform


def __getattr__(name: str):
    import importlib

    where = _EXPORTS.get(name)
    if where is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = importlib.import_module(f".{where}", __name__)
    # `where` names either the module that defines `name`, or (for the four
    # utility submodules re-exported wholesale) the submodule itself.
    value = module if where.rsplit(".", 1)[-1] == name else getattr(module, name)
    globals()[name] = value
    return value
