"""Mirobody: the AI-native health data engine.

The public surface of a `pip install mirobody`, in three imports::

    from mirobody import resolve, resolve_reading   # ② Translate, offline
    from mirobody.units import normalize_unit, unit_family
    from mirobody.lexical import normalize, word_tokens

    resolve("血红蛋白").loinc                        # -> "718-7", no key, no network

``resolve`` and the vocabulary modules need nothing but numpy. Reading a
document (:func:`mirobody.parse_file`) needs the extraction stack and one model
key: ``pip install 'mirobody[parse]'``. The HTTP/chat/MCP server is the Docker
application, not a library surface: ``git clone && ./deploy.sh``.

**What is stable.** The names in ``__all__`` here, plus ``__all__`` in
:mod:`mirobody.units`, :mod:`mirobody.lexical`, :mod:`mirobody.engine` and
:mod:`mirobody.bundle`. Everything else: ``mirobody.translate``,
``mirobody.collect``, ``mirobody.server``, ``mirobody.agent``, anything
underscore-prefixed: is internal and moves without notice.

:mod:`mirobody.bundle` is the build-time half of that surface: the axis table
and the alias sources, for tools that generate a seed or a corpus from LOINC
rather than asking for one answer. It is deliberately not re-exported here, so
``import mirobody`` stays exactly as cheap as the paragraph below says.

Exports resolve lazily (PEP 562), and that is load-bearing rather than style:
``import mirobody`` must not import numpy, must not open the 15 MB data bundle,
and must not drag in the extraction stack. Measured, it costs about as much as
``import json``.
"""

from typing import TYPE_CHECKING

#: name -> submodule it lives in
_EXPORTS = {
    "OfflineResolver": "engine",
    "Reading": "engine",
    "Resolution": "engine",
    "get_resolver": "engine",
    "parse_file": "engine",
    "resolve": "engine",
    "resolve_reading": "engine",
    "standardize_reading": "engine",
}

__all__ = [*_EXPORTS, "BUNDLE_VERSION", "__version__"]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .engine import (
        OfflineResolver,
        Reading,
        Resolution,
        get_resolver,
        parse_file,
        resolve,
        resolve_reading,
        standardize_reading,
    )

    BUNDLE_VERSION: str


def __getattr__(name: str):
    if name in _EXPORTS:
        import importlib

        return getattr(importlib.import_module(f".{_EXPORTS[name]}", __name__), name)
    if name == "BUNDLE_VERSION":
        # The corpus release the shipped bundle was cut from, e.g.
        # `2026.08.28-963df348633d`. Deliberately not a module constant: it
        # reads a tar member, and `import mirobody` is meant to be free.
        from ._bundle import bundle_version

        return bundle_version()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)


# Version, resolved in this order:
#   1. importlib.metadata, once the package is installed (wheel or editable).
#   2. MIROBODY_VERSION, which CI exports from the git tag before
#      `python -m build`; the isolated build env has no installed metadata.
#   3. The literal below, the canonical version of this source tree and what
#      `pip install -e .` bakes in. Bump it per release to match the CHANGELOG
#      and the tag: the release workflow refuses a tag that disagrees, and the
#      suite pins it to the CHANGELOG and the READMEs.
try:
    from importlib.metadata import version as _version
    __version__ = _version("mirobody")
except Exception:
    import os
    __version__ = os.environ.get("MIROBODY_VERSION") or "1.5.0"
