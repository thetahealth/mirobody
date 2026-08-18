"""Which providers are installed, without importing any of them.

`ProviderPlatform.load_providers()` answers the same question authoritatively, but
it imports every provider module and calls `create_provider(config)` — it needs
configuration, and transitively a database. Reading the directory names instead
is a filesystem call with no imports, which is what makes this answerable from
a bare wheel with nothing configured.

The directory name is enough: the platform's own convention is one
`mirobody_<slug>/provider_<slug>.py` per provider, which is exactly what
`load_providers` scans for. `test_installed.py` guards that convention — a
`mirobody_*/` directory holding no `provider_*.py` loads nothing, silently.

This module also had `installed_vendor_ids`, which mapped these slugs onto the
`pulse/vendor` catalogue so `mirobody vendors` could mark the sources served by
a real provider. Both the catalogue and that command are gone, and the mapping
went with them — it described nothing that still exists.
"""

from __future__ import annotations

import os
from pathlib import Path

_PROVIDER_DIR_PREFIX = "mirobody_"


def installed_provider_slugs(extra_dirs: list[str] | None = None) -> set[str]:
    """Directory slugs of every installed provider, e.g. ``{"oura", "whoop"}``.

    Scans the packaged provider directory, plus `extra_dirs` for deployments
    that add their own (the same `PROVIDER_DIRS` config `load_providers` reads).
    """
    roots = [Path(__file__).parent]
    for d in extra_dirs or []:
        if not d:
            continue
        p = Path(d)
        roots.append(p if p.is_absolute() else (Path(os.getcwd()) / p))

    slugs: set[str] = set()
    for root in roots:
        try:
            entries = list(root.iterdir())
        except OSError:
            # A configured directory that does not exist is not an error here;
            # it is one at load time, where it is reported properly.
            continue
        for entry in entries:
            if entry.is_dir() and entry.name.startswith(_PROVIDER_DIR_PREFIX):
                slugs.add(entry.name[len(_PROVIDER_DIR_PREFIX):])
    return slugs
