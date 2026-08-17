"""Which Theta providers are installed, without importing any of them.

`ThetaPlatform.load_providers()` answers the same question authoritatively, but
it imports every provider module and calls `create_provider(config)` — it needs
configuration, and transitively a database. That is the right cost at server
start and the wrong cost for `mirobody vendors`, which is an offline listing.

The directory name is enough: the platform's own convention is one
`mirobody_<slug>/provider_<slug>.py` per provider, which is exactly what
`load_providers` scans for. Reading the directory names is a filesystem call
with no imports, so this stays usable from the CLI on a bare wheel.

This exists because the alternative was worse. `cli._cmd_vendors` carried a
hardcoded `{"garmin", "oura", "whoop"}` with a comment explaining that without
it "the listing looks like it contradicts VENDORS.md" — the vendor catalogue
grades all 24 entries `metadata`, including three that Theta serves in
production. A hardcoded set is right until someone adds a fifth provider, at
which point the listing quietly resumes lying.
"""

from __future__ import annotations

import os
from pathlib import Path

_PROVIDER_DIR_PREFIX = "mirobody_"

# A provider directory's slug does not always equal the vendor id it serves:
# the Garmin provider is `mirobody_garmin_connect` (Garmin's product is "Garmin
# Connect") while the catalogue lists the vendor as `garmin`.
_SLUG_TO_VENDOR_ID = {
    "garmin_connect": "garmin",
}


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


def installed_vendor_ids(extra_dirs: list[str] | None = None) -> set[str]:
    """Installed providers, mapped onto vendor-catalogue ids.

    Providers with no catalogue counterpart (``pgsql`` is a database, not a
    vendor) simply do not match anything, which is correct — they are real
    providers that the vendor catalogue was never meant to describe.
    """
    return {
        _SLUG_TO_VENDOR_ID.get(slug, slug)
        for slug in installed_provider_slugs(extra_dirs)
    }
