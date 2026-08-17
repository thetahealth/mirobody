"""The vendor listing must describe the providers that are actually installed.

`mirobody vendors` prints the vendor catalogue, where all 24 entries grade
`metadata` on their own transport layer — while three of those sources are
served in production by a `pulse/theta` provider. The listing marked those
three from a hardcoded literal, which was true on the day it was written and
becomes wrong the first time anyone adds a provider.

These tests are the guard on that: the annotation is derived, and deriving it
must not drag the server stack into an offline CLI.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

from .installed import installed_provider_slugs, installed_vendor_ids


def test_slugs_match_the_provider_directories_on_disk():
    """The convention `load_providers` scans for is the one we read."""
    theta = Path(__file__).parent
    on_disk = {
        d.name[len("mirobody_"):]
        for d in theta.iterdir()
        if d.is_dir() and d.name.startswith("mirobody_")
    }
    assert on_disk, "expected at least one packaged provider directory"
    assert installed_provider_slugs() == on_disk

    # Every provider directory must actually hold a provider module, or the
    # slug is real but the provider is not. The contract is `load_providers`'s
    # own glob, `mirobody_*/provider_*.py` — note the file name need NOT match
    # the directory slug: `mirobody_garmin_connect/` holds `provider_garmin.py`.
    # (theta/README.md states the stricter `provider_<slug>.py`; the code is the
    # authority here, and Garmin has always violated the documented form.)
    for slug in on_disk:
        modules = list((theta / f"mirobody_{slug}").glob("provider_*.py"))
        assert modules, f"mirobody_{slug}/ has no provider_*.py — it would load nothing"


def test_directory_slug_is_mapped_to_the_catalogue_id():
    """`mirobody_garmin_connect` serves the vendor catalogued as `garmin`.

    Without the mapping the listing silently drops the annotation for Garmin —
    the failure is invisible, since an unannotated row looks like every other
    unimplemented vendor.
    """
    slugs = installed_provider_slugs()
    ids = installed_vendor_ids()
    if "garmin_connect" in slugs:
        assert "garmin" in ids
        assert "garmin_connect" not in ids


def test_every_installed_provider_is_either_catalogued_or_deliberately_not():
    """A provider with no catalogue entry is fine; an unnoticed one is not."""
    from ..vendor import all_vendor_info

    catalogued = {v.id for v in all_vendor_info()}
    ids = installed_vendor_ids()

    # `pgsql` is a database, not a health-data vendor — it has no catalogue
    # entry by design. Anything else uncatalogued is worth a look.
    expected_uncatalogued = {"pgsql"}
    assert (ids - catalogued) == expected_uncatalogued, (
        f"installed providers with no vendor-catalogue entry: "
        f"{sorted(ids - catalogued)}. If that is intentional, add it to "
        f"expected_uncatalogued and say why."
    )


def test_discovery_imports_nothing_third_party():
    """`mirobody vendors` runs offline; it must not load a database driver.

    Run in a subprocess: the in-process check is worthless once another test
    has already imported the server stack.
    """
    code = textwrap.dedent(
        """
        import sys
        before = set(sys.modules)
        from mirobody.pulse.theta.installed import installed_vendor_ids
        installed_vendor_ids()
        new = {m.split(".")[0] for m in set(sys.modules) - before}
        heavy = new - {m for m in new
                       if m.startswith("_") or m in sys.stdlib_module_names
                       or m.startswith("mirobody")}
        print(",".join(sorted(heavy)))
        """
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True, text=True,
        cwd=Path(__file__).parents[3],
    )
    assert out.returncode == 0, out.stderr
    heavy = [m for m in out.stdout.strip().split(",") if m]
    assert not heavy, (
        "discovery pulled in third-party modules: " + ", ".join(heavy) +
        ". `mirobody/pulse/theta/__init__.py` is lazy (PEP 562) precisely so "
        "reading a directory listing does not import fastapi and psycopg."
    )
