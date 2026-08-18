"""Provider discovery reads the directory convention, and stays import-free.

`ProviderPlatform.load_providers()` finds providers by globbing
`mirobody_*/provider_*.py`. Two things about that are worth a guard:

* a `mirobody_*/` directory with no `provider_*.py` inside loads nothing, and
  says nothing — the provider looks installed and silently is not;
* reading the listing must not import the server stack, which is why
  `providers/__init__.py` is lazy (PEP 562).

These tests used to be framed around `mirobody vendors`, the CLI listing that
annotated the `pulse/vendor` catalogue with the providers actually installed.
That catalogue and command are deleted; the two guards above outlived them,
because they are about the plugin convention, not about the listing.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

from .installed import installed_provider_slugs


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


def test_discovery_imports_nothing_third_party():
    """Reading the listing must not load a database driver.

    Run in a subprocess: the in-process check is worthless once another test
    has already imported the server stack.
    """
    code = textwrap.dedent(
        """
        import sys
        before = set(sys.modules)
        from mirobody.pulse.providers.installed import installed_provider_slugs
        installed_provider_slugs()
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
        ". `mirobody/pulse/providers/__init__.py` is lazy (PEP 562) precisely so "
        "reading a directory listing does not import fastapi and psycopg."
    )
