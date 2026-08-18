"""Every packaged provider must actually load.

This is the test that was missing when the platform logged
`provider platform loaded 0 providers` on every boot and nobody noticed for a
release. The loader put the provider directory on `sys.path` and imported
`mirobody_oura.provider_oura` as a TOP-LEVEL module; a top-level package has no
parent, so `provider_oura.py`'s `from ....utils.tasks import spawn` raised
"attempted relative import beyond top-level package". The loader caught that as
a warning and moved on, so Garmin, Oura and Whoop were all absent and the only
evidence was one INFO line saying zero.

`test_installed.py` next door guards the naming convention — that a
`mirobody_*/` directory carries a `provider_*.py`. It cannot catch this: the
files were all there and correctly named. What was broken was the import.
"""

from __future__ import annotations

import importlib
import logging
from pathlib import Path

import pytest

PROVIDER_DIR = Path(__file__).resolve().parent
PACKAGE = "mirobody.pulse.providers"


def _packaged_provider_modules() -> list[tuple[str, str]]:
    return [
        (f.parent.name, f"{PACKAGE}.{f.parent.name}.{f.stem}")
        for f in sorted(PROVIDER_DIR.glob("mirobody_*/provider_*.py"))
    ]


MODULES = _packaged_provider_modules()


def test_the_scan_found_providers():
    """Guard against this file passing because the glob broke."""
    assert len(MODULES) >= 4, f"expected the packaged providers, found {MODULES}"


@pytest.mark.parametrize("slug,module", MODULES, ids=[s for s, _ in MODULES])
def test_every_packaged_provider_imports(slug: str, module: str):
    """By its REAL dotted path — which is what makes relative imports resolve."""
    importlib.import_module(module)


def test_the_loader_reports_no_failures_on_the_packaged_directory(caplog):
    """The loader's own path, not a hand-rolled import.

    Instances are NOT asserted: `create_provider` returning None is how a
    provider declines when its credentials are absent, which is the normal
    state of a fresh checkout. What must not happen is a *load* failure.
    """
    from mirobody.pulse.providers.platform.platform import ProviderPlatform

    platform = ProviderPlatform({})
    with caplog.at_level(logging.WARNING):
        platform._load_providers_from_directory(PROVIDER_DIR)

    failures = [r.message for r in caplog.records if "Failed to load provider" in r.message]
    assert not failures, f"provider modules failed to load: {failures}"
