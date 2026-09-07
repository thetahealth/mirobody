"""Every name a module advertises must exist.

`__all__` is the public surface: it drives `from x import *`, and readers and
tools treat it as the answer to "what does this module offer". A name left in it
after the symbol is deleted turns a star-import into an `AttributeError` at
import time, and nothing else in the suite notices. A repo-wide check costs
nothing and catches the whole class, including the lazy PEP 562 packages where
`__all__` and the export table must agree.
"""

from __future__ import annotations

import ast
import importlib
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1] / "mirobody"


def _modules_with_all() -> list[tuple[str, list[str]]]:
    found = []
    for path in sorted(ROOT.rglob("*.py")):
        if "__pycache__" in str(path) or path.name.startswith("test_") or "gate_tests" in str(path):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        names = None
        for node in tree.body:
            if isinstance(node, ast.Assign) and any(
                getattr(t, "id", "") == "__all__" for t in node.targets
            ):
                try:
                    value = ast.literal_eval(node.value)
                except Exception:
                    value = None
                if isinstance(value, (list, tuple)):
                    names = list(value)
        if names:
            dotted = (
                str(path.relative_to(ROOT.parent).with_suffix(""))
                .replace("/", ".")
                .removesuffix(".__init__")
            )
            found.append((dotted, names))
    return found


MODULES = _modules_with_all()


def test_the_scan_found_modules():
    """Guard against this passing because the AST walk broke."""
    assert len(MODULES) >= 5, f"expected modules declaring __all__, found {len(MODULES)}"


@pytest.mark.parametrize("module,names", MODULES, ids=[m for m, _ in MODULES])
def test_every_advertised_name_resolves(module: str, names: list[str]):
    try:
        mod = importlib.import_module(module)
    except Exception as exc:  # an optional-extra module; other tests cover importability
        pytest.skip(f"{module} not importable here: {type(exc).__name__}")

    # `hasattr` only swallows AttributeError. On a lazy PEP 562 package the
    # attribute access RUNS the deferred import, so on an install without the
    # extra it raises ModuleNotFoundError straight through `hasattr` and the
    # test errors instead of skipping — which is what a `[test]`-only install
    # saw for `mirobody.agent.chat`. Resolve each name explicitly.
    missing = []
    for n in names:
        try:
            getattr(mod, n)
        except AttributeError:
            missing.append(n)
        except Exception as exc:
            pytest.skip(f"{module}.{n} needs an extra that is not installed: {type(exc).__name__}")

    assert not missing, (
        f"{module}.__all__ advertises {missing}, which the module does not define — "
        f"`from {module} import *` raises AttributeError"
    )
