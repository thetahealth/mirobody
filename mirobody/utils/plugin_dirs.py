"""Resolving a configured plugin directory to something importable.

Three loaders — MCP tools (`mcp/tool.py`), chat agents (`agent/chat/agent.py`)
and background tasks (`task/loader.py`) — each took a directory string from
config and turned it into module names the same way, and each carried the same
two defects:

1. **Path spelling decided whether the directory worked at all.** The rule was
   `directory.replace(os.sep, ".")`, guarded by
   `removeprefix(os.getcwd())`. That strips the CWD prefix only when the string
   literally starts with it, so on macOS — where `/tmp` is a symlink to
   `/private/tmp` and `os.getcwd()` reports the real path — the SAME directory
   loaded one tool spelled `mytools` and zero spelled `/tmp/toolhost/mytools`.
   Any directory outside the tree failed outright with a warning nobody reads.
   The README tells users to "add your own directory" to `MCP_TOOL_DIRS`; that
   promise held only for directories that happened to double as importable
   dotted packages.

2. **No fallback when the dotted name is not importable.** A real directory on
   disk that is not on `sys.path` produced one warning per file and no tools.

Both are fixed by deciding once, here: a directory that IS an importable
package keeps package semantics (so its modules may use relative imports); any
other directory is loaded file by file, which is exactly the shape the docs
describe for a plugin ("a tool is a plain Python function" in one `.py`).

Not merged with the provider loader in `pulse/providers/platform/platform.py`:
that one discovers `mirobody_<slug>/provider_<slug>.py` SUBDIRECTORIES, not
flat files, and its packaged branch must import by real dotted path for the
same relative-import reason (see `test_provider_loading.py`). Same disease,
different anatomy; folding them together would fit neither.
"""

from __future__ import annotations

import importlib.util
import logging
import os
from types import ModuleType


def resolve_plugin_dir(configured: str) -> tuple[str | None, str | None]:
    """Map a configured directory onto ``(directory_on_disk, dotted_prefix)``.

    ``dotted_prefix`` is None when the directory is real but not importable as a
    package — the caller should then load each file by location. Both None means
    nothing usable was found and the caller should warn and skip.
    """
    raw = (configured or "").strip()
    if not raw:
        return None, None

    # The historical spelling: CWD-relative, which doubles as a dotted path.
    relative = raw.removeprefix(os.getcwd()).removeprefix(os.sep).strip()
    dotted = relative.replace(os.path.sep, ".") if relative else ""

    if dotted:
        try:
            spec = importlib.util.find_spec(dotted)
        except Exception:
            spec = None
        if spec and spec.origin:
            # An importable package. Trust ITS location, not the string: this is
            # also how a caller may pass a dotted name instead of a path.
            return os.path.dirname(spec.origin), dotted

    # Not importable — but the directory may still exist, under either spelling.
    for candidate in (raw, relative):
        if candidate and os.path.isdir(candidate):
            return os.path.abspath(candidate), None

    return None, None


def import_plugin_module(directory: str, dotted_prefix: str | None, filename: str) -> tuple[str, ModuleType]:
    """Import one plugin file, by package path when there is one.

    Returns ``(module_name, module)``. The name is what the caller keys tools or
    agents by, so it stays readable for a file-loaded module too — the directory
    name plus the stem, rather than a synthetic token.
    """
    stem = filename[:-3] if filename.endswith(".py") else filename

    if dotted_prefix:
        module_name = f"{dotted_prefix}.{stem}"
        return module_name, importlib.import_module(module_name)

    path = os.path.join(directory, filename)
    module_name = f"{os.path.basename(directory.rstrip(os.sep))}.{stem}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if not spec or not spec.loader:
        raise ImportError(f"Cannot load plugin module from {path}")
    module = importlib.util.module_from_spec(spec)
    # Registered before exec so a module that imports itself (or is re-entered
    # by a decorator) sees a partially-initialised module rather than looping.
    import sys

    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module_name, module
