"""Load task modules so their `BaseRedisTask` subclasses self-register.

Built-in tasks in `mirobody/task/` are always loaded — unlike MCP tools or
agents, task built-ins should run in every deployment, so callers don't
need to re-declare this package in their `TASK_DIRS`.

Each extra entry in `dirs` may be a filesystem path or a dotted package name
(e.g. `myproj.tasks`); for package names, `importlib.util.find_spec` resolves
the on-disk location. Every non-private `.py` module is imported, and any
`BaseRedisTask` subclass it declares becomes visible to `iter_redis_tasks()`.
"""

from __future__ import annotations

import importlib
import importlib.util
import logging
import os

from ..utils.plugin_dirs import import_plugin_module, resolve_plugin_dir

#-----------------------------------------------------------------------------

_BUILTIN_DIR = __package__  # "mirobody.task"

# Modules in the built-in package that aren't themselves tasks.
_BUILTIN_SKIP = {"base", "loader"}


def load_tasks_from_directories(dirs: list[str]) -> None:
    _load_tasks_from_directory(_BUILTIN_DIR, skip=_BUILTIN_SKIP)

    seen: set[str] = {_BUILTIN_DIR}
    for d in dirs or []:
        if not d or d in seen:
            continue
        seen.add(d)
        _load_tasks_from_directory(d)


def _load_tasks_from_directory(dir: str, skip: set[str] | None = None) -> None:
    target = (dir or "").strip()
    if not target:
        return

    # Same resolution as MCP tools and chat agents — see utils/plugin_dirs.py
    # for why the path-string-to-module-name rule this replaces was unsound.
    target, module_prefix = resolve_plugin_dir(target)

    if not target:
        logging.warning(f"No task directory found at {dir!r}")
        return

    try:
        entries = os.scandir(target)
    except Exception as e:
        logging.warning(f"Error scanning task directory {target}: {e}")
        return

    logging.debug(f"Loading tasks from {target}")

    for entry in entries:
        if entry.is_dir() or \
           not entry.name.lower().endswith(".py") or \
           entry.name.startswith("_") or \
           entry.name.startswith("test_"):
            continue

        stem = entry.name[:-3]
        if skip and stem in skip:
            continue

        try:
            module_name, _ = import_plugin_module(target, module_prefix, entry.name)
            logging.info(f"Loaded task module: {module_name}")
        except Exception as e:
            logging.warning(f"Error importing task module {entry.name} from {target}: {e}")

#-----------------------------------------------------------------------------
