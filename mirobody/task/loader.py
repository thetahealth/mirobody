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

    target = target.removeprefix(os.getcwd()).removeprefix(os.sep).strip()
    if not target:
        return

    module_prefix = target.replace(os.path.sep, ".")

    if not os.path.isdir(target):
        try:
            spec = importlib.util.find_spec(module_prefix)
        except Exception:
            spec = None

        if not spec or not spec.origin:
            logging.warning(f"No task found at {module_prefix}")
            return

        target = os.path.dirname(spec.origin)

    try:
        entries = os.scandir(target)
    except Exception as e:
        logging.warning(f"Error scanning task directory {target}: {e}")
        return

    logging.debug(f"Loading tasks from {target}")

    for entry in entries:
        if entry.is_dir() or \
           not entry.name.lower().endswith(".py") or \
           entry.name.startswith("_"):
            continue

        stem = entry.name[:-3]
        if skip and stem in skip:
            continue

        module_name = f"{module_prefix}.{stem}"
        try:
            importlib.import_module(module_name)
            logging.info(f"Loaded task module: {module_name}")
        except Exception as e:
            logging.warning(f"Error importing task module {module_name}: {e}")

#-----------------------------------------------------------------------------
