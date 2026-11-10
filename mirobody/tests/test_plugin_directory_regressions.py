"""A configured path must retain the actual package's relative imports."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from uuid import uuid4

import pytest

pytest.importorskip("dotenv")
pytest.importorskip("ruamel.yaml")
pytest.importorskip("mcp")

import mirobody
from mirobody.mcp.tool import load_tools_from_directory
from mirobody.utils.plugin_dirs import import_plugin_module, resolve_plugin_dir

REPO = Path(mirobody.__file__).resolve().parent.parent
TOOLS = REPO / "mirobody/agent/tools"


@pytest.mark.parametrize("location", ["repo", "outside", "nested"])
@pytest.mark.parametrize("spelling", ["absolute", "trailing", "dotdot", "relative"])
def test_packaged_tool_schema_is_independent_of_cwd_and_path_spelling(monkeypatch, tmp_path, location, spelling):
    cwd = REPO if location == "repo" else tmp_path / location
    cwd.mkdir(exist_ok=True)
    monkeypatch.chdir(cwd)
    paths = {"absolute": str(TOOLS), "trailing": str(TOOLS) + os.sep,
             "dotdot": str(TOOLS / "../tools"), "relative": os.path.relpath(TOOLS, cwd)}
    canonical, expected = load_tools_from_directory("mirobody.agent.tools")
    actual, descriptions = load_tools_from_directory(paths[spelling])
    assert resolve_plugin_dir(paths[spelling])[1] == "mirobody.agent.tools"
    assert actual.keys() == canonical.keys()
    assert descriptions == expected
    assert all(info["instance"].__module__.startswith("mirobody.agent.tools.") for info in actual.values())


@pytest.fixture
def package(monkeypatch, tmp_path):
    host = tmp_path / "host"
    name = "loader_probe_" + uuid4().hex
    root = host / name
    directory = root / "plugins"
    directory.mkdir(parents=True)
    (root / "__init__.py").write_text("")
    (directory / "__init__.py").write_text("")
    (directory / "_shared.py").write_text("MARKER = 'relative-helper-loaded'\n")
    (directory / "probe.py").write_text("from ._shared import MARKER\n")
    monkeypatch.syspath_prepend(str(host))
    cwd = tmp_path / "working"
    cwd.mkdir()
    monkeypatch.chdir(cwd)
    return name, directory


def test_absolute_external_importable_package_keeps_relative_helper(package):
    name, path = package
    directory, dotted = resolve_plugin_dir(str(path))
    assert dotted == name + ".plugins"
    module_name, module = import_plugin_module(directory, dotted, "probe.py")
    assert module_name == name + ".plugins.probe"
    assert module.MARKER == "relative-helper-loaded"


def test_symlink_to_importable_package_keeps_canonical_context(package, tmp_path):
    name, path = package
    link = tmp_path / "alias"
    link.symlink_to(path, target_is_directory=True)
    directory, dotted = resolve_plugin_dir(str(link))
    assert Path(directory).samefile(path)
    assert dotted == name + ".plugins"
    assert import_plugin_module(directory, dotted, "probe.py")[1].MARKER == "relative-helper-loaded"


def test_existing_shadow_directory_cannot_select_an_imported_namesake(package, tmp_path):
    name, path = package
    # Importing the canonical package fixes what importlib can actually find.
    import_plugin_module(str(path), name + ".plugins", "probe.py")
    shadow = tmp_path / "shadow" / name / "plugins"
    shadow.mkdir(parents=True)
    (shadow.parent / "__init__.py").write_text("")
    (shadow / "__init__.py").write_text("")
    (shadow / "probe.py").write_text("MARKER = 'explicit-shadow-file'\n")
    directory, dotted = resolve_plugin_dir(str(shadow))
    assert Path(directory).samefile(shadow)
    assert dotted is None
    assert import_plugin_module(directory, dotted, "probe.py")[1].MARKER == "explicit-shadow-file"


def test_plain_external_files_keep_the_documented_fallback(tmp_path):
    directory = tmp_path / ("plain_" + uuid4().hex)
    directory.mkdir()
    (directory / "probe.py").write_text("MARKER = 'plain-file'\n")
    resolved, dotted = resolve_plugin_dir(str(directory))
    assert dotted is None and Path(resolved).samefile(directory)
    assert import_plugin_module(resolved, dotted, "probe.py")[1].MARKER == "plain-file"


def test_unimportable_package_does_not_extend_sys_path(monkeypatch, tmp_path):
    directory = tmp_path / "not-on-sys-path" / ("hidden_" + uuid4().hex) / "plugins"
    directory.mkdir(parents=True)
    (directory / "__init__.py").write_text("")
    (directory.parent / "__init__.py").write_text("")
    (directory / "probe.py").write_text("from ._shared import MARKER\n")
    (directory / "_shared.py").write_text("MARKER = 'hidden'\n")
    monkeypatch.chdir(tmp_path)
    before = sys.path[:]
    resolved, dotted = resolve_plugin_dir(str(directory))
    assert dotted is None and sys.path == before
    with pytest.raises((ImportError, ModuleNotFoundError)):
        import_plugin_module(resolved, dotted, "probe.py")


def test_explicit_nonexistent_path_is_not_a_package(tmp_path):
    assert resolve_plugin_dir(str(tmp_path / "absent")) == (None, None)


@pytest.mark.parametrize("outside", [False, True])
def test_regular_package_below_namespace_parent_keeps_imports(monkeypatch, tmp_path, outside):
    host = tmp_path / "namespace-host"
    namespace = "namespace_probe_" + uuid4().hex
    directory = host / namespace / "plugins"
    directory.mkdir(parents=True)
    (directory / "__init__.py").write_text("")
    (directory / "_shared.py").write_text("MARKER = 'namespace-relative-helper'\n")
    (directory / "probe.py").write_text("from ._shared import MARKER\n")
    monkeypatch.syspath_prepend(str(host))
    monkeypatch.chdir(tmp_path if outside else host)
    resolved, dotted = resolve_plugin_dir(str(directory))
    assert dotted == namespace + ".plugins"
    assert import_plugin_module(resolved, dotted, "probe.py")[1].MARKER == "namespace-relative-helper"


async def test_agent_directory_loader_uses_the_same_package_context(package, monkeypatch):
    from mirobody.agent import registry

    _, path = package
    (path / "probe.py").write_text("from ._shared import MARKER\n"
        "class LocalProbeAgent:\n"
        "    async def generate_response(self, *args, **kwargs):\n"
        "        yield {'type': 'text', 'text': MARKER}\n")
    monkeypatch.setattr(registry, "entry_point_modules", lambda _: [])
    monkeypatch.setattr(registry, "_agent_class", None)
    monkeypatch.setattr(registry, "_llm_clients", {})
    klass = registry.load_agent([str(path)])
    assert klass is not None
    assert [part async for part in klass().generate_response()] == [{"type": "text", "text": "relative-helper-loaded"}]


def test_task_directory_loader_imports_relative_helpers_without_running_jobs(package):
    from mirobody.task import iter_tasks
    from mirobody.task.loader import _load_tasks_from_directory

    name, path = package
    (path / "probe.py").write_text("from ._shared import MARKER\n"
        "from mirobody.task import BaseTask\n"
        "class LocalProbeTask(BaseTask):\n"
        f"    queue_key = '{name}'\n"
        "    probe_marker = MARKER\n")
    _load_tasks_from_directory(str(path))
    matches = [task for task in iter_tasks() if task.queue_key == name]
    try:
        assert len(matches) == 1 and matches[0].probe_marker == "relative-helper-loaded"
    finally:
        for task in matches:
            task.queue_key = ""
