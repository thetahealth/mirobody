"""Directory spelling must not decide whether a plugin loads.

The rule these functions replace was `directory.replace(os.sep, ".")` guarded by
`removeprefix(os.getcwd())`, which made the SAME directory load one tool spelled
`mytools` and zero spelled `/private/tmp/host/mytools`. Three loaders shared it
(MCP tools, chat agents, background tasks) and all three told users in the docs
to "add your own directory".
"""

from __future__ import annotations

import os
import textwrap

import pytest

from mirobody.utils.plugin_dirs import import_plugin_module, resolve_plugin_dir


@pytest.fixture
def plugin_dir(tmp_path):
    d = tmp_path / "acme_tools"
    d.mkdir()
    (d / "probe.py").write_text(
        textwrap.dedent(
            """
            MARKER = "loaded-by-file"

            def probe(x: str) -> dict:
                return {"echo": x}
            """
        ),
        encoding="utf-8",
    )
    return d


#--- resolve ------------------------------------------------------------------


def test_an_importable_package_keeps_its_dotted_name(monkeypatch):
    """Package semantics matter: a packaged plugin may use relative imports."""
    monkeypatch.chdir(_repo_root())
    directory, dotted = resolve_plugin_dir("mirobody/agent/tools")
    assert dotted == "mirobody.agent.tools"
    assert directory and directory.endswith(os.path.join("agent", "tools"))


def test_a_dotted_name_resolves_as_well_as_a_path(monkeypatch):
    monkeypatch.chdir(_repo_root())
    by_path = resolve_plugin_dir("mirobody/agent/tools")
    by_name = resolve_plugin_dir("mirobody.agent.tools")
    assert by_path == by_name


def test_an_outside_directory_resolves_with_no_dotted_name(plugin_dir):
    directory, dotted = resolve_plugin_dir(str(plugin_dir))
    assert dotted is None, "not an importable package — must fall back to file loading"
    assert directory == str(plugin_dir)


def test_the_same_directory_resolves_however_it_is_spelled(plugin_dir, monkeypatch):
    """The actual defect: absolute vs relative used to give different answers."""
    monkeypatch.chdir(plugin_dir.parent)
    absolute, _ = resolve_plugin_dir(str(plugin_dir))
    relative, _ = resolve_plugin_dir("acme_tools")
    assert os.path.realpath(absolute) == os.path.realpath(relative)


def test_nothing_usable_resolves_to_nothing():
    assert resolve_plugin_dir("/nope/not/here") == (None, None)
    assert resolve_plugin_dir("") == (None, None)
    assert resolve_plugin_dir("   ") == (None, None)


#--- import -------------------------------------------------------------------


def test_a_file_outside_any_package_still_imports(plugin_dir):
    name, module = import_plugin_module(str(plugin_dir), None, "probe.py")
    assert module.MARKER == "loaded-by-file"
    assert name == "acme_tools.probe", "the key stays readable, not a synthetic token"


def test_a_packaged_module_imports_by_package_path(monkeypatch):
    monkeypatch.chdir(_repo_root())
    directory, dotted = resolve_plugin_dir("mirobody/agent/tools")
    name, module = import_plugin_module(directory, dotted, "terminology_service.py")
    assert name == "mirobody.agent.tools.terminology_service"
    assert module.__name__ == name


def test_a_missing_file_raises_rather_than_returning_none(plugin_dir):
    with pytest.raises(Exception):
        import_plugin_module(str(plugin_dir), None, "absent.py")


#--- the loaders that use them ------------------------------------------------


def test_tool_loader_reads_a_directory_outside_the_package(plugin_dir):
    """End to end: this returned zero tools before, with only a warning."""
    from mirobody.mcp.tool import load_tools_from_directory

    (plugin_dir / "probe.py").write_text(
        textwrap.dedent(
            '''
            def acme_probe(x: str) -> dict:
                """Probe.

                Args:
                    x: anything.

                Returns:
                    echo
                """
                return {"echo": x}
            '''
        ),
        encoding="utf-8",
    )
    tools, _ = load_tools_from_directory(str(plugin_dir))
    assert tools, "an external tool directory must load — MCP_TOOL_DIRS documents it"


def test_tool_loader_still_reads_the_packaged_directory(monkeypatch):
    from mirobody.mcp.tool import load_tools_from_directory

    monkeypatch.chdir(_repo_root())
    tools, _ = load_tools_from_directory("mirobody/agent/tools")
    assert len(tools) >= 4, f"packaged tools regressed: {sorted(tools)}"


def _repo_root() -> str:
    import mirobody

    return os.path.dirname(os.path.dirname(os.path.abspath(mirobody.__file__)))
