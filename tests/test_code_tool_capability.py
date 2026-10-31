"""Synthetic query-artifact -> Python regressions; Docker checks run when available."""

from __future__ import annotations

import asyncio
import json
import logging
import socketserver
import tempfile
import threading
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from types import SimpleNamespace

import pytest
from deepagents.backends.protocol import ExecuteResponse
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.prebuilt import ToolNode
from langgraph.graph import END, START, MessagesState, StateGraph

from mirobody.agent.python_sandbox import LocalPythonSandbox
from mirobody.agent.python_tool import python_exec, source_results
from mirobody.agent.tools._render import render_compact
from mirobody.kernel import tools


def message(rows, *, call_id="query-1", name="query_health_indicators", partial=False):
    envelope = tools.Envelope(
        tools.STATUS_PARTIAL if partial else tools.STATUS_OK,
        data=rows, meta=tools.Meta(row_count=len(rows), truncated=partial),
    )
    return ToolMessage(content=render_compact(envelope), artifact=envelope, name=name, tool_call_id=call_id)


class RecordingSandbox:
    inputs = None

    def __init__(self, **kwargs):
        pass

    def run(self, code, inputs):
        RecordingSandbox.inputs = inputs
        return ExecuteResponse(json.dumps(inputs["data"]), 0)

    def close(self):
        pass


@pytest.mark.parametrize("rows", [
    [{"rid": "r1", "indicator": "Weight", "value": 7.0, "unit": "kg"}],
    [{"rid": "r1", "indicator": "Weight", "value": 7.0, "unit": "kg"},
     {"rid": "r2", "indicator": "Weight", "value": 7.0, "unit": "kg"}],
    [{"rid": "r1", "rsid": "rs1", "GT": "0|1", "annotations": {"calls": [1, 2]}}],
])
async def test_hidden_runtime_uses_original_rows_after_rendering_or_eviction(monkeypatch, rows):
    from mirobody.agent import python_tool
    monkeypatch.setattr(python_tool, "LocalPythonSandbox", RecordingSandbox)
    source = message(rows)
    assert "(constants:" in source.content
    source.content = "Result was evicted; use read_file to see its display."
    call = AIMessage(content="", tool_calls=[{"name": "python_exec", "args": {"code": "print(data)"},
                                               "id": "compute-1", "type": "tool_call"}])
    graph = StateGraph(MessagesState)
    graph.add_node("tools", ToolNode([python_exec]))
    graph.add_edge(START, "tools")
    graph.add_edge("tools", END)
    result = await graph.compile().ainvoke({"messages": [HumanMessage("Calculate"), source, call]})
    output = result["messages"][-1]
    assert json.loads(output.content) == rows
    assert RecordingSandbox.inputs["data"] == rows
    assert output.artifact["source_tool_call_ids"] == ["query-1"]


async def test_multiple_source_tools_and_partial_metadata_are_preserved(monkeypatch):
    from mirobody.agent import python_tool
    monkeypatch.setattr(python_tool, "LocalPythonSandbox", RecordingSandbox)
    first = message([{"rid": "r1", "value": 7.0}], call_id="query-1")
    second = message([{"rid": "r2", "name": "Example medication", "slots": [{"taken": True}]}],
                     call_id="query-2", name="query_medications", partial=True)
    text, artifact = await python_exec.coroutine(
        code="print(data)", runtime=SimpleNamespace(state={"messages": [HumanMessage("Combine"), first, second]}),
    )
    assert RecordingSandbox.inputs["data"] == list(second.artifact.data)
    assert RecordingSandbox.inputs["results"][0]["data"] == list(first.artifact.data)
    assert RecordingSandbox.inputs["results"][1]["meta"]["truncated"]
    assert artifact["source_partial"]
    assert "Source data is partial" in text


def test_empty_failed_and_previous_turn_sources_are_refused():
    old = message([{"value": 1.0}])
    with pytest.raises(ValueError, match="no original"):
        source_results([old, HumanMessage("A new question")])
    with pytest.raises(ValueError, match="no rows"):
        source_results([HumanMessage("Calculate"), message([])])
    failed = ToolMessage(content="error", artifact=tools.invalid_arguments("synthetic refusal"),
                         name="query_health_indicators", tool_call_id="failed")
    with pytest.raises(ValueError, match="lookup failed"):
        source_results([HumanMessage("Calculate"), old, failed])


@pytest.mark.parametrize("name", ["__empty_answer__", "__last_model_call__"])
def test_harness_nudges_do_not_hide_this_turn_original_results(name):
    source = message([{"value": 7.0}])
    messages = [HumanMessage("Calculate"), source, HumanMessage("Use the source above", name=name)]
    assert source_results(messages)[0]["data"] == [{"value": 7.0}]
    with pytest.raises(ValueError, match="no original"):
        source_results([*messages, HumanMessage("A different question", name="person")])


def test_model_schema_contains_code_and_source_handles_without_runtime_or_table():
    properties = python_exec.tool_call_schema.model_json_schema()["properties"]
    assert set(properties) == {"code", "timeout_s"}


async def test_output_truncation_is_visible_to_the_model(monkeypatch):
    from mirobody.agent import python_tool

    class TruncatedSandbox(RecordingSandbox):
        def run(self, code, inputs):
            return ExecuteResponse("synthetic output", 0, True)

    monkeypatch.setattr(python_tool, "LocalPythonSandbox", TruncatedSandbox)
    text, artifact = await python_exec.coroutine(
        code="print(data)", runtime=SimpleNamespace(state={"messages": [HumanMessage("Calculate"),
                                                                         message([{"value": 1.0}])]}),
    )
    assert "Output truncated" in text and artifact["truncated"]


async def test_unavailable_executor_does_not_claim_the_record_is_empty(monkeypatch):
    from mirobody.agent import python_tool

    class UnavailableSandbox:
        def __init__(self, **kwargs):
            raise FileNotFoundError("synthetic missing executor")

    monkeypatch.setattr(python_tool, "LocalPythonSandbox", UnavailableSandbox)
    text, artifact = await python_exec.coroutine(
        code="print(data)", runtime=SimpleNamespace(state={"messages": [HumanMessage("Calculate"),
                                                                         message([{"value": 1.0}])]}),
    )
    assert artifact["error_kind"] == "unavailable"
    assert artifact["error_class"] == tools.ERROR_UNRECOVERABLE
    assert "readings remain valid" in text


async def test_agent_registers_one_python_tool_and_omits_it_when_disallowed(monkeypatch):
    from mirobody.agent.agent import MirobodyAgent
    from mirobody.agent import python_tool, tool_loader
    notes = []

    async def loaded(**kwargs):
        notes.append(kwargs["result_note"])
        return []

    monkeypatch.setattr(tool_loader, "load_global_tools", loaded)
    monkeypatch.setattr(python_tool, "executor_ready", lambda: True)
    agent = MirobodyAgent()
    assert [tool.name for tool in await agent._load_tools("synthetic")] == ["python_exec"]
    assert "original rows" in notes[-1]
    agent.disallowed_tools = ["python_exec"]
    assert await agent._load_tools("synthetic") == []
    assert notes[-1] == ""


class _FakeDaemon(BaseHTTPRequestHandler):
    """The two requests a Docker client makes before any work: the API
    version it negotiates on connect, and the ping."""

    def do_GET(self):
        body = json.dumps({"ApiVersion": "1.47", "Version": "27.0.0"}).encode() \
            if self.path.endswith("/version") else b"OK"
        self.send_response(200)
        self.send_header("Content-Type", "application/json" if body != b"OK" else "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass


@pytest.fixture
def socket_dir():
    # Under /tmp, not tmp_path: macOS caps a Unix socket path at 104 bytes.
    with tempfile.TemporaryDirectory(dir="/tmp") as directory:
        yield Path(directory)


@pytest.fixture
def fresh_probe():
    from mirobody.agent import python_tool
    python_tool.executor_ready.cache_clear()
    yield python_tool
    python_tool.executor_ready.cache_clear()


async def _offered(monkeypatch):
    from mirobody.agent.agent import MirobodyAgent
    from mirobody.agent import tool_loader
    notes = []

    async def loaded(**kwargs):
        notes.append(kwargs["result_note"])
        return []

    monkeypatch.setattr(tool_loader, "load_global_tools", loaded)
    agent = MirobodyAgent()
    names = [[tool.name for tool in await agent._load_tools("synthetic")] for _ in range(2)]
    return names, notes


async def test_python_exec_is_left_out_without_a_docker_socket(monkeypatch, socket_dir, fresh_probe, caplog):
    monkeypatch.setenv("PYTHON_SANDBOX_DOCKER_HOST", f"unix://{socket_dir / 'docker.sock'}")
    with caplog.at_level(logging.WARNING, logger=fresh_probe.__name__):
        names, notes = await _offered(monkeypatch)
    assert names == [[], []]
    assert notes == ["", ""]
    reasons = [r.getMessage() for r in caplog.records if "python_exec left out" in r.getMessage()]
    assert len(reasons) == 1, "probed once per process, not once per turn"
    assert "error_type=" in reasons[0]


async def test_python_exec_is_offered_when_a_docker_socket_answers(monkeypatch, socket_dir, fresh_probe):
    path = socket_dir / "docker.sock"
    server = socketserver.UnixStreamServer(str(path), _FakeDaemon)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        monkeypatch.setenv("PYTHON_SANDBOX_DOCKER_HOST", f"unix://{path}")
        names, notes = await _offered(monkeypatch)
    finally:
        server.shutdown()
        server.server_close()
    assert names == [["python_exec"], ["python_exec"]]
    assert all("original rows" in note for note in notes)


def test_the_startup_hook_probes_unless_the_tool_is_disallowed(monkeypatch):
    from mirobody.agent import agent as agent_module, python_tool
    probed = []
    monkeypatch.setattr(python_tool, "executor_ready", lambda: probed.append(True) or True)
    monkeypatch.setattr(agent_module, "build_llm_clients", lambda *args, **kwargs: {})

    class Settings:
        disallowed = []

        def get_agent_settings(self):
            return {"disallowed_tools": self.disallowed}

    settings = Settings()
    monkeypatch.setattr(agent_module, "global_config", lambda: settings)
    agent_module.MirobodyAgent.load_llm_clients({})
    assert probed == [True]
    settings.disallowed = ["python_exec"]
    agent_module.MirobodyAgent.load_llm_clients({})
    assert probed == [True]


@pytest.mark.parametrize("offered", [True, False])
async def test_the_prompt_names_the_calculator_only_when_it_is_offered(offered):
    from mirobody.agent.prompt import build_system_prompt
    template = (Path(__file__).resolve().parents[1] / "mirobody" / "agent" / "prompts" / "mirobody.jinja").read_text()
    prompt = await build_system_prompt(template, [python_exec] if offered else [], "Mirobody")
    assert ("python_exec" in prompt) is offered
    assert ("code tool" in prompt) is offered
    assert ("# Computation" in prompt) is offered
    # The direction rule once sat inside the computation section, so it went
    # missing with the calculator.
    assert "上升/下降/持平" in prompt


@pytest.fixture
def local_docker():
    try:
        sandbox = LocalPythonSandbox(timeout=3)
        sandbox._client.ping()
        sandbox._client.images.get(sandbox.image)
    except Exception as exc:
        pytest.skip(f"local Docker/image unavailable ({type(exc).__name__})")
    return sandbox


def test_local_python_calculates_original_numeric_values_and_tears_down(local_docker):
    result = local_docker.run("print(sum(row['value'] for row in data))",
                             {"data": [{"value": 7.0}, {"value": 7.0}], "results": []})
    assert result.exit_code == 0 and result.output.strip() == "14.0"
    assert not local_docker._client.containers.list(all=True, filters={"name": local_docker.id})


def test_local_python_timeout_removes_the_container(local_docker):
    local_docker.timeout = 1
    result = local_docker.run("import time; time.sleep(3)", {"data": [], "results": []})
    assert result.exit_code in (124, 137)
    assert not local_docker._client.containers.list(all=True, filters={"name": local_docker.id})


def test_local_python_streams_a_bounded_output(local_docker):
    local_docker.max_output = 100
    result = local_docker.run("print('x' * 10000)", {"data": [], "results": []})
    assert result.exit_code == 0 and result.truncated
    assert len(result.output.encode()) == 100


def test_remote_docker_endpoints_are_refused(monkeypatch):
    from mirobody.agent.python_sandbox import _local_endpoint
    monkeypatch.setenv("PYTHON_SANDBOX_DOCKER_HOST", "tcp://example.invalid:2375")
    with pytest.raises(ValueError, match="local Unix-socket"):
        _local_endpoint()


async def test_cancellation_closes_the_inflight_executor(monkeypatch):
    from mirobody.agent import python_tool
    started = asyncio.Event()
    closed = asyncio.Event()

    class BlockingSandbox(RecordingSandbox):
        def run(self, code, inputs):
            return ExecuteResponse("unused", 0)

        def close(self):
            pass

    async def threaded(func, *args, **kwargs):
        if func.__name__ == "run":
            started.set()
            await closed.wait()
            return ExecuteResponse("cancelled", 1)
        result = func(*args, **kwargs)
        if func.__name__ == "close":
            closed.set()
        return result

    monkeypatch.setattr(python_tool, "LocalPythonSandbox", BlockingSandbox)
    monkeypatch.setattr(python_tool.asyncio, "to_thread", threaded)
    task = asyncio.create_task(python_exec.coroutine(
        code="print(data)", runtime=SimpleNamespace(state={"messages": [HumanMessage("Calculate"),
                                                                         message([{"value": 1.0}])]}),
    ))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()
