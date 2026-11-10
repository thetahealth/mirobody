"""Synthetic query-artifact -> code_exec regressions, in both engines; Docker checks run when available."""

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
from mirobody.agent.code_tool import javascript_code, python_code, source_results
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
    from mirobody.agent import code_tool
    monkeypatch.setattr(code_tool, "LocalPythonSandbox", RecordingSandbox)
    source = message(rows)
    assert "(constants:" in source.content
    source.content = "Result was evicted; use read_file to see its display."
    call = AIMessage(content="", tool_calls=[{"name": "code_exec", "args": {"code": "print(data)"},
                                               "id": "compute-1", "type": "tool_call"}])
    graph = StateGraph(MessagesState)
    graph.add_node("tools", ToolNode([python_code]))
    graph.add_edge(START, "tools")
    graph.add_edge("tools", END)
    result = await graph.compile().ainvoke({"messages": [HumanMessage("Calculate"), source, call]})
    output = result["messages"][-1]
    assert json.loads(output.content) == rows
    assert RecordingSandbox.inputs["data"] == rows
    assert output.artifact["source_tool_call_ids"] == ["query-1"]


async def test_multiple_source_tools_and_partial_metadata_are_preserved(monkeypatch):
    from mirobody.agent import code_tool
    monkeypatch.setattr(code_tool, "LocalPythonSandbox", RecordingSandbox)
    first = message([{"rid": "r1", "value": 7.0}], call_id="query-1")
    second = message([{"rid": "r2", "name": "Example medication", "slots": [{"taken": True}]}],
                     call_id="query-2", name="query_medications", partial=True)
    text, artifact = await python_code.coroutine(
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


@pytest.mark.parametrize("engine", [python_code, javascript_code])
def test_model_schema_contains_code_and_source_handles_without_runtime_or_table(engine):
    assert engine.name == "code_exec"
    properties = engine.tool_call_schema.model_json_schema()["properties"]
    assert set(properties) == {"code", "timeout_s"}


async def test_output_truncation_is_visible_to_the_model(monkeypatch):
    from mirobody.agent import code_tool

    class TruncatedSandbox(RecordingSandbox):
        def run(self, code, inputs):
            return ExecuteResponse("synthetic output", 0, True)

    monkeypatch.setattr(code_tool, "LocalPythonSandbox", TruncatedSandbox)
    text, artifact = await python_code.coroutine(
        code="print(data)", runtime=SimpleNamespace(state={"messages": [HumanMessage("Calculate"),
                                                                         message([{"value": 1.0}])]}),
    )
    assert "Output truncated" in text and artifact["truncated"]


async def test_unavailable_executor_does_not_claim_the_record_is_empty(monkeypatch):
    from mirobody.agent import code_tool

    class UnavailableSandbox:
        def __init__(self, **kwargs):
            raise FileNotFoundError("synthetic missing executor")

    monkeypatch.setattr(code_tool, "LocalPythonSandbox", UnavailableSandbox)
    text, artifact = await python_code.coroutine(
        code="print(data)", runtime=SimpleNamespace(state={"messages": [HumanMessage("Calculate"),
                                                                         message([{"value": 1.0}])]}),
    )
    assert artifact["error_kind"] == "unavailable"
    assert artifact["error_class"] == tools.ERROR_UNRECOVERABLE
    assert "readings remain valid" in text


def _turn(rows=None, *more):
    return SimpleNamespace(state={"messages": [HumanMessage("Calculate"),
                                               message(rows or [{"rid": "r1", "value": 7.0}]), *more]})


async def test_javascript_computes_on_the_original_rows_in_process():
    rows = [{"rid": "r1", "value": 7.0}, {"rid": "r2", "value": 8.5}]
    text, artifact = await javascript_code.coroutine(
        code="const v = data.map(r => r.value);\n"
             "console.log(JSON.stringify({mean: v.reduce((a, b) => a + b) / v.length, rids: data.map(r => r.rid)}))",
        runtime=_turn(rows),
    )
    assert json.loads(text) == {"mean": 7.75, "rids": ["r1", "r2"]}
    assert artifact["status"] == tools.STATUS_OK and artifact["source_tool_call_ids"] == ["query-1"]


async def test_javascript_sees_every_source_and_the_partial_flag():
    first = message([{"rid": "r1", "value": 7.0}], call_id="query-1")
    second = message([{"rid": "r2", "name": "Example medication"}], call_id="query-2",
                     name="query_medications", partial=True)
    text, artifact = await javascript_code.coroutine(
        code="console.log(results.length, results[1].meta.truncated, data[0].rid)",
        runtime=SimpleNamespace(state={"messages": [HumanMessage("Combine"), first, second]}),
    )
    assert text.endswith("2 true r2")
    assert "Source data is partial" in text and artifact["source_partial"]


@pytest.mark.parametrize("code, expected", [
    ("data.length * 2", "2"),                       # the last expression's value
    ("return data[0].value", "7"),                  # a top-level return, run as a function body
    ("console.log(typeof fetch, typeof require, typeof process)", "undefined undefined undefined"),
])
async def test_javascript_answers_the_way_a_model_writes_it(code, expected):
    text, artifact = await javascript_code.coroutine(code=code, runtime=_turn())
    assert text == expected and artifact["exit_code"] == 0


@pytest.mark.parametrize("code, exit_code, shown", [
    ("throw new Error('synthetic failure')", 1, "error (exit 1): Error: synthetic failure"),
    ("const x = ;", 1, "SyntaxError"),
    ("while (true) {}", 124, "error: JavaScript computation timed out"),
])
async def test_javascript_failures_reach_the_model_as_errors(code, exit_code, shown):
    text, artifact = await javascript_code.coroutine(code=code, runtime=_turn(), timeout_s=1)
    assert shown in text
    assert artifact["exit_code"] == exit_code and artifact["status"] == tools.STATUS_ERROR


def test_javascript_output_is_bounded():
    from mirobody.agent import js_sandbox
    result = js_sandbox.run("console.log('x'.repeat(10000))", {"data": [], "results": []}, timeout=5, max_output=100)
    assert result.exit_code == 0 and result.truncated and len(result.output) == 100


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
    from mirobody.agent import code_tool
    code_tool.executor_ready.cache_clear()
    yield code_tool
    code_tool.executor_ready.cache_clear()


@pytest.fixture
def daemon(socket_dir, monkeypatch):
    """A socket that answers like a Docker daemon, at the path the sandbox reads."""
    path = socket_dir / "docker.sock"
    server = socketserver.UnixStreamServer(str(path), _FakeDaemon)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setenv("PYTHON_SANDBOX_DOCKER_HOST", f"unix://{path}")
    yield path
    server.shutdown()
    server.server_close()


@pytest.fixture
def no_daemon(socket_dir, monkeypatch):
    monkeypatch.setenv("PYTHON_SANDBOX_DOCKER_HOST", f"unix://{socket_dir / 'docker.sock'}")


async def _offered(monkeypatch, *, local: bool, turns: int = 2):
    from mirobody.agent.agent import MirobodyAgent
    from mirobody.agent import prompt, tool_loader
    notes = []

    async def loaded(**kwargs):
        notes.append(kwargs["result_note"])
        return []

    monkeypatch.setattr(tool_loader, "load_global_tools", loaded)
    monkeypatch.setattr(prompt, "runs_locally", lambda alias: local)
    agent = MirobodyAgent()
    offered = [await agent._load_tools("synthetic", model="synthetic") for _ in range(turns)]
    return [[(tool.name, tool.metadata["language"]) for tool in tools] for tools in offered], notes


async def test_a_cloud_model_computes_in_javascript_without_asking_docker(monkeypatch, fresh_probe):
    pinged = []
    monkeypatch.setattr(fresh_probe, "ping", lambda: pinged.append(True))
    offered, notes = await _offered(monkeypatch, local=False)
    assert offered == [[("code_exec", "JavaScript")]] * 2
    assert all("original rows" in note for note in notes)
    assert not pinged


async def test_a_local_model_computes_in_python_when_a_docker_socket_answers(monkeypatch, fresh_probe, daemon):
    offered, _ = await _offered(monkeypatch, local=True)
    assert offered == [[("code_exec", "Python")]] * 2


async def test_a_local_model_without_a_docker_socket_computes_in_javascript(monkeypatch, fresh_probe, no_daemon,
                                                                            caplog):
    with caplog.at_level(logging.WARNING, logger=fresh_probe.__name__):
        offered, _ = await _offered(monkeypatch, local=True)
    assert offered == [[("code_exec", "JavaScript")]] * 2
    reasons = [r.getMessage() for r in caplog.records if "runs JavaScript for local models" in r.getMessage()]
    assert len(reasons) == 1, "probed once per process, not once per turn"
    assert "error_type=" in reasons[0]


async def test_disallowing_code_exec_removes_the_calculator_and_its_note(monkeypatch, fresh_probe):
    from mirobody.agent.agent import MirobodyAgent
    from mirobody.agent import tool_loader
    notes = []

    async def loaded(**kwargs):
        notes.append(kwargs["result_note"])
        return []

    monkeypatch.setattr(tool_loader, "load_global_tools", loaded)
    agent = MirobodyAgent(disallowed_tools=["code_exec"])
    assert await agent._load_tools("synthetic") == []
    assert notes == [""]


@pytest.mark.parametrize("old", ["python_exec", "eval"])
def test_an_earlier_calculator_name_in_a_tool_list_still_hides_it(old, caplog):
    from mirobody.utils.config.agent_options import tool_names
    with caplog.at_level(logging.WARNING):
        assert tool_names([old, "code_exec", "query_medications"]) == ["code_exec", "query_medications"]
    assert any(old in r.getMessage() and "code_exec" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("base_url, llm_type, local", [
    ("http://host.docker.internal:18097/v1", "openai", True),
    ("http://127.0.0.1:8080/v1", "openai", True),
    ("http://192.168.1.20:11434/v1", "openai", True),
    ("https://openrouter.ai/api/v1", "openai", False),
    ("", "anthropic", False),
])
def test_where_a_model_runs_picks_its_engine(monkeypatch, base_url, llm_type, local):
    from mirobody.agent import prompt
    monkeypatch.setattr(prompt, "_chat_route", lambda alias: ("synthetic", base_url, llm_type))
    assert prompt.runs_locally("synthetic") is local


def test_an_entry_with_no_route_counts_as_remote(monkeypatch):
    from mirobody.agent import prompt
    monkeypatch.setattr(prompt, "_chat_route", lambda alias: None)
    assert prompt.runs_locally("synthetic") is False


@pytest.mark.parametrize("local, disallowed, probes", [
    (True, [], 1),
    (False, [], 0),                # no local model: the daemon is never asked
    (True, ["code_exec"], 0),
])
def test_the_startup_hook_probes_only_for_a_local_model(monkeypatch, local, disallowed, probes):
    from mirobody.agent import agent as agent_module, code_tool, prompt
    probed = []
    monkeypatch.setattr(code_tool, "executor_ready", lambda: probed.append(True) or True)
    monkeypatch.setattr(prompt, "runs_locally", lambda alias: local)
    monkeypatch.setattr(agent_module, "build_llm_clients", lambda *args, **kwargs: {})
    monkeypatch.setattr(agent_module, "global_config",
                        lambda: SimpleNamespace(get_agent_settings=lambda: {"disallowed_tools": disallowed}))
    agent_module.MirobodyAgent.load_llm_clients({"synthetic": {}})
    assert len(probed) == probes


@pytest.mark.parametrize("engine, write", [(python_code, "`print(...)`"), (javascript_code, "`console.log(...)`"),
                                           (None, None)])
async def test_the_prompt_names_the_calculator_and_its_language_only_when_offered(engine, write):
    from mirobody.agent.prompt import build_system_prompt
    template = (Path(__file__).resolve().parents[1] / "mirobody" / "agent" / "prompts" / "mirobody.jinja").read_text()
    prompt = await build_system_prompt(template, [engine] if engine else [], "Mirobody")
    assert ("code_exec" in prompt) is bool(engine)
    assert ("# Computation" in prompt) is bool(engine)
    assert "python_exec" not in prompt
    if engine:
        assert f"`code_exec` with {engine.metadata['language']}\ncode only" in prompt
        assert write in prompt
    # The direction rule once sat inside the computation section, so it went
    # missing with the calculator; spelled in Chinese, it leaked into English
    # answers.
    assert "rose, fell or unchanged" in prompt and "下降" not in prompt


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
    from mirobody.agent import code_tool
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

    monkeypatch.setattr(code_tool, "LocalPythonSandbox", BlockingSandbox)
    monkeypatch.setattr(code_tool.asyncio, "to_thread", threaded)
    task = asyncio.create_task(python_code.coroutine(
        code="print(data)", runtime=SimpleNamespace(state={"messages": [HumanMessage("Calculate"),
                                                                         message([{"value": 1.0}])]}),
    ))
    await started.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert closed.is_set()
