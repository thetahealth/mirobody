"""Python on original tool artifacts, with runtime injection instead of row copying.

This tool belongs to chat, outside the directory scanned for the public MCP
surface. Authentication and database access remain in the existing record
tools; the calculator gets only results those tools already returned.
"""

from __future__ import annotations

import asyncio
import functools
import logging
from dataclasses import asdict

from langchain.tools import ToolRuntime
from langchain_core.messages import ToolMessage
from langchain_core.tools import tool
from deepagents.backends.protocol import ExecuteResponse

from mirobody.kernel import tools
from .models.messages import user_turn_start
from .python_sandbox import LocalPythonSandbox, ping

logger = logging.getLogger(__name__)

COMPUTATION_NOTE = (
    "The original rows from this call are already available as data in the code tool. "
    "Use data directly; do not redefine it or copy the numbers. "
    "Inside the computation, /workspace/data.json also contains the original JSON object "
    "with data and results. Earlier lookups are available in results as full original envelopes."
)


@functools.cache
def executor_ready() -> bool:
    """Whether a local Docker daemon answered, decided once per process.

    `MirobodyAgent` offers `python_exec` only when one did. The default
    compose stack mounts no Docker socket into the app; offering the tool
    there made every call answer "unavailable" (2026-10-30), and a model that
    then did the arithmetic itself got a division wrong and carried it into
    the next turn. A daemon that comes up after the app needs a restart.
    """
    try:
        ping()
    except Exception as exc:
        logger.warning("python_exec left out: no local Docker daemon answered: error_type=%s "
                       "(docs/python-analysis.md)", type(exc).__name__)
        return False
    return True


def source_results(messages: list, *, computation_tool: str = "python_exec") -> list[dict]:
    """Original results in this turn, in call order, including their methodology."""
    start = user_turn_start(messages)
    available = [
        {"tool": message.name, "tool_call_id": message.tool_call_id, **asdict(message.artifact)}
        for message in messages[start:]
        if isinstance(message, ToolMessage) and isinstance(message.artifact, tools.Envelope)
        and message.name != computation_tool
    ]
    if not available:
        raise ValueError("Call a record tool before computing; no original result exists in this turn")
    if available[-1]["status"] == tools.STATUS_ERROR:
        raise ValueError("A source lookup failed; obtain a successful result before computing")
    if not available[-1]["data"]:
        raise ValueError("The source returned no rows; missing readings must not become a zero result")
    return [result for result in available if result["status"] != tools.STATUS_ERROR]


@tool(response_format="content_and_artifact")
async def python_exec(code: str, runtime: ToolRuntime, timeout_s: int = 15) -> tuple[str, dict]:
    """Compute in local Python on original results from record tools.

    Query first, then provide only Python code; do not copy a table or its
    numbers. data contains the original rows of the most recent record tool
    result, with numeric types, all columns and citation rids preserved.
    results contains all successful envelopes from this turn, including data,
    meta, tool and tool_call_id; use results to combine earlier lookups. This works for
    health indicators, medications, genetics and pharmacogenomics alike.
    Inspect results[i]['meta']['truncated'] before interpreting a total or
    mean as complete. Preserve the source rows' rids with computed numbers.
    Use print(...) to return results. The Python standard library is
    available; execution has no network, host files or application credentials.
    Inside Python, /workspace/data.json is the original JSON object with data
    and results; you may load that file instead of using the injected variables.
    For example: values = [float(row['value']) for row in data]; print(sum(values) / len(values)).
    """
    results = source_results(runtime.state.get("messages", []))
    inputs = {"data": results[-1]["data"], "results": results}
    seconds = max(1, min(int(timeout_s or 15), 30))
    try:
        sandbox = await asyncio.to_thread(LocalPythonSandbox, timeout=seconds)
    except Exception as exc:
        logger.warning("local Python executor unavailable: error_type=%s", type(exc).__name__)
        return ("error: local Python execution is unavailable. The queried readings remain valid; "
                "this is an executor setup problem, not missing data."), {
            "status": tools.STATUS_ERROR, "error_class": tools.ERROR_UNRECOVERABLE,
            "error_kind": "unavailable", "exception_type": type(exc).__name__,
        }
    task = asyncio.create_task(asyncio.to_thread(sandbox.run, code, inputs))
    try:
        result = await asyncio.wait_for(asyncio.shield(task), seconds + 10)
    except TimeoutError:
        await asyncio.shield(asyncio.to_thread(sandbox.close))
        task.add_done_callback(lambda completed: completed.exception() if not completed.cancelled() else None)
        result = ExecuteResponse("error: Python computation timed out", 124)
    except asyncio.CancelledError:
        await asyncio.shield(asyncio.to_thread(sandbox.close))
        task.add_done_callback(lambda completed: completed.exception() if not completed.cancelled() else None)
        raise
    text = result.output.strip() or "(no output; use print(...))"
    if result.exit_code in (124, 137):
        logger.warning("python computation timed out: duration_limit_s=%s", seconds)
        text = "error: Python computation timed out"
    elif result.exit_code != 0:
        text = f"error (exit {result.exit_code}): {text}"
    if result.truncated:
        text += "\n[Output truncated; print fewer rows or a smaller summary.]"
    partial = any(r["meta"]["truncated"] or r["status"] == tools.STATUS_PARTIAL for r in results)
    if partial:
        text = "[Source data is partial; check each result's metadata before describing a complete aggregate.]\n" + text
    return text, {"status": tools.STATUS_ERROR if result.exit_code != 0 else tools.STATUS_OK,
                  "exit_code": result.exit_code, "truncated": result.truncated, "source_partial": partial,
                  "source_tool_call_ids": [r["tool_call_id"] for r in results]}
