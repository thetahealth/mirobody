"""`code_exec` with nothing in this turn to compute on says why, as a result.

Asked "画个图看看" right after a cholesterol lookup, MiniCPM5-2B sent the
numbers from the turn before to `code_exec`. The tool refused by raising, the
fault middleware (which never passes an exception's message on) turned that
into "check the tool's parameter list", and the model sent the same call five
times (2026-11-10, a local stack on the 1.6.0-fix.1 image).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

pytest.importorskip("deepagents")
pytest.importorskip("langchain")

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage

from mirobody.agent.code_tool import javascript_code, python_code
from mirobody.kernel import tools

#: The chart call the model made, numbers typed into the code.
TYPED = 'console.log(JSON.stringify([{time: "2025-03-10", value: 5.48}, {time: "2026-06-20", value: 4.9}]))'


def _follow_up() -> SimpleNamespace:
    """A lookup answered in the turn before, then a new question."""
    lookup = ToolMessage(content="rid|value", name="query_health_indicators", tool_call_id="lookup",
                         artifact=tools.Envelope(tools.STATUS_OK, data=[{"rid": "r1", "value": 5.48}]))
    return SimpleNamespace(state={"messages": [
        HumanMessage("我的总胆固醇这几次检查有什么变化？"), lookup, AIMessage("5.48 → 4.90 mmol/L"),
        HumanMessage("画个图看看"),
    ]})


@pytest.mark.parametrize("calculator", [javascript_code, python_code], ids=["javascript", "python"])
async def test_a_follow_up_without_a_lookup_is_told_what_to_do(calculator):
    text, artifact = await calculator.coroutine(code=TYPED, runtime=_follow_up())
    assert artifact == {"status": tools.STATUS_ERROR, "error_class": tools.ERROR_RECOVERABLE,
                        "error_kind": "no_source"}
    assert "no original result exists in this turn" in text
    assert "call the record tool again" in text and "chart block" in text
    assert "parameter list" not in text


async def test_a_failed_or_empty_lookup_is_said_too():
    for artifact, said in ((tools.invalid_arguments("synthetic"), "lookup failed"),
                           (tools.Envelope(tools.STATUS_OK, data=[]), "no rows")):
        lookup = ToolMessage(content="x", name="query_health_indicators", tool_call_id="t", artifact=artifact)
        text, result = await javascript_code.coroutine(
            code="data.length", runtime=SimpleNamespace(state={"messages": [HumanMessage("Calculate"), lookup]}))
        assert said in text and result["error_kind"] == "no_source"
