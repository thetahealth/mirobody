"""`RefCollectingInterpreter`: the QuickJS `eval` tool with the citation
contract attached to its results.

`CodeInterpreterMiddleware` builds the model-visible result of an `eval` from
whatever the JS returns — usually `null` when the model fetched rows with
`tools.queryHealthIndicators(...)` and computed a number. The derived answer
then cites nothing, and the judge's citation check has no defined input
(plan-review §2.2). This subclass brackets every eval with a rid sink
(`agent/tools/_refs.py`): the readings tool reports each surfaced row's rid
into it, and the eval's result message is rewritten to the documented
`{result, refs}` shape before the library formats it.

Refs are exactly the rids surfaced in THIS eval call, in first-surfaced
order — never the conversation's accumulated set, because a value computed
here may only cite what this computation read. A returned object's own
`refs` key wins untouched: "return only what my computed value actually
used" is a narrowing the judge, not the harness, is there to grade. When the
model logs its numbers and returns nothing, the console TAIL (`console_tail`,
last 2000 chars) rides into the envelope beside the refs instead of being
invisible to the citation check.

The class overrides `_build_tool`, copying the body of langchain_quickjs
0.3.8's (the pinned version) with two marked deltas — the sink bracket and
the refs rewrite — because that method's closures are the only seam that
sees both the runtime and the outcome; upstream offers no hook between them.
`tests/agent/test_eval_refs.py` pins the behaviour so a silent divergence on
upgrade fails there, not in a chat.
"""

from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Annotated, Any

from langchain.tools import ToolRuntime
from langchain_core.messages import ToolMessage
from langchain_core.tools import StructuredTool
from langchain_quickjs import CodeInterpreterMiddleware
from langchain_quickjs._format import format_outcome
from langchain_quickjs._prompt import (
    render_eval_tool_code_doc,
    render_eval_tool_description,
)
from langchain_quickjs.middleware import EvalSchema

from mirobody.agent.tools._refs import (
    bracket_eval_rids,
    console_tail,
    is_object_shaped,
    refs_result_text,
    unbracket_eval_rids,
)


#: The model-facing citation contract, appended to the eval tool's
#: description. The prompt's own refs paragraph (both jinja templates) says
#: computed values cite the refs the eval returned; what it cannot say is
#: WHY silent arithmetic is uncheckable, and measured teacher behaviour
#: (pilot2/pilot3: ~90% of evals return null while the answer quotes numbers
#: the model computed in its reasoning channel) shows that part does not
#: land from prompt prose alone. Tool descriptions are the text models read
#: most reliably.
EVAL_CITATION_CONTRACT = (
    "CITATION CONTRACT. Every number your answer will quote must be visible "
    "in this tool's output: print it with `console.log(...)` or return it "
    "(returned values are wrapped as `{result, refs}`). Anything you compute "
    "silently in your reasoning cannot be cited and will be rejected as "
    "unverifiable. Before answering, ask: for each number I want to say, did "
    "an eval print or return it?"
)


class RefCollectingInterpreter(CodeInterpreterMiddleware):
    """A `CodeInterpreterMiddleware` whose eval results carry citation refs."""

    def _build_tool(self) -> StructuredTool:
        tool_name = self._tool_name
        max_chars = self._max_result_chars
        middleware = self
        code_doc = render_eval_tool_code_doc(mode=self._mode)
        tool_description = (
            render_eval_tool_description(mode=self._mode) + " " + EVAL_CITATION_CONTRACT
        )

        def _make_tool_message(
            outcome: Any,
            tool_call_id: str | None,
            refs: list[str],
        ) -> ToolMessage:
            # DELTA 2/2: on a successful eval, the result body becomes the
            # {result, refs} contract. An errored eval keeps the library's
            # <error> shape: a crashed computation cites nothing and the
            # error, not prose, is what the model must see. On the wrapped
            # branch (no object returned) the console TAIL rides into the
            # envelope as `console` and the <stdout> block goes — one copy of
            # the log, beside the refs; an object return keeps <stdout>,
            # because nothing of it rode the envelope.
            if outcome.error_type is None:
                console = "" if is_object_shaped(outcome.result) else console_tail(outcome.stdout)
                outcome = replace(
                    outcome,
                    result=refs_result_text(outcome.result, refs, console=console),
                    stdout="" if console else outcome.stdout,
                )
            return ToolMessage(
                content=format_outcome(outcome, max_result_chars=max_chars),
                tool_call_id=tool_call_id,
                name=tool_name,
            )

        def sync_eval(
            runtime: ToolRuntime[None, Any],
            code: Annotated[str, code_doc],
        ) -> ToolMessage:
            slot_id = middleware._slot_id(runtime.state)
            repl = middleware._repl_for_eval(slot_id)
            # DELTA 1/2: fresh sink per eval; whatever the readings tool
            # surfaces while it is set is this eval's citation refs.
            sink, token = bracket_eval_rids()
            try:
                outcome = repl.eval_sync(
                    code,
                    outer_runtime=runtime,
                )
            finally:
                unbracket_eval_rids(token)
                if middleware._mode == "call":
                    middleware._registry.reset_repl(slot_id)
            return _make_tool_message(outcome, runtime.tool_call_id, sink)

        async def async_eval(
            runtime: ToolRuntime[None, Any],
            code: Annotated[str, code_doc],
        ) -> ToolMessage:
            slot_id = middleware._slot_id(runtime.state)
            repl = middleware._repl_for_eval(slot_id)
            sink, token = bracket_eval_rids()
            try:
                outcome = await repl.eval_async(
                    code,
                    outer_runtime=runtime,
                    outer_loop=asyncio.get_running_loop(),
                )
            finally:
                unbracket_eval_rids(token)
                if middleware._mode == "call":
                    middleware._registry.reset_repl(slot_id)
            return _make_tool_message(outcome, runtime.tool_call_id, sink)

        # `from __future__ import annotations` keeps `code: Annotated[str,
        # code_doc]` a string; langgraph's `_get_all_injected_args` evaluates
        # it against module globals where the closure local `code_doc` does
        # not exist (NameError, every chat turn). Pin it to the eager object
        # upstream's future-import-free module gets for free.
        for fn in (sync_eval, async_eval):
            fn.__annotations__["code"] = Annotated[str, code_doc]

        return StructuredTool.from_function(
            name=tool_name,
            description=tool_description,
            func=sync_eval,
            coroutine=async_eval,
            infer_schema=False,
            args_schema=EvalSchema,
            metadata={"ls_code_input_language": "javascript"},
        )


__all__ = ["EVAL_CITATION_CONTRACT", "RefCollectingInterpreter"]
