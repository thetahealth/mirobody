"""Keep genotype tool rows out of DeepAgents summarization and history files."""

from __future__ import annotations

from typing import Any
from contextlib import contextmanager
from uuid import uuid4

from deepagents.middleware.summarization import (
    SUMMARIZATION_EVENT_KEY,
    SUMMARIZATION_SESSION_ID_KEY,
    SummarizationMiddleware as DeepSummarizationMiddleware,
    compute_summarization_defaults,
)
from langchain.agents.middleware import ModelRequest
from langchain.agents.middleware.summarization import _summary_config
from langchain_core.exceptions import ContextOverflowError
from langchain_core.messages import HumanMessage
from langgraph.config import get_stream_writer

from mirobody.agent.wire.blocks import CONTEXT

from mirobody.agent.middleware.genotype_row_guard import (
    GenotypeRowGuardMiddleware,
    redact_genotype_history,
)

_SAFE_SUMMARY = "Earlier conversation context was omitted because it may include genotype results. Query the active upload again."
_SAFE_MARKER = "genotype_rows_redacted"


#: A model with at most this input window is a local one (the `local` entry
#: declares 48000; on a 32k server the agent narrows it to 24576).
_LOCAL_WINDOW = 65536


@contextmanager
def _summary_activity():
    """Expose actual work without sending summary contents to the browser."""
    try:
        writer = get_stream_writer()
    except RuntimeError:
        # Direct library invocations have no graph stream to notify.
        def writer(event):
            return None
    event = {"type": CONTEXT, "phase": "summarization", "id": str(uuid4())}
    writer({**event, "status": "started"})
    try:
        yield
    except BaseException:
        writer({**event, "status": "failed"})
        raise
    else:
        # Generation completed; committed cutoffs remain in usage.context.
        writer({**event, "status": "completed"})


def _is_local(model: Any) -> bool:
    window = (getattr(model, "profile", None) or {}).get("max_input_tokens")
    return isinstance(window, int) and window <= _LOCAL_WINDOW


def history_path_for(model: Any, file_path: str | None) -> str | None:
    """The history file a compaction summary names: none for a local model.
    Told the path, MiniCPM5-2B read it page by page, each read compacting the
    context again and lengthening the file (270 to 1,653 lines) until the turn
    ran out. No cloud model read it in the 570 answers of 2026-10-10."""
    # A local model gets the path too. Without it a compacted conversation has no
    # way back to a detail the summary dropped: the long-context turn ended with
    # no answer at all. The page-reading loop the original comment measured is
    # bounded in the prompt now - one page, only when the summary lacks what the
    # question needs - and ``LOCAL_HISTORY_OFFLOAD=off`` restores the old rule.
    import os as _os

    if _is_local(model) and _os.environ.get("LOCAL_HISTORY_OFFLOAD", "on").lower() == "off":
        return None
    return file_path


#: A local window at least this wide keeps a quarter of itself after compaction.
_WIDE_WINDOW = 40000


def _defaults(model: Any) -> dict:
    """deepagents' thresholds, but a wide local window (the GPU preset's 48k)
    keeps a quarter of itself, 12k, not a tenth: at a tenth the report pages
    the turn had just read were gone, and the model read them again. A narrow
    one keeps the tenth. On the CPU preset's 24576 the input budget, 0.95 of
    the window less the 6144-token reply, is about 17k beside a 9k prompt; a
    quarter left no room for a report page and deepagents raised
    ContextOverflowError (2026-10-12)."""
    defaults = compute_summarization_defaults(model)
    if _is_local(model) and model.profile["max_input_tokens"] >= _WIDE_WINDOW:
        defaults = {**defaults, "keep": ("tokens", model.profile["max_input_tokens"] // 4)}
    return defaults


class GenotypeSafeSummarizationMiddleware(DeepSummarizationMiddleware):
    """Preserve selected history within the summarizer's actual input budget.

    Trigger and keep policies remain DeepAgents' defaults for the model. Its
    separate 4000-token tail trim discarded early evidence before the summary
    model could read it. Selected history is now complete or the call fails
    explicitly; oversized histories are not recursively summarized here.
    """

    def __init__(self, model: Any, backend: Any, guard: GenotypeRowGuardMiddleware) -> None:
        super().__init__(model=model, backend=backend, trim_tokens_to_summarize=None, **_defaults(model))
        self._genotype_guard = guard

    @property
    def name(self) -> str:
        # DeepAgents replaces a core middleware only when the names match.
        return "SummarizationMiddleware"

    def _safe_request(self, request: Any) -> Any:
        messages, *_ = self._genotype_guard._guard_messages(request.messages)
        messages = [
            HumanMessage(content=_SAFE_SUMMARY,
                         additional_kwargs={"lc_source": "summarization", _SAFE_MARKER: True})
            if isinstance(message, HumanMessage)
            and message.additional_kwargs.get("lc_source") == "summarization"
            and not message.additional_kwargs.get(_SAFE_MARKER)
            else message
            for message in messages
        ]
        event = request.state.get(SUMMARIZATION_EVENT_KEY)
        if not isinstance(event, dict):
            return request.override(messages=messages)
        summary = event.get("summary_message")
        if getattr(summary, "additional_kwargs", {}).get(_SAFE_MARKER):
            return request.override(messages=messages)
        # Old checkpoints may contain only the summary and its history path;
        # the raw tool message can already have been pruned from messages.
        self._genotype_guard.mark_genetic_seen()
        safe_event = {**event, "summary_message": HumanMessage(content=_SAFE_SUMMARY,
                      additional_kwargs={"lc_source": "summarization", _SAFE_MARKER: True}),
                      "file_path": None}
        return request.override(messages=messages,
                                state={**request.state, SUMMARIZATION_EVENT_KEY: safe_event,
                                       SUMMARIZATION_SESSION_ID_KEY: None})

    def _build_new_messages_with_path(self, summary: str, file_path: str | None) -> list[Any]:
        messages = super()._build_new_messages_with_path(summary, history_path_for(self.model, file_path))
        return [message.model_copy(update={"additional_kwargs": {
            **message.additional_kwargs, _SAFE_MARKER: True,
        }}) for message in messages]

    def _offload_inline_media(self, backend: Any, messages: list[Any]) -> tuple[list[Any], int]:
        return super()._offload_inline_media(backend, redact_genotype_history(messages))

    async def _aoffload_inline_media(self, backend: Any, messages: list[Any]) -> tuple[list[Any], int]:
        return await super()._aoffload_inline_media(backend, redact_genotype_history(messages))

    def _offload_to_backend(self, backend: Any, messages: list[Any], session_id: str) -> str | None:
        return super()._offload_to_backend(backend, redact_genotype_history(messages), session_id)

    async def _aoffload_to_backend(self, backend: Any, messages: list[Any], session_id: str) -> str | None:
        return await super()._aoffload_to_backend(backend, redact_genotype_history(messages), session_id)

    def _checked_summary_prompt(self, messages: list[Any]) -> str:
        prompt = self._lc_helper._format_summary_prompt(redact_genotype_history(messages))
        request = ModelRequest(model=self.model, messages=[HumanMessage(prompt)], tools=[])
        budget = self._input_budget(request)
        if budget is None:
            raise ContextOverflowError("A bounded summary requires the model's max_input_tokens profile.")
        tokens = self._count_tokens(request.messages, None, None)
        if tokens > budget:
            raise ContextOverflowError(
                "Complete summary input exceeds the model input budget; no history was omitted. "
                f"input_tokens={tokens}, input_budget={budget}."
            )
        return prompt

    @staticmethod
    def _summary_text(response: Any) -> str:
        metadata = getattr(response, "response_metadata", None) or {}
        finish = str(metadata.get("finish_reason") or metadata.get("stop_reason") or "").lower()
        if finish in {"length", "max_tokens"} or metadata.get("status") == "incomplete":
            raise ContextOverflowError("Summary generation reached its output limit; the incomplete summary was not applied.")
        text = response.text.strip()
        if not text:
            raise RuntimeError("Summary generation returned no text; history was not replaced.")
        return text

    def _create_summary(self, messages_to_summarize: list[Any]) -> str:
        if not messages_to_summarize:
            return "No previous conversation history."
        # The upstream overflow fallback omits the middle of this prompt. Keep
        # its retry wrapper and internal-call metadata, but propagate overflow
        # instead of turning unexamined source history into a success event.
        with _summary_activity():
            prompt = self._checked_summary_prompt(messages_to_summarize)
            response = self._lc_helper._summary_model.invoke(prompt, config=_summary_config())
            return self._summary_text(response)

    async def _acreate_summary(self, messages_to_summarize: list[Any]) -> str:
        if not messages_to_summarize:
            return "No previous conversation history."
        with _summary_activity():
            prompt = self._checked_summary_prompt(messages_to_summarize)
            response = await self._lc_helper._summary_model.ainvoke(prompt, config=_summary_config())
            return self._summary_text(response)

    def _call_with_budget(self, request: Any, handler: Any, *, error: Exception | None = None,
                          rejected: Any = None) -> Any:
        # Overflow recovery can persist clipped trailing tool results under
        # /large_tool_results; clip only the redacted view.
        if error is not None or self._over_budget(request):
            request = request.override(messages=redact_genotype_history(request.messages))
        return super()._call_with_budget(request, handler, error=error, rejected=rejected)

    async def _acall_with_budget(self, request: Any, handler: Any, *, error: Exception | None = None,
                                 rejected: Any = None) -> Any:
        if error is not None or self._over_budget(request):
            request = request.override(messages=redact_genotype_history(request.messages))
        return await super()._acall_with_budget(request, handler, error=error, rejected=rejected)

    def wrap_model_call(self, request: Any, handler: Any) -> Any:
        return super().wrap_model_call(self._safe_request(request), handler)

    async def awrap_model_call(self, request: Any, handler: Any) -> Any:
        return await super().awrap_model_call(self._safe_request(request), handler)


__all__ = ["GenotypeSafeSummarizationMiddleware"]
