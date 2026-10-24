"""Actual DeepAgents thresholds must compact twice without pre-dropping evidence."""

from __future__ import annotations

import re

from deepagents.middleware.summarization import SUMMARIZATION_EVENT_KEY
from langchain_core.exceptions import ContextOverflowError
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from langgraph.checkpoint.memory import InMemorySaver
from pydantic import Field
import pytest

from mirobody.agent import harness
from mirobody.agent.middleware.genotype_row_guard import GenotypeRowGuardMiddleware
from mirobody.agent.middleware.genotype_summarization import GenotypeSafeSummarizationMiddleware
from mirobody.agent.wire.stream import TokenUsageCallback, TurnStream

EARLY = "FACT early_pressure date=2026-08-01 indicator=systolic value=118 unit=mmHg"
CORRECTION = "CORRECTION early_pressure date=2026-08-01 indicator=systolic value=121 unit=mmHg supersedes=118"
MISSING = "CONSTRAINT sleep date=2026-08-02 state=unrecorded must_not_equal=0"
PRIVATE = "PRIVATE_GENOTYPE_DO_NOT_RESTORE"
_EVIDENCE = re.compile(r"(?:FACT|CORRECTION|CONSTRAINT) [^\n<]+")


class ObservedSummaryModel(BaseChatModel):
    max_tokens: int = 512
    observed: list[dict] = Field(default_factory=list)
    summary_failure: str = ""

    @property
    def _llm_type(self):
        return "synthetic-auto-summary-contract"

    def bind_tools(self, tools, **kwargs):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kwargs):
        text = "\n".join(str(message.content) for message in messages)
        is_summary = (getattr(run_manager, "metadata", {}) or {}).get("lc_source") == "summarization"
        evidence = list(dict.fromkeys(_EVIDENCE.findall(text)))
        self.observed.append({"summary": is_summary, "messages": messages, "evidence": evidence, "text": text})
        # The stub can only retain evidence it actually received. Returning a
        # canned expected summary would hide pre-summarization data loss.
        content = "\n".join(evidence) if is_summary else "Recorded this synthetic note."
        if is_summary and self.summary_failure == "overflow":
            raise ContextOverflowError("Synthetic provider context overflow")
        if is_summary and self.summary_failure == "empty":
            return ChatResult(generations=[ChatGeneration(message=AIMessage(""))])
        if is_summary and self.summary_failure == "length":
            return ChatResult(generations=[ChatGeneration(message=AIMessage(content, response_metadata={"finish_reason": "length"}))])
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content or "No source evidence was supplied."))])


def _graph(model, backend, permissions, checkpoint):
    guard = GenotypeRowGuardMiddleware()
    summary = GenotypeSafeSummarizationMiddleware(model, backend, guard)
    graph = harness.assemble(
        model=model, tools=[], system_prompt="Keep source dates, units, corrections and missingness distinct.",
        backend=backend, permissions=permissions, checkpointer=checkpoint,
        middleware=harness.standard_middleware(tail=[summary, guard], model_call_limit=4), model_call_limit=4,
    )
    return graph, summary


async def _exercise_auto_compaction(*, genotype=False, model=None, activity=None):
    model = model or ObservedSummaryModel(profile={"max_input_tokens": 12000})
    backend, permissions = harness.read_only_mounts({})
    checkpoint = InMemorySaver()
    config = {"configurable": {"thread_id": "genuine-auto-summary"}}
    cutoffs = []
    receipts = []
    for turn in range(8):
        # Production rebuilds its guard/middleware each user turn while the
        # checkpoint keeps conversation state. No summary is injected here.
        graph, summary = _graph(model, backend, permissions, checkpoint)
        counter, presentation = TokenUsageCallback(), TurnStream()
        config = {**config, "callbacks": [counter]}
        source = EARLY + "\n" + MISSING + "\n" if turn == 0 else CORRECTION + "\n" if turn == 1 else ""
        note = source + f"Background note {turn}. " + "Neutral background detail with no new measured facts. " * 200
        messages = [HumanMessage(note)]
        if genotype and turn == 0:
            messages = [HumanMessage("Earlier genotype question"),
                        AIMessage("", tool_calls=[{"name": "query_genetic_data", "args": {}, "id": "private-call"}]),
                        ToolMessage(PRIVATE, name="query_genetic_data", tool_call_id="private-call"),
                        AIMessage(PRIVATE, additional_kwargs={"reasoning_content": PRIVATE}), *messages]
        async for mode, item in graph.astream({"messages": messages}, stream_mode=["messages", "updates", "custom"], config=config):
            rendered = list(presentation.blocks(mode, item))
            if activity is not None:
                activity.extend(block for block in rendered if block["type"] == "context")
            if mode == "updates":
                for update in item.values():
                    if isinstance(update, dict) and (event := update.get(SUMMARIZATION_EVENT_KEY)):
                        cutoffs.append(event["cutoff_index"])
        receipts.append(counter.context(presentation, model))
        if len(cutoffs) >= 2:
            break
    snapshot = await graph.aget_state(config)
    return model, summary, cutoffs, snapshot.values, receipts


async def test_two_genuine_auto_compactions_preserve_early_facts_corrections_and_missingness():
    activity = []
    model, summary, cutoffs, state, receipts = await _exercise_auto_compaction(activity=activity)
    assert [block["status"] for block in activity] == ["started", "completed"] * 2
    assert activity[0]["id"] == activity[1]["id"] != activity[2]["id"]
    assert all(set(block) == {"type", "phase", "status", "id"} for block in activity)
    assert len(cutoffs) >= 2 and cutoffs == sorted(set(cutoffs))
    summaries = [item for item in model.observed if item["summary"]]
    assert len(summaries) >= 2
    assert summary._lc_helper.trigger == ("fraction", .85)
    assert summary._lc_helper.keep == ("fraction", .10)
    for item in summaries:
        assert EARLY in item["evidence"]
        assert CORRECTION in item["evidence"]
        assert MISSING in item["evidence"]
        assert summary._count_tokens(item["messages"], None, None) <= int(12000 * .95) - model.max_tokens
    assert {EARLY, CORRECTION, MISSING} <= set(model.observed[-1]["evidence"])
    assert all("_summarization_event" not in item["text"] for item in summaries)
    history_path = state[SUMMARIZATION_EVENT_KEY]["file_path"]
    assert history_path
    # A local model is given the path so a compacted turn can look up a detail the
    # summary dropped; the prompt bounds it to one page. Set LOCAL_HISTORY_OFFLOAD=off
    # to keep the path out of the summary again.
    with_path = [item for item in summaries if history_path in item["text"]]
    assert with_path or not summaries
    assert state[SUMMARIZATION_EVENT_KEY]["cutoff_index"] == cutoffs[-1]
    assert sum(receipt["summaries"] for receipt in receipts) >= 2
    assert [cutoff for receipt in receipts for cutoff in receipt["summary_cutoffs"]] == cutoffs
    assert sum(receipt["summary_calls_started"] for receipt in receipts) == len(summaries)
    assert sum(receipt["summary_calls_completed"] for receipt in receipts) == len(summaries)
    assert sum(receipt["summary_calls_failed"] for receipt in receipts) == 0
    assert all(receipt["input_window_tokens"] == 12000 for receipt in receipts)
    assert all(EARLY not in str(receipt) for receipt in receipts)


async def test_auto_summarization_keeps_genotype_redaction_ahead_of_history_preservation():
    model, _, cutoffs, state, receipts = await _exercise_auto_compaction(genotype=True)
    assert len(cutoffs) >= 2
    assert any(item["summary"] for item in model.observed)
    assert all(PRIVATE not in item["text"] for item in model.observed)
    assert EARLY in model.observed[-1]["evidence"]
    assert state.get("files"), "The real offload path must also be exercised"
    assert PRIVATE not in str(state["files"])
    assert sum(receipt["summary_calls_completed"] for receipt in receipts) >= 2


async def test_oversized_full_history_fails_before_summary_inference_without_tail_trimming():
    model = ObservedSummaryModel(profile={"max_input_tokens": 12000})
    backend, permissions = harness.read_only_mounts({})
    graph, _ = _graph(model, backend, permissions, InMemorySaver())
    messages = []
    for i in range(8):
        messages += [HumanMessage(f"Source note {i}. " + "Complete synthetic evidence remains in the history. " * 200),
                     AIMessage("Recorded.")]
    messages.append(HumanMessage("Reconcile the earlier records."))
    with pytest.raises(ContextOverflowError, match="Complete summary input exceeds"):
        await graph.ainvoke({"messages": messages}, {"configurable": {"thread_id": "oversized-history"}})
    assert model.observed == []


@pytest.mark.parametrize("failure,error", [("overflow", ContextOverflowError), ("length", ContextOverflowError), ("empty", RuntimeError)])
async def test_failed_summary_does_not_retry_with_omitted_source_history(failure, error):
    model = ObservedSummaryModel(profile={"max_input_tokens": 12000}, summary_failure=failure)
    activity = []
    with pytest.raises(error):
        await _exercise_auto_compaction(model=model, activity=activity)
    assert [block["status"] for block in activity] == ["started", "failed"]
    summaries = [item for item in model.observed if item["summary"]]
    assert summaries
    assert all({EARLY, MISSING, CORRECTION} <= set(item["evidence"]) for item in summaries)
    assert all("middle of conversation omitted" not in item["text"] for item in summaries)
