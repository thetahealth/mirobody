"""`mirobody doctor --probe`: real requests through the call the product makes.

`mirobody doctor` reads configuration and says which entry each surface would
use. That answers "is a key there", not "can this model do the job": a local
server can be up and serve a model that cannot call a tool, read an image or
keep to a schema. Single-round checks are a floor, not a verdict (a model that
passes a short tool round can still loop in a real multi-round turn), which is why the
report says PASS rather than "ready".

Sends real requests, so it costs a few tokens on a paid endpoint and minutes on
a slow local one; that is why it is a flag and not the default.
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from typing import Any

_TOOL = {
    "type": "function",
    "function": {
        "name": "query_health_indicators",
        "description": "Read the person's recorded health readings.",
        "parameters": {
            "type": "object",
            "properties": {
                "keywords": {"type": "array", "items": {"type": "string"}},
                "days": {"type": "integer", "description": "how many days back"},
            },
            "required": ["keywords"],
        },
    },
}

_SCHEMA = {
    "type": "json_schema",
    "json_schema": {
        "name": "reading",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {"name": {"type": "string"}, "value": {"type": "number"}, "unit": {"type": "string"}},
            "required": ["name", "value", "unit"],
            "additionalProperties": False,
        },
    },
}


#: An OCR answer this many times longer than the line it read did not stop.
_RUNAWAY = 20


@dataclass(frozen=True)
class ProbeResult:
    surface: str
    passed: bool
    seconds: float
    detail: str


async def _chat(name: str | None = None, resolve: Any = None, entry: dict | None = None) -> tuple[bool, str]:
    """A streamed tool call and synthetic result through the chat entry `name`;
    `resolve` reads its key and address (`clients.Resolver`) and `entry` is
    the entry itself, when they are not what the environment says, as for a
    key and model the setup page has not saved yet."""
    from langchain_core.messages import HumanMessage, ToolMessage, message_chunk_to_message

    from mirobody.agent.models.clients import build_chat_model, normalize_thinking
    from mirobody.agent.models.messages import message_reasoning, message_text
    from mirobody.utils.config import safe_read_cfg
    from mirobody.utils.config.llm import default_model
    from mirobody.utils.config.llm import chat_entries

    name = name or default_model()
    if not name:
        return False, "no chat entry is usable"
    selected = entry or chat_entries()[name]
    effort = normalize_thinking(selected.get("thinking_level"))
    if effort in (None, "off"):
        effort = normalize_thinking(safe_read_cfg("AGENT_THINKING")) or "medium"
    model = build_chat_model(
        selected, alias=name, resolve=resolve,
        thinking=effort,
        require_thinking=True,
    ).bind_tools([_TOOL])
    started = time.monotonic()
    first_reasoning: float | None = None

    async def streamed(messages):
        nonlocal first_reasoning
        reply = None
        async for chunk in model.astream(messages):
            if first_reasoning is None and message_reasoning(chunk).strip():
                first_reasoning = time.monotonic() - started
            reply = chunk if reply is None else reply + chunk
        return message_chunk_to_message(reply) if reply is not None else None

    # A native CPU endpoint can spend minutes on prefill. It still needs a
    # finite probe: setup applies its own shorter deadline; doctor allows ten
    # minutes for both calls and never retries a model that asks for more tools.
    async with asyncio.timeout(600):
        messages = [HumanMessage("What was my fasting glucose over the last 90 days? Look it up once, then answer briefly from the result.")]
        reply = await streamed(messages)
        calls = getattr(reply, "tool_calls", None) or []
        if len(calls) != 1 or getattr(reply, "invalid_tool_calls", None):
            return False, f"{name}: expected one valid tool call, received {len(calls)}"
        call = calls[0]
        args = call.get("args")
        valid = (
            call.get("name") == "query_health_indicators" and bool(call.get("id"))
            and isinstance(args, dict) and isinstance(args.get("keywords"), list)
            and bool(args["keywords"]) and all(isinstance(word, str) and word.strip() for word in args["keywords"])
            and ("days" not in args or type(args["days"]) is int and args["days"] > 0)
        )
        if not valid:
            return False, f"{name}: tool name, call id or parsed arguments did not match the probe schema"
        messages.extend([reply, ToolMessage(
            content=json.dumps({"readings": [{"value": 5.4, "unit": "mmol/L"}], "count": 1, "complete": True}),
            tool_call_id=call["id"],
        )])
        final = await streamed(messages)
        if not message_text(final).strip() or getattr(final, "tool_calls", None) or getattr(final, "invalid_tool_calls", None):
            return False, f"{name}: no final answer after the synthetic tool result"
    # Thinking is reported, not required. A model that thinks adaptively
    # decides per request: on this question claude-sonnet-5.5 and gpt-6-luna
    # on OpenRouter spent 0 reasoning tokens at medium and at high effort, and
    # both streamed reasoning on a question that needed it (2026-11-01). As a
    # failure it made the setup page refuse every working OpenRouter key.
    if first_reasoning is None:
        return True, f"{name}: tool round passed; no thinking was shown on this short question (a model that thinks adaptively may skip it)"
    return True, f"{name}: tool round and native thinking passed; first thinking {first_reasoning:.2f}s"


async def _text() -> tuple[bool, str]:
    from mirobody.utils.config.llm import resolve_route
    from mirobody.utils.llm.utils import async_get_structured_output

    spec = resolve_route("text")
    if spec is None:
        return False, "no text entry is usable"
    out = await async_get_structured_output(
        [{"role": "user", "content": "Extract the reading: Hemoglobin 13.5 g/dL"}], _SCHEMA)
    ok = isinstance(out, dict) and out.get("value") == 13.5 and "g/dl" in str(out.get("unit", "")).lower()
    return ok, f"{spec.alias} ({spec.response_format}): {json.dumps(out, ensure_ascii=False) if out is not None else 'no parsable JSON'}"


async def _vision() -> tuple[bool, str]:
    from mirobody.documents.render import text_image
    from mirobody.utils.config.llm import resolve_route
    from mirobody.utils.llm import vision_extract

    spec = resolve_route("vision")
    if spec is None:
        return False, "no vision entry is usable"
    text = await vision_extract(text_image("Glucose 5.4 mmol/L"), "image/png", "Transcribe the text in this image exactly.")
    return "5.4" in text, f"{spec.alias}: {text.strip()[:80]!r}"


async def _ocr() -> tuple[bool, str]:
    from mirobody.documents.ocr import vision_ocr
    from mirobody.documents.render import text_image
    from mirobody.utils.config.llm import resolve_route

    spec = resolve_route("ocr")
    printed = "Glucose 5.4 mmol/L"
    text = await vision_ocr(text_image(printed), "image/png")
    if len(text) > _RUNAWAY * len(printed):
        # Ollama's GLM-OCR read this line and then wrote 8,279 tokens of
        # fences: the value was in there, and the probe passed it.
        return False, f"{spec.alias}: {len(text):,} characters for a {len(printed)}-character line, it did not stop"
    return "5.4" in text, f"{spec.alias}: {text.strip()[:80]!r}"


def _local_models() -> set[tuple[str, str]]:
    """(endpoint, model) for every surface on a local server: an entry whose
    base_url is a variable, the way the shipped local entries are written."""
    from mirobody.utils.config.llm import default_model
    from mirobody.utils.config.llm import chat_entries, endpoint_value, is_endpoint_name, resolve_route

    pairs = set()
    name = default_model()
    entry = chat_entries().get(name) or {} if name else {}
    base = str(entry.get("base_url") or "")
    if is_endpoint_name(base) and endpoint_value(base):
        pairs.add((endpoint_value(base), str(entry.get("model"))))
    for surface in ("text", "vision", "ocr"):
        spec = resolve_route(surface)
        if spec is not None and spec.base_url_env:
            pairs.add((spec.base_url, spec.model))
    return pairs


async def _served() -> tuple[bool, str]:
    """Each local server runs the model its entry names. llama-server answers
    any model name with whatever it loaded, so a swapped file shows up only here."""
    import urllib.request

    def ids(base: str) -> list[str]:
        with urllib.request.urlopen(base.rstrip("/") + "/models", timeout=10) as response:
            return [str(m.get("id")) for m in json.load(response).get("data") or []]

    wrong, seen = [], []
    for base, model in sorted(_local_models()):
        served = await asyncio.to_thread(ids, base)
        seen.append(f"{base} {'/'.join(served)}")
        # Ollama lists a pulled model with its tag, `minicpm5-2b:latest`, and
        # answers to the bare name as well.
        if model not in served and model not in {s.removesuffix(":latest") for s in served}:
            wrong.append(f"{base} serves {', '.join(served) or 'nothing'}, the entry names {model}")
    return not wrong, "; ".join(wrong) if wrong else "; ".join(seen)


async def probe_surfaces() -> list[ProbeResult]:
    """chat, text, vision (and ocr when routed), one after the other (a local server may have one slot)."""
    from mirobody.utils.config.llm import resolve_route

    probes = [("chat", _chat), ("text", _text)]
    # With no model that sees but an OCR entry routed (the small local pair:
    # MiniCPM5-2B cannot see), photos and scans go to the OCR entry, whose
    # probe below covers them; a vision FAIL would fail a working deployment.
    if resolve_route("vision") is not None or resolve_route("ocr") is None:
        probes.append(("vision", _vision))
    if resolve_route("ocr") is not None:
        probes.append(("ocr", _ocr))
    if _local_models():
        probes.insert(0, ("served", _served))
    results = []
    for surface, probe in probes:
        start = time.monotonic()
        try:
            passed, detail = await probe()
        except Exception as exc:
            passed, detail = False, f"{type(exc).__name__}: {str(exc)[:160]}"
        results.append(ProbeResult(surface, passed, time.monotonic() - start, detail))
    return results


def format_probes(results: list[ProbeResult]) -> str:
    lines = ["Probes (chat: two streamed requests; utilities: one request each)", "-" * 72]
    for r in results:
        lines.append(f"  {r.surface:<6}  {'PASS' if r.passed else 'FAIL'}  {r.seconds:6.1f}s  {r.detail}")
    lines.append("-" * 72)
    return "\n".join(lines)


def run() -> list[ProbeResult]:
    return asyncio.run(probe_surfaces())
