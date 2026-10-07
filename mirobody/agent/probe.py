"""`mirobody doctor --probe`: one real request per surface, through the call the product makes.

`mirobody doctor` reads configuration and says which entry each surface would
use. That answers "is a key there", not "can this model do the job": a local
server can be up and serve a model that cannot call a tool, read an image or
keep to a schema. Single-round checks are a floor, not a verdict (a model that
passes all three can still loop in a real multi-round turn), which is why the
report says PASS rather than "ready".

Sends real requests, so it costs a few tokens on a paid endpoint and minutes on
a slow local one; that is why it is a flag and not the default.
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
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


@dataclass(frozen=True)
class ProbeResult:
    surface: str
    passed: bool
    seconds: float
    detail: str


async def _chat(name: str | None = None, resolve: Any = None, entry: dict | None = None) -> tuple[bool, str]:
    """One tool call through the chat entry `name`, or the default one;
    `resolve` reads its key and address (`clients.Resolver`) and `entry` is
    the entry itself, when they are not what the environment says, as for a
    key and model the setup page has not saved yet."""
    from mirobody.agent.models.clients import build_chat_model
    from mirobody.agent.registry import default_model
    from mirobody.utils.config.llm import chat_entries

    name = name or default_model()
    if not name:
        return False, "no chat entry is usable"
    model = build_chat_model(entry or chat_entries()[name], alias=name, resolve=resolve).bind_tools([_TOOL])
    reply = await model.ainvoke("What was my fasting glucose over the last 90 days? Look it up.")
    calls = getattr(reply, "tool_calls", None) or []
    if not calls:
        return False, f"{name}: answered without calling the tool"
    call = calls[0]
    return call.get("name") == "query_health_indicators", f"{name}: {call.get('name')}({json.dumps(call.get('args'), ensure_ascii=False)})"


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
    from mirobody.utils.llm.file_processors.dispatch import unified_file_extract

    spec = resolve_route("vision")
    if spec is None:
        return False, "no vision entry is usable"
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as handle:
        handle.write(text_image("Glucose 5.4 mmol/L"))
    try:
        text = await unified_file_extract(handle.name, "Transcribe the text in this image exactly.", content_type="image/png")
    finally:
        os.unlink(handle.name)
    return "5.4" in (text or ""), f"{spec.alias}: {(text or '').strip()[:80]!r}"


async def _ocr() -> tuple[bool, str]:
    from mirobody.documents.ocr import vision_ocr
    from mirobody.documents.render import text_image
    from mirobody.utils.config.llm import resolve_route

    spec = resolve_route("ocr")
    text = await vision_ocr(text_image("Glucose 5.4 mmol/L"), "image/png")
    return "5.4" in text, f"{spec.alias}: {text.strip()[:80]!r}"


def _local_models() -> set[tuple[str, str]]:
    """(endpoint, model) for every surface on a local server: an entry whose
    base_url is a variable, the way the shipped local entries are written."""
    from mirobody.agent.registry import default_model
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
        if model not in served:
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
    lines = ["Probes (one real request each)", "-" * 72]
    for r in results:
        lines.append(f"  {r.surface:<6}  {'PASS' if r.passed else 'FAIL'}  {r.seconds:6.1f}s  {r.detail}")
    lines.append("-" * 72)
    return "\n".join(lines)


def run() -> list[ProbeResult]:
    return asyncio.run(probe_surfaces())
