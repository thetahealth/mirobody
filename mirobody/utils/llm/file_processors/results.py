"""Asking for JSON and reading it back: the pure half of the model surfaces.

No provider, no network, no filesystem: strings in, strings out, shared by the
vision surface and both text backends so an answer is read the same way
whichever model wrote it.
"""

from __future__ import annotations

import json
from typing import Any

from mirobody.utils.llm_output import strip_code_fence


def salvage_truncated_json(text: str) -> Any | None:
    """The complete part of a JSON answer cut off at max_tokens, or None.

    A model that loops (MiniCPM5-2B repeated rows of a handwritten blood-
    pressure log for 30,067 tokens until its context was full) is cut mid-
    value, and the whole answer used to be discarded with every good row
    written before the loop began (benchmarks/local_ocr, 2026-10-07). This
    keeps everything up to the last value that closed inside a container,
    and closes what is still open. A repeated row is the caller's to drop.
    """
    stack: list[str] = []
    cuts: list[tuple[int, tuple[str, ...]]] = []
    in_string = escaped = False
    for i, ch in enumerate(text):
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch in "{[":
            stack.append(ch)
        elif ch in "}]":
            if not stack:
                return None
            stack.pop()
            if stack:
                cuts.append((i + 1, tuple(stack)))
    for end, open_ in reversed(cuts[-64:]):
        closing = "".join("}" if c == "{" else "]" for c in reversed(open_))
        try:
            return json.loads(text[:end] + closing)
        except json.JSONDecodeError:
            continue
    return None


def parse_json_answer(content: str, *, cut: bool) -> Any:
    """A model's JSON answer, fence and all. When the answer was `cut` at
    max_tokens, the part that closed (`salvage_truncated_json`); otherwise,
    or when nothing closed, the `json.JSONDecodeError` of a failed call.

    A model told to answer in JSON by the prompt wraps it in a ```json fence
    (measured on Anthropic's compatibility endpoint, 2026-09-10); stripping one
    from an answer that has none changes nothing."""
    body = strip_code_fence(content)
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        if not cut:
            raise
        salvaged = salvage_truncated_json(body)
        if salvaged is None:
            raise
        return salvaged


def json_prompt(prompt: str, schema: dict[str, Any] | None = None) -> str:
    """`prompt` asking for JSON, in `schema`'s shape when there is one: the
    channel an endpoint without a native schema parameter has."""
    if not schema:
        return prompt + "\n\nPlease return the result in JSON format."
    return f"""{prompt}

Please return the result in JSON format that strictly follows this schema:
```json
{json.dumps(schema, indent=2, ensure_ascii=False)}
```"""
