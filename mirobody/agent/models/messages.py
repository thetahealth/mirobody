"""Reading a LangChain message across provider shapes.

Every provider family puts the same two things somewhere different. The
visible answer is a plain string on OpenAI-compatible models and a LIST of
content blocks on Anthropic and Gemini; the reasoning is
``additional_kwargs.reasoning_content`` on DashScope/DeepSeek, a ``thinking``
or ``reasoning`` block on Anthropic/Gemini, and — on some qwen models — an
inline ``<think>…</think>`` inside the text itself. These two functions are
the one place that knows all of that, so a chat surface, an OpenAI-compatible
relay and a background summariser read a message the same way. Never
``str(msg.content)`` on a list: that is a Python repr, not text.
"""

from __future__ import annotations

import re

_THINK_RE = re.compile(r"(?s)<think\b[^>]*>.*?</think>")
_REASONING_BLOCKS = ("thinking", "reasoning")


def message_reasoning(msg) -> str:
    """The reasoning/thinking text of a message or streaming chunk, or ``""``."""
    extra = getattr(msg, "additional_kwargs", None) or {}
    reasoning = extra.get("reasoning_content")
    if reasoning:
        return reasoning if isinstance(reasoning, str) else str(reasoning)
    content = getattr(msg, "content", None)
    if isinstance(content, list):
        return "".join(
            block.get("thinking") or block.get("reasoning") or block.get("text") or ""
            for block in content
            if isinstance(block, dict) and block.get("type") in _REASONING_BLOCKS
        )
    return ""


def message_text(msg) -> str:
    """The VISIBLE answer text of a message or chunk: the string content, or the
    text blocks of a block list (reasoning blocks skipped), with any inline
    ``<think>…</think>`` — balanced or dangling — removed."""
    content = getattr(msg, "content", None)
    if isinstance(content, str):
        raw = content
    elif isinstance(content, list):
        raw = "".join(
            block.get("text") or ""
            for block in content
            if isinstance(block, dict) and block.get("type") not in _REASONING_BLOCKS
        )
    else:
        raw = "" if content is None else str(content)
    raw = _THINK_RE.sub("", raw)
    if "<think>" in raw or "</think>" in raw:  # unbalanced or dangling tag
        raw = raw.replace("<think>", "").replace("</think>", "")
    return raw
