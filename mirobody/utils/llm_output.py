"""Normalising a model's free-text output — the other half of `utils.prompts`:
the prompt goes out, text comes back.

**A prompt asks a model only for what it does well; edge cases are
recognised here, in code, not fenced off with prompt wording.**

Models are good at "produce in the given form" and bad at both "produce
nothing" and "add not one extra character". Two rules follow:

* When "nothing to say" is a meaningful outcome, the prompt names a
  **sentinel** the model copies verbatim, and this module translates it back
  to empty. Never write "output an empty string": that is a contract a model
  cannot keep, so it improvises — in one production sample of sixty daily
  summaries, eleven spelled emptiness as ``空字符串``, ``（空字符串）`` and
  ``""``, the last of which contains none of the words the prompt used. The
  root cause is not the wording; it is the absence of a definite form.
* Models wrap answers anyway — code fences, quotes, brackets, a "Summary:"
  label — with "no quotes" in the prompt. **The wrapper is peeled here, not
  banned there.**

The sentinel also makes "nothing to say" a positive, verifiable signal: the
sentinel means the model looked and found nothing; a genuinely empty string
means unclear (an API failure, a truncation), and the caller keeps treating
that as a failure rather than overwriting what it had.

The sentinel's spelling is the caller's — it is a promise between one prompt
and one parser, so each passes its own via ``sentinel=``. The defensive net
underneath (`_NOTHING_TO_SAY_FALLBACKS`) is shared: it lists how models spell
"nothing" when they ignore the protocol, and it is finite, closed and pinned
by tests. Extending it is engineering; the correct fix is always to give the
model a sentinel.
"""

from __future__ import annotations

import json
import re

#: How models spell "nothing" when they ignore the protocol — the forms
#: observed after peeling wrappers. A defensive net, not a contract.
_NOTHING_TO_SAY_FALLBACKS = frozenset(
    {
        "",
        "-",
        "--",
        "empty",
        "empty string",
        "emptystring",
        "n/a",
        "na",
        "nil",
        "none",
        "null",
        "nothing",
        "nothing to say",
        "nothing to report",
        "无",
        "无内容",
        "无摘要",
        "暂无",
        "暂无内容",
        "暂无摘要",
        "空",
        "空字符串",
        "空白",
    }
)

#: Paired wrappers. Listed by what models put around an answer, not by what a
#: prompt ever wrote: the full-width parentheses in ``（空字符串）`` appeared
#: in no prompt.
_WRAPPERS = (
    ('"', '"'),
    ("'", "'"),
    ("`", "`"),
    ("“", "”"),
    ("‘", "’"),
    ("（", "）"),
    ("(", ")"),
    ("「", "」"),
    ("『", "』"),
    ("【", "】"),
    ("[", "]"),
)

#: Trailing punctuation that may go while *deciding whether* a text is the
#: sentinel or a placeholder. Never applied to a real answer's body.
_TRIM_CHARS = " \t\r\n。.！!？?，,；;：:"

#: ```lang ... ``` around the whole text. ``(?s)`` lets ``.`` cross newlines;
#: the language tag is optional.
_FENCE_RE = re.compile(r"(?s)^\s*```[^\n`]*\n?(.*?)```\s*$")
#: An opening fence with no close — what an answer cut off at max_tokens looks like.
_OPEN_FENCE_RE = re.compile(r"^\s*```[^\n`]*\n?")

#: Label prefixes models like to add ("Summary:", "摘要："). Only the one at the
#: very start is eaten; never a whole-text replacement.
_LABEL_RE = re.compile(r"^\s*(?:摘要|总结|概要|标题|输出|结果|Summary|Title|Output|Result)\s*[:：]\s*")


def strip_code_fence(text: str | None) -> str:
    """Remove a ```` ``` ```` fence (with or without a language tag) that wraps
    the whole text.

    A fence that does not wrap the whole text is left alone: a code block
    inside a body is content, not a wrapper.
    """
    body = (text or "").strip()
    match = _FENCE_RE.match(body)
    if match:
        return match.group(1).strip()
    # Open without close = the answer was truncated. The opening fence still
    # goes, or three backticks land in storage with the text.
    return _OPEN_FENCE_RE.sub("", body).strip() if body.startswith("```") else body


def strip_wrapping(text: str | None, *, max_layers: int = 4) -> str:
    """Peel quotes and brackets that wrap the whole text, and a leading label
    such as "Summary:", one layer at a time.

    Only a *matching pair at both ends* is peeled, so ``he said “yes”`` is
    untouched (its first character is not a quote).
    """
    body = (text or "").strip()
    for _ in range(max_layers):
        peeled = _LABEL_RE.sub("", body).strip()
        for left, right in _WRAPPERS:
            if len(peeled) >= len(left) + len(right) and peeled.startswith(left) and peeled.endswith(right):
                peeled = peeled[len(left) : -len(right)].strip()
                break
        if peeled == body:
            break
        body = peeled
    return body


def means_nothing_to_say(text: str | None, *, sentinel: str | None = None) -> bool:
    """Whether this output says "nothing to say".

    The sentinel (the protocol) is checked first, with and without its own
    wrapping; then the defensive net. Only the *whole* text is compared, never
    a substring: a real summary is never wholly equal to "none", while a
    substring scan kills "None of the readings are abnormal".
    """
    body = (text or "").strip()
    accepted = set()
    if sentinel:
        accepted.add(sentinel)
        accepted.add(strip_wrapping(sentinel))
    if body in accepted:
        return True
    probe = strip_wrapping(body).strip(_TRIM_CHARS).strip()
    if probe in accepted:
        return True
    return probe.lower() in _NOTHING_TO_SAY_FALLBACKS


def clean_text(raw: str | None, *, sentinel: str | None = None, limit: int | None = None) -> str:
    """A free-text answer → the body that can be stored; "nothing to say"
    becomes the empty string.

    Fixed order: the fence goes first (it is the outermost wrapper), then HTML
    comment markers (generated text never carries a comment — a rewrite
    watermark lives in one, and a model writing its own would corrupt the
    schedule), then the sentinel check.

    **Quotes are peeled only to decide emptiness.** A real answer a model put
    in quotes has a blemish, not an error; peeling risks eating a matching
    pair inside the body. The emptiness check is boolean, so a mis-peel there
    costs at most an unrecognised sentinel.
    """
    body = strip_code_fence(raw)
    body = body.replace("<!--", "").replace("-->", "").strip()
    if means_nothing_to_say(body, sentinel=sentinel):
        return ""
    return body[:limit] if limit else body


def parse_json_object(raw: str | None) -> dict | None:
    """A model's "one JSON object" → dict, or ``None`` (the caller degrades).

    Three attempts, each more lenient: the whole text, the text without its
    fence, then the outermost ``{...}`` cut out of it. The third is for a
    model that adds pleasantries around the JSON with "no prose" in the
    prompt — caught here rather than relied on the prompt to prevent.
    """
    body = (raw or "").strip()
    if not body:
        return None
    for candidate in (body, strip_code_fence(body)):
        try:
            parsed = json.loads(candidate)
        except (ValueError, TypeError):
            continue
        if isinstance(parsed, dict):
            return parsed
    unfenced = strip_code_fence(body)
    start, end = unfenced.find("{"), unfenced.rfind("}")
    if start == -1 or end <= start:
        return None
    try:
        parsed = json.loads(unfenced[start : end + 1])
    except (ValueError, TypeError):
        return None
    return parsed if isinstance(parsed, dict) else None
