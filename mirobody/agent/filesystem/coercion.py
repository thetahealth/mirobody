"""
Argument Coercion Utilities

LLMs (notably Qwen and DeepSeek) do not reliably honor the declared JSON
schema of tool parameters. Common failure modes observed in production:

- A `list[str]` parameter is returned as a plain string — e.g. the model
  sends ``"[\"/uploads/a.pdf\"]"`` or just ``"/uploads/a.pdf"`` instead of a
  real array. ``for x in sources`` then iterates the *characters* of the
  string, producing nonsense like ``'[': File not found``.
- An `int` parameter (offset/limit) arrives as a string ``"100"`` or ``None``,
  so comparisons such as ``len(lines) > limit`` raise
  ``"'>' not supported between instances of 'int' and 'str'"``.
- A `bool` parameter arrives as the string ``"true"``/``"false"``.

These helpers normalize such values at the tool boundary so the rest of the
code can assume well-typed arguments. They never raise — on anything
unparseable they fall back to a sensible default.
"""

import ast
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


def coerce_to_list(value: Any) -> list[Any]:
    """Normalize a value into a list.

    Handles the case where an LLM sends a `list[str]` parameter as a single
    string (a JSON/Python-literal encoded list, or a lone item). Iterating such
    a string character-by-character is the bug this prevents.

    Returns an empty list for None/empty input.
    """
    if value is None:
        return []

    if isinstance(value, (list, tuple)):
        return list(value)

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []

        # Try strict JSON first ('["a", "b"]'), then Python-literal style
        # ("['a', 'b']" with single quotes, which Qwen frequently emits).
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(text)
            except (ValueError, SyntaxError, TypeError):
                continue
            if isinstance(parsed, (list, tuple)):
                return list(parsed)
            # Parsed to a scalar (e.g. a bare number/quoted string): treat the
            # original text as a single item rather than splitting it.
            return [text]

        # Not parseable as a collection: a single bare path/url/key.
        return [text]

    # Any other scalar (int, etc.): wrap it.
    return [value]


def coerce_to_int(value: Any, default: int) -> int:
    """Coerce a value to int, falling back to `default` on failure.

    Accepts ints, numeric strings ("100", " 100 "), and float-like strings
    ("100.0"). None and anything unparseable yield `default`.
    """
    if value is None:
        return default
    if isinstance(value, bool):
        # bool is a subclass of int; treat as unset to avoid True->1 surprises.
        return default
    if isinstance(value, int):
        return value
    try:
        if isinstance(value, float):
            return int(value)
        return int(str(value).strip())
    except (TypeError, ValueError):
        try:
            return int(float(str(value).strip()))
        except (TypeError, ValueError):
            return default


def coerce_to_bool(value: Any, default: bool = False) -> bool:
    """Coerce a value to bool, falling back to `default` on failure.

    Accepts real bools, and the string forms LLMs emit
    ("true"/"false"/"1"/"0"/"yes"/"no", case-insensitive).
    """
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in ("true", "1", "yes", "y", "on"):
            return True
        if text in ("false", "0", "no", "n", "off", ""):
            return False
    return default


__all__ = [
    "coerce_to_list",
    "coerce_to_int",
    "coerce_to_bool",
]
