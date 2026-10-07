"""A filesystem tool's integer arguments, as LLMs send them.

LLMs (notably Qwen and DeepSeek) do not reliably honour a tool's declared
schema: an `int` parameter (`read_file`'s offset and limit) arrives as a
string ``"100"`` or ``None``, so a comparison such as ``len(lines) > limit``
raises ``"'>' not supported between instances of 'int' and 'str'"``.
`coerce_to_int` normalises such a value at the tool boundary, and never
raises.
"""

from typing import Any


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


__all__ = ["coerce_to_int"]
