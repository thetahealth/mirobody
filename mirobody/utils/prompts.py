"""Jinja for prompts: one environment, strict.

Every prompt a model sees is a ``.jinja`` template: reviewed, diffed and tuned
without touching Python, and rendered through `environment()`.

Rendering is strict (``StrictUndefined``): a misspelled or missing variable
raises at render time instead of silently printing an empty string into the
system prompt. A prompt that reads "Tools:" followed by nothing is a bug the
model then hides for you.
"""

from __future__ import annotations

from jinja2 import Environment, StrictUndefined


def environment(**overrides) -> Environment:
    """The one way a prompt is rendered: strict, plain text, block tags
    that do not leave blank lines behind. ``overrides`` reach the
    ``Environment`` constructor (``loader=``, ``enable_async=True``)."""
    options: dict = {
        "undefined": StrictUndefined,
        "autoescape": False,  # prompts are text, not HTML
        "trim_blocks": True,
        "lstrip_blocks": True,
    }
    options.update(overrides)
    return Environment(**options)
