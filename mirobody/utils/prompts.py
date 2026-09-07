"""Jinja for prompts: one environment, strict.

Every prompt a model sees is a ``.jinja`` file in a ``prompts/`` package next
to the code that uses it — reviewed, diffed and tuned without touching Python.
Each such package exposes ``render = make_renderer(__file__)``.

Rendering is strict (``StrictUndefined``): a misspelled or missing variable
raises at render time instead of silently printing an empty string into the
system prompt. A prompt that reads "Tools:" followed by nothing is a bug the
model then hides for you.
"""

from __future__ import annotations

from collections.abc import Callable
from functools import cache
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, StrictUndefined


def environment(**overrides) -> Environment:
    """The one way a prompt is rendered — strict, plain text, block tags
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


@cache
def _env(prompt_dir: str) -> Environment:
    return environment(loader=FileSystemLoader(prompt_dir))


def make_renderer(package_file: str) -> Callable[..., str]:
    """``render(name, **context)`` bound to the ``.jinja`` files beside
    ``package_file`` (pass ``__file__`` from a ``prompts/`` package)."""
    prompt_dir = str(Path(package_file).resolve().parent)

    def render(template: str, /, **context) -> str:
        """Render ``<template>.jinja`` from this directory, outer whitespace
        stripped. The template name is positional-only so a template may
        have a variable called ``name`` (or ``template``)."""
        return _env(prompt_dir).get_template(f"{template}.jinja").render(**context).strip()

    return render
