"""Prompt templates only name tools that exist.

A system prompt that says "call ``search_health_indicators`` then
``fetch_health_data``" after those tools were merged, or offers ``TodoWrite``
after the todo middleware was dropped, costs a wasted round trip on every
turn — the model tries, the harness says no such tool. This lint pulls every
backticked identifier out of a template and compares it with the tool
registry the harness actually exposes. Pure; stdlib only.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

_BACKTICKED = re.compile(r"`([A-Za-z_][A-Za-z0-9_./\-]*)(?:\([^`]*\))?`")
_TOOLISH = re.compile(
    r"^(?:[a-z]+(?:_[a-z0-9]+)+|[A-Z][a-z]+(?:[A-Z][a-z]+)+)$"
)  # snake_case_with_underscore or CamelCase


@dataclass(frozen=True)
class PromptFinding:
    identifier: str
    line: int
    reason: str  # not_a_tool | excluded_tool

    def __str__(self) -> str:
        return f"line {self.line}: `{self.identifier}` — {self.reason}"


def lint_prompt(
    template: str, *, registry: Iterable[str], excluded: Iterable[str] = (), allow: Iterable[str] = ()
) -> tuple[PromptFinding, ...]:
    """Backticked, tool-looking identifiers that are not in ``registry``.

    ``excluded`` names tools the harness deliberately does not provide (a
    mention is always a finding, whatever the registry says); ``allow`` names
    identifiers that look like tools but are parameters, paths or file names
    the template legitimately mentions.
    """
    known = set(registry)
    banned = set(excluded)
    allowed = set(allow)
    out: list[PromptFinding] = []
    for lineno, line in enumerate(template.splitlines(), start=1):
        for m in _BACKTICKED.finditer(line):
            ident = m.group(1)
            if ident in banned:
                out.append(PromptFinding(ident, lineno, "excluded_tool"))
                continue
            if ident in known or ident in allowed or not _TOOLISH.match(ident):
                continue
            out.append(PromptFinding(ident, lineno, "not_a_tool"))
    return tuple(out)


__all__ = ["PromptFinding", "lint_prompt"]
