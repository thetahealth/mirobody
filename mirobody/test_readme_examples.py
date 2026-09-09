"""Run the resolver examples the two live READMEs print, and check their answers.

Both READMEs quote eight `resolve(...)` / `resolve_reading(...)` results with the
LOINC code written in the trailing comment. Nothing checked them. Every other
number in those files is guarded (`test_readme_numbers.py`), so the examples —
the part a reader actually copies — were the one unguarded claim, and this repo's
own working notes list "our README said so" as a recurring source of false
belief. The block is short and the parse is trivial, so there is no reason for it
to be an assertion rather than a test.

The parse is deliberately dumb: find the fenced ```python block that imports from
`mirobody.engine`, take every line of the form

    <expr>   # 'CODE'  free text
    <expr>   # False   free text

and assert `eval(expr)` equals it. A README that adds an example gets it checked
for free; one that changes a code without changing the call fails here.
"""

from __future__ import annotations

import ast
import os
import re

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_READMES = ["README.md", "README.zh-CN.md"]  # the live editions; see archived/README.md

# `resolve("x").loinc   # '718-7'  comment` / `resolve("血脂").resolved  # False`
#
# The lookahead, not `\b`: a word boundary after the closing `'` of `'718-7'`
# never matches, because both sides of that position are non-word characters.
# With `\b` this file parsed exactly one of the eight lines and still passed.
_EXAMPLE = re.compile(
    r"^(resolve\w*\(.+?\)(?:\.\w+)?)\s+#\s*('[\d-]+'|False|True)(?=\s|$)"
)


def _examples(name: str) -> list[tuple[str, str]]:
    path = os.path.join(_ROOT, name)
    with open(path, encoding="utf-8") as fh:
        text = fh.read()
    for block in re.findall(r"```python\n(.*?)```", text, re.S):
        if "from mirobody.engine import" not in block:
            continue
        found = []
        for line in block.splitlines():
            m = _EXAMPLE.match(line.strip())
            if m:
                found.append((m.group(1), m.group(2)))
        return found
    return []


def test_the_readme_block_was_found_and_is_not_empty():
    """A rename of the import line would otherwise make this file vacuous."""
    for name in _READMES:
        assert len(_examples(name)) >= 5, f"{name}: parsed {len(_examples(name))}"


@pytest.mark.parametrize("name", _READMES)
def test_readme_resolver_examples_produce_what_they_claim(name):
    from mirobody.engine import resolve, resolve_reading  # noqa: F401

    for expr, expected in _examples(name):
        got = eval(expr)  # noqa: S307 — the input is this repo's own README
        # Two lines write the call without `.loinc` and comment it with the
        # code. That is what they mean, so read the code off the Resolution.
        code = getattr(got, "loinc", got)
        assert code == ast.literal_eval(expected), (
            f"{name}: `{expr}` -> {code!r}, README says {expected}"
        )


def test_both_readmes_run_the_same_calls():
    """Translations may reword the comment; they must not diverge on the call."""
    baseline = [expr for expr, _ in _examples("README.md")]
    for name in _READMES[1:]:
        assert [e for e, _ in _examples(name)] == baseline, name
