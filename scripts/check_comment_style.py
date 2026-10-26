#!/usr/bin/env python3
"""Comment style gate: short blocks, no em-dashes in prose.

Both rules exist because the codebase drifted into a register no human writes
in: 0.19 comment lines per line of code (sqlalchemy 0.09, langchain 0.08,
fastapi 0.02) and 27 em-dashes per 1000 lines (langchain 1.4, pydantic 0.09).
Measured 2026-09-14 against the installed copies of thirteen libraries.

Runtime strings are exempt: an em-dash there is output a person reads.
"""

from __future__ import annotations

import ast
import io
import sys
import tokenize
from pathlib import Path

# Consecutive `#` lines. Eight is stricter than the longest block in any
# library measured for this rule (langchain-core 13, fastapi 19, rich 7),
# and it is the length at which a comment stops being a note and becomes
# an essay. A longer explanation belongs in the module docstring, or in
# internal/ with one line here pointing at it.
MAX_BLOCK = 8

SKIP_DIRS = ("__pycache__", "/tests/")
# Deleted wholesale by the 1.5.0 refactor of ② (then named Translate); cleaning
# it is wasted work.
SKIP_PREFIXES = ("mirobody/indicator/", "mirobody/res/")


def _docstring_lines(tree: ast.AST) -> set[int]:
    spans = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Module | ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
            continue
        first = node.body[0] if node.body else None
        if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant) and isinstance(first.value.value, str):
            spans.update(range(first.value.lineno, first.value.end_lineno + 1))
    return spans


def check(path: Path) -> list[str]:
    src = path.read_text(encoding="utf-8")
    problems: list[str] = []

    # --- em-dash, in comments and docstrings only
    docs = _docstring_lines(ast.parse(src))
    for tok in tokenize.generate_tokens(io.StringIO(src).readline):
        if "—" not in tok.string:
            continue
        if tok.type == tokenize.COMMENT or (tok.type == tokenize.STRING and tok.start[0] in docs):
            problems.append(f"{path}:{tok.start[0]}: em-dash in prose; use a period, colon or parentheses")

    # --- comment block length
    run = 0
    for lineno, line in enumerate(src.splitlines(), 1):
        if line.lstrip().startswith("#"):
            run += 1
            continue
        if run > MAX_BLOCK:
            problems.append(f"{path}:{lineno - run}: {run}-line comment block (max {MAX_BLOCK}); "
                            f"keep the invariant and the measurement, drop the narrative")
        run = 0
    if run > MAX_BLOCK:
        problems.append(f"{path}: trailing {run}-line comment block (max {MAX_BLOCK})")

    return problems


def main(argv: list[str]) -> int:
    roots = [Path(a) for a in argv[1:]] or [Path("mirobody")]
    problems: list[str] = []
    for root in roots:
        files = [root] if root.is_file() else sorted(root.rglob("*.py"))
        for path in files:
            posix = path.as_posix()
            if any(d in posix for d in SKIP_DIRS) or posix.startswith(SKIP_PREFIXES):
                continue
            try:
                problems.extend(check(path))
            except (SyntaxError, tokenize.TokenError) as e:
                problems.append(f"{path}: unparseable ({e})")
    for p in problems:
        print(p)
    print(f"\n{len(problems)} problems", file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
