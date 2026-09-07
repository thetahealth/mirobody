"""A static check that log statements cannot carry health data.

The rule (``mirobody.kernel.ops``): a log line may interpolate identifiers, counts,
durations, status codes and type names — nothing else. This lint walks the
AST of the given files and reports every ``logger.<level>(...)`` call whose
interpolated expressions are not of an allowed *shape*:

* a name (or attribute) whose last component matches :data:`SAFE_NAME` —
  ``user_id``, ``row_count``, ``duration_ms``, ``error_type``, ``status``…;
* ``len(...)``, ``type(...).__name__``, ``str()``/``int()``/``float()`` of an
  allowed expression;
* a numeric, boolean or string literal.

``exc_info=True`` and ``logger.exception`` are reported when the enclosing
``except`` catches a broad type (``Exception``, ``BaseException``, bare): a
database driver's exception text quotes the statement with its bound
parameters. A non-constant ``exc_info=...`` expression (a guard such as
``exc_info=not is_driver_exception(e)``) is accepted.

The lint is deliberately shape-based, not name-based: it cannot prove a
variable holds no PHI, but it makes "log the whole tool result" impossible
to write without an explicit escape (``# phi: ok <reason>`` on the line).
Pure; stdlib only.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

SAFE_NAME = re.compile(
    r"^(?:.*_)?(?:id|ids|uid|count|counts|len|length|n|i|idx|index|ms|seconds|secs|minutes|hours|days|kind|class|"
    r"type|status|code|slug|version|level|size|bytes|total|attempt|attempts|limit|offset|step|steps|round|rounds|mode|"
    r"action|method|reason|state|flag|ok|success|enabled|elapsed|duration|rate|pct|percent|ratio|threshold|tokens)$"
)
#: Exact names that are safe although they end in a word the regex does not know.
SAFE_EXACT = frozenset(
    {
        "tool_name",
        "tool_names",
        "tool_names_str",
        "provider_name",
        "model_name",
        "class_name",
        "func_name",
        "function_name",
        "name_of_tool",
        "trace_id",
        "provider",
        "model",
        "tool",
        "slug",
        "platform",
        "data_type",
        "table",
        "column",
        "field",
        "key",
        "keys",
        # Config KEY NAMES, for the 1.3.x -> 1.4.0 alias warnings in
        # `utils/config/config.py`. Exact names, not a `.*_key$` rule: that
        # would also wave through `api_key` and `secret_key`, which are the
        # one thing a log line must never carry.
        "old_key",
        "new_key",
        "locale",
        "language",
        "tz",
        "timezone",
        "self.info.slug",
        "__name__",
    }
)
LOG_METHODS = frozenset({"debug", "info", "warning", "warn", "error", "exception", "critical", "log"})
LOGGER_NAMES = re.compile(r"^(?:.*\.)?(?:logger|log|_logger|_log|LOGGER|LOG)$")
BROAD_EXCEPTIONS = frozenset({"Exception", "BaseException"})
ESCAPE = re.compile(r"#\s*phi:\s*ok\b")


@dataclass(frozen=True)
class Finding:
    path: str
    line: int
    kind: str  # interpolation | exc_info | extra_key
    detail: str

    def key(self) -> str:
        """A line-number-free identity, stable across unrelated edits — what a
        baseline file stores."""
        return f"{self.path}\t{self.kind}\t{self.detail}"

    def __str__(self) -> str:
        return f"{self.path}:{self.line}: {self.kind}: {self.detail}"


def _dotted(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        base = _dotted(node.value)
        return f"{base}.{node.attr}" if base else None
    return None


def is_safe_expr(node: ast.AST) -> bool:
    """Whether an interpolated expression has an allowed shape."""
    if isinstance(node, ast.Constant):
        return True
    if isinstance(node, ast.FormattedValue):
        return is_safe_expr(node.value)
    if isinstance(node, ast.JoinedStr):
        return all(is_safe_expr(v) for v in node.values)
    dotted = _dotted(node)
    if dotted is not None:
        last = dotted.rsplit(".", 1)[-1]
        return dotted in SAFE_EXACT or last in SAFE_EXACT or bool(SAFE_NAME.match(last))
    if isinstance(node, ast.Call):
        fn = _dotted(node.func)
        if fn == "len":
            return True
        if fn in ("str", "int", "float", "bool", "repr", "round", "abs", "sorted", "list", "tuple"):
            return all(is_safe_expr(a) for a in node.args)
        if fn in ("type", "getattr") or (fn or "").endswith(".get"):
            return False
        # type(e).__name__ is an Attribute over a Call: handled below
        return False
    if isinstance(node, ast.Attribute) and node.attr == "__name__":
        return True
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod | ast.Add):
        return is_safe_expr(node.left) and is_safe_expr(node.right)
    if isinstance(node, ast.IfExp):
        return is_safe_expr(node.body) and is_safe_expr(node.orelse)
    if isinstance(node, ast.Subscript):
        return False
    return False


def _is_logger_call(node: ast.Call) -> str | None:
    if not isinstance(node.func, ast.Attribute) or node.func.attr not in LOG_METHODS:
        return None
    base = _dotted(node.func.value)
    if base is None or not LOGGER_NAMES.match(base):
        return None
    return node.func.attr


def _message_parts(call: ast.Call, method: str) -> tuple[ast.AST | None, list[ast.AST]]:
    args = list(call.args)
    if method == "log":
        args = args[1:]  # the level comes first
    if not args:
        return None, []
    return args[0], args[1:]


def _interpolations(msg: ast.AST | None, rest: Sequence[ast.AST]) -> list[ast.AST]:
    out: list[ast.AST] = []
    if isinstance(msg, ast.JoinedStr):
        out += [v.value for v in msg.values if isinstance(v, ast.FormattedValue)]
    elif isinstance(msg, ast.Call) and isinstance(msg.func, ast.Attribute) and msg.func.attr == "format":
        out += list(msg.args) + [k.value for k in msg.keywords]
    elif isinstance(msg, ast.BinOp) and isinstance(msg.op, ast.Mod):
        right = msg.right
        out += list(right.elts) if isinstance(right, ast.Tuple) else [right]
    elif msg is not None and not isinstance(msg, ast.Constant):
        out.append(msg)  # a variable used as the whole message
    out += list(rest)  # %-style positional arguments
    return out


def _enclosing_except(stack: Sequence[ast.AST]) -> ast.ExceptHandler | None:
    for node in reversed(stack):
        if isinstance(node, ast.ExceptHandler):
            return node
    return None


def _is_broad(handler: ast.ExceptHandler | None) -> bool:
    if handler is None:
        return True  # exc_info outside an except: nothing bounds what it prints
    if handler.type is None:
        return True
    names = [handler.type] if not isinstance(handler.type, ast.Tuple) else list(handler.type.elts)
    return any((_dotted(n) or "").rsplit(".", 1)[-1] in BROAD_EXCEPTIONS for n in names)


class _Visitor(ast.NodeVisitor):
    def __init__(self, path: str, lines: Sequence[str]):
        self.path = path
        self.lines = lines
        self.stack: list[ast.AST] = []
        self.findings: list[Finding] = []

    def generic_visit(self, node: ast.AST) -> None:
        self.stack.append(node)
        super().generic_visit(node)
        self.stack.pop()

    def _escaped(self, lineno: int) -> bool:
        line = self.lines[lineno - 1] if 0 < lineno <= len(self.lines) else ""
        return bool(ESCAPE.search(line))

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802 — ast visitor API
        method = _is_logger_call(node)
        if method and not self._escaped(node.lineno):
            msg, rest = _message_parts(node, method)
            for expr in _interpolations(msg, rest):
                if not is_safe_expr(expr):
                    snippet = ast.get_source_segment("\n".join(self.lines), expr) or type(expr).__name__
                    self.findings.append(Finding(self.path, node.lineno, "interpolation", snippet[:80]))
            exc_info = next((k.value for k in node.keywords if k.arg == "exc_info"), None)
            traceback_on = method == "exception" or (isinstance(exc_info, ast.Constant) and exc_info.value is True)
            if traceback_on and _is_broad(_enclosing_except(self.stack)):
                self.findings.append(
                    Finding(
                        self.path,
                        node.lineno,
                        "exc_info",
                        "traceback of a broad except: driver exceptions quote SQL and parameters",
                    )
                )
            for kw in node.keywords:
                if kw.arg == "extra" and isinstance(kw.value, ast.Dict):
                    for k in kw.value.keys:
                        if (
                            isinstance(k, ast.Constant)
                            and isinstance(k.value, str)
                            and not (SAFE_NAME.match(k.value) or k.value in SAFE_EXACT)
                        ):
                            self.findings.append(Finding(self.path, node.lineno, "extra_key", k.value))
        self.generic_visit(node)


def lint_source(source: str, path: str = "<string>") -> tuple[Finding, ...]:
    tree = ast.parse(source, filename=path)
    v = _Visitor(path, source.splitlines())
    v.visit(tree)
    return tuple(v.findings)


def lint_paths(paths: Iterable[Path | str], *, root: Path | None = None) -> tuple[Finding, ...]:
    """Lint every ``.py`` file under the given paths; ``root`` makes the
    reported paths relative."""
    out: list[Finding] = []
    for p in paths:
        p = Path(p)
        files = sorted(p.rglob("*.py")) if p.is_dir() else [p]
        for f in files:
            if f.name.startswith("test_"):
                continue
            rel = str(f.relative_to(root)) if root else str(f)
            out.extend(lint_source(f.read_text(encoding="utf-8"), rel))
    return tuple(out)


def baseline_lines(findings: Iterable[Finding]) -> list[str]:
    return sorted({f.key() for f in findings})


def new_findings(findings: Iterable[Finding], baseline: Iterable[str]) -> tuple[Finding, ...]:
    """Findings whose key is not in the baseline — what CI fails on."""
    known = set(baseline)
    return tuple(f for f in findings if f.key() not in known)


__all__ = ["Finding", "SAFE_NAME", "baseline_lines", "is_safe_expr", "lint_paths", "lint_source", "new_findings"]


def main(argv: Sequence[str] | None = None) -> int:
    """``python -m mirobody.testing.phi_lint PATH... [--root R] [--baseline F] [--write-baseline]``"""
    import argparse
    import sys

    ap = argparse.ArgumentParser(description="log statements must carry ids, counts and type names only")
    ap.add_argument("paths", nargs="+")
    ap.add_argument("--root", default=".")
    ap.add_argument("--baseline", default="")
    ap.add_argument("--write-baseline", action="store_true")
    ns = ap.parse_args(argv)
    root = Path(ns.root).resolve()
    findings = lint_paths([root / p for p in ns.paths], root=root)
    if ns.write_baseline:
        target = Path(ns.baseline or root / "mirobody" / "testing" / "phi_baseline.txt")
        target.write_text("\n".join(baseline_lines(findings)) + "\n", encoding="utf-8")
        print(f"{len(findings)} findings written to {target}")
        return 0
    baseline = Path(ns.baseline).read_text(encoding="utf-8").splitlines() if ns.baseline else []
    fresh = new_findings(findings, baseline)
    for f in fresh:
        print(f, file=sys.stderr)
    return 1 if fresh else 0


if __name__ == "__main__":
    raise SystemExit(main())
