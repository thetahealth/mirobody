"""Background tasks must outlive the statement that started them.

Guards `utils.tasks.spawn`, and — more usefully — guards the *absence* of bare
`asyncio.create_task(...)` in statement position anywhere in the package. That
pattern is a documented CPython hazard, not a style preference:

    A task that isn't referenced elsewhere may get garbage collected at any
    time, even before it's done.
    — https://docs.python.org/3/library/asyncio-task.html

There were 15 such call sites, running the work a user would most notice losing:
file processing after upload, OAuth token exchange, vendor pulls, embedding
updates. The failure mode is silence — no exception, no log, just a job that
never happened — so a test that pins the pattern is worth more than one that
tries to reproduce a garbage-collection race.
"""

from __future__ import annotations

import ast
import asyncio
import pathlib

from .tasks import pending_count, spawn


async def test_spawn_runs_and_self_cleans():
    seen: list[int] = []

    async def work(n: int) -> None:
        await asyncio.sleep(0)
        seen.append(n)

    for i in range(50):
        spawn(work(i), name=f"w{i}")

    await asyncio.sleep(0.05)
    assert seen == list(range(50))
    assert pending_count() == 0, "finished tasks must not be retained"


async def test_spawn_returns_an_awaitable_task():
    """Callers that DO want to await or cancel must still be able to."""
    async def work() -> str:
        return "done"

    assert await spawn(work()) == "done"


async def test_failure_is_retired_not_swallowed():
    """A failing task must clear its reference; the callback logs the error."""
    async def boom() -> None:
        raise ValueError("boom")

    task = spawn(boom(), name="boom")
    await asyncio.sleep(0.05)
    assert task.done() and isinstance(task.exception(), ValueError)
    assert pending_count() == 0


def test_no_bare_create_task_in_statement_position():
    """`asyncio.create_task(...)` as a bare statement drops the only reference.

    Assigning it (`task = asyncio.create_task(...)`) or awaiting it is fine —
    the reference lives on. This only rejects the discard-the-result form.
    """
    offenders: list[str] = []
    root = pathlib.Path(__file__).parent.parent

    for path in root.rglob("*.py"):
        if "__pycache__" in str(path) or path.name.startswith("test_"):
            continue
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:  # pragma: no cover - would fail the import walk first
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Expr) or not isinstance(node.value, ast.Call):
                continue
            func = node.value.func
            if (
                isinstance(func, ast.Attribute)
                and func.attr == "create_task"
                and getattr(func.value, "id", "") == "asyncio"
            ):
                offenders.append(f"{path.relative_to(root.parent)}:{node.lineno}")

    assert not offenders, (
        "bare asyncio.create_task(...) can be garbage collected mid-execution; "
        "use mirobody.utils.tasks.spawn instead. Offenders: " + ", ".join(offenders)
    )
