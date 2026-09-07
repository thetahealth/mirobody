"""Assembling a deepagents agent — the part that is the same for every agent.

Two agents built on deepagents (this repository's reference agent and the
first production consumer's) each carried the same forty lines: register the
harness profile that disables the general-purpose ``task`` subagent under the
key deepagents will *actually* look up for this model instance, mount the
read-only projections into a ``CompositeBackend`` and deny writes to each
mount, order the middleware stack, call ``create_deep_agent`` with
``subagents=[]``, set the recursion limit. What differs between agents is what
goes IN — the tools, the mounts, the prompt, the limits — and that stays with
each agent. This module is the assembly.

Two facts about deepagents that this module exists to remember:

* **The no-subagent configuration has two halves**: a profile with
  ``general_purpose_subagent(enabled=False)`` registered under the provider key
  deepagents resolves for THIS model, *and* ``subagents=[]`` passed to
  ``create_deep_agent``. Either half alone leaves ``task`` reachable.
* **That key is not the config's ``llm_type``.** deepagents asks the model
  instance (``get_model_provider``), and a class that does not override
  ``_get_ls_params`` falls back to a class-name derivation — ``ChatAnthropicVertex``
  → ``anthropicvertex``. Registering under the wrong key is a silent no-op; it
  happened in production once, and the model called ``task``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any

logger = logging.getLogger(__name__)


def provider_key(model: Any) -> str | None:
    """The provider key deepagents resolves for this model instance — asked of
    deepagents itself, with the langchain-core ``ls_provider`` as the fallback."""
    try:
        from deepagents._models import get_model_provider

        key = get_model_provider(model)
        if key:
            return key
    except Exception as exc:
        logger.warning("could not derive the harness provider key from deepagents: error_type=%s", type(exc).__name__)
    try:
        return (model._get_ls_params() or {}).get("ls_provider")
    except Exception:
        return None


def disable_general_purpose_subagent(model: Any, *, excluded_tools: Iterable[str] = ()) -> str | None:
    """Register, for the key this model resolves to, a harness profile without the
    general-purpose subagent and without ``excluded_tools`` (native tools the
    agent must not expose — ``delete`` on a backend that does not implement it).
    Returns the key, or ``None`` when it could not be derived (logged: the
    subagent may then stay enabled for this build). Idempotent: registration is a
    field-wise merge."""
    key = provider_key(model)
    if not key:
        logger.warning("no harness provider key for this model; the 'task' subagent may stay enabled")
        return None
    from deepagents import GeneralPurposeSubagentProfile, HarnessProfile, register_harness_profile

    register_harness_profile(
        key,
        HarnessProfile(
            general_purpose_subagent=GeneralPurposeSubagentProfile(enabled=False),
            excluded_tools=frozenset(excluded_tools),
        ),
    )
    return key


def read_only_mounts(routes: Mapping[str, Any], *, default: Any = None) -> tuple[Any, list]:
    """A ``CompositeBackend`` over ``routes`` (``"/memories/" -> backend``) with
    writes DENIED on every mount, and the scratch root (``default``, a
    checkpointed ``StateBackend`` unless given) left writable.

    Every mount is a projection of something another process owns, and a
    projection cannot be written to. Declaring that as a permission rather than
    leaving it to each backend's own refusal is what stops the model spending a
    tool call to find out. Returns ``(backend, permissions)``.
    """
    from deepagents.backends import CompositeBackend, StateBackend
    from deepagents.middleware.filesystem import FilesystemPermission

    permissions = [
        FilesystemPermission(operations=["write"], paths=[f"{prefix.rstrip('/')}/**"], mode="deny")
        for prefix in routes
    ]
    return CompositeBackend(default=default or StateBackend(), routes=dict(routes)), permissions


def standard_middleware(
    *,
    fault_middleware: Any = None,
    retry_limit: int = 2,
    model_call_limit: int = 50,
    tool_call_limits: Mapping[str, int] | None = None,
    interpreter: Any = None,
    tail: Iterable[Any] = (),
) -> list:
    """The middleware stack every agent here runs, outermost first.

    1. fault containment — contains faults from every tool AND from the
       wrappers below it (`ToolFaultMiddleware` unless one is injected);
    2. retry governance — refuses a repeat of a call that already failed
       unrecoverably, inside the fault middleware so its own bugs are
       contained, reading the envelope that middleware attaches;
    3. invalid-call repair — a call whose JSON never parsed reaches no tool;
       this feeds the parse error back instead of ending the turn empty;
    4. the per-turn model-call budget, ending the run *gracefully* so the model
       still writes its answer — a legitimate multi-step task runs to
       completion while a pathological loop still terminates;
    5. one cap per named tool (``exit_behavior="continue"``: the tool is
       removed for the rest of the turn, the turn goes on);
    6. the code interpreter, if given;
    7. ``tail`` — whatever the agent adds last (skills, prompt caching).

    A fresh stack per build is a fresh retry ledger per turn, which is what
    "already tried that" has to mean.
    """
    from langchain.agents.middleware import ModelCallLimitMiddleware, ToolCallLimitMiddleware

    from .middleware import InvalidToolCallRepairMiddleware, RetryGovernanceMiddleware, ToolFaultMiddleware

    stack: list = [
        fault_middleware if fault_middleware is not None else ToolFaultMiddleware(),
        RetryGovernanceMiddleware(limit=retry_limit),
        InvalidToolCallRepairMiddleware(),
        ModelCallLimitMiddleware(run_limit=model_call_limit, exit_behavior="end"),
    ]
    for tool_name, limit in (tool_call_limits or {}).items():
        stack.append(ToolCallLimitMiddleware(tool_name=tool_name, run_limit=limit, exit_behavior="continue"))
    if interpreter is not None:
        stack.append(interpreter)
    stack.extend(tail)
    return stack


def assemble(
    *,
    model: Any,
    tools: list,
    system_prompt: str,
    backend: Any,
    permissions: list | None = None,
    middleware: Iterable[Any] = (),
    checkpointer: Any = None,
    store: Any = None,
    interrupt_on: Mapping[str, Any] | None = None,
    recursion_limit: int | None = None,
    excluded_native_tools: Iterable[str] = (),
) -> Any:
    """``create_deep_agent`` with both halves of the no-subagent configuration
    applied, and the recursion limit set as a last-resort runaway net.

    The recursion limit must sit well above the model-call budget in the
    middleware: one tool round costs several LangGraph super-steps, because each
    built-in middleware compiles its own graph node, and the graceful budget must
    fire first.
    """
    from deepagents import create_deep_agent

    disable_general_purpose_subagent(model, excluded_tools=excluded_native_tools)
    kwargs: dict[str, Any] = {
        "model": model,
        "tools": list(tools),
        "system_prompt": system_prompt,
        "backend": backend,
        "middleware": list(middleware),
        "subagents": [],
        "interrupt_on": dict(interrupt_on) if interrupt_on else None,
    }
    if permissions is not None:
        kwargs["permissions"] = permissions
    if checkpointer is not None:
        kwargs["checkpointer"] = checkpointer
    if store is not None:
        kwargs["store"] = store
    agent = create_deep_agent(**kwargs)
    if recursion_limit:
        agent = agent.with_config({"recursion_limit": recursion_limit})
    return agent
