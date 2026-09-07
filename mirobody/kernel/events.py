"""The canonical, wire-neutral ``AgentEvent`` stream.

An agent runtime emits these; every wire protocol (a consumer's SSE dialect,
OpenAI-compatible Responses/Chat Completions, a WebSocket) translates them
independently. No framework type — LangChain, LangGraph, FastAPI — may enter
this module; it is stdlib and ``typing`` only, so a consumer can depend on
it without depending on how the reference agent is built.

The variants, and why each exists:

* ``TextDelta`` / ``ReasoningDelta`` — visible reply vs. thinking text, two
  types because they are two channels on every protocol that exists.
* ``Notice`` — a system-originated line for the user ("the model is
  unavailable, falling back"), which used to be smuggled through the
  thinking channel and was indistinguishable from the model's own trace.
* ``ToolCallStarted`` / ``ToolArgumentsDelta`` / ``ToolCallCompleted`` /
  ``ToolResult`` — one tool call's life cycle. ``ToolCallStarted.kind``
  classifies WHAT the call is (a health-data query, a citation search) so an
  adapter never needs a tool-name allowlist. ``ToolResult.status`` is the
  fact an error middleware produced this result — losing it is how a failed
  tool used to render as an ordinary one.
* ``UsageDelta`` — provider-true token counts per model call, attributed to
  the model that produced them.
* ``Interrupted`` — the run is paused; ``action_requests`` is the ONLY
  authoritative source of what is pending.
* ``Completed`` / ``Failed`` — exactly one terminal outcome per stream and
  nothing legitimately follows it. ``Completed.finish_reason="length"`` is
  the fact a budget ended the turn; ``degrade`` names a graceful-degrade
  branch whose user-facing wording differs per surface — the event carries
  the fact, never localised copy.

THE SINGLE-TERMINAL-OUTCOME INVARIANT: exactly one of ``Completed``,
``Interrupted``, ``Failed``, and nothing after it. :func:`is_terminal` and
:func:`enforce_single_terminal_outcome` are the shared enforcement, proven
here against synthetic sequences; the runtime wraps its own output with it.

These definitions are shared verbatim with mirovital's agent runtime; the
consumer's ``AgentRequest`` (a wire contract) deliberately is not here.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from typing import Literal, Protocol

__all__ = [
    "TextDelta",
    "ReasoningDelta",
    "Notice",
    "ToolCallStarted",
    "ToolArgumentsDelta",
    "ToolCallCompleted",
    "TextContent",
    "AgentContent",
    "ArtifactRef",
    "ToolResult",
    "ActionRequest",
    "Interrupt",
    "UsageDelta",
    "Completed",
    "MEDIA_DECODE_DEGRADE",
    "Interrupted",
    "Failed",
    "AgentFailure",
    "AgentEvent",
    "AgentRunner",
    "is_terminal",
    "ProtocolViolation",
    "enforce_single_terminal_outcome",
]


# --- non-terminal content / tool / usage events ----------------------------------------


@dataclass(frozen=True, slots=True)
class TextDelta:
    """One chunk of the assistant's visible reply text."""

    text: str


@dataclass(frozen=True, slots=True)
class ReasoningDelta:
    """One chunk of the model's reasoning/thinking trace."""

    text: str


@dataclass(frozen=True, slots=True)
class Notice:
    """A system-originated message for the user, not written by the model:
    a fallback, a degraded feature, a limit reached. ``level`` is
    ``info`` | ``warning``."""

    text: str
    level: Literal["info", "warning"] = "info"


@dataclass(frozen=True, slots=True)
class ToolCallStarted:
    """A tool call has begun. ``execution`` names who runs it; ``kind``
    classifies what it is (the tool catalogue's own vocabulary, e.g.
    ``health_data`` / ``knowledge`` / ``filesystem`` / ``generic``)."""

    call_id: str
    name: str
    execution: Literal["server", "client"]
    kind: str


@dataclass(frozen=True, slots=True)
class ToolArgumentsDelta:
    """One chunk of a tool call's arguments as they are assembled. Current
    runtimes emit exactly one (the whole JSON) before ``ToolCallCompleted``;
    the type exists so a streaming runtime has somewhere to put pieces."""

    call_id: str
    arguments_delta: str


@dataclass(frozen=True, slots=True)
class ToolCallCompleted:
    """A tool call's arguments are fully assembled and ready to execute."""

    call_id: str
    arguments: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class TextContent:
    """Plain-text tool-result content."""

    text: str


type AgentContent = TextContent


@dataclass(frozen=True, slots=True)
class ArtifactRef:
    """A reference to a non-text tool-result artifact (a file, an image, a
    generated document)."""

    kind: str
    uri: str
    name: str | None = None


@dataclass(frozen=True, slots=True)
class ToolResult:
    """The outcome of one tool call, correlated by ``call_id``. ``status``
    is ``failed`` when an error middleware produced the content."""

    call_id: str
    status: Literal["completed", "failed", "cancelled"]
    content: tuple[AgentContent, ...]
    artifacts: tuple[ArtifactRef, ...] = ()


@dataclass(frozen=True, slots=True)
class ActionRequest:
    """One pending client-tool call inside an ``Interrupt``, in the order the
    runtime paused on it. ``call_id`` is minted by the runtime so every
    protocol reads one stable id off one place."""

    call_id: str
    name: str
    arguments: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class Interrupt:
    """The run is paused awaiting one or more answers. ``continuation_token``
    is echoed back by the caller on resume; ``action_requests`` is ordered
    and is the only authoritative source of what is pending."""

    interrupt_id: str
    continuation_token: str
    action_requests: tuple[ActionRequest, ...]


@dataclass(frozen=True, slots=True)
class UsageDelta:
    """Provider-reported token accounting for one model call inside the
    turn. ``model``/``source`` say which call produced it, so a turn that
    used two models is not silently summed into one number."""

    input_tokens: int
    output_tokens: int
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0
    reasoning_tokens: int = 0
    billable_input_tokens: int | None = None
    model: str | None = None
    source: str | None = None


# --- terminal outcomes — mutually exclusive, exactly one per stream -------------------

#: The one ``Completed.degrade`` value today: a model rejected an attached
#: binary block and the turn completed without it.
MEDIA_DECODE_DEGRADE = "media_decode"


@dataclass(frozen=True, slots=True)
class Completed:
    """The turn finished producing output. ``finish_reason`` is an open
    string: ``"stop"`` normally, ``"length"`` when a step/recursion budget
    ended the turn (a fact adapters used to lose)."""

    finish_reason: str
    degrade: str | None = None


@dataclass(frozen=True, slots=True)
class Interrupted:
    """The turn is paused; nothing further is emitted on this stream until a
    new request resumes it with the continuation token."""

    interrupt: Interrupt


@dataclass(frozen=True, slots=True)
class AgentFailure:
    """A classified, wire-neutral failure. ``code``/``param`` carry an
    upstream error envelope losslessly; ``category="cancelled"`` is a
    client disconnect."""

    category: Literal["upstream_error", "invalid_request", "cancelled", "internal"]
    message: str
    retryable: bool = False
    code: str | None = None
    param: str | None = None


@dataclass(frozen=True, slots=True)
class Failed:
    """The turn ended in failure (including client-disconnect cancellation)."""

    error: AgentFailure


type AgentEvent = (
    TextDelta
    | ReasoningDelta
    | Notice
    | ToolCallStarted
    | ToolArgumentsDelta
    | ToolCallCompleted
    | ToolResult
    | UsageDelta
    | Completed
    | Interrupted
    | Failed
)

_TERMINAL_TYPES: tuple[type, ...] = (Completed, Interrupted, Failed)


class AgentRunner(Protocol):
    """The minimal seam a runtime implements and every protocol adapter
    depends on instead of the framework underneath it. ``request`` is the
    consumer's own request type."""

    def run(self, request: object) -> AsyncIterator[AgentEvent]: ...


def is_terminal(event: AgentEvent) -> bool:
    """True for exactly ``Completed`` / ``Interrupted`` / ``Failed``."""
    return isinstance(event, _TERMINAL_TYPES)


class ProtocolViolation(RuntimeError):
    """An ``AgentEvent`` stream broke the single-terminal-outcome contract: a
    second terminal event, or any event after the first terminal one."""


async def enforce_single_terminal_outcome(events: AsyncIterator[AgentEvent]) -> AsyncIterator[AgentEvent]:
    """Wrap a stream so a contract violation raises immediately instead of
    silently reaching an adapter."""
    seen_terminal = False
    async for event in events:
        if seen_terminal:
            raise ProtocolViolation(f"event {type(event).__name__} emitted after a terminal outcome")
        if is_terminal(event):
            seen_terminal = True
        yield event
