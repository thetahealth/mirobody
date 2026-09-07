"""What an agent tool returns, how it fails, and when it may be called again.

The model reads a rendered string; everything a *program* needs to know
about a tool call — did it work, is retrying pointless, how much was cut —
travels out of band in a :class:`ToolResult`. Three rules from production:

* A tool's failure text carries the tool name and the exception's **type**,
  never its message: driver messages quote SQL with bound parameters, HTTP
  messages quote payloads, and models echo whatever they are given.
* A call that failed as ``unrecoverable`` is not retried with the same
  arguments rephrased. :class:`RetryLedger` keys calls by their *normalised*
  arguments, so ``["a","b"]`` and ``["b","a"]`` are one call.
* Caps on how many times a tool runs per turn are the agent framework's job
  (``ToolCallLimitMiddleware``); this module does not count budgets.

Pure; stdlib only. The middleware that plugs these into an agent framework
lives with that framework.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Hashable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Literal

# --- faults --------------------------------------------------------------------------

FAULT_BAD_ARGUMENTS = "bad_arguments"
FAULT_TRANSIENT = "transient"
FAULT_DENIED = "denied"
FAULT_NOT_FOUND = "not_found"
FAULT_INTERNAL = "internal"
FAULT_KINDS = frozenset({FAULT_BAD_ARGUMENTS, FAULT_TRANSIENT, FAULT_DENIED, FAULT_NOT_FOUND, FAULT_INTERNAL})

_TRANSIENT_NAMES = {
    "TimeoutError",
    "ConnectionError",
    "ConnectionResetError",
    "ConnectionRefusedError",
    "BrokenPipeError",
    "ClientConnectorError",
    "ServerDisconnectedError",
    "OperationalError",
    "InterfaceError",
    "TooManyRequests",
    "RateLimitError",
    "APITimeoutError",
    "APIConnectionError",
    "ServiceUnavailable",
}
_BAD_ARGUMENT_NAMES = {
    "ValueError",
    "TypeError",
    "ValidationError",
    "KeyError",
    "JSONDecodeError",
    "UnicodeDecodeError",
}
_DENIED_NAMES = {"PermissionError", "Forbidden", "Unauthorized", "AuthenticationError", "PermissionDeniedError"}
_NOT_FOUND_NAMES = {"FileNotFoundError", "LookupError", "NotFound", "NotFoundError", "IndexError"}


def classify_fault(exc: BaseException) -> str:
    """A tool exception → one of five kinds the model can act on. By type
    name (including bases), so a driver's ``OperationalError`` is transient
    without importing the driver."""
    names = {c.__name__ for c in type(exc).__mro__}
    if names & _DENIED_NAMES:
        return FAULT_DENIED
    if names & _TRANSIENT_NAMES:
        return FAULT_TRANSIENT
    if names & _NOT_FOUND_NAMES and "KeyError" not in names:
        return FAULT_NOT_FOUND
    if names & _BAD_ARGUMENT_NAMES:
        return FAULT_BAD_ARGUMENTS
    return FAULT_INTERNAL


_FAULT_HINT = {
    FAULT_BAD_ARGUMENTS: "The arguments were not accepted. Check the tool's parameter list and try once with corrected arguments.",
    FAULT_TRANSIENT: "The tool is temporarily unavailable. Continue with what you already have or try a narrower request; do not repeat this exact call.",
    FAULT_DENIED: "You are not allowed to read this. Say so; do not try another route to the same data.",
    FAULT_NOT_FOUND: "Nothing matched. Treat this as 'not on file', which is not the same as 'not true'.",
    FAULT_INTERNAL: "The tool failed. Continue with what you already have; do not repeat this exact call and never show this message verbatim.",
}


def fault_text(kind: str, tool_name: str, exc: BaseException | None = None) -> str:
    """The JSON the model sees in place of a crashed tool result — tool name,
    fault kind, exception type, guidance. Never the exception message."""
    body = {"error": f"{tool_name} failed ({kind})", "error_kind": kind, "hint": _FAULT_HINT[kind]}
    if exc is not None:
        body["exception_type"] = type(exc).__name__
    return json.dumps(body, ensure_ascii=False)


# --- results ------------------------------------------------------------------------

STATUS_OK = "ok"
STATUS_PARTIAL = "partial"
STATUS_ERROR = "error"

ERROR_RECOVERABLE = "recoverable"
ERROR_UNRECOVERABLE = "unrecoverable"

#: The closed set of ``error_kind`` values a tool may report.
ERROR_KINDS = frozenset(
    {"invalid_arguments", "timeout", "unavailable", "no_data", "denied", "internal", "repeated_call"}
)

#: Fault kind → whether retrying the same call can help, and the kind reported.
_ERROR_CLASS_FOR = {
    FAULT_BAD_ARGUMENTS: ERROR_RECOVERABLE,
    FAULT_TRANSIENT: ERROR_RECOVERABLE,
    FAULT_DENIED: ERROR_UNRECOVERABLE,
    FAULT_NOT_FOUND: ERROR_UNRECOVERABLE,
    FAULT_INTERNAL: ERROR_UNRECOVERABLE,
}
_ERROR_KIND_FOR = {
    FAULT_BAD_ARGUMENTS: "invalid_arguments",
    FAULT_TRANSIENT: "unavailable",
    FAULT_DENIED: "denied",
    FAULT_NOT_FOUND: "no_data",
    FAULT_INTERNAL: "internal",
}


def error_class_for(fault_kind: str) -> str:
    return _ERROR_CLASS_FOR[fault_kind]


def error_kind_for(fault_kind: str) -> str:
    return _ERROR_KIND_FOR[fault_kind]


NEXT_NARROW_WINDOW = "narrow_window"
NEXT_AGGREGATE = "aggregate"
NEXT_USE_INDICATORS = "use_indicators"
NEXT_PICK_FROM_CATALOG = "pick_from_catalog"
NEXT_PROBE_UNMAPPED = "probe_unmapped"
NEXT_STEPS = frozenset(
    {NEXT_NARROW_WINDOW, NEXT_AGGREGATE, NEXT_USE_INDICATORS, NEXT_PICK_FROM_CATALOG, NEXT_PROBE_UNMAPPED}
)


@dataclass(frozen=True)
class Meta:
    """How the answer was produced — methodology, never findings."""

    window: tuple[str, str] = ("", "")  # local dates, inclusive
    tz: str = ""
    window_semantics: str = "tz_exact"  # or date_padded_naive, for stores without a tz-aware time column
    resolution: str = ""
    aggregate: str = ""
    aggregate_basis: str = ""  # readings | buckets | daily
    row_count: int = 0
    truncated: bool = False
    catalog_total: int = 0


@dataclass(frozen=True)
class Envelope:
    """The out-of-band record of one tool call (named to stay apart from
    ``events.ToolResult``, the stream event). The rendered text the model
    reads is derived from ``data`` by a renderer the caller owns; governance
    reads ``status``/``error_class`` from here, never from the text (which
    a harness may truncate or evict)."""

    status: Literal["ok", "partial", "error"]
    data: Sequence[Mapping[str, object]] | None = None
    meta: Meta = field(default_factory=Meta)
    error_class: Literal["recoverable", "unrecoverable"] | None = None
    error_kind: str | None = None  # one of ERROR_KINDS
    provenance: Mapping[str, str] = field(
        default_factory=dict
    )  # per indicator: measured|computed|elected:<rule>|unknown
    assumptions: tuple[str, ...] = ()
    next_steps: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.status == STATUS_ERROR and self.error_class is None:
            raise ValueError("an error result says whether it is recoverable")
        if self.status != STATUS_ERROR and self.error_class is not None:
            raise ValueError("only an error result carries an error_class")
        bad = set(self.next_steps) - NEXT_STEPS
        if bad:
            raise ValueError(f"unknown next_steps {sorted(bad)}")
        if self.error_kind is not None and self.error_kind not in ERROR_KINDS:
            raise ValueError(f"unknown error_kind {self.error_kind!r}")


def invalid_arguments(reason: str) -> Envelope:
    """The structured refusal for arguments the schema does not accept.
    Recoverable: the model can fix its call."""
    return Envelope(
        STATUS_ERROR,
        error_class=ERROR_RECOVERABLE,
        error_kind="invalid_arguments",
        assumptions=(reason,),
    )


def fault_envelope(exc: BaseException) -> Envelope:
    """The envelope a fault middleware attaches as the tool message's
    artifact, so governance learns the class without parsing text."""
    kind = classify_fault(exc)
    return Envelope(STATUS_ERROR, error_class=error_class_for(kind), error_kind=error_kind_for(kind))


def repeated_call(tool: str, reason: str) -> Envelope:
    """The refusal a retry ledger returns instead of running the tool."""
    return Envelope(
        STATUS_ERROR,
        error_class=ERROR_UNRECOVERABLE if reason == "unrecoverable" else ERROR_RECOVERABLE,
        error_kind="repeated_call",
        assumptions=(f"{tool}: {reason}",),
        next_steps=(NEXT_NARROW_WINDOW, NEXT_USE_INDICATORS),
    )


# --- retry governance ------------------------------------------------------------


def canonical_key(args: Mapping[str, object]) -> str:
    """Arguments normalised for equality: keys sorted, ``None`` dropped,
    lists of scalars sorted, strings stripped. Two calls that differ only
    in order or in an omitted ``None`` are the same call."""

    def norm(v: object) -> object:
        if isinstance(v, Mapping):
            return {k: norm(x) for k, x in sorted(v.items()) if x is not None}
        if isinstance(v, list | tuple | set | frozenset):
            items = [norm(x) for x in v]
            try:
                return sorted(items)  # type: ignore[type-var]
            except TypeError:
                return items
        if isinstance(v, str):
            return v.strip()
        return v

    return json.dumps(norm(args), sort_keys=True, ensure_ascii=False, default=str)


@dataclass
class RetryLedger:
    """Per-turn memory of what was called with what, and how it went.

    ``limit`` is the number of times one ``(tool, key)`` may run in a turn;
    after a result marked ``unrecoverable`` the same key is refused at once.
    No default limit: a research agent and a triage bot want different
    numbers, and a wrong default is invisible until it bites."""

    limit: int
    key: Callable[[Mapping[str, object]], Hashable] = canonical_key
    #: Optional ``similar(new_key, dead_key)`` for free-text parameters, so a
    #: rephrased keyword search of an unrecoverable call is refused too.
    similar: Callable[[Hashable, Hashable], bool] | None = None
    _calls: dict[tuple[str, Hashable], int] = field(default_factory=dict, init=False)
    _dead: set[tuple[str, Hashable]] = field(default_factory=set, init=False)

    def __post_init__(self) -> None:
        if self.limit < 1:
            raise ValueError("limit must be >= 1")

    def allows(self, tool: str, args: Mapping[str, object]) -> tuple[bool, str]:
        k = (tool, self.key(args))
        if k in self._dead:
            return False, "unrecoverable"
        if self.similar is not None and any(t == tool and self.similar(k[1], dk) for t, dk in self._dead):
            return False, "unrecoverable"
        if self._calls.get(k, 0) >= self.limit:
            return False, "limit"
        return True, ""

    def record(self, tool: str, args: Mapping[str, object], *, error_class: str | None = None) -> None:
        k = (tool, self.key(args))
        self._calls[k] = self._calls.get(k, 0) + 1
        if error_class == ERROR_UNRECOVERABLE:
            self._dead.add(k)

    def refusal_text(self, tool: str, reason: str) -> str:
        why = (
            "This exact request already failed in a way that will not change on retry."
            if reason == "unrecoverable"
            else f"{tool} has already run with these arguments the maximum number of times this turn."
        )
        return json.dumps(
            {
                "error": f"{tool} not run",
                "error_kind": "retry_refused",
                "hint": why + " Change the question, or answer with what you have.",
            },
            ensure_ascii=False,
        )


# --- tool specs -------------------------------------------------------------------

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{2,63}$")


def lint_tools(
    specs: Sequence[Mapping[str, object]], *, min_description: int = 80, max_description: int = 4000
) -> tuple[str, ...]:
    """Problems with a list of tool specs (``{name, description, inputSchema}``)
    a model will see: names not snake_case, descriptions too short to guide
    or too long to read, schemas without ``additionalProperties: false``,
    string parameters with a small closed set of values but no ``enum``,
    duplicate names. Returns human-readable findings; empty means clean."""
    out: list[str] = []
    seen: set[str] = set()
    for spec in specs:
        name = str(spec.get("name", ""))
        if not _NAME_RE.match(name):
            out.append(f"{name or '<unnamed>'}: name must match {_NAME_RE.pattern}")
        if name in seen:
            out.append(f"{name}: duplicate tool name")
        seen.add(name)
        desc = str(spec.get("description", "") or "")
        if len(desc) < min_description:
            out.append(
                f"{name}: description is {len(desc)} chars; say when to use it, when not to, and what comes back"
            )
        if len(desc) > max_description:
            out.append(f"{name}: description is {len(desc)} chars; move usage detail into the result's next_steps")
        schema = spec.get("inputSchema") or spec.get("input_schema") or {}
        if not isinstance(schema, Mapping):
            out.append(f"{name}: inputSchema is not an object")
            continue
        if schema.get("additionalProperties", True) is not False:
            out.append(f"{name}: inputSchema should set additionalProperties: false")
        props = schema.get("properties") or {}
        if isinstance(props, Mapping):
            for pname, p in props.items():
                if not isinstance(p, Mapping):
                    continue
                if not str(p.get("description", "")).strip() and "enum" not in p:
                    out.append(f"{name}.{pname}: parameter has no description")
                d = str(p.get("description", "")).lower()
                if p.get("type") == "string" and "enum" not in p and ("one of" in d or "either" in d):
                    out.append(f"{name}.{pname}: description lists allowed values; declare them as an enum")
    return tuple(out)


__all__ = [
    "ERROR_KINDS",
    "ERROR_RECOVERABLE",
    "ERROR_UNRECOVERABLE",
    "Envelope",
    "FAULT_KINDS",
    "Meta",
    "NEXT_STEPS",
    "RetryLedger",
    "canonical_key",
    "classify_fault",
    "error_class_for",
    "error_kind_for",
    "fault_envelope",
    "fault_text",
    "invalid_arguments",
    "lint_tools",
    "repeated_call",
]
