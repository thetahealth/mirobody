"""Observability without health data in it.

A log line, a metric tag or a trace attribute may carry identifiers, counts,
durations, status codes and type names — and nothing else. Not a reading's
value, not a comment, not a drug name, not a chat message, not an email, and
not a database driver's exception text (which quotes the statement with its
bound parameters). Three layers enforce it: a static lint over the source
(``mirobody.testing.phi_lint``), the runtime :class:`PHIFilter` installed by
:meth:`PHIPolicy.install`, and an end-to-end canary the deployment runs.

This module is the runtime layer. Stdlib only.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field

#: The ``extra=`` keys a log record may carry. Everything else is dropped by
#: the filter. Names, not values: a key called ``user_id`` may carry an id,
#: a key called ``value`` may not exist.
LOG_FIELDS: frozenset[str] = frozenset(
    {
        "user_id",
        "subject_id",
        "session_id",
        "request_id",
        "msg_id",
        "task_id",
        "tool_name",
        "provider",
        "data_type",
        "indicator_count",
        "row_count",
        "record_count",
        "error_type",
        "error_kind",
        "status",
        "status_code",
        "duration_ms",
        "attempt",
        "limit",
        "model",
        "input_tokens",
        "output_tokens",
        "finish_reason",
        "kind",
        "count",
    }
)

#: Exception classes whose ``str()`` is known to quote SQL and parameters.
DRIVER_EXCEPTION_PREFIXES: tuple[str, ...] = ("psycopg", "asyncpg", "sqlalchemy", "pymysql", "aiomysql", "sqlite3")

_STANDARD_ATTRS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys()) | {
    "message",
    "asctime",
    "taskName",
}


def safe_error_text(exc: BaseException) -> str:
    """What may be said about an exception in a log or to a client: its type
    name. The message can carry a statement, a token or a payload fragment;
    the traceback goes to the log's ``exc_info`` handling, which the filter
    also scrubs for driver exceptions."""
    return type(exc).__name__


def is_driver_exception(exc: BaseException) -> bool:
    mod = type(exc).__module__ or ""
    return mod.split(".")[0] in DRIVER_EXCEPTION_PREFIXES


def redact(record: Mapping[str, object], allowed: Iterable[str] = LOG_FIELDS) -> dict[str, object]:
    """A mapping reduced to its allowed keys — for structured logs, metric
    tags and trace attributes alike."""
    allow = set(allowed)
    return {k: v for k, v in record.items() if k in allow}


@dataclass(frozen=True)
class PHIPolicy:
    """What the runtime filter enforces. ``max_message_chars`` bounds a
    formatted message (a 40 kB message is a payload dump, whatever it says);
    ``allowed_fields`` are the ``extra`` keys that survive."""

    max_message_chars: int = 300
    allowed_fields: frozenset[str] = field(default_factory=lambda: LOG_FIELDS)
    driver_prefixes: tuple[str, ...] = DRIVER_EXCEPTION_PREFIXES

    def install(self, logger: logging.Logger | None = None) -> PHIFilter:
        """Attach the filter to ``logger`` (the root by default) — once; a
        second install returns the existing filter."""
        target = logger or logging.getLogger()
        for f in target.filters:
            if isinstance(f, PHIFilter):
                return f
        flt = PHIFilter(self)
        target.addFilter(flt)
        for h in target.handlers:  # handlers filter independently of the logger
            h.addFilter(flt)
        return flt


class PHIFilter(logging.Filter):
    """Truncates long messages, drops non-allowlisted ``extra`` attributes and
    replaces a database driver's exception text with the type name."""

    def __init__(self, policy: PHIPolicy):
        super().__init__()
        self.policy = policy
        self.dropped_fields = 0
        self.truncated = 0
        self.scrubbed_exceptions = 0

    def filter(self, record: logging.LogRecord) -> bool:
        if getattr(record, "_phi_seen", False):  # installed on the logger and its handlers: filter once
            return True
        record._phi_seen = True  # noqa: SLF001 — our own marker
        for key in list(vars(record)):
            if key == "_phi_seen":
                continue
            if key not in _STANDARD_ATTRS and key not in self.policy.allowed_fields:
                delattr(record, key)
                self.dropped_fields += 1
        if record.exc_info and record.exc_info[1] is not None:
            exc = record.exc_info[1]
            if (type(exc).__module__ or "").split(".")[0] in self.policy.driver_prefixes:
                record.exc_info = None
                record.exc_text = None
                record.msg = f"{record.getMessage()} [{safe_error_text(exc)}]"
                record.args = ()
                self.scrubbed_exceptions += 1
        try:
            msg = record.getMessage()
        except Exception:  # a bad format string is not this filter's problem
            return True
        if len(msg) > self.policy.max_message_chars:
            record.msg = msg[: self.policy.max_message_chars] + f"… [{len(msg)} chars truncated by PHIFilter]"
            record.args = ()
            self.truncated += 1
        return True


__all__ = [
    "DRIVER_EXCEPTION_PREFIXES",
    "LOG_FIELDS",
    "PHIFilter",
    "PHIPolicy",
    "is_driver_exception",
    "redact",
    "safe_error_text",
]
