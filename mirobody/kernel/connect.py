"""The connection contract: what a provider integration promises about a
credential, a pull, and what it can do at all.

Three things every deployment re-learned on its own:

* a credential has a **life cycle** (``linked → refreshing → expired →
  revoked``) and the pull loop must not hammer a vendor with a token it has
  already been told is dead — the third consecutive authorization failure
  in a row is not a transient error, it is the credential telling you it
  expired;
* a **backfill** is a bounded list of windows, never "everything since
  forever" in one request;
* what a provider can do is a **set of capabilities**, some of which exclude
  each other, checked once at registration instead of at every call site.

Pure; stdlib only. The IO (OAuth dances, HTTP, token storage) stays with
whoever runs it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Protocol

# --- credential life cycle --------------------------------------------------------

LINKED = "linked"
REFRESHING = "refreshing"
EXPIRED = "expired"
REVOKED = "revoked"
STATES = frozenset({LINKED, REFRESHING, EXPIRED, REVOKED})

EV_REFRESH_STARTED = "refresh_started"
EV_REFRESH_OK = "refresh_ok"
EV_REFRESH_FAILED = "refresh_failed"
EV_UNAUTHORIZED = "unauthorized"  # the vendor answered 401/403 to a data call
EV_USER_UNLINKED = "user_unlinked"
EV_RELINKED = "relinked"  # the user went through the link flow again

_TRANSITIONS: dict[tuple[str, str], str] = {
    (LINKED, EV_REFRESH_STARTED): REFRESHING,
    (LINKED, EV_UNAUTHORIZED): EXPIRED,
    (LINKED, EV_USER_UNLINKED): REVOKED,
    (REFRESHING, EV_REFRESH_OK): LINKED,
    (REFRESHING, EV_REFRESH_FAILED): EXPIRED,
    (REFRESHING, EV_USER_UNLINKED): REVOKED,
    (EXPIRED, EV_REFRESH_STARTED): REFRESHING,
    (EXPIRED, EV_RELINKED): LINKED,
    (EXPIRED, EV_USER_UNLINKED): REVOKED,
    (REVOKED, EV_RELINKED): LINKED,
}


def transition(state: str, event: str) -> str:
    """The next credential state, or ``ValueError`` for an illegal move — a
    pull loop that refreshes a revoked token has a bug, not a state."""
    try:
        return _TRANSITIONS[(state, event)]
    except KeyError:
        raise ValueError(f"credential cannot go from {state!r} on {event!r}") from None


@dataclass(frozen=True)
class Credential:
    """What the framework knows about one linked account — never the secret
    itself. ``failures`` counts consecutive authorization failures."""

    provider: str
    subject_id: str
    state: str = LINKED
    failures: int = 0
    last_failure_ms: int = 0
    external_user_id: str = ""

    def __post_init__(self) -> None:
        if self.state not in STATES:
            raise ValueError(f"unknown credential state {self.state!r}")


@dataclass(frozen=True)
class DebouncePolicy:
    """After ``threshold`` consecutive authorization failures the credential
    is treated as expired; a retry is allowed only ``cooldown_ms`` after the
    last failure. No defaults — a vendor with a flaky auth server wants a
    higher threshold than one that never fails."""

    threshold: int
    cooldown_ms: int

    def __post_init__(self) -> None:
        if self.threshold < 1 or self.cooldown_ms < 0:
            raise ValueError("threshold >= 1, cooldown_ms >= 0")


def record_failure(cred: Credential, *, now_ms: int, policy: DebouncePolicy) -> Credential:
    """One more authorization failure. Crossing the threshold expires the
    credential; a pull loop then leaves it alone until the user relinks."""
    n = cred.failures + 1
    state = EXPIRED if n >= policy.threshold and cred.state in (LINKED, REFRESHING) else cred.state
    return replace(cred, failures=n, last_failure_ms=now_ms, state=state)


def record_success(cred: Credential) -> Credential:
    return replace(cred, failures=0, last_failure_ms=0, state=LINKED if cred.state == REFRESHING else cred.state)


def may_attempt(cred: Credential, *, now_ms: int, policy: DebouncePolicy) -> bool:
    """Whether the pull loop should try this credential now: not when it is
    expired or revoked, and not inside the cooldown after a failure."""
    if cred.state in (EXPIRED, REVOKED):
        return False
    if cred.failures and now_ms - cred.last_failure_ms < policy.cooldown_ms:
        return False
    return True


# --- capabilities ------------------------------------------------------------------

CAP_PULL = "pull"  # we call the vendor on a schedule
CAP_WEBHOOK = "webhook"  # the vendor calls us
CAP_BACKFILL = "backfill"  # history can be requested by window
CAP_INTRADAY = "intraday"  # sub-daily samples
CAP_MULTI_ACCOUNT = "multi_account"  # one subject may link several accounts
CAP_MEDICATIONS = "medications"
CAP_DOCUMENTS = "documents"
CAPABILITIES = frozenset(
    {CAP_PULL, CAP_WEBHOOK, CAP_BACKFILL, CAP_INTRADAY, CAP_MULTI_ACCOUNT, CAP_MEDICATIONS, CAP_DOCUMENTS}
)

#: Pairs that make no sense together; checked at registration.
EXCLUSIVE: tuple[tuple[str, str], ...] = ()
#: A capability that presumes another.
REQUIRES: dict[str, str] = {CAP_BACKFILL: CAP_PULL}


def validate_capabilities(caps: frozenset[str] | set[str]) -> frozenset[str]:
    caps = frozenset(caps)
    unknown = caps - CAPABILITIES
    if unknown:
        raise ValueError(f"unknown capabilities {sorted(unknown)}")
    for a, b in EXCLUSIVE:
        if a in caps and b in caps:
            raise ValueError(f"{a} and {b} exclude each other")
    for cap, needs in REQUIRES.items():
        if cap in caps and needs not in caps:
            raise ValueError(f"{cap} requires {needs}")
    return caps


# --- coverage ------------------------------------------------------------------------


@dataclass(frozen=True)
class Coverage:
    """What a connector ACTUALLY carries, as opposed to what it could.

    A capability list says a connector can pull, back-fill and speak webhooks.
    It does not say whether it brings blood pressure, and a person choosing a
    device wants the second answer. Nor does the presence of a decoder: a
    factory that supports a vendor and a vendor that supplies a metric are
    different claims, and conflating them is how a documentation page comes to
    promise data nobody's integration produces.

    Derived from the decoder's own table (`vendors.coverage_of`), never
    hand-written, so it cannot drift from the code. Two consequences worth
    stating: a metric here is one the decoder can EMIT — whether a given
    person's device records it is a different question again — and a
    connector with an empty `timeseries` is a connector that decodes nothing,
    which is a bug rather than a modest integration.
    """

    provider: str
    timeseries: frozenset[str] = frozenset()
    data_types: frozenset[str] = frozenset()
    medications: bool = False
    documents: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "timeseries", frozenset(self.timeseries))
        object.__setattr__(self, "data_types", frozenset(self.data_types))

    @property
    def metric_count(self) -> int:
        return len(self.timeseries)

    def covers(self, metric: str) -> bool:
        return metric in self.timeseries

    def missing(self, wanted: frozenset[str] | set[str]) -> frozenset[str]:
        """What `wanted` asks for and this connector cannot give — the answer
        to "will switching to this device keep my charts working"."""
        return frozenset(wanted) - self.timeseries


# --- pulls and backfills -----------------------------------------------------------


@dataclass(frozen=True)
class PullWindow:
    start_ms: int
    end_ms: int  # exclusive

    def __post_init__(self) -> None:
        if self.end_ms <= self.start_ms:
            raise ValueError("window end must be after start")


@dataclass(frozen=True)
class RawBatch:
    """What one pull returned: the vendor's records untouched, the cursor to
    continue from, and when we asked."""

    records: tuple[Mapping[str, object], ...]
    pulled_at_ms: int
    cursor: str = ""
    data_type: str = ""


class Puller(Protocol):
    def pull(self, credential: Credential, window: PullWindow, *, data_type: str, cursor: str = "") -> RawBatch: ...


def backfill_windows(start_ms: int, end_ms: int, *, chunk_ms: int, max_chunks: int) -> tuple[PullWindow, ...]:
    """The bounded list of windows a backfill walks, oldest first. A request
    that would need more than ``max_chunks`` is refused rather than
    silently truncated: the caller decides what to drop."""
    if chunk_ms <= 0 or max_chunks <= 0:
        raise ValueError("chunk_ms and max_chunks must be positive")
    if end_ms <= start_ms:
        return ()
    n = -(-(end_ms - start_ms) // chunk_ms)
    if n > max_chunks:
        raise ValueError(f"backfill needs {n} windows, more than max_chunks={max_chunks}")
    return tuple(PullWindow(s, min(s + chunk_ms, end_ms)) for s in range(start_ms, end_ms, chunk_ms))


__all__ = [
    "CAPABILITIES",
    "Coverage",
    "Credential",
    "DebouncePolicy",
    "EXPIRED",
    "LINKED",
    "Puller",
    "RawBatch",
    "REFRESHING",
    "REVOKED",
    "STATES",
    "PullWindow",
    "backfill_windows",
    "may_attempt",
    "record_failure",
    "record_success",
    "transition",
    "validate_capabilities",
]
