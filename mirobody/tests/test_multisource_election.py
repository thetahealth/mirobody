"""Multi-source arbitration: when two streams disagree about one day, who wins?

This is the mechanism behind the paper's "multi-source" claim: the layer does not
average conflicting readings, it elects one per (subject, member, day), by a
stated policy, and records the ones it refused. The policy is a pure function
(`kernel.series.elect`), so it can be tested without a database.

Each test below encodes one clause of the documented policy:

  a measurer beats a profile echo   a scale MEASURES weight; a watch ECHOES the
                                    weight typed in months ago, dated today
  an unchanged echo is no         `echo_changed is False` means it says nothing
  information                     about today, so it is skipped entirely
  coverage breaks a class tie     the stream that watched more of the day
  freshness breaks the next tie   the measurement instant, never the write time
  static priority breaks the last the deployment's own list
  a validator can reject          an impossible number is arithmetic, not opinion
"""

from __future__ import annotations

from mirobody.kernel.series import (
    SOURCE_MANUAL,
    SOURCE_MEASURER,
    SOURCE_PROFILE_ECHO,
    Candidate,
    elect,
)

DAY_MS = 86_400_000


def cand(identity, source_class, value, *, measured=None, coverage=0.0, echo_changed=None):
    return Candidate(
        identity=identity,
        source_class=source_class,
        values={"weight": value},
        measured_at_ms=measured,
        coverage_ms=coverage,
        echo_changed=echo_changed,
    )


def test_measurer_beats_profile_echo():
    """A scale reading beats the profile value echoed onto today's date."""
    decision = elect([
        cand("watch", SOURCE_PROFILE_ECHO, 80.0, measured=DAY_MS, echo_changed=True),
        cand("scale", SOURCE_MEASURER, 81.2, measured=DAY_MS - 3_600_000),
    ])
    assert decision.elected is not None
    assert decision.elected.identity == "scale", decision.elected.identity


def test_unchanged_echo_carries_no_information():
    """The same echoed number as last time is skipped, not merely ranked low."""
    echo = cand("watch", SOURCE_PROFILE_ECHO, 80.0, measured=DAY_MS, echo_changed=False)
    assert not echo.is_information
    decision = elect([echo, cand("scale", SOURCE_MEASURER, 81.2, measured=DAY_MS - 3_600_000)])
    assert decision.elected.identity == "scale"


def test_coverage_breaks_a_class_tie():
    """Two measurers: the one that watched more of the day wins."""
    decision = elect([
        cand("phone", SOURCE_MEASURER, 7412, measured=DAY_MS, coverage=2 * 3_600_000),
        cand("watch", SOURCE_MEASURER, 7890, measured=DAY_MS, coverage=9 * 3_600_000),
    ])
    assert decision.elected.identity == "watch", decision.elected.identity


def test_freshness_breaks_a_coverage_tie():
    """Same class, same coverage: the more recent measurement wins."""
    decision = elect([
        cand("old", SOURCE_MEASURER, 80.0, measured=DAY_MS - 8 * 3_600_000, coverage=60_000),
        cand("new", SOURCE_MEASURER, 82.0, measured=DAY_MS - 60_000, coverage=60_000),
    ])
    assert decision.elected.identity == "new", decision.elected.identity


def test_static_priority_breaks_the_last_tie():
    """The deployment's own ordering decides when everything else is equal."""
    a = cand("a", SOURCE_MANUAL, 80.0, measured=DAY_MS, coverage=1.0)
    b = cand("b", SOURCE_MANUAL, 81.0, measured=DAY_MS, coverage=1.0)
    assert elect([a, b], priorities=("b", "a")).elected.identity == "b"
    assert elect([a, b], priorities=("a", "b")).elected.identity == "a"


def test_a_validator_can_reject_and_the_next_wins():
    """An impossible value is refused, and the refusal is reported."""
    def too_high(c: Candidate):
        return ["weight 300 kg is not plausible"] if (c.values.get("weight") or 0) > 300 else []

    decision = elect([
        cand("broken", SOURCE_MEASURER, 3000.0, measured=DAY_MS, coverage=9e5),
        cand("ok", SOURCE_MEASURER, 81.0, measured=DAY_MS, coverage=1.0),
    ], validators=(too_high,))
    assert decision.elected.identity == "ok"
    assert decision.blocked.get("broken"), decision.blocked


def test_election_is_deterministic_and_fingerprinted():
    """Same inputs, same decision and same fingerprint: a rerun is a no-op."""
    cands = [
        cand("a", SOURCE_MEASURER, 80.0, measured=DAY_MS, coverage=10.0),
        cand("b", SOURCE_MEASURER, 81.0, measured=DAY_MS, coverage=5.0),
    ]
    d1 = elect(list(cands))
    d2 = elect(list(cands))
    assert d1.elected.identity == d2.elected.identity == "a"
    assert d1.fingerprint == d2.fingerprint


if __name__ == "__main__":  # runs without pytest
    failures = 0
    for name, fn in sorted(globals().items()):
        if not name.startswith("test_") or not callable(fn):
            continue
        try:
            fn()
            print(f"  PASS {name}")
        except AssertionError as exc:
            failures += 1
            print(f"  FAIL {name}: {str(exc)[:180]}")
        except Exception as exc:
            failures += 1
            print(f"  ERROR {name}: {exc!r}")
    raise SystemExit(1 if failures else 0)
