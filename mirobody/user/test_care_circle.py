"""The care-circle authorization throat, and the promises the README makes about it.

`docs/images/your-care-circle.svg` — the diagram the README embeds — states
four things, and every one of them is a security property, not decoration:

    "acceptance required to join"          -> status, and pending is not accepted
    "health stays off until you allow it"  -> health_access DEFAULT 0
    "your switch — off by default"         -> on YOUR row, about YOUR record
    "mutual — each member controls their own"

Pinned here at the policy level with a fake repository, so these run with no
database.
"""

from __future__ import annotations

import pytest

from mirobody.user import care_circle as cc


@pytest.fixture
def grants(monkeypatch):
    """Control what the subject grants, without a database."""
    state = {"access": None}

    async def fake(operator_id, subject_id):
        if state["access"] is None:
            return None
        return cc.Membership(health_access=state["access"])

    monkeypatch.setattr(cc, "accepted_membership", fake)
    return state


async def test_no_membership_is_denied(grants):
    grants["access"] = None
    with pytest.raises(cc.CareCircleDenied):
        await cc.resolve_subject(1, 2)


async def test_joining_a_circle_is_not_being_seen(grants):
    """The default. A member with health_access 0 shares nothing, and the
    diagram says so twice — "off by default", "stays off until you allow it"."""
    grants["access"] = cc.ACCESS_NONE
    with pytest.raises(cc.CareCircleDenied, match="has not shared their health record"):
        await cc.resolve_subject(1, 2)


async def test_view_grants_read_and_refuses_write(grants):
    grants["access"] = cc.ACCESS_VIEW
    assert (await cc.resolve_subject(1, 2)).access == cc.ACCESS_VIEW
    with pytest.raises(cc.CareCircleDenied, match="write access"):
        await cc.resolve_subject(1, 2, require_write=True)


async def test_a_read_request_on_a_write_grant_still_gets_read(grants):
    """The grant is trimmed to the request, not to the relationship's ceiling.

    A caller that asked to read must not silently receive the ability to write
    just because the underlying membership allows it — otherwise every read path
    in the app carries write authority it never asked for.
    """
    grants["access"] = cc.ACCESS_EDIT
    assert (await cc.resolve_subject(1, 2)).access == cc.ACCESS_VIEW
    assert (await cc.resolve_subject(1, 2, require_write=True)).access == cc.ACCESS_EDIT


async def test_acting_on_your_own_record_is_not_proxy_access(grants):
    grants["access"] = None          # no membership at all
    for subject in (None, "", 7, "7"):
        s = await cc.resolve_subject(7, subject)
        assert s.subject_id == 7 and s.access == cc.ACCESS_EDIT and s.is_self


async def test_an_int_caller_and_a_string_target_are_the_same_person(grants):
    """Ids arrive as ints from the JWT subject and strings from URL parameters.

    A type-sensitive comparison would take the cross-user branch against the
    caller themselves — demanding a care-circle grant to read your own record.
    """
    grants["access"] = None
    assert (await cc.resolve_subject(7, "7")).is_self
    assert (await cc.resolve_subject("7", 7)).is_self


async def test_denial_raises_rather_than_returning_a_falsy_field():
    """The shape, not just the outcome.

    A denial returned as `{"success": False, ...}` relies on every caller
    remembering to read that key — one that forgets proceeds as if authorized.
    An exception cannot be ignored by accident.
    """
    assert issubclass(cc.CareCircleDenied, Exception)
    import inspect
    src = inspect.getsource(cc.resolve_subject)
    assert "raise CareCircleDenied" in src
    assert "return None" not in src, "denial must not have a falsy return path"


def test_the_wire_values_are_the_ones_in_the_table():
    """These integers are written into every row and must never be renumbered."""
    assert (cc.STATUS_PENDING, cc.STATUS_ACCEPTED, cc.STATUS_DECLINED) == (1, 2, 3)
    assert (cc.ACCESS_NONE, cc.ACCESS_VIEW, cc.ACCESS_EDIT) == (0, 1, 2)
    assert (cc.ROLE_MEMBER, cc.ROLE_MAINTAINER, cc.ROLE_OWNER) == (0, 1, 2)


def test_the_schema_pins_what_the_diagram_promises():
    """Read from the DDL, because a Python default cannot protect a column.

    A row written by anything other than this module — a migration, psql, the
    next service — still has to land on 0.
    """
    import pathlib
    import re

    ddl = pathlib.Path(__file__).resolve().parents[2] / "mirobody" / "schema" / "a2_care_circles.sql"
    sql = " ".join(ddl.read_text().split())          # column alignment is not the contract

    assert re.search(r"health_access\s+SMALLINT\s+NOT NULL DEFAULT 0", sql), "the switch must default to off"
    assert "CHECK (health_access BETWEEN 0 AND 2)" in sql
    assert "CHECK (status BETWEEN 1 AND 3)" in sql

    # `status` has no DEFAULT on purpose: an invitation and an acceptance are
    # different events and the writer must say which — a default status is a
    # membership state nobody asserted.
    assert re.search(r"status\s+SMALLINT\s+NOT NULL CHECK", sql)
    assert not re.search(r"status\s+SMALLINT\s+NOT NULL DEFAULT", sql)

    # Real foreign keys: a membership row must reference actual users and an
    # actual circle, or a deleted account keeps its grants.
    assert sql.count("REFERENCES health_app_user(id)") == 2
    assert "REFERENCES care_circles(id)" in sql


def test_set_health_access_takes_no_subject_argument():
    """It always sets the caller's own row. Whose access it sets is not a
    parameter, because the answer is always "the caller's" — the grant must
    live on a row its owner writes, or "your switch" is not yours."""
    import inspect
    params = list(inspect.signature(cc.set_health_access).parameters)
    assert params == ["user_id", "circle_id", "access"]


@pytest.mark.parametrize("bad", [-1, 3, 99])
async def test_an_out_of_range_switch_is_refused(bad):
    with pytest.raises(ValueError):
        await cc.set_health_access(1, 1, bad)
