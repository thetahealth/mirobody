"""Who may authorize a care-circle share.

The share_id branch of `authorize_invitation` ran

    UPDATE th_share_relationship SET status='authorized', permissions=:p
    WHERE share_id = :share_id

with no predicate naming the caller. Any authenticated user could flip any row
to `authorized` with permissions of their choosing — insert a pending share
against a victim's email, authorize it, read the victim's whole record, no
notification on their side. The (owner, member) branch was safe only because
one router happened to pass the caller's own id into one of the slots; safety
by call-site convention, which the other branch did not follow.

`execute_query` is the only seam; no database.
"""

from __future__ import annotations

import pytest

from mirobody.user.sharing import SharingService


@pytest.fixture
def svc(monkeypatch):
    """A service whose UPDATE reports back what it would have matched."""
    seen: list[tuple[str, dict]] = []

    # One row exists: share_id "S1", owner 100, member 200.
    ROW = {"share_id": "S1", "owner_user_id": "100", "member_user_id": "200"}

    async def fake_execute_query(query, params=None, *a, **kw):
        seen.append((query, params or {}))
        if "UPDATE th_share_relationship" in query:
            acting = str((params or {}).get("acting_user_id", ""))
            if "acting_user_id" in query:
                if acting not in (ROW["owner_user_id"], ROW["member_user_id"]):
                    return []          # predicate matched nothing
                return [ROW]
            return [ROW]
        if "health_app_user" in query:
            return [{"name": "Victim", "email": "victim@example.com"}]
        return []

    monkeypatch.setattr("mirobody.user.sharing.execute_query", fake_execute_query)
    service = SharingService()
    service._seen = seen
    return service


@pytest.mark.asyncio
async def test_a_third_party_cannot_authorize_someone_elses_share(svc):
    """The finding. Caller 999 is neither owner (100) nor member (200)."""
    out = await svc.authorize_invitation(
        share_id="S1", permission={"all": 1}, acting_user_id="999"
    )
    assert out["code"] != 0


@pytest.mark.asyncio
async def test_the_owner_may_authorize(svc):
    out = await svc.authorize_invitation(
        share_id="S1", permission={"all": 1}, acting_user_id="100"
    )
    assert out["code"] == 0


@pytest.mark.asyncio
async def test_the_member_may_authorize(svc):
    """Both sides are parties: the owner approves someone they share with, the
    member accepts an invitation addressed to them."""
    out = await svc.authorize_invitation(
        share_id="S1", permission={"all": 1}, acting_user_id="200"
    )
    assert out["code"] == 0


@pytest.mark.asyncio
async def test_the_update_actually_carries_the_caller(svc):
    """Pinned at the SQL, not at the return value: a future refactor that drops
    the predicate would still return code 0 from the fake."""
    await svc.authorize_invitation(share_id="S1", permission={"all": 1},
                                   acting_user_id="100")
    sql, params = next((q, p) for q, p in svc._seen if "UPDATE th_share_relationship" in q)
    assert "acting_user_id" in sql
    assert "owner_user_id = :acting_user_id" in sql
    assert "member_user_id = :acting_user_id" in sql
    assert params["acting_user_id"] == "100"


@pytest.mark.asyncio
async def test_an_anonymous_call_is_refused(svc):
    out = await svc.authorize_invitation(share_id="S1", permission={"all": 1})
    assert out["code"] != 0


@pytest.mark.asyncio
async def test_the_pair_form_also_requires_the_caller_to_be_a_party(svc):
    out = await svc.authorize_invitation(
        owner_user_id="100", query_user_id="200",
        permission={"all": 1}, acting_user_id="999",
    )
    assert out["code"] != 0


@pytest.mark.asyncio
async def test_required_email_verification_cannot_be_skipped_by_omission(svc):
    """`if email and verification_code:` meant omitting BOTH skipped the check,
    while the endpoint's docstring said it always requires one."""
    out = await svc.authorize_invitation(
        share_id="S1", permission={"all": 1}, acting_user_id="200",
        require_email_verification=True,
    )
    assert out["code"] != 0
    assert "verification" in out["msg"].lower()


@pytest.mark.asyncio
async def test_a_refused_caller_and_a_missing_row_look_the_same(svc):
    """Otherwise the endpoint becomes an oracle for which share_ids exist."""
    refused = await svc.authorize_invitation(
        share_id="S1", permission={"all": 1}, acting_user_id="999")
    missing = await svc.authorize_invitation(
        share_id="NOPE", permission={"all": 1}, acting_user_id="999")
    assert refused == missing
