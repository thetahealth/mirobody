"""HTTP surface for the care circle (`/invitation/*`).

Seven endpoints where there were seventeen. The seventeen were a symptom of the
model underneath: because a share was a DIRECTED row (owner O grants member M),
every operation needed a "by me" copy and a "with me" copy, and nicknames needed
three (`shared-by-me/update-nickname`, `shared-with-me/update-nickname`,
`shared/update-nickname`) for one job. A circle is symmetric — the row that says
"you were invited" is the row that says "you are a member" — so one endpoint
answers where two did.

**The endpoint this adds is the one the product always promised.**
`docs/images/your-care-circle.svg` says "health stays off until you allow it ·
your switch — off by default · mutual — each member controls their own". There
was no API for that switch. The only way permissions were ever set was
`shared-by-me/authorize`, where the INVITER chose them, defaulting to
`{"all": 1}` — read everything. `POST /invitation/health-access` is the member's
own switch, and `care_circle_members.health_access` defaults to 0.

Two paths are byte-identical to what the web client calls, because it calls
exactly two: `shared-by-me/list` and `shared-by-me/remove` (grep of
`frontend/assets/*.js`). Their response fields — `status: "authorized"`,
`share_id`, `query_user_id` — are the client's vocabulary, mapped here from the
integers the table stores. The wire stays; the storage got fixed.
"""

import logging

from typing import Optional

from fastapi import APIRouter
from pydantic import BaseModel, Field

from mirobody.server.auth import verify_token
from fastapi import Depends

from ...user import care_circle as cc
from ...user.user import get_user

router = APIRouter(prefix="/invitation", tags=["invitation"])


def _ok(data=None, msg: str = "ok"):
    return {"code": 0, "msg": msg, "data": data if data is not None else {}}


def _err(code: int, msg: str):
    return {"code": code, "msg": msg, "data": {}}


# ── request models ───────────────────────────────────────────────────────────

class InviteRequest(BaseModel):
    email: str = Field(..., description="Address to invite; a shell account is created if new")
    nickname: Optional[str] = Field(None, description="Label this member carries in the circle")


class RemoveRequest(BaseModel):
    # `share_id` is the client's name for the membership handle. It used to be a
    # uuid on `th_share_relationship`; it is now `care_circle_members.id`.
    share_id: Optional[str] = None
    query_user_id: Optional[str] = None


class RespondRequest(BaseModel):
    circle_id: int
    accept: bool = True


class HealthAccessRequest(BaseModel):
    circle_id: int
    access: int = Field(..., ge=0, le=2, description="0 none, 1 read, 2 read-write")


class NicknameRequest(BaseModel):
    share_id: str
    nickname: Optional[str] = None
    avatar_key: Optional[str] = None


# ── the two the web client calls ─────────────────────────────────────────────

@router.post("/shared-by-me/list")
async def shared_by_me_list(user_id: str = Depends(verify_token)):
    """Members of the circles I belong to, in the client's vocabulary.

    `status: "authorized"` rather than 2, because the client filters on that
    string. Keeping the mapping here rather than in the table is the point: a
    varchar status with no CHECK is what let `'pending'` (the column default)
    and `'authorized'` (the only value that worked) drift apart.
    """
    try:
        rows = await cc.circle_members(user_id)
        me = int(user_id)
        return _ok([
            {
                "share_id": str(r["member_row_id"]),
                "query_user_id": str(r["user_id"]),
                "owner_user_id": str(r["owner_user_id"]),
                "circle_id": r["circle_id"],
                "status": cc.STATUS_NAMES[r["status"]].replace("accepted", "authorized"),
                "role": cc.ROLE_NAMES[r["role"]],
                "health_access": r["health_access"],
                "nickname": r.get("nickname"),
                "name": r.get("name"),
                "email": r.get("email"),
            }
            for r in rows if int(r["user_id"]) != me
        ])
    except Exception as e:
        logging.error(f"shared-by-me/list: {e}", exc_info=True)
        return _err(-1, "Could not list your circle.")


@router.post("/shared-by-me/remove")
async def shared_by_me_remove(request: RemoveRequest, user_id: str = Depends(verify_token)):
    """Remove a member from a circle I administer.

    Authorization is not "did you send a share_id" — the row that the handle
    resolves to names its circle, and the caller must be a maintainer of THAT
    circle. The endpoint this replaces ran `UPDATE ... WHERE share_id = :id`
    with no predicate naming the caller.
    """
    handle = (request.share_id or "").strip()
    if not handle.isdigit():
        return _err(-1, "A membership handle is required.")
    resolved = await cc.resolve_member_row(int(handle))
    if resolved is None:
        return _err(-2, "No such membership.")
    circle_id, member_user_id = resolved
    try:
        await cc.require_maintainer(user_id, circle_id)
    except cc.CareCircleDenied as denied:
        # Same answer for "not yours" and "does not exist", so the endpoint is
        # not an oracle for which handles are real.
        logging.info(f"remove refused for user {user_id} on member {handle}: {denied}")
        return _err(-2, "No such membership.")
    if member_user_id == int(user_id):
        return _err(-3, "Leave the circle from your own side instead.")
    return _ok({"removed": await cc.remove_member(int(handle))})


# ── invite and respond ───────────────────────────────────────────────────────

@router.post("/shared-by-me/send")
async def invite_member(request: InviteRequest, user_id: str = Depends(verify_token)):
    """Invite an address into my circle, creating the circle on first use.

    The invitee's account is minted here if the address is new, because that is
    what an invitation is for. Their `health_access` starts at 0 either way: an
    invitation asks someone to join, it does not decide what they share.
    """
    member_id = await cc.resolve_email_to_user(request.email)
    if member_id is None:
        return _err(-1, "A valid email address is required.")
    if member_id == int(user_id):
        return _err(-2, "You are already in your own circle.")
    circle_id = await cc.ensure_own_circle(user_id)
    handle = await cc.invite(circle_id, member_id, nickname=request.nickname)
    return _ok({"share_id": str(handle), "circle_id": circle_id,
                "query_user_id": str(member_id), "status": "pending"})


@router.post("/shared-with-me/list")
async def shared_with_me_list(user_id: str = Depends(verify_token)):
    """Circles I am in or have been invited to, and who shares with me.

    One endpoint for both because it is one query: a pending row and a member
    row are the same row at different statuses.
    """
    rows = await cc.circle_members(user_id)
    me = int(user_id)
    mine = [r for r in rows if int(r["user_id"]) == me]
    return _ok({
        "invitations": [
            {"circle_id": r["circle_id"], "owner_user_id": str(r["owner_user_id"]),
             "status": cc.STATUS_NAMES[r["status"]], "nickname": r.get("nickname")}
            for r in mine if r["status"] == cc.STATUS_PENDING
        ],
        "circles": [
            {"circle_id": r["circle_id"], "name": r["circle_name"],
             "role": cc.ROLE_NAMES[r["role"]], "health_access": r["health_access"]}
            for r in mine if r["status"] == cc.STATUS_ACCEPTED
        ],
        "sharing_with_me": [
            {"user_id": str(r["user_id"]), "name": r.get("name"),
             "nickname": r.get("nickname"), "health_access": r["health_access"]}
            for r in await cc.shared_with_me(user_id)
        ],
    })


@router.post("/shared-with-me/authorize")
async def respond(request: RespondRequest, user_id: str = Depends(verify_token)):
    """Accept or decline an invitation addressed to me.

    Scoped to the caller's own pending row, so this cannot accept on anyone
    else's behalf — which the endpoint it replaces could, for any `share_id`.
    """
    moved = await cc.respond_to_invitation(user_id, request.circle_id, accept=request.accept)
    if not moved:
        return _err(-1, "No pending invitation for you in that circle.")
    return _ok({"status": "accepted" if request.accept else "declined"})


# ── the switch the product promised ──────────────────────────────────────────

@router.post("/health-access")
async def set_health_access(request: HealthAccessRequest, user_id: str = Depends(verify_token)):
    """Choose what MY record shows to one circle. 0 none, 1 read, 2 read-write.

    Always the caller's own row — there is no parameter for whose access this
    sets. Before this endpoint existed, the level lived on a row the *other*
    party wrote, defaulting to read-everything.
    """
    updated = await cc.set_health_access(user_id, request.circle_id, request.access)
    if not updated:
        return _err(-1, "You are not an accepted member of that circle.")
    return _ok({"circle_id": request.circle_id,
                "health_access": request.access,
                "meaning": cc.ACCESS_NAMES[request.access]})


# ── labels ───────────────────────────────────────────────────────────────────

@router.post("/shared/update-nickname")
async def update_label(request: NicknameRequest, user_id: str = Depends(verify_token)):
    """Set the label or picture a member carries in the circle.

    One endpoint. There were three, for one job, because `th_share_user_config`
    keyed nicknames by (setter, target, context) — a nickname per viewer, with a
    `context` column nothing ever set to anything but `'default'`.
    """
    if not (request.share_id or "").strip().isdigit():
        return _err(-1, "A membership handle is required.")
    resolved = await cc.resolve_member_row(int(request.share_id))
    if resolved is None:
        return _err(-2, "No such membership.")
    circle_id, member_user_id = resolved
    if member_user_id != int(user_id):
        try:
            await cc.require_maintainer(user_id, circle_id)
        except cc.CareCircleDenied:
            return _err(-2, "No such membership.")
    ok = await cc.set_member_label(
        int(request.share_id), nickname=request.nickname, avatar_key=request.avatar_key
    )
    return _ok({"updated": ok})
