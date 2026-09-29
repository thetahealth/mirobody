"""Personal MCP links: `{origin}/mcp/<secret>`, a URL that is its own credential.

A link reads ONE person's record (the subject) on behalf of the person who
made it (the creator): the subject themselves, or a family member whose care
circle lets them read the subject. The URL alone grants that, so:

* The secret is never stored, only its SHA-256 (`th_personal_mcp_url`).
* A link is one row per creator and subject. A person's own link and one a
  family member made for them are separate, and replacing or revoking one
  leaves the other working.
* Every use checks the link again (`authorize`), and any failure is "no one":
  expired or revoked; the creator or the subject gone; the link older than the
  creator's `tokens_valid_after` (an account taken back by its owner, H1 in
  1.5.2); or, for a family member's link, the care circle no longer letting
  the creator read the subject, the same `resolve_subject` the REST routes
  ask. Unsharing, removal from a circle, deleting an account and taking one
  back therefore end the link without anyone revoking it.
* Unsharing and removal also revoke it (`revoke_unshared`, called by
  `care_circle`), so sharing again later does not bring an old link back: the
  person stopped sharing for a reason, and a leaked link is one of them.
* A link lives a fixed number of days from when it was made and is never
  extended by use. Making a new one for the same creator and subject revokes
  the old one in the same transaction.
* The creator can revoke the links they made; the subject can list and revoke
  every link that reads their record.

Before 1.5.3 a link was a key in a shared store naming only the subject; none
of those carry over (there is no creator to check), so everyone makes a new
one once.
"""

from __future__ import annotations

import hashlib
import logging
import secrets
from datetime import datetime

from mirobody.utils import db, execute_query

logger = logging.getLogger(__name__)

#: Days a link lives, from when it is made. Not extended by use: a link that
#: stays valid while it keeps being used is a link nobody ever has to look at.
DEFAULT_TTL_DAYS = 10

#: How often `last_used_at` is written for a link in steady use. Every MCP call
#: presents the link; one write per call would make each tool call a write.
_TOUCH_EVERY = "1 minute"

_COLUMNS = "l.id, l.creator_id, l.subject_id, l.created_at, l.expires_at, l.last_used_at"


def _hash(secret: str) -> bytes:
    return hashlib.sha256(secret.encode("utf-8")).digest()


def _row(r: dict, *, viewer: str) -> dict:
    """A link as the settings page lists it. Never the secret: it was shown
    once, when the link was made, and is not stored."""
    def iso(v):
        return v.isoformat() if isinstance(v, datetime) else None

    creator, subject = str(r["creator_id"]), str(r["subject_id"])
    return {
        "id": int(r["id"]),
        "creator_id": creator,
        "creator_name": str(r.get("creator_name") or ""),
        "subject_id": subject,
        "subject_name": str(r.get("subject_name") or ""),
        "own": creator == subject,
        "made_by_me": creator == str(viewer),
        "created_at": iso(r.get("created_at")),
        "expires_at": iso(r.get("expires_at")),
        "last_used_at": iso(r.get("last_used_at")),
    }


async def mint(creator_id: str, subject_id: str, *, ttl_days: int = DEFAULT_TTL_DAYS) -> tuple[str, dict]:
    """A new link for this creator and subject, and the secret it carries.
    Whether the creator may read the subject is the caller's to check first.
    Any live link of the same pair is revoked in the same transaction, so
    there is one at a time and "regenerate" is this."""
    secret = secrets.token_urlsafe(48)
    async with db.transaction() as tx:
        # Two concurrent mints for one pair would both find nothing to revoke
        # and collide on the one-live-link index; the second waits here instead.
        await tx.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended('personal_mcp:' || :creator || ':' || :subject, 0))",
            {"creator": str(creator_id), "subject": str(subject_id)},
        )
        await tx.execute(
            "UPDATE th_personal_mcp_url SET revoked_at = now(), revoked_by = :creator"
            " WHERE creator_id = :creator AND subject_id = :subject AND revoked_at IS NULL",
            {"creator": str(creator_id), "subject": str(subject_id)},
        )
        rows = await tx.execute(
            "INSERT INTO th_personal_mcp_url (secret_hash, creator_id, subject_id, expires_at)"
            " VALUES (:hash, :creator, :subject, now() + make_interval(days => :days))"
            " RETURNING id, creator_id, subject_id, created_at, expires_at, last_used_at",
            {"hash": _hash(secret), "creator": str(creator_id), "subject": str(subject_id),
             "days": max(1, int(ttl_days))},
        )
    row = _row(dict(rows[0]), viewer=creator_id)
    link_id = row["id"]
    logger.info("personal MCP link made: id=%s creator_id=%s subject_id=%s", link_id, creator_id, subject_id)
    return secret, row


async def authorize(secret: str) -> str:
    """The subject this link reads, or "" when it may not be used now.

    Everything is checked on every call, so a link ends the moment any of it
    stops being true; see the module docstring for the list."""
    if not secret:
        return ""
    try:
        rows = await execute_query(
            "SELECT id, creator_id, subject_id, EXTRACT(EPOCH FROM created_at) AS created"
            " FROM th_personal_mcp_url"
            " WHERE secret_hash = :hash AND revoked_at IS NULL AND expires_at > now()",
            {"hash": _hash(secret)}, log_sql=False,
        ) or []
    except Exception as e:
        logger.warning("personal MCP link lookup failed: error_type=%s", type(e).__name__)
        return ""
    if not rows:
        return ""
    link_id, creator, subject = rows[0]["id"], str(rows[0]["creator_id"]), str(rows[0]["subject_id"])
    created = float(rows[0]["created"])

    from mirobody.user.user import is_active_account

    # The creator's account must exist and not have revoked, since this link
    # was made, every credential it had issued (tokens_valid_after): a link a
    # squatter made before the owner took the account back is one of those.
    if not await is_active_account(creator, minted_at=created):
        return ""
    if subject != creator:
        # Also the check that the subject's account still exists:
        # `accepted_membership` joins both accounts on `is_del = false`.
        from mirobody.user.care_circle import CareCircleDenied, resolve_subject

        try:
            await resolve_subject(creator, subject)
        except CareCircleDenied:
            return ""
    await _touch(link_id)
    return subject


async def _touch(link_id: int) -> None:
    try:
        await execute_query(
            "UPDATE th_personal_mcp_url SET last_used_at = now()"
            f" WHERE id = :id AND (last_used_at IS NULL OR last_used_at < now() - interval '{_TOUCH_EVERY}')",
            {"id": int(link_id)}, log_sql=False,
        )
    except Exception as e:
        # The link was valid; failing to note that it was used must not refuse it.
        logger.warning("personal MCP link touch failed: error_type=%s", type(e).__name__)


async def listed(viewer_id: str) -> dict:
    """The viewer's live links: the ones they made (their own, and any for
    someone else), and the ones someone else made that read their record."""
    select = (
        f"SELECT {_COLUMNS}, c.name AS creator_name, s.name AS subject_name"
        " FROM th_personal_mcp_url l"
        " LEFT JOIN health_app_user c ON c.id::text = l.creator_id"
        " LEFT JOIN health_app_user s ON s.id::text = l.subject_id"
        " WHERE l.revoked_at IS NULL AND l.expires_at > now() AND {who}"
        " ORDER BY l.created_at DESC"
    )
    made = await execute_query(select.format(who="l.creator_id = :me"), {"me": str(viewer_id)}, log_sql=False) or []
    held = await execute_query(select.format(who="l.subject_id = :me AND l.creator_id <> :me"), {"me": str(viewer_id)}, log_sql=False) or []
    return {
        "made": [_row(dict(r), viewer=viewer_id) for r in made],
        "holders": [_row(dict(r), viewer=viewer_id) for r in held],
    }


async def revoke(link_id: int, by_id: str) -> bool:
    """Revoke one link, if `by_id` made it or it reads their record. False,
    and nothing changed, for any other link or one already revoked."""
    rows = await execute_query(
        "UPDATE th_personal_mcp_url SET revoked_at = now(), revoked_by = :by"
        " WHERE id = :id AND revoked_at IS NULL AND (creator_id = :by OR subject_id = :by)"
        " RETURNING id",
        {"id": int(link_id), "by": str(by_id)}, log_sql=False,
    ) or []
    if rows:
        logger.info("personal MCP link revoked: id=%s by=%s", link_id, by_id)
    return bool(rows)


async def revoke_unshared(user_ids: list[str]) -> int:
    """Revoke every live family link to or from these people that the care
    circle no longer allows. Called after access is lowered or a membership
    ends; a link the creator still reaches through another circle stays."""
    from mirobody.user.care_circle import CareCircleDenied, resolve_subject

    ids = sorted({str(u) for u in user_ids if u})
    if not ids:
        return 0
    rows = await execute_query(
        "SELECT id, creator_id, subject_id FROM th_personal_mcp_url"
        " WHERE revoked_at IS NULL AND creator_id <> subject_id"
        " AND (creator_id = ANY(:ids) OR subject_id = ANY(:ids))",
        {"ids": ids}, log_sql=False,
    ) or []
    ended = []
    for r in rows:
        try:
            await resolve_subject(str(r["creator_id"]), str(r["subject_id"]))
        except CareCircleDenied:
            ended.append(int(r["id"]))
    if ended:
        await execute_query(
            "UPDATE th_personal_mcp_url SET revoked_at = now(), revoked_by = 'care_circle'"
            " WHERE id = ANY(:ids) AND revoked_at IS NULL",
            {"ids": ended}, log_sql=False,
        )
        logger.info("personal MCP links revoked by the care circle: count=%d", len(ended))
    return len(ended)


async def revoke_mine(creator_id: str, subject_id: str) -> int:
    """Revoke the link this creator made for this subject: a family member
    revoking their link to Mom's record leaves Mom's own link working."""
    rows = await execute_query(
        "UPDATE th_personal_mcp_url SET revoked_at = now(), revoked_by = :creator"
        " WHERE creator_id = :creator AND subject_id = :subject AND revoked_at IS NULL RETURNING id",
        {"creator": str(creator_id), "subject": str(subject_id)}, log_sql=False,
    ) or []
    return len(rows)


__all__ = ["DEFAULT_TTL_DAYS", "authorize", "listed", "mint", "revoke", "revoke_mine", "revoke_unshared"]
