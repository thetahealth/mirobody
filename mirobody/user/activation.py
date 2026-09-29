"""Handing a managed account to the person it describes.

A managed account (the web client calls it a virtual member) is a family
member whose record someone else keeps; nobody signs in to it. Its creator
names the person's real address and gets a one-time link to pass on. Whoever
opens the link proves that address, by a code sent there or, where this
deployment sends no mail, by a password, and the account becomes theirs: it
takes the address, or it is merged into the account that already holds it.
They also choose what the creator keeps: edit, view or nothing.

The link alone proves nothing. The creator sees it, and the creator typed the
address, so the proof has to come from the address.
"""

from __future__ import annotations

import hashlib
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from mirobody.user import care_circle as cc
from mirobody.user.user import get_user
from mirobody.utils.db import execute_query

#: Reserved by RFC 2606: no mail is ever delivered to it, so no sign-in code
#: can reach a managed account's address.
PLACEHOLDER_DOMAIN = "virtual.invalid"
#: What the web client minted before the server did.
LEGACY_PLACEHOLDER_DOMAIN = "virtual.mirobody.ai"
LINK_TTL = timedelta(days=7)
MIN_PASSWORD_LEN = 8

ACCESS_BY_NAME = {"edit": cc.ACCESS_EDIT, "view": cc.ACCESS_VIEW, "none": cc.ACCESS_NONE}


class ActivationError(Exception):
    """Refused, with a message the person on the page can act on."""

    def __init__(self, code: int, message: str):
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class Activation:
    id: int
    member_id: int
    email: str
    created_by: int
    member_name: str
    creator_name: str
    expires_at: datetime


def placeholder_email() -> str:
    return f"member_{secrets.token_hex(12)}@{PLACEHOLDER_DOMAIN}"


def is_placeholder(email: str | None) -> bool:
    domain = (email or "").rsplit("@", 1)[-1].lower()
    return domain in (PLACEHOLDER_DOMAIN, LEGACY_PLACEHOLDER_DOMAIN)


def _digest(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


async def create(creator_id: int, member_id: int, email: str) -> tuple[str, datetime]:
    """A new link for `member_id`, replacing any unused one."""
    clean = (email or "").strip().lower()
    if "@" not in clean or " " in clean or is_placeholder(clean):
        raise ActivationError(-2, "A valid email address is required.")
    member = await get_user(user_id=member_id)
    creator = await get_user(user_id=creator_id)
    if not member or not creator or member.get("managed_by") != int(creator_id):
        raise ActivationError(-3, "Only the person who added this member can invite them.")
    if (creator.get("email") or "").strip().lower() == clean:
        raise ActivationError(-4, "That is your own address. Enter the address of the person you added.")

    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(UTC) + LINK_TTL
    await execute_query(
        "UPDATE th_account_activation SET used_at = now() WHERE member_id = :m AND used_at IS NULL",
        {"m": int(member_id)},
        log_sql=False,
    )
    await execute_query(
        "INSERT INTO th_account_activation (member_id, email, created_by, token_hash, expires_at)"
        " VALUES (:m, :e, :c, :h, :x)",
        {"m": int(member_id), "e": clean, "c": int(creator_id), "h": _digest(token), "x": expires_at},
        log_sql=False,
    )
    return token, expires_at


async def lookup(token: str) -> Activation | None:
    """The live activation behind `token`: unused, unexpired, and its member
    still managed by the person who made the link."""
    if not token or len(token) > 200:
        return None
    rows = await execute_query(
        "SELECT id, member_id, email, created_by, expires_at FROM th_account_activation"
        " WHERE token_hash = :h AND used_at IS NULL AND expires_at > now()",
        {"h": _digest(token)},
        log_sql=False,
    )
    if not rows:
        return None
    r = rows[0]
    member = await get_user(user_id=r["member_id"])
    creator = await get_user(user_id=r["created_by"])
    if not member or not creator or member.get("managed_by") != int(r["created_by"]):
        return None
    return Activation(id=int(r["id"]), member_id=int(r["member_id"]), email=r["email"],
                      created_by=int(r["created_by"]), member_name=member.get("name") or "",
                      creator_name=creator.get("name") or "", expires_at=r["expires_at"])


async def existing_account(email: str) -> dict | None:
    """The live, self-managed account already holding `email`."""
    row = await get_user(email=email)
    return row if row and not row.get("managed_by") else None


async def password_matches(user_id: int, password: str) -> bool:
    rows = await execute_query(
        "SELECT 1 FROM health_app_user WHERE id = :u AND password_hash IS NOT NULL"
        " AND password_hash = crypt(:p, password_hash)",
        {"u": int(user_id), "p": password},
        log_sql=False,
    )
    return bool(rows)


async def claim(activation_id: int) -> bool:
    """Take the link, once. A second request with the same link gets False."""
    rows = await execute_query(
        "UPDATE th_account_activation SET used_at = now()"
        " WHERE id = :i AND used_at IS NULL AND expires_at > now() RETURNING id",
        {"i": int(activation_id)},
        log_sql=False,
    )
    return bool(rows)


async def release(activation_id: int) -> None:
    await execute_query("UPDATE th_account_activation SET used_at = NULL WHERE id = :i",
                        {"i": int(activation_id)}, log_sql=False)


async def hand_over(db_pool, act: Activation, *, into: int | None, password: str, access: int) -> int:
    """Make the managed account the person's; returns the account they now own.

    `into` is the account that already holds the address, if any: the managed
    one is merged into it. Otherwise the managed account takes the address.
    The creator's membership is then set to what the person chose, accepted,
    because a pending row would grant nothing whatever the choice.
    """
    from mirobody.user.account_merge import merge_accounts

    if into:
        _, err = await merge_accounts(db_pool, losing_user_id=act.member_id,
                                      winning_user_id=into, reason="activation")
        if err:
            raise ActivationError(-9, "Could not move the record. Nothing was changed; try again.")
        owner = into
        if password:
            await execute_query(
                "UPDATE health_app_user SET password_hash = crypt(:p, gen_salt('bf', 12)), update_at = now()"
                " WHERE id = :u AND password_hash IS NULL",
                {"u": owner, "p": password}, log_sql=False,
            )
    else:
        rows = await execute_query(
            """
            UPDATE health_app_user
               SET email = :e, managed_by = NULL, update_at = now(),
                   password_hash = CASE WHEN :p = '' THEN password_hash
                                        ELSE crypt(:p, gen_salt('bf', 12)) END
             WHERE id = :m AND managed_by = :c AND is_del = false
            RETURNING id
            """,
            {"e": act.email, "p": password, "m": act.member_id, "c": act.created_by},
            log_sql=False,
        )
        if not rows:
            raise ActivationError(-1, "This link is no longer valid. Ask for a new one.")
        owner = act.member_id

    circle_id = await cc.own_circle_id(act.created_by)
    if circle_id:
        await execute_query(
            "UPDATE care_circle_members SET status = :accepted, health_access = :a, updated_at = now()"
            " WHERE user_id = :u AND care_circle_id = :c AND deleted_at IS NULL",
            {"accepted": cc.STATUS_ACCEPTED, "a": int(access), "u": owner, "c": circle_id},
            log_sql=False,
        )
    return owner
