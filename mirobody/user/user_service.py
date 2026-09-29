import logging

from psycopg_pool import AsyncConnectionPool
from mirobody.utils.ephemeral import EphemeralStore

from .auth.jwt import AbstractTokenValidator
from .auth.email import create_email_validator
from .auth.webauthn import WebAuthnService

from .user import (
    ensure_user,
    del_user,
    get_user,
    prove_address,
    set_addresses_provable,
    update_user_name,
)

from .account_merge import merge_accounts
from . import activation
from .auth.email import DummyEmailCodeValidator

from mirobody.utils import execute_query, json_response_with_code, json_response, Request, Response, Route

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

class UserService:
    def __init__(
        self,
        token_validator : AbstractTokenValidator,

        uri_prefix      : str = "",
        routes          : list | None = None,
        
        db_pool         : AsyncConnectionPool | None = None,
        ephemeral           : EphemeralStore | None = None,

        email_smtp_host : str = "",
        email_smtp_port : int = 0,
        email_smtp_user : str = "",
        email_from      : str = "",
        email_from_name : str = "",
        email_template  : str = "",
        email_password  : str = "",
        email_predefined: str | bytes | bytearray | dict[str, str] | None = None,

        # WebAuthn (AAL2).
        webauthn_rp_id      : str = "",
        webauthn_rp_name    : str = "",
        webauthn_origin     : str = "",
        webauthn_mfa_ticket_ttl : int = 300,
    ):
        self._token_validator = token_validator

        self._email_validator = create_email_validator(
            smtp_host       = email_smtp_host,
            smtp_port       = email_smtp_port,
            smtp_user       = email_smtp_user if email_smtp_user else email_from,
            smtp_pass       = email_password,
            mandrill_api_key= email_password,
            from_email      = email_from,
            from_name       = email_from_name if email_from_name else "Theta Wellness",
            template        = email_template,
            predefined_codes= email_predefined,
            ephemeral           = ephemeral
        )
        
         #-------------------------------------------------

        self._db_pool = db_pool
        self._ephemeral   = ephemeral

        # Without mail, an address is only a username (SECURITY.md).
        set_addresses_provable(self.sends_mail())

        # WebAuthn service (enabled only when rp_id is configured).
        self._webauthn_service = WebAuthnService(
            token_validator = token_validator,
            uri_prefix      = uri_prefix,
            routes          = routes,
            db_pool         = db_pool,
            ephemeral           = ephemeral,
            rp_id           = webauthn_rp_id,
            rp_name         = webauthn_rp_name,
            origin          = webauthn_origin,
            mfa_ticket_ttl  = webauthn_mfa_ticket_ttl,
        ) if webauthn_rp_id else None

         #-------------------------------------------------

        if routes is not None:
            self.routes = routes
        else:
            self.routes = []

        self.routes.append(Route(f"{uri_prefix}/email/login", endpoint=self.email_login_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/email/verify", endpoint=self.email_verify_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/email/bind", endpoint=self.email_bind_handler, methods=["POST", "OPTIONS"]))
        # Password login. Additive: the email-code routes above are unchanged, and
        # an account with `password_hash IS NULL` can still only use those.
        self.routes.append(Route(f"{uri_prefix}/password/register", endpoint=self.password_register_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/password/login", endpoint=self.password_login_handler, methods=["POST", "OPTIONS"]))

        # Handing a managed (virtual) member's account to that person.
        self.routes.append(Route(f"{uri_prefix}/account/activation", endpoint=self.activation_create_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/account/activation/info", endpoint=self.activation_info_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/account/activation/complete", endpoint=self.activation_complete_handler, methods=["POST", "OPTIONS"]))

        self.routes.append(Route(f"{uri_prefix}/user/del", endpoint=self.user_unregister_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/user/update_name", endpoint=self.user_update_name_handler, methods=["POST", "OPTIONS"]))

    #-------------------------------------------------------------------------

    async def email_login_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        
        if not self._email_validator:
            return json_response_with_code(-1, "Invalid email validator.", request=request)

        try:
            data = await request.json()
            email = data.get("email")

            err = await self._email_validator.send(email)
            if err:
                return json_response_with_code(-2, err, request=request)

        except Exception as e:
            return json_response_with_code(-3, str(e), request=request)

        return json_response_with_code(data={"email": email}, request=request)
    
    #-------------------------------------------------------------------------

    async def email_verify_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        if not self._email_validator:
            return json_response_with_code(-1, "Invalid email validator.", request=request)

        try:
            data = await request.json()
            email = data.get("email")
            code = data.get("code")

            err = await self._email_validator.verify(email, code)
            if err:
                return json_response_with_code(-2, err, request=request)
        
        except Exception as e:
            return json_response_with_code(-3, str(e), request=request)
        
        #-------------------------------------------------

        id = await ensure_user(email)
        if not id:
            return json_response_with_code(-4, "Invalid email.", request=request)

        # The code proves the address. A password somebody set on it without
        # that proof is cleared, and their sessions end.
        if await prove_address(id):
            logger.warning("unproven password cleared at code sign-in: user_id=%s", id)

        #-------------------------------------------------

        return await self._generate_auth_response(id, email, "email", request)

    #-------------------------------------------------------------------------

    # Password login exists because the email-code path needs Mandrill or SMTP,
    # which someone who cloned the repo to try it does not have: without this
    # the only accounts that could sign in were the hardcoded
    # EMAIL_PREDEFINE_CODES ones, a demo rather than a sign-up. Hashing is
    # bcrypt inside Postgres (`pgcrypto`), so no hash is built, compared or
    # logged in Python. See `10_accounts.sql`.

    #: Short enough to be typed, long enough that bcrypt is not the weak link.
    _MIN_PASSWORD_LEN = 8

    @staticmethod
    def _read_credentials(data: dict) -> tuple[str, str]:
        """`email` or `username`: both name the same column; whichever arrived."""
        email = (data.get("email") or data.get("username") or "").strip().lower()
        return email, data.get("password") or ""

    async def password_register_handler(self, request: Request) -> Response:
        """Create a NEW account with a password. An existing email is refused.

        This endpoint takes no proof of ownership, so it may not touch an
        account that exists. It used to set a password on any account without
        one, which is every account made by email code, Apple or Google, and
        answer with that account's tokens: measured 2026-09-28 on the seeded
        stack, posting `mom@mirobody.ai` with a new password returned a token
        for her account. Adding a password to an existing account belongs
        behind an authenticated route.
        """
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        try:
            data = await request.json()
            email, password = self._read_credentials(data)
        except Exception as e:
            return json_response_with_code(-1, str(e), request=request)

        if not email or "@" not in email:
            return json_response_with_code(-2, "A valid email is required.", request=request)
        if len(password) < self._MIN_PASSWORD_LEN:
            return json_response_with_code(
                -3, f"Password must be at least {self._MIN_PASSWORD_LEN} characters.", request=request
            )

        # Where a code can reach the address, registering proves it: anyone
        # could otherwise claim an address first and be the one its owner's
        # code sign-in lands in. Without mail an address is only a username.
        proven = self.sends_mail()
        if proven:
            code = str(data.get("code") or "")
            if not code:
                return json_response_with_code(-5, "Enter the code sent to this address.", request=request)
            err = await self._email_validator.verify(email, code)
            if err:
                return json_response_with_code(-6, err, request=request)

        rows = await execute_query(
            """
            INSERT INTO health_app_user (is_del, email, name, password_hash, email_verified_at)
            VALUES (FALSE, :email, :name, crypt(:password, gen_salt('bf', 12)),
                    CASE WHEN :proven THEN now() END)
            ON CONFLICT (email) WHERE (is_del = false) DO NOTHING
            RETURNING id
            """,
            {"email": email, "name": email.split("@")[0], "password": password, "proven": proven},
            log_sql=False,
        )
        if not rows:
            # The email has an account, whichever way it signs in.
            return json_response_with_code(
                -4, "An account with this email already exists. Sign in instead.", request=request
            )

        return await self._generate_auth_response(rows[0]["id"], email, "password", request)

    async def password_login_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        try:
            email, password = self._read_credentials(await request.json())
        except Exception as e:
            return json_response_with_code(-1, str(e), request=request)

        if not email or not password:
            return json_response_with_code(-2, "Email and password are required.", request=request)

        # The comparison happens in SQL: `stored = crypt(candidate, stored)` re-runs
        # bcrypt with the stored salt and cost. `password_hash IS NOT NULL` keeps an
        # account that never set one from being reachable through this route.
        rows = await execute_query(
            """
            SELECT id FROM health_app_user
             WHERE email = :email AND is_del = FALSE
               AND password_hash IS NOT NULL
               AND password_hash = crypt(:password, password_hash)
             LIMIT 1
            """,
            {"email": email, "password": password},
            log_sql=False,
        )
        if not rows:
            # One message for "no such account", "no password set" and "wrong
            # password" alike: telling them apart is an account-enumeration gift.
            return json_response_with_code(-3, "Incorrect email or password.", request=request)

        return await self._generate_auth_response(rows[0]["id"], email, "password", request)

    #-------------------------------------------------------------------------

    async def email_bind_handler(self, request: Request) -> Response:
        """
        Bind a real email to the currently-authenticated user.

        Use case: a user whose health_app_user.email is a synthesized
        placeholder (from an identity provider that gives no real address)
        verifies a real email and promotes that to be their canonical one.

        If the verified email already belongs to a different active user, the
        two accounts are merged: the email-side account wins, the current
        account's data is moved over and its health_app_user row is
        soft-deleted.
        """
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        if not self._email_validator:
            return json_response_with_code(-1, "Invalid email validator.", request=request)

        if not request.state.user_id or \
            not isinstance(request.state.user_id, int) or \
            request.state.user_id <= 0:

            return json_response("Unauthorized", status_code=401, request=request)

        current_user_id = request.state.user_id

        try:
            data = await request.json()
            email = data.get("email")
            code  = data.get("code")

            if not email or not code:
                return json_response_with_code(-2, "Email and code are required.", request=request)

            err = await self._email_validator.verify(email, code)
            if err:
                return json_response_with_code(-3, err, request=request)

        except Exception as e:
            return json_response_with_code(-4, str(e), request=request)

        #-------------------------------------------------

        lower_email = email.strip().lower()

        try:
            row = await get_user(email=lower_email)
            existing_owner = row["id"] if row else 0

        except Exception as e:
            return json_response_with_code(-5, str(e), request=request)

        #-------------------------------------------------

        if existing_owner == current_user_id:
            # Already bound to me: nothing to do, just refresh token.
            await self._mark_proven(current_user_id)
            return await self._generate_auth_response(current_user_id, lower_email, "email_bind", request)

        if existing_owner and existing_owner != current_user_id:
            # Conflict: the verified email belongs to another live user.
            # Merge current_user (losing) into existing_owner (winning), after
            # the proof ends any session somebody holds there without it.
            await prove_address(existing_owner)
            affected, err = await merge_accounts(
                self._db_pool,
                losing_user_id  = current_user_id,
                winning_user_id = existing_owner,
                reason          = "email_link",
            )
            if err:
                logger.error(
                    f"merge_accounts failed: losing={current_user_id} winning={existing_owner}: {err}",
                    extra={"affected": affected}
                )
                return json_response_with_code(-6, err, request=request)

            return await self._generate_auth_response(existing_owner, lower_email, "email_bind", request)

        #-------------------------------------------------
        # No conflict: simply rewrite the current user's email column.

        try:
            async with self._db_pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE health_app_user SET email=%s, email_verified_at=now(), update_at=CURRENT_TIMESTAMP"
                        " WHERE id=%s;",
                        [lower_email, current_user_id]
                    )
                    await conn.commit()

        except Exception as e:
            return json_response_with_code(-7, str(e), request=request)

        return await self._generate_auth_response(current_user_id, lower_email, "email_bind", request)

    @staticmethod
    async def _mark_proven(user_id: int) -> None:
        """The caller proved their own address: their password stays theirs."""
        await execute_query(
            "UPDATE health_app_user SET email_verified_at = now() WHERE id = :id AND is_del = false",
            {"id": int(user_id)},
        )

    #-------------------------------------------------------------------------

    async def user_unregister_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        #-------------------------------------------------

        if not request.state.user_id or \
            not isinstance(request.state.user_id, int) or \
            request.state.user_id <= 0:

            return json_response("Unauthorized", status_code=401, request=request)

        user_id = request.state.user_id

        #-------------------------------------------------
        # Deletion is immediate and cannot be undone, so the request has to
        # name the account it deletes: `confirm` is the account's own email.
        # A bare POST with a valid token (a replayed or leaked one) is refused.

        try:
            body = await request.json()
        except Exception:
            body = {}
        row = await get_user(user_id=user_id)
        if row is None:
            return json_response("Unauthorized", status_code=401, request=request)
        expected = str(row.get("email") or "").strip().lower()
        given = str((body or {}).get("confirm") or "").strip().lower() if isinstance(body, dict) else ""
        if not expected or given != expected:
            return json_response_with_code(
                -2, "To delete this account, send confirm set to its email address.", request=request
            )

        err = await del_user(self._db_pool, user_id)
        if err:
            return json_response_with_code(-1, err, request=request)

        return json_response_with_code(request=request)

    #-------------------------------------------------------------------------

    async def user_update_name_handler(self, request: Request) -> Response:
        """Update the current user's display name (health_app_user.name).

        Used to overwrite a placeholder nickname supplied by an identity
        provider. Email/Google users can use it too.
        """
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        if not request.state.user_id or \
            not isinstance(request.state.user_id, int) or \
            request.state.user_id <= 0:

            return json_response("Unauthorized", status_code=401, request=request)

        user_id = request.state.user_id

        try:
            data = await request.json()
        except Exception as e:
            return json_response_with_code(-1, f"Invalid JSON: {e}", request=request)

        name = data.get("name") if isinstance(data, dict) else None
        if not isinstance(name, str):
            return json_response_with_code(-2, "name is required.", request=request)

        name = name.strip()
        if not name:
            return json_response_with_code(-3, "name cannot be empty.", request=request)
        if len(name) > 100:
            return json_response_with_code(-4, "name too long (max 100).", request=request)

        err = await update_user_name(self._db_pool, user_id, name)
        if err:
            return json_response_with_code(-5, err, request=request)

        return json_response_with_code(data={"name": name}, request=request)

    #-------------------------------------------------------------------------

    async def requires_second_factor(self, user_id: int) -> bool:
        """For the JWT middleware: whether `user_id` needs an AAL2 token."""
        return bool(self._webauthn_service) and await self._webauthn_service.requires_second_factor(user_id)

    #-------------------------------------------------------------------------

    def sends_mail(self) -> bool:
        """Whether a code can reach an address, so an address can be proven."""
        return bool(self._email_validator) and not isinstance(self._email_validator, DummyEmailCodeValidator)

    async def activation_create_handler(self, request: Request) -> Response:
        """A one-time link that hands a managed member's account to them.

        Only the member's creator may ask. The link goes back to the caller to
        pass on: it proves nothing by itself, the address it names does.
        """
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        creator = request.state.user_id
        if not isinstance(creator, int) or creator <= 0:
            return json_response("Unauthorized", status_code=401, request=request)
        try:
            data = await request.json()
            member_id = int(data.get("member_id"))
        except Exception:
            return json_response_with_code(-1, "member_id is required.", request=request)
        try:
            token, expires_at = await activation.create(creator, member_id, data.get("email") or "")
        except activation.ActivationError as e:
            return json_response_with_code(e.code, str(e), request=request)
        from mirobody.utils.http import request_origin

        return json_response_with_code(data={
            "url": f"{request_origin(request)}/activate#{token}",
            "expires_at": expires_at.isoformat(),
            "sends_mail": self.sends_mail(),
        }, request=request)

    async def activation_info_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        try:
            token = (await request.json()).get("token") or ""
        except Exception:
            token = ""
        act = await activation.lookup(token)
        if act is None:
            return json_response_with_code(-1, "This link is no longer valid. Ask for a new one.", request=request)
        return json_response_with_code(data={
            "member_name": act.member_name,
            "creator_name": act.creator_name,
            "email": act.email,
            "expires_at": act.expires_at.isoformat(),
            "sends_mail": self.sends_mail(),
        }, request=request)

    async def activation_complete_handler(self, request: Request) -> Response:
        """Prove the address, take the account, choose what the creator keeps.

        The proof is one of: a session already signed in as that address, a
        code sent there, or, where this deployment sends no mail, a password
        (the existing account's, or a new one).
        """
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)
        try:
            data = await request.json()
        except Exception:
            data = {}
        act = await activation.lookup(str(data.get("token") or ""))
        if act is None:
            return json_response_with_code(-1, "This link is no longer valid. Ask for a new one.", request=request)
        access = activation.ACCESS_BY_NAME.get(str(data.get("access") or ""))
        if access is None:
            return json_response_with_code(-6, "Choose what the person who added you can see.", request=request)
        password = str(data.get("password") or "")
        if password and len(password) < activation.MIN_PASSWORD_LEN:
            return json_response_with_code(
                -5, f"Password must be at least {activation.MIN_PASSWORD_LEN} characters.", request=request)

        existing = await activation.existing_account(act.email)
        caller = request.state.user_id if isinstance(request.state.user_id, int) else 0
        me = await get_user(user_id=caller) if caller > 0 else None
        proven = False
        if (me and (me.get("email") or "").strip().lower() == act.email
                and not (self.sends_mail() and me.get("unproven"))):
            proven = self.sends_mail()
        elif self.sends_mail():
            err = await self._email_validator.verify(act.email, str(data.get("code") or ""))
            if err:
                return json_response_with_code(-2, err, request=request)
            proven = True
        elif existing:
            if not existing["has_password"]:
                return json_response_with_code(
                    -7, "This address already has an account. Sign in to it first, then open the link again.",
                    request=request)
            if not await activation.password_matches(int(existing["id"]), password):
                return json_response_with_code(-4, "The password for this address is wrong.", request=request)
        elif not password:
            return json_response_with_code(
                -5, f"Set a password of at least {activation.MIN_PASSWORD_LEN} characters.", request=request)

        if not await activation.claim(act.id):
            return json_response_with_code(-1, "This link is no longer valid. Ask for a new one.", request=request)
        try:
            owner_id = await activation.hand_over(
                self._db_pool, act, into=int(existing["id"]) if existing else None,
                password=password, access=access, proven=proven)
        except activation.ActivationError as e:
            await activation.release(act.id)
            return json_response_with_code(e.code, str(e), request=request)
        except Exception as e:
            await activation.release(act.id)
            logger.error("activation failed: error_type=%s", type(e).__name__)
            return json_response_with_code(-9, "Could not finish. Nothing was changed; try again.", request=request)
        logger.info("managed account handed over: member_id=%s owner_id=%s", act.member_id, owner_id)
        return await self._generate_auth_response(owner_id, act.email, "activation", request)

    async def _generate_auth_response(
        self,
        user_id: int,
        email: str,
        auth_method: str = "",
        request: Request | None = None,
    ) -> Response:
        """Generate final auth response, with MFA challenge if required."""
        # Check if WebAuthn MFA is required.
        if self._webauthn_service:
            mfa_challenge = await self._webauthn_service.check_mfa_required(user_id, email)
            if mfa_challenge:
                # Include a fallback AAL1 token so frontend can degrade gracefully
                # if user cancels WebAuthn (e.g. Touch ID dismissed).
                fallback_token, fallback_refresh, _ = await self._token_validator.generate_tokens(
                    str(user_id), email, auth_method,
                    gen_claims_func=lambda _uid, _em: {"aal": 1},
                )
                if fallback_token:
                    mfa_challenge["fallback_token"] = fallback_token
                    # TODO: Remove backward-compat token fields below once Flutter
                    # handles "status": "mfa_required" and uses fallback_token directly.
                    mfa_challenge["access_token"] = fallback_token
                    mfa_challenge["token_type"] = "Bearer"
                    mfa_challenge["expires_in"] = (
                        self._token_validator.get_expires_in()
                        if self._token_validator else 60 * 60 * 24 * 7
                    )
                    if fallback_refresh:
                        mfa_challenge["refresh_token"] = fallback_refresh
                return json_response_with_code(data=mfa_challenge, request=request)

        # No MFA required: issue AAL1 token directly.
        aal_level = 1 if self._webauthn_service else None

        access_token, refresh_token, err = await self._token_validator.generate_tokens(
            str(user_id),
            email,
            auth_method,
            gen_claims_func=(lambda _uid, _em: {"aal": aal_level}) if aal_level else None,
        )
        if err:
            return json_response_with_code(-100, err, request=request)

        result = self._generate_verification_response(
            access_token, refresh_token, user_id=user_id, email=email
        )

        # Tell frontend WebAuthn is not yet registered for this user.
        if self._webauthn_service:
            result["webauthn_registered"] = False

        return json_response_with_code(data=result, request=request)

    #-------------------------------------------------------------------------

    def _generate_verification_response(
        self,
        access_token    : str,
        refresh_token   : str | None = None,
        scope           : str | None = None,
        user_id         : int = 0,
        email           : str = ""
    ) -> dict:

        result = {
            "access_token"  : access_token,
            "token_type"    : "Bearer",
            "expires_in"    : self._token_validator.get_expires_in() if self._token_validator else 60*60*24*7,
            # "user_id"       : str(user_id),
            # "email"         : email,
            # "name"          : email.split("@")[0]
        }

        if refresh_token:
            result["refresh_token"] = refresh_token
        if scope:
            result["scope"] = scope

        return result

#-----------------------------------------------------------------------------
