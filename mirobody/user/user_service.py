import logging

from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from .auth.jwt import AbstractTokenValidator
from .auth.email import create_email_validator
from .auth.apple import AppleTokenValidator
from .auth.google import GoogleTokenValidator
from .auth.webauthn import WebAuthnService

from .user import (
    add_or_get_user,
    del_user,
    get_user,
    get_user_via_apple_subject,
    update_user_name,
)

from .account_merge import merge_accounts

from ..utils import (
    execute_query,
    secret_fingerprint,
    json_response_with_code,
    json_response,

    Request,
    Response,
    Route
)

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

class UserService:
    def __init__(
        self,
        token_validator : AbstractTokenValidator,

        uri_prefix      : str = "",
        routes          : list | None = None,
        
        db_pool         : AsyncConnectionPool | None = None,
        redis           : Redis | None = None,

        email_smtp_host : str = "",
        email_smtp_port : int = 0,
        email_smtp_user : str = "",
        email_from      : str = "",
        email_from_name : str = "",
        email_template  : str = "",
        email_password  : str = "",
        email_predefined: str | bytes | bytearray | dict[str, str] | None = None,

        apple_client_id : str = "",
        apple_team_id   : str = "",
        apple_key_id    : str = "",
        apple_private_key   : str = "",
        apple_auth_client_id: str = "",

        google_client_id    : str = "",

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
            redis           = redis
        )
        
        if apple_client_id and apple_team_id and apple_key_id and apple_private_key:
            self._apple_validator = AppleTokenValidator(
                apple_client_id,
                apple_team_id,
                apple_key_id,
                apple_private_key,
                auth_client_id = apple_auth_client_id
            )
        else:
            self._apple_validator = None

        if google_client_id:
            self._google_validator = GoogleTokenValidator(google_client_id)
        else:
            self._google_validator = None

         #-------------------------------------------------

        self._db_pool = db_pool
        self._redis   = redis

        # WebAuthn service (enabled only when rp_id is configured).
        self._webauthn_service = WebAuthnService(
            token_validator = token_validator,
            uri_prefix      = uri_prefix,
            routes          = routes,
            db_pool         = db_pool,
            redis           = redis,
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

        self.routes.append(Route(f"{uri_prefix}/user/del", endpoint=self.user_unregister_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/user/update_name", endpoint=self.user_update_name_handler, methods=["POST", "OPTIONS"]))

        if self._apple_validator:
            self.routes.append(Route(f"{uri_prefix}/apple/verify", endpoint=self.apple_verify_handler, methods=["POST", "OPTIONS"]))

        if self._google_validator:
            self.routes.append(Route(f"{uri_prefix}/google/verify", endpoint=self.google_verify_handler, methods=["POST", "OPTIONS"]))

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

        id, err = await add_or_get_user(self._db_pool, email)
        if err:
            return json_response_with_code(-4, err, request=request)

        #-------------------------------------------------

        return await self._generate_auth_response(id, email, "email", request)

    #-------------------------------------------------------------------------

    # ── password login ───────────────────────────────────────────────────────
    #
    # Why this exists at all: the email-code path needs Mandrill or SMTP, which
    # someone who cloned the repo to try it out does not have. Without this the
    # only accounts that could ever sign in were the hardcoded ones in
    # EMAIL_PREDEFINE_CODES — a demo, not a sign-up.
    #
    # Hashing is bcrypt inside Postgres (`pgcrypto`), so no hash is ever built,
    # compared or logged in Python. See `a1_add_password_login.sql`.

    #: Short enough to be typed, long enough that bcrypt is not the weak link.
    _MIN_PASSWORD_LEN = 8

    @staticmethod
    def _read_credentials(data: dict) -> tuple[str, str]:
        """`email` or `username` — both name the same column; whichever arrived."""
        email = (data.get("email") or data.get("username") or "").strip().lower()
        return email, data.get("password") or ""

    async def password_register_handler(self, request: Request) -> Response:
        """Create an account with a password, or set one on an account without.

        Deliberately NOT a password *change*: an account that already has a hash
        is refused rather than overwritten, because this endpoint takes no proof
        of ownership. Rotation belongs behind an authenticated route, and
        pretending otherwise here would be a takeover primitive.
        """
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        try:
            email, password = self._read_credentials(await request.json())
        except Exception as e:
            return json_response_with_code(-1, str(e), request=request)

        if not email or "@" not in email:
            return json_response_with_code(-2, "A valid email is required.", request=request)
        if len(password) < self._MIN_PASSWORD_LEN:
            return json_response_with_code(
                -3, f"Password must be at least {self._MIN_PASSWORD_LEN} characters.", request=request
            )

        rows = await execute_query(
            """
            INSERT INTO health_app_user (is_del, email, name, password_hash)
            VALUES (FALSE, :email, :name, crypt(:password, gen_salt('bf', 12)))
            ON CONFLICT (email) DO UPDATE
                SET password_hash = crypt(:password, gen_salt('bf', 12))
                -- Only when there is none to overwrite. `WHERE` on DO UPDATE
                -- makes the conflicting row survive untouched instead.
                WHERE health_app_user.password_hash IS NULL
            RETURNING id
            """,
            {"email": email, "name": email.split("@")[0], "password": password},
            log_sql=False,
        )
        if not rows:
            # The row exists and already had a hash, so nothing was updated.
            return json_response_with_code(
                -4, "That account already has a password. Sign in instead.", request=request
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
            # password" alike — telling them apart is an account-enumeration gift.
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

            return json_response(status_code=401, request=request)

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
            # Already bound to me — nothing to do, just refresh token.
            return await self._generate_auth_response(current_user_id, lower_email, "email_bind", request)

        if existing_owner and existing_owner != current_user_id:
            # Conflict: the verified email belongs to another live user.
            # Merge current_user (losing) into existing_owner (winning).
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
                        "UPDATE health_app_user SET email=%s, update_at=CURRENT_TIMESTAMP WHERE id=%s;",
                        [lower_email, current_user_id]
                    )
                    await conn.commit()

        except Exception as e:
            return json_response_with_code(-7, str(e), request=request)

        return await self._generate_auth_response(current_user_id, lower_email, "email_bind", request)

    #-------------------------------------------------------------------------

    async def user_unregister_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        #-------------------------------------------------

        if not request.state.user_id or \
            not isinstance(request.state.user_id, int) or \
            request.state.user_id <= 0:

            return json_response(status_code=401, request=request)

        user_id = request.state.user_id

        #-------------------------------------------------

        err = del_user(self._db_pool, user_id)
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

            return json_response(status_code=401, request=request)

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

    async def apple_verify_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        try:
            request_json = await request.json()

            token   = request_json.get("token")
            code    = request_json.get("code")
            if not token and not code:
                return json_response_with_code(-1, "Apple ID token or authorization code is required.", request=request)

            email   = request_json.get("email")
            name    = request_json.get("name")

            payload = None

            #---------------------------------------------

            if code:
                # The authorization code is a live credential (exchangeable for
                # tokens until it expires); fingerprint it like the JWT below
                # instead of writing it verbatim — DEBUG logs are not a safe
                # place for it either.
                logger.debug("Apple authorization code: %s", secret_fingerprint(code))

                payload, err = await self._apple_validator.verify_authorization_code(code)
                if err:
                    logger.error(err, extra={"code": secret_fingerprint(code), "email": email})

                    if not token:
                        return json_response_with_code(-2, err, request=request)

            #---------------------------------------------

            if not payload:
                if not token:
                    return json_response_with_code(-3, "Apple ID token is required.", request=request)

                logger.debug("Apple JWT token: %s", secret_fingerprint(token))

                payload, err = await self._apple_validator.verify_token(token)
                if err:
                    return json_response_with_code(-4, err, request=request)
                if not payload:
                    return json_response_with_code(-5, "Empty payload.", request=request)

            #---------------------------------------------

            apple_subject = payload.get("sub")      # Apple user ID.
            if not apple_subject:
                return json_response_with_code(-6, "Empty Apple subject.", request=request)

            id, email, err = await get_user_via_apple_subject(apple_subject)
            if err:
                logger.warning(err, extra={"apple_subject": apple_subject})

                email = payload.get("email")
                if not email:
                    email = f"{apple_subject}@apple-private.com"

                id, err = await add_or_get_user(self._db_pool, email, name, apple_subject)
                if err:
                    return json_response_with_code(-7, err, request=request)
                if not id:
                    return json_response_with_code(-8, "Empty user ID.", request=request)

            #---------------------------------------------

            return await self._generate_auth_response(id, email, "apple", request)

        except Exception as e:
            return json_response_with_code(-10, str(e), request=request)

    #-------------------------------------------------------------------------

    async def google_verify_handler(self, request: Request) -> Response:
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        try:
            request_json = await request.json()
            
            token = request_json.get("token")
            if not token:
                return json_response_with_code(-1, "Google ID token is required", request=request)

            #---------------------------------------------

            payload, err = await self._google_validator.verify_token(token)
            if err:
                logger.warning(err, extra={"token": secret_fingerprint(token)})
            if not payload:
                return json_response_with_code(-2, "Invalid Google ID token.", request=request)

            #---------------------------------------------

            verified_email = payload.get("email")
            if not verified_email:
                return json_response_with_code(-3, "No email in verified token.", request=request)

            id, err = await add_or_get_user(self._db_pool, verified_email)
            if err:
                return json_response_with_code(-4, err, request=request)
            
            #-------------------------------------------------

            return await self._generate_auth_response(id, verified_email, "google", request)

        except Exception as e:
            return json_response_with_code(-6, str(e), request=request)

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

        # No MFA required — issue AAL1 token directly.
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
