import base64, json, logging, secrets, time

from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from webauthn import (
    generate_registration_options,
    verify_registration_response,
    generate_authentication_options,
    verify_authentication_response,
    options_to_json,
    base64url_to_bytes,
)
from webauthn.helpers.structs import (
    AuthenticatorSelectionCriteria,
    AuthenticatorTransport,
    PublicKeyCredentialDescriptor,
    ResidentKeyRequirement,
    UserVerificationRequirement,
    AuthenticatorAttachment,
)

from .jwt import AbstractTokenValidator
from .user import add_or_get_user

from ..utils import (
    json_response_with_code,
    json_response,
    get_jwt_token,
    Request,
    Response,
    Route,
)

#-----------------------------------------------------------------------------

_CHALLENGE_PREFIX = "mirobody:webauthn:challenge:"
_MFA_TICKET_PREFIX = "mirobody:webauthn:mfa_ticket:"

# NIST 800-63B Section 7 session management constants (AAL2 only).
AAL2_SESSION_IDLE_TIMEOUT = 30 * 60       # 30 minutes
AAL2_SESSION_MAX_LIFETIME = 12 * 60 * 60  # 12 hours
AAL2_REAUTH_MAX_AGE      = 24 * 60 * 60  # Max age for expired token re-auth


def _aal2_claims(session_start: int | None = None) -> dict:
    """Build JWT extra claims for an AAL2 token with session tracking."""
    return {
        "aal": 2,
        "session_start": session_start or int(time.time()),
    }

# Map transport strings to AuthenticatorTransport enum values.
_TRANSPORT_MAP = {t.value: t for t in AuthenticatorTransport}

def _to_transport_enums(transports: list[str] | None) -> list[AuthenticatorTransport]:
    if not transports:
        return []
    return [_TRANSPORT_MAP[t] for t in transports if t in _TRANSPORT_MAP]

#-----------------------------------------------------------------------------

class WebAuthnService:
    """WebAuthn (FIDO2) service for AAL2 multi-factor authentication.

    Provides credential registration, authentication, and MFA ticket
    management. Enabled only when rp_id is provided.
    """

    def __init__(
        self,
        token_validator : AbstractTokenValidator,

        uri_prefix      : str = "",
        routes          : list | None = None,

        db_pool         : AsyncConnectionPool | None = None,
        redis           : Redis | None = None,

        rp_id           : str = "",
        rp_name         : str = "",
        origin          : str = "",
        mfa_ticket_ttl  : int = 300,
    ):
        self._token_validator = token_validator
        self._get_jwt_token = get_jwt_token
        self._db_pool = db_pool
        self._redis = redis

        self._rp_id = rp_id
        self._rp_name = rp_name or "Theta Health"
        self._origin = origin
        self._mfa_ticket_ttl = mfa_ticket_ttl

        # Only register routes if WebAuthn is configured.
        if not self._rp_id:
            return

        if routes is not None:
            self.routes = routes
        else:
            self.routes = []

        self.routes.append(Route(f"{uri_prefix}/auth/webauthn/register/options", endpoint=self.register_options_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/auth/webauthn/register/verify", endpoint=self.register_verify_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/auth/webauthn/login/options", endpoint=self.login_options_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/auth/webauthn/login/verify", endpoint=self.login_verify_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/auth/webauthn/upgrade/options", endpoint=self.upgrade_options_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/auth/webauthn/upgrade/verify", endpoint=self.upgrade_verify_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/auth/session/renew", endpoint=self.session_renew_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/auth/session/reauth/options", endpoint=self.session_reauth_options_handler, methods=["POST", "OPTIONS"]))
        self.routes.append(Route(f"{uri_prefix}/auth/session/reauth/verify", endpoint=self.session_reauth_verify_handler, methods=["POST", "OPTIONS"]))

        logging.info(f"WebAuthn enabled: rp_id={self._rp_id}, origin={self._origin}")

    #-------------------------------------------------------------------------
    # Database operations
    #-------------------------------------------------------------------------

    async def get_credentials_for_user(self, user_id: int) -> list[dict]:
        """Get all WebAuthn credentials for a user."""
        if not self._db_pool:
            return []

        async with self._db_pool.connection() as conn:
            rows = await conn.execute(
                "SELECT id, credential_id, public_key, sign_count, transports "
                "FROM webauthn_credentials WHERE user_id = %s AND is_del = FALSE",
                (user_id,)
            )
            results = await rows.fetchall()
            return [
                {
                    "id": r[0],
                    "credential_id": bytes(r[1]),
                    "public_key": bytes(r[2]),
                    "sign_count": r[3],
                    "transports": r[4] or [],
                }
                for r in results
            ]

    async def save_credential(
        self,
        user_id: int,
        credential_id: bytes,
        public_key: bytes,
        sign_count: int,
        transports: list[str] | None = None,
        aaguid: str | None = None,
    ) -> str | None:
        """Save a new WebAuthn credential. Returns error string or None."""
        if not self._db_pool:
            return "No database connection"

        try:
            async with self._db_pool.connection() as conn:
                await conn.execute(
                    "INSERT INTO webauthn_credentials "
                    "(user_id, credential_id, public_key, sign_count, transports, aaguid) "
                    "VALUES (%s, %s, %s, %s, %s, %s)",
                    (user_id, credential_id, public_key, sign_count, transports, aaguid)
                )
        except Exception as e:
            logging.error(f"Failed to save WebAuthn credential: {e}")
            return str(e)

        return None

    async def update_sign_count(self, credential_id: bytes, new_sign_count: int):
        """Update sign count and last_used_at after successful authentication."""
        if not self._db_pool:
            return

        try:
            async with self._db_pool.connection() as conn:
                await conn.execute(
                    "UPDATE webauthn_credentials "
                    "SET sign_count = %s, last_used_at = CURRENT_TIMESTAMP "
                    "WHERE credential_id = %s",
                    (new_sign_count, credential_id)
                )
        except Exception as e:
            logging.error(f"Failed to update sign count: {e}")

    #-------------------------------------------------------------------------
    # Challenge storage (Redis)
    #-------------------------------------------------------------------------

    async def _store_challenge(self, key: str, challenge: bytes, ttl: int = 300):
        if self._redis:
            # Store as base64 to avoid UTF-8 decode issues with raw bytes.
            encoded = base64.b64encode(challenge).decode("ascii")
            await self._redis.set(
                _CHALLENGE_PREFIX + key,
                encoded,
                ex=ttl,
            )

    async def _get_and_delete_challenge(self, key: str) -> bytes | None:
        if not self._redis:
            return None

        val = await self._redis.get(_CHALLENGE_PREFIX + key)
        if val:
            await self._redis.delete(_CHALLENGE_PREFIX + key)
            if isinstance(val, bytes):
                val = val.decode("ascii")
            return base64.b64decode(val)
        return None

    #-------------------------------------------------------------------------
    # MFA Ticket
    #-------------------------------------------------------------------------

    async def create_mfa_ticket(self, user_id: int, email: str) -> str | None:
        """Create a short-lived MFA ticket after first-factor verification.
        Returns the ticket string, or None if Redis is unavailable.
        """
        if not self._redis:
            return None

        ticket = secrets.token_urlsafe(32)
        data = json.dumps({"user_id": user_id, "email": email})
        await self._redis.set(
            _MFA_TICKET_PREFIX + ticket,
            data,
            ex=self._mfa_ticket_ttl,
        )
        return ticket

    async def validate_mfa_ticket(self, ticket: str) -> tuple[int, str, str | None]:
        """Validate and consume an MFA ticket (one-time use).
        Returns (user_id, email, error).
        """
        if not self._redis:
            return 0, "", "Redis unavailable"

        data = await self._redis.get(_MFA_TICKET_PREFIX + ticket)
        if not data:
            return 0, "", "Invalid or expired MFA ticket"

        # One-time use: delete immediately.
        await self._redis.delete(_MFA_TICKET_PREFIX + ticket)

        try:
            parsed = json.loads(data)
            return parsed["user_id"], parsed["email"], None
        except Exception as e:
            return 0, "", str(e)

    #-------------------------------------------------------------------------
    # MFA check (called by UserService after first-factor auth)
    #-------------------------------------------------------------------------

    async def _is_mfa_enabled(self, user_id: int) -> bool:
        """Check if MFA is enabled for this user in their settings."""
        if not self._db_pool:
            return False

        try:
            async with self._db_pool.connection() as conn:
                row = await conn.execute(
                    "SELECT mfa_enabled FROM health_app_user WHERE id = %s AND is_del = FALSE",
                    (user_id,)
                )
                result = await row.fetchone()
                return bool(result and result[0])
        except Exception as e:
            logging.warning(f"Failed to check mfa_enabled for user {user_id}: {e}")
            return False

    async def check_mfa_required(self, user_id: int, email: str) -> dict | None:
        """Check if MFA is required for this user.

        MFA triggers only when ALL conditions are met:
        1. WebAuthn is configured (rp_id set)
        2. User has mfa_enabled = TRUE in settings
        3. User has registered WebAuthn credentials

        Returns an MFA challenge dict, or None if MFA is not required.
        """
        if not self._rp_id:
            return None

        # Check user's MFA setting first (cheap DB query).
        if not await self._is_mfa_enabled(user_id):
            return None

        credentials = await self.get_credentials_for_user(user_id)
        if not credentials:
            return None

        ticket = await self.create_mfa_ticket(user_id, email)
        if not ticket:
            return None

        return {
            "status": "mfa_required",
            "mfa_ticket": ticket,
            "methods": [
                {"type": "webauthn", "registered": True}
            ],
        }

    #-------------------------------------------------------------------------
    # Route handlers
    #-------------------------------------------------------------------------

    async def register_options_handler(self, request: Request) -> Response:
        """Generate WebAuthn registration options (requires JWT auth)."""
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        # Verify JWT token.
        token = self._get_jwt_token(request)
        payload, err = self._token_validator.verify_token(token)
        if err:
            return json_response({"message": "Unauthorized"}, status_code=401, request=request)

        user_id = int(payload.get("sub", 0))
        email = payload.get("email", "")
        if not user_id:
            return json_response_with_code(-1, "Invalid user", request=request)

        # Exclude already registered credentials.
        existing = await self.get_credentials_for_user(user_id)
        exclude_credentials = [
            PublicKeyCredentialDescriptor(
                id=cred["credential_id"],
                transports=_to_transport_enums(cred["transports"]),
            )
            for cred in existing
        ]

        options = generate_registration_options(
            rp_id=self._rp_id,
            rp_name=self._rp_name,
            user_id=str(user_id).encode(),
            user_name=email,
            user_display_name=email.split("@")[0],
            authenticator_selection=AuthenticatorSelectionCriteria(
                authenticator_attachment=AuthenticatorAttachment.PLATFORM,
                resident_key=ResidentKeyRequirement.PREFERRED,
                user_verification=UserVerificationRequirement.REQUIRED,
            ),
            exclude_credentials=exclude_credentials,
        )

        # Store challenge for verification.
        await self._store_challenge(f"reg:{user_id}", options.challenge)

        return json_response_with_code(
            data=json.loads(options_to_json(options)),
            request=request,
        )

    #-------------------------------------------------------------------------

    async def register_verify_handler(self, request: Request) -> Response:
        """Verify WebAuthn registration response (requires JWT auth)."""
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        # Verify JWT token.
        token = self._get_jwt_token(request)
        payload, err = self._token_validator.verify_token(token)
        if err:
            return json_response({"message": "Unauthorized"}, status_code=401, request=request)

        user_id = int(payload.get("sub", 0))
        if not user_id:
            return json_response_with_code(-1, "Invalid user", request=request)

        # Get stored challenge.
        challenge = await self._get_and_delete_challenge(f"reg:{user_id}")
        if not challenge:
            return json_response_with_code(-2, "Challenge expired or not found", request=request)

        try:
            data = await request.json()
            credential_json = json.dumps(data.get("credential", data))

            verification = verify_registration_response(
                credential=credential_json,
                expected_challenge=challenge,
                expected_rp_id=self._rp_id,
                expected_origin=self._origin,
                require_user_verification=True,
            )
        except Exception as e:
            logging.error(f"WebAuthn registration verification failed: {e}")
            return json_response_with_code(-3, str(e), request=request)

        # Extract transports from the original request (not from verification result).
        transports = None
        try:
            cred_data = data.get("credential", data)
            resp = cred_data.get("response", {})
            transports = resp.get("transports")
        except Exception:
            pass

        # Save credential to database.
        err = await self.save_credential(
            user_id=user_id,
            credential_id=verification.credential_id,
            public_key=verification.credential_public_key,
            sign_count=verification.sign_count,
            transports=transports,
            aaguid=str(verification.aaguid) if hasattr(verification, "aaguid") and verification.aaguid else None,
        )
        if err:
            return json_response_with_code(-4, err, request=request)

        # Issue a new AAL2 token so the user doesn't need to re-login.
        email = payload.get("email", "")
        access_token, refresh_token, err = await self._token_validator.generate_tokens(
            str(user_id),
            email,
            gen_claims_func=lambda uid, em: _aal2_claims(),
            expires_in=AAL2_SESSION_IDLE_TIMEOUT,
        )
        if err:
            return json_response_with_code(-5, err, request=request)

        result = {
            "registered": True,
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": AAL2_SESSION_IDLE_TIMEOUT,
        }
        if refresh_token:
            result["refresh_token"] = refresh_token

        return json_response_with_code(
            data=result,
            request=request,
        )

    #-------------------------------------------------------------------------

    async def login_options_handler(self, request: Request) -> Response:
        """Generate WebAuthn authentication options (requires mfa_ticket)."""
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        try:
            data = await request.json()
            mfa_ticket = data.get("mfa_ticket", "")
        except Exception:
            return json_response_with_code(-1, "Invalid request body", request=request)

        if not mfa_ticket:
            return json_response_with_code(-1, "MFA ticket is required", request=request)

        # Peek at the ticket (don't consume yet — will be consumed on verify).
        if not self._redis:
            return json_response_with_code(-2, "Redis unavailable", request=request)

        ticket_data = await self._redis.get(_MFA_TICKET_PREFIX + mfa_ticket)
        if not ticket_data:
            return json_response_with_code(-3, "Invalid or expired MFA ticket", request=request)

        parsed = json.loads(ticket_data)
        user_id = parsed["user_id"]

        # Get user's credentials.
        credentials = await self.get_credentials_for_user(user_id)
        if not credentials:
            return json_response_with_code(-4, "No WebAuthn credentials registered", request=request)

        allow_credentials = [
            PublicKeyCredentialDescriptor(
                id=cred["credential_id"],
                transports=_to_transport_enums(cred["transports"]),
            )
            for cred in credentials
        ]

        options = generate_authentication_options(
            rp_id=self._rp_id,
            allow_credentials=allow_credentials,
            user_verification=UserVerificationRequirement.REQUIRED,
        )

        # Store challenge keyed by mfa_ticket (not user_id) to bind them.
        await self._store_challenge(f"auth:{mfa_ticket}", options.challenge)

        return json_response_with_code(
            data=json.loads(options_to_json(options)),
            request=request,
        )

    #-------------------------------------------------------------------------

    async def login_verify_handler(self, request: Request) -> Response:
        """Verify WebAuthn authentication and issue AAL2 JWT."""
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        try:
            data = await request.json()
            mfa_ticket = data.get("mfa_ticket", "")
            credential_data = data.get("credential", data)
        except Exception:
            return json_response_with_code(-1, "Invalid request body", request=request)

        if not mfa_ticket:
            return json_response_with_code(-1, "MFA ticket is required", request=request)

        # Get and delete challenge.
        challenge = await self._get_and_delete_challenge(f"auth:{mfa_ticket}")
        if not challenge:
            return json_response_with_code(-2, "Challenge expired or not found", request=request)

        # Consume the MFA ticket.
        user_id, email, err = await self.validate_mfa_ticket(mfa_ticket)
        if err:
            return json_response_with_code(-3, err, request=request)

        # Find matching credential.
        credentials = await self.get_credentials_for_user(user_id)
        credential_json = json.dumps(credential_data)

        # Try to find the matching credential by parsing the response.
        matched_cred = None
        try:
            parsed_cred = json.loads(credential_json) if isinstance(credential_json, str) else credential_json
            raw_id = parsed_cred.get("rawId") or parsed_cred.get("id", "")
            incoming_cred_id = base64url_to_bytes(raw_id) if isinstance(raw_id, str) else raw_id
            for cred in credentials:
                if cred["credential_id"] == incoming_cred_id:
                    matched_cred = cred
                    break
        except Exception:
            pass

        if not matched_cred:
            return json_response_with_code(-4, "Credential not found", request=request)

        # Verify the authentication response.
        try:
            verification = verify_authentication_response(
                credential=credential_json,
                expected_challenge=challenge,
                expected_rp_id=self._rp_id,
                expected_origin=self._origin,
                credential_public_key=matched_cred["public_key"],
                credential_current_sign_count=matched_cred["sign_count"],
                require_user_verification=True,
            )
        except Exception as e:
            logging.error(f"WebAuthn authentication verification failed: {e}")
            return json_response_with_code(-5, str(e), request=request)

        # Update sign count.
        await self.update_sign_count(
            matched_cred["credential_id"],
            verification.new_sign_count,
        )

        # Generate AAL2 JWT with session tracking.
        access_token, refresh_token, err = await self._token_validator.generate_tokens(
            str(user_id),
            email,
            gen_claims_func=lambda uid, em: _aal2_claims(),
            expires_in=AAL2_SESSION_IDLE_TIMEOUT,
        )
        if err:
            return json_response_with_code(-6, err, request=request)

        result = {
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": AAL2_SESSION_IDLE_TIMEOUT,
        }
        if refresh_token:
            result["refresh_token"] = refresh_token

        return json_response_with_code(data=result, request=request)

    #-------------------------------------------------------------------------
    # Session upgrade handlers (in-session AAL1 → AAL2, triggered by 403 interceptor)
    #-------------------------------------------------------------------------

    async def upgrade_options_handler(self, request: Request) -> Response:
        """Generate WebAuthn authentication options for session upgrade (requires JWT)."""
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        token = self._get_jwt_token(request)
        payload, err = self._token_validator.verify_token(token)
        if err:
            return json_response({"message": "Unauthorized"}, status_code=401, request=request)

        user_id = int(payload.get("sub", 0))
        if not user_id:
            return json_response_with_code(-1, "Invalid user", request=request)

        credentials = await self.get_credentials_for_user(user_id)
        if not credentials:
            return json_response_with_code(-2, "No WebAuthn credentials registered", request=request)

        allow_credentials = [
            PublicKeyCredentialDescriptor(
                id=cred["credential_id"],
                transports=_to_transport_enums(cred["transports"]),
            )
            for cred in credentials
        ]

        options = generate_authentication_options(
            rp_id=self._rp_id,
            allow_credentials=allow_credentials,
            user_verification=UserVerificationRequirement.REQUIRED,
        )

        await self._store_challenge(f"upgrade:{user_id}", options.challenge)

        return json_response_with_code(
            data=json.loads(options_to_json(options)),
            request=request,
        )

    #-------------------------------------------------------------------------

    async def upgrade_verify_handler(self, request: Request) -> Response:
        """Verify WebAuthn and issue AAL2 token for session upgrade (requires JWT)."""
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        token = self._get_jwt_token(request)
        payload, err = self._token_validator.verify_token(token)
        if err:
            return json_response({"message": "Unauthorized"}, status_code=401, request=request)

        user_id = int(payload.get("sub", 0))
        email = payload.get("email", "")
        if not user_id:
            return json_response_with_code(-1, "Invalid user", request=request)

        challenge = await self._get_and_delete_challenge(f"upgrade:{user_id}")
        if not challenge:
            return json_response_with_code(-2, "Challenge expired or not found", request=request)

        try:
            data = await request.json()
            credential_data = data.get("credential", data)
            credential_json = json.dumps(credential_data)
        except Exception:
            return json_response_with_code(-3, "Invalid request body", request=request)

        # Find matching credential.
        credentials = await self.get_credentials_for_user(user_id)
        matched_cred = None
        try:
            parsed_cred = json.loads(credential_json) if isinstance(credential_json, str) else credential_json
            raw_id = parsed_cred.get("rawId") or parsed_cred.get("id", "")
            incoming_cred_id = base64url_to_bytes(raw_id) if isinstance(raw_id, str) else raw_id
            for cred in credentials:
                if cred["credential_id"] == incoming_cred_id:
                    matched_cred = cred
                    break
        except Exception:
            pass

        if not matched_cred:
            return json_response_with_code(-4, "Credential not found", request=request)

        try:
            verification = verify_authentication_response(
                credential=credential_json,
                expected_challenge=challenge,
                expected_rp_id=self._rp_id,
                expected_origin=self._origin,
                credential_public_key=matched_cred["public_key"],
                credential_current_sign_count=matched_cred["sign_count"],
                require_user_verification=True,
            )
        except Exception as e:
            logging.error(f"WebAuthn upgrade verification failed: {e}")
            return json_response_with_code(-5, str(e), request=request)

        await self.update_sign_count(
            matched_cred["credential_id"],
            verification.new_sign_count,
        )

        # Generate AAL2 JWT with session tracking.
        access_token, refresh_token, err = await self._token_validator.generate_tokens(
            str(user_id),
            email,
            gen_claims_func=lambda uid, em: _aal2_claims(),
            expires_in=AAL2_SESSION_IDLE_TIMEOUT,
        )
        if err:
            return json_response_with_code(-6, err, request=request)

        result = {
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": AAL2_SESSION_IDLE_TIMEOUT,
        }
        if refresh_token:
            result["refresh_token"] = refresh_token

        return json_response_with_code(data=result, request=request)

    #-------------------------------------------------------------------------
    # Session management handlers (NIST 800-63B Section 7)
    #-------------------------------------------------------------------------

    async def session_renew_handler(self, request: Request) -> Response:
        """Silent session renewal for active AAL2 users. No WebAuthn needed.

        Extends the token exp by another idle timeout period while preserving
        the original session_start. Rejects if max session lifetime exceeded.
        """
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        token = self._get_jwt_token(request)
        payload, err = self._token_validator.verify_token(token)
        if err:
            return json_response({"message": "Unauthorized"}, status_code=401, request=request)

        # Only AAL2 sessions can be renewed.
        aal = payload.get("aal", 0)
        if not isinstance(aal, int) or aal < 2:
            return json_response_with_code(-1, "Only AAL2 sessions support renewal", request=request)

        user_id = payload.get("sub", "")
        email = payload.get("email", "")
        session_start = payload.get("session_start", 0)

        if not user_id or not session_start:
            return json_response_with_code(-2, "Invalid session token", request=request)

        # Check max session lifetime (12 hours).
        now = int(time.time())
        if now - session_start > AAL2_SESSION_MAX_LIFETIME:
            return json_response(
                {
                    "detail": {
                        "code": "ERROR_SESSION_MAX_LIFETIME",
                        "message": "Session exceeded maximum lifetime. Please re-authenticate.",
                    }
                },
                status_code=403,
                request=request,
            )

        # Issue renewed token with same session_start.
        access_token, _, err = await self._token_validator.generate_tokens(
            str(user_id),
            email,
            gen_claims_func=lambda uid, em: _aal2_claims(session_start=session_start),
            expires_in=AAL2_SESSION_IDLE_TIMEOUT,
        )
        if err:
            return json_response_with_code(-3, err, request=request)

        return json_response_with_code(
            data={
                "access_token": access_token,
                "token_type": "Bearer",
                "expires_in": AAL2_SESSION_IDLE_TIMEOUT,
            },
            request=request,
        )

    #-------------------------------------------------------------------------

    async def session_reauth_options_handler(self, request: Request) -> Response:
        """Generate WebAuthn options for session re-auth after timeout.

        Accepts expired JWT tokens (signature verified, expiry ignored)
        so users can re-authenticate without a full email OTP flow.
        """
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        token = self._get_jwt_token(request)
        payload, err = self._token_validator.verify_token_allow_expired(
            token, max_age=AAL2_REAUTH_MAX_AGE,
        )
        if err:
            return json_response({"message": "Unauthorized"}, status_code=401, request=request)

        aal = payload.get("aal", 0)
        if not isinstance(aal, int) or aal < 2:
            return json_response({"message": "Unauthorized"}, status_code=401, request=request)

        user_id = int(payload.get("sub", 0))
        if not user_id:
            return json_response_with_code(-1, "Invalid user", request=request)

        credentials = await self.get_credentials_for_user(user_id)
        if not credentials:
            return json_response_with_code(-2, "No WebAuthn credentials registered", request=request)

        allow_credentials = [
            PublicKeyCredentialDescriptor(
                id=cred["credential_id"],
                transports=_to_transport_enums(cred["transports"]),
            )
            for cred in credentials
        ]

        options = generate_authentication_options(
            rp_id=self._rp_id,
            allow_credentials=allow_credentials,
            user_verification=UserVerificationRequirement.REQUIRED,
        )

        await self._store_challenge(f"reauth:{user_id}", options.challenge)

        return json_response_with_code(
            data=json.loads(options_to_json(options)),
            request=request,
        )

    #-------------------------------------------------------------------------

    async def session_reauth_verify_handler(self, request: Request) -> Response:
        """Verify WebAuthn for session re-auth. Issues a fresh AAL2 token."""
        if request.method == "OPTIONS":
            return json_response_with_code(disable_log=True)

        token = self._get_jwt_token(request)
        payload, err = self._token_validator.verify_token_allow_expired(
            token, max_age=AAL2_REAUTH_MAX_AGE,
        )
        if err:
            return json_response({"message": "Unauthorized"}, status_code=401, request=request)

        aal = payload.get("aal", 0)
        if not isinstance(aal, int) or aal < 2:
            return json_response({"message": "Unauthorized"}, status_code=401, request=request)

        user_id = int(payload.get("sub", 0))
        email = payload.get("email", "")
        if not user_id:
            return json_response_with_code(-1, "Invalid user", request=request)

        challenge = await self._get_and_delete_challenge(f"reauth:{user_id}")
        if not challenge:
            return json_response_with_code(-2, "Challenge expired or not found", request=request)

        try:
            data = await request.json()
            credential_data = data.get("credential", data)
            credential_json = json.dumps(credential_data)
        except Exception:
            return json_response_with_code(-3, "Invalid request body", request=request)

        # Find matching credential.
        credentials = await self.get_credentials_for_user(user_id)
        matched_cred = None
        try:
            parsed_cred = json.loads(credential_json) if isinstance(credential_json, str) else credential_json
            raw_id = parsed_cred.get("rawId") or parsed_cred.get("id", "")
            incoming_cred_id = base64url_to_bytes(raw_id) if isinstance(raw_id, str) else raw_id
            for cred in credentials:
                if cred["credential_id"] == incoming_cred_id:
                    matched_cred = cred
                    break
        except Exception:
            pass

        if not matched_cred:
            return json_response_with_code(-4, "Credential not found", request=request)

        try:
            verification = verify_authentication_response(
                credential=credential_json,
                expected_challenge=challenge,
                expected_rp_id=self._rp_id,
                expected_origin=self._origin,
                credential_public_key=matched_cred["public_key"],
                credential_current_sign_count=matched_cred["sign_count"],
                require_user_verification=True,
            )
        except Exception as e:
            logging.error(f"WebAuthn session re-auth verification failed: {e}")
            return json_response_with_code(-5, str(e), request=request)

        await self.update_sign_count(
            matched_cred["credential_id"],
            verification.new_sign_count,
        )

        # Issue fresh AAL2 JWT with new session (new session_start).
        access_token, refresh_token, err = await self._token_validator.generate_tokens(
            str(user_id),
            email,
            gen_claims_func=lambda uid, em: _aal2_claims(),
            expires_in=AAL2_SESSION_IDLE_TIMEOUT,
        )
        if err:
            return json_response_with_code(-6, err, request=request)

        result = {
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": AAL2_SESSION_IDLE_TIMEOUT,
        }
        if refresh_token:
            result["refresh_token"] = refresh_token

        return json_response_with_code(data=result, request=request)

#-----------------------------------------------------------------------------
