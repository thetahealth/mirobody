import jwt, secrets, time

from typing import Callable

from starlette.requests import Request

#-----------------------------------------------------------------------------

class AbstractTokenValidator:
    def get_expires_in(self) -> int: ...
    def verify_token(self, token: str) -> tuple[dict|None, str|None]: ...
    def generate_token(self, subject: str, extra: dict | None = None, expires_in: int = 0) -> str: ...

    #-----------------------------------------------------

    async def generate_tokens(
        self,
        user_id         : str,
        email           : str,
        auth_method     : str = "",
        client_id       : str = "",
        scope           : str = "",
        gen_claims_func : Callable[[str, str], dict] | None = None,
        expires_in      : int = 0,
    ) -> tuple[
        str,        # Access token.
        str,        # Refresh token.
        str | None  # Error message.
    ]:
        extra_claims = gen_claims_func(user_id, email) if callable(gen_claims_func) else {}

        extra_claims.update(
            {
                "email"     : email.strip().lower(),
                # "client_id" : f"mcp_{auth_method}_auth" if auth_method else "data_server",
                "token_type": "oauth_access_token"
            }
        )

        if client_id:
            extra_claims["client_id"] = client_id
        if scope:
            extra_claims["scope"] = scope

        access_token = self.generate_token(user_id, extra_claims, expires_in)

        #-------------------------------------------------

        extra_claims = {
            # "client_id" : f"mcp_{auth_method}_auth" if auth_method else "data_server",
            "token_type": "oauth_refresh_token"
        }

        if client_id:
            extra_claims["client_id"] = client_id
        if scope:
            extra_claims["scope"] = scope

        refresh_expires = (expires_in * 2) if expires_in > 60 else self.get_expires_in() * 2
        refresh_token = self.generate_token(user_id, extra_claims, refresh_expires)

        #-------------------------------------------------

        return access_token, refresh_token, None

    #-----------------------------------------------------

    def refresh_token(
        self,
        grant_type      : str,
        refresh_token   : str,
        scope           : str | None = None
    ) -> tuple[
        str,        # Access token.
        str,        # Refresh token.
        str | None  # Error message.
    ]:
        # Not implemented. An earlier stub returned ("", "", None) — success
        # with EMPTY tokens — for any non-empty refresh_token, which is a
        # landmine for whoever wires this endpoint up: the caller sees no
        # error and hands the client blank credentials. Fail honestly until
        # a real rotation flow exists.
        return "", "", "unsupported_grant_type"

    #-----------------------------------------------------

    def verify_http_token(self, request: Request) -> tuple[str | None, str | None]:
        if not request:
            return None, "Invalid request."

        token = request.headers.get("Authorization")
        if not token or not isinstance(token, str):
            return None, "Invalid JWT token"

        while token.startswith("Bearer "):
            token = token[7:]

        if not token:
            return None, "Empty token."
        if not isinstance(token, str):
            return None, "Invalid token."

        payload, err = self.verify_token(token)
        if err:
            return None, err
        if not payload:
            return None, "Empty payload."
        if not isinstance(payload, dict):
            return None, "Invalid payload."
        
        if "sub" not in payload:
            return None, "No subject."
        
        return payload["sub"], None

#-----------------------------------------------------------------------------

class JwtTokenValidator(AbstractTokenValidator):
    def __init__(
        self,
        key: str,
        algorithms  : list[str] = [],
        iss         : str       = "",
        aud         : str       = "",
        client_id   : str       = "",
        scope       : str       = "",
        expires_in  : int       = 0
    ):
        self._key       = key
        self._algorithms= algorithms if algorithms else ["HS256"]
        self._iss       = iss if isinstance(iss, str) else "theta_oauth"
        self._aud       = aud if isinstance(aud, str) else "theta"
        self._client_id = client_id if isinstance(client_id, str) else "theta_data"
        self._scope     = scope if isinstance(scope, str) else "mcp:read mcp:write"
        self._expires_in= expires_in if expires_in > 0 else 60*60*24*30

    #-----------------------------------------------------

    def get_expires_in(self) -> int:
        return self._expires_in

    #-----------------------------------------------------

    def verify_token(self, token: str) -> tuple[dict|None, str|None]:
        if not self._key:
            return None, "Invalid JWT key"
        
        if not token or not isinstance(token, str):
            return None, "Invalid JWT token"

        while token.startswith("Bearer "):
            token = token[7:]

        if not token:
            return None, "Empty JWT token"
        
        #-------------------------------------------------

        try:
            payload = jwt.decode(
                jwt         = token,
                key         = self._key,
                algorithms  = self._algorithms,
                options     = {
                    "verify_signature"  : True,
                    "verify_exp"        : True,
                    "verify_orig_iat"   : False,
                    "verify_aud"        : False,
                    "verify_iss"        : False
                }
            )
            return payload, None
        
        except Exception as e:
            return None, f"Failed to decode JWT token: {str(e)}"
    
    #-----------------------------------------------------

    def verify_token_allow_expired(self, token: str, max_age: int = 86400) -> tuple[dict | None, str | None]:
        """Verify token signature but allow expired tokens (up to max_age seconds).

        Used for session re-auth where the token has expired due to idle timeout
        but the user can still re-authenticate via WebAuthn within a grace period.
        """
        if not self._key:
            return None, "Invalid JWT key"

        if not token or not isinstance(token, str):
            return None, "Invalid JWT token"

        while token.startswith("Bearer "):
            token = token[7:]

        if not token:
            return None, "Empty JWT token"

        try:
            payload = jwt.decode(
                jwt=token,
                key=self._key,
                algorithms=self._algorithms,
                options={
                    "verify_signature": True,
                    "verify_exp": False,
                    "verify_orig_iat": False,
                    "verify_aud": False,
                    "verify_iss": False,
                },
            )

            # Enforce max staleness to prevent reuse of very old tokens.
            exp = payload.get("exp", 0)
            now = int(time.time())
            if exp and (now - exp) > max_age:
                return None, "Token expired beyond max re-auth age"

            return payload, None

        except Exception as e:
            return None, f"Failed to decode JWT token: {str(e)}"

    #-----------------------------------------------------

    def generate_token(self, subject: str, extra: dict | None = None, expires_in: int = 0) -> str:
        now = int(time.time()) - 60

        payload = {
            "sub"       : subject,                  # Subject of the token (usually user ID).
            "iss"       : self._iss,                # Issuer of the token.
            "aud"       : self._aud,                # Audience for the token.
            "iat"       : now,                      # Issued at time (when the token was created).
            "orig_iat"  : now,
            "nbf"       : now,                      # Not before time (token is invalid before this time).
            "exp"       : now + (self._expires_in if (not isinstance(expires_in, int) or expires_in <= 60) else expires_in),
                                                    # Expiration time of the token (Unix timestamp).

            "client_id" : self._client_id,
            "scope"     : self._scope,
            "jti"       : secrets.token_urlsafe(16) # JWT ID (unique identifier for the token).
        }

        if extra:
            payload.update(extra)

        return jwt.encode(payload=payload, key=self._key, algorithm=self._algorithms[0])

#-----------------------------------------------------------------------------


#-----------------------------------------------------------------------------

def validator_from_config() -> "JwtTokenValidator":
    """A validator carrying the SAME claims the server issues.

    `user_router` used to hand-roll the whole payload with `pyjwt.encode` when
    downgrading a session to AAL1, hardcoding `iss: ""` and `aud: ""` while
    every other token in the system carries the configured JWT_ISS/JWT_AUD.
    Nothing rejected it, because `verify_token` runs with `verify_iss` and
    `verify_aud` both False — so the divergence was invisible right up until
    someone hardens verification, at which point exactly one token type in the
    system stops validating and only for users who just disabled MFA.

    Claim shape belongs in one place. This is that place.
    """
    from ..utils.config import global_config

    opts = global_config().get_jwt_options()
    return JwtTokenValidator(
        key         = opts.get("jwt_key", ""),
        iss         = opts.get("jwt_iss", ""),
        aud         = opts.get("jwt_aud", ""),
        client_id   = opts.get("jwt_client_id", ""),
        scope       = opts.get("jwt_scope", ""),
        expires_in  = opts.get("jwt_expires_in") or 60 * 60 * 24 * 30,
    )
