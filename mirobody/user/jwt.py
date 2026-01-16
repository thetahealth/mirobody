import jwt, secrets, time

from typing import Callable
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

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
        gen_claims_func : Callable[[str, str], dict] | None = None
    ) -> tuple[
        str,        # Access token.
        str,        # Refresh token.
        str | None  # Error message.
    ]:
        extra_claims = gen_claims_func(user_id, email) if callable(gen_claims_func) else {}

        extra_claims.update(
            {
                "email"     : email.strip().lower(),
                "client_id" : f"mcp_{auth_method}_auth" if auth_method else "data_server",
                "token_type": "oauth_access_token"
            }
        )

        if client_id:
            extra_claims["client_id"] = client_id
        if scope:
            extra_claims["scope"] = scope
        
        access_token = self.generate_token(user_id, extra_claims)

        #-------------------------------------------------

        extra_claims = {
            "client_id" : f"mcp_{auth_method}_auth" if auth_method else "data_server",
            "token_type": "oauth_refresh_token"
        }

        if client_id:
            extra_claims["client_id"] = client_id
        if scope:
            extra_claims["scope"] = scope

        refresh_token = self.generate_token(user_id, extra_claims, self.get_expires_in()*2)

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
        if not refresh_token:
            return "", "", "invalid_request"
        
        return "", "", None

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
        iss         : str       = "theta_oauth",
        aud         : str       = "theta",
        client_id   : str       = "theta_data",
        scope       : str       = "mcp:read mcp:write",
        expires_in  : int       = 0
    ):

        self._key       = key
        self._algorithms= algorithms if algorithms else ["HS256"]
        self._iss       = iss
        self._aud       = aud
        self._client_id = client_id
        self._scope     = scope
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
    
    def generate_token(self, subject: str, extra: dict | None = None, expires_in: int = 0) -> str:
        now = int(time.time())

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

class JwtRsaTokenValidator(AbstractTokenValidator):
    def __init__(
        self,
        keys: list[str],
        iss: str,
        aud: str,
        scopt: str,
        expires_in: int = 60*60*24*30
    ):
        super().__init__()

        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048
        )
        public_key = private_key.public_key()

#-----------------------------------------------------------------------------
