"""Bearer-token verification for HTTP requests.

The FastAPI-facing half of authentication: pull the token off the request,
verify it, and turn it into a user id — or raise 401. Token *issuance* and
claim shape live in `mirobody/user/jwt.py`; this module only consumes them.

Was `utils_auth.py`, then `mirobody/utils/auth.py`. It is FastAPI all the way
down — `Header` defaults, `HTTPException` — and FastAPI ships in the
`[server]` extra, so it never belonged in the engine's utils package: every
one of its live callers is a router right next door. `verify_token_string` is
the one function with a non-router caller in history, and that caller — the
care-circle permission check, then `utils/permissions.py`, now
`user/care_circle.py` — imported it without ever using it.

Two functions did not come along, both with zero callers anywhere:
`verify_token_from_websocket` (the only reason this module imported
`WebSocket`) and `set_id_decoder`, already listed as dead in docs/roadmap.md.
"""

import jwt
import logging

from typing import Optional
from urllib.parse import unquote
from fastapi import Header, HTTPException

from ..utils.config import global_config
from ..utils.log import secret_fingerprint
from ..utils.req_ctx import get_req_ctx, update_req_ctx

#-----------------------------------------------------------------------------

async def verify_token_string(token_string: str) -> str:
    # raise HTTPException(status_code=401, detail=f"Token decode failed, token: {token_string}")
    try:
        # Decode it beforehand.
        token = unquote(token_string)
    except Exception as e:
        raise HTTPException(status_code=401, detail="Failed to decode authorization header")

    if not isinstance(token, str):
        logging.warning(f"Invalid token type: {type(token)}")
        raise HTTPException(status_code=401, detail="Invalid authorization header")

    # Remove Bearer prefix.
    while token.startswith("Bearer "):
        token = token[7:]
        logging.debug(f"Remove one Bearer prefix, and the length of rest token: '{token[:50]}...'")

    jwt_key = global_config().get("JWT_KEY")
    if not jwt_key:
        logging.error("Invalid JWT key")
        raise HTTPException(status_code=500, detail="JWT key not configured")

    #-----------------------------------------------------

    try:
        decoded = jwt.decode(
            token,
            jwt_key,
            algorithms  = ["HS256"],
            options     = {
                "verify_signature"  : True,
                "verify_exp"        : True,
                "verify_orig_iat"   : False,
                "verify_aud"        : False,
                "verify_iss"        : False
            },
            audience    = None,  # Ignore audience.
        )

    except Exception as e:
        logging.warning(f"Failed to decode JWT token: {str(e)}")
        decoded = None

    if not decoded:
        # The full undecoded bearer token used to go out in this response
        # BODY — to the caller, and onward into their proxy logs, browser
        # console and error tracker. A 401 must not hand back the credential it
        # just rejected; the fingerprint goes to our log instead.
        logging.warning("JWT decode failed", extra={"token": secret_fingerprint(token)})
        raise HTTPException(status_code=401, detail="Token decode failed")
    
    #-----------------------------------------------------

    user_id = 0
    
    # Get user ID via subject field,
    #   it should be an integer string.
    subject = decoded.get("sub")
    if subject:
        try:
            user_id = int(subject)
        except Exception:
            user_id = None

    if not user_id or user_id <= 0:
        raise HTTPException(status_code=401, detail="Invalid user ID")

    user_id = str(user_id)
    update_req_ctx(token=token, user_id=user_id)

    return user_id

#-----------------------------------------------------------------------------

async def verify_token_optional(authorization: Optional[str] = Header(None)) -> Optional[str]:
    if not authorization:
        return None

    try:
        user_id = await verify_token_string(authorization)
        return str(user_id)
    
    except Exception as e:
        logging.warning(str(e))
        return None


async def verify_token(authorization: str = Header(...)) -> str:
    try:
        cached_user_id = get_req_ctx("user_id")
        if cached_user_id:
            return str(cached_user_id)
        
    except Exception as e:
        logging.warning(str(e))

    user_id = await verify_token_string(authorization)
    return str(user_id)
