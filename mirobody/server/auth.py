"""Bearer-token verification for HTTP requests, and whose record a request reads.

The FastAPI-facing half of authentication: pull the token off the request,
verify it, and turn it into a user id, or raise 401. Token *issuance* and
claim shape live in `mirobody/user/jwt.py`; this module only consumes them.
`subject_for` is the routers' one way from a `target_user_id` to a record.

Was `utils_auth.py`, then `mirobody/utils/auth.py`. It is FastAPI all the way
down (`Header` defaults, `HTTPException`) and FastAPI ships in the
`[app]` extra, so it never belonged in the engine's utils package: every
one of its live callers is a router right next door. `verify_token_string` is
the one function with a non-router caller in history, and that caller (the
care-circle permission check, then `utils/permissions.py`, now
`user/care_circle.py`) imported it without ever using it.

Two functions did not come along, both with zero callers anywhere:
`verify_token_from_websocket` (the only reason this module imported
`WebSocket`) and `set_id_decoder`, already listed as dead in docs/roadmap.md.
"""

import logging

from urllib.parse import unquote
from fastapi import Header, HTTPException

from mirobody.user.auth.bearer import bearer_subject
from mirobody.user.auth.jwt import JwtTokenValidator
from mirobody.user.care_circle import CareCircleDenied, resolve_subject
from mirobody.utils.config import global_config
from mirobody.utils.log import secret_fingerprint
from mirobody.utils.req_ctx import get_req_ctx, update_req_ctx

logger = logging.getLogger(__name__)

#-----------------------------------------------------------------------------

async def verify_token_claims(token_string: str) -> tuple[str, dict]:
    """The account a token speaks for, and its decoded claims.

    For a route that takes its token from the query string: it must check
    the claims' `aal` itself (`user.auth.bearer.lacks_second_factor`), because
    the JWT middleware only reads the Authorization header.
    """
    try:
        # Decode it beforehand.
        token = unquote(token_string)
    except Exception:
        raise HTTPException(status_code=401, detail="Failed to decode authorization header")

    if not isinstance(token, str):
        logger.warning(f"Invalid token type: {type(token)}")
        raise HTTPException(status_code=401, detail="Invalid authorization header")

    # Remove Bearer prefix.
    while token.startswith("Bearer "):
        token = token[7:]

    jwt_key = global_config().get("JWT_KEY")
    if not jwt_key:
        logger.error("Invalid JWT key")
        raise HTTPException(status_code=500, detail="JWT key not configured")

    #-----------------------------------------------------

    # One decoder: the middleware's. This was a second copy of the same
    # `jwt.decode` call, and it is where a refresh token presented as a bearer
    # credential got in after the middleware had refused it.
    decoded, err = JwtTokenValidator(jwt_key).verify_token(token)
    if err:
        logger.warning("JWT decode failed: error_type=%s", err.split(":")[0])  # phi: ok the decoder's error class
        decoded = None

    if not decoded:
        # The full undecoded bearer token used to go out in this response
        # BODY: to the caller, and onward into their proxy logs, browser
        # console and error tracker. A 401 must not hand back the credential it
        # just rejected; the fingerprint goes to our log instead.
        logger.warning("JWT decode failed", extra={"token": secret_fingerprint(token)})
        raise HTTPException(status_code=401, detail="Token decode failed")

    #-----------------------------------------------------

    # A refresh token, an MCP client's token, a closed account and a revoked
    # session all stop here; `bearer_subject` is the rule for every route.
    user_id = await bearer_subject(decoded)
    if not user_id:
        raise HTTPException(status_code=401, detail="Not a valid session")

    user_id = str(user_id)
    update_req_ctx(token=token, user_id=user_id)

    return user_id, decoded


async def verify_token_string(token_string: str) -> str:
    user_id, _ = await verify_token_claims(token_string)
    return user_id

#-----------------------------------------------------------------------------

async def verify_token_optional(authorization: str | None = Header(None)) -> str | None:
    if not authorization:
        return None

    try:
        user_id = await verify_token_string(authorization)
        return str(user_id)
    
    except Exception as e:
        logger.warning(str(e))
        return None


async def verify_token(authorization: str = Header(...)) -> str:
    try:
        cached_user_id = get_req_ctx("user_id")
        if cached_user_id:
            return str(cached_user_id)
        
    except Exception as e:
        logger.warning(str(e))

    user_id = await verify_token_string(authorization)
    return str(user_id)

#-----------------------------------------------------------------------------

async def subject_for(caller: str, target: str | None, *, write: bool = False) -> str | None:
    """Whose record a request from `caller` runs against, or None when the
    care circle does not grant it.

    `target` is the member id a client sent (`target_user_id`,
    `owner_user_id`): empty means the caller's own record, and anyone else's
    takes an accepted membership that grants read, or write with `write`. The
    answer is the id `resolve_subject` decided on, never the parameter as
    sent: twelve hand-written copies returned the raw value, so "07" passed
    the check for member 7 and the write was filed under "07".
    """
    try:
        subject = await resolve_subject(caller, target, require_write=write)
    except CareCircleDenied:
        return None
    return str(subject.subject_id)
