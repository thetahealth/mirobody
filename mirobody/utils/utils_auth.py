import jwt
import logging

from typing import Optional
from urllib.parse import unquote
from fastapi import Header, HTTPException, WebSocket

from .config import global_config
from .req_ctx import get_req_ctx, update_req_ctx

#-----------------------------------------------------------------------------

_external_id_decoder = None

def set_id_decoder(decoder=None):
    global _external_id_decoder

    if callable(decoder):
        _external_id_decoder = decoder

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
        raise HTTPException(status_code=401, detail=f"Token decode failed, token: {token}")
    
    #-----------------------------------------------------

    user_id = 0
    
    # Get user ID via subject field,
    #   it should be an integer string.
    subject = decoded.get("sub")
    if subject:
        try:
            user_id = int(subject)
        except:
            user_id = None

    if not user_id or user_id <= 0:
        if callable(_external_id_decoder):
            user_id = _external_id_decoder(decoded)

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

async def verify_token_from_websocket(websocket: WebSocket) -> int:
    try:
        token = None

        if websocket.headers and "Authorization" in websocket.headers:
            token = websocket.headers["Authorization"]

        if not token and websocket.query_params and "token" in websocket.query_params:
            token = websocket.query_params["token"]

        if not token:
            logging.warning("No JWT token found")
            raise HTTPException(status_code=401, detail="No token found in session")

        user_id = await verify_token_string(token)
        return user_id

    except HTTPException:
        raise

    except Exception as e:
        logging.error(f"Failed to verify WebSocket token: {str(e)}")
        raise HTTPException(status_code=401, detail="Authentication failed")
