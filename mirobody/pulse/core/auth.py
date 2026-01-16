"""
Authentication utilities for Pulse management APIs
"""

import logging
from typing import Optional
from fastapi import HTTPException, Query, Depends
from mirobody.utils.config import global_config


async def verify_manage_key(sk: Optional[str] = Query(None, description="Management secret key")) -> bool:
    """
    Verify management secret key (can be used as FastAPI Depends dependency)
    
    Args:
        sk: Secret key from query parameter
        
    Returns:
        True if valid, raises HTTPException otherwise
        
    Raises:
        HTTPException: If key is missing or invalid
        
    Example:
        @router.get("/some-endpoint")
        async def some_endpoint(authorized: bool = Depends(verify_manage_key)):
            # authorized will be True if authentication succeeds
            pass
    """
    if not sk:
        logging.warning("Management API called without sk parameter")
        raise HTTPException(status_code=401, detail="Missing management key (sk parameter)")
    
    # Get expected key from config (backend_server_sk)
    expected_sk = global_config().get("backend_server_sk")
    if not expected_sk:
        logging.error("backend_server_sk not configured in config file")
        raise HTTPException(status_code=500, detail="Server configuration error: management key not configured")
    
    # Verify key
    if sk != expected_sk:
        logging.warning(f"Invalid management key attempted: {sk[:10]}...")
        raise HTTPException(status_code=403, detail="Invalid management key")
    
    logging.debug("Management key verified successfully")
    return True

