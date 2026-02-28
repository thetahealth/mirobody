"""
Router composition module

Imports all sub-routers and assembles the public router.
apple_router is included into public_router (nested prefix).
All other routers are exported individually for registration in the app.

Sub-routers:
    public_router   — /api/v1/pulse/*   main user-facing API (providers, link, webhook, OAuth)
    manage_router   — /api/v1/manage/*  admin/management endpoints
    file_router     — file upload endpoints
    food_router     — food recognition endpoints
    user_router     — user profile endpoints
    apple_router    — Apple Health specific endpoints (included in public_router)
    session_share_router — session sharing
    skill_router    — skill endpoints
"""

from .apple_router import router as apple_router
from .apple_router import old_router as old_apple_router
from .manage_router import router as manage_router
from .public_router import router as public_router

from .user_router import router as user_router
from .file_router import router as file_router
from .food_router import router as food_router
from .session_share_router import router as session_share_router
from .skill_router import router as skill_router

public_router.include_router(apple_router)

__all__ = [
    "public_router",
    "manage_router",
    "apple_router",
    "old_apple_router",
    "user_router",
    "file_router",
    "food_router",
    "session_share_router",
    "skill_router",
]
