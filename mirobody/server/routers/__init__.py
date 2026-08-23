"""
Router composition module

Imports all sub-routers and assembles the public router.
apple_router is included into public_router (nested prefix).
All other routers are exported individually for registration in the app.

Sub-routers:
    public_router   — /api/v1/pulse/*   main user-facing API (providers, link, webhook, OAuth)
    file_router     — file upload endpoints
    user_router     — user profile endpoints
    apple_router    — Apple Health specific endpoints (included in public_router)
    session_share_router — session sharing
    sharing_router  — /invitation/*  data-sharing between users

Removed: `manage_router` (/api/v1/manage/*), `food_router` (/api/v1/food/*)
and `skill_router` (/api/skills/*).
Both were client-less product surfaces with no consumer left in the project:
the web client called neither, and there is no mobile client. food_router's
writes were also unreachable by design — it stored records as
`th_messages.message_type = 'food'`, which `get_chat_history` filters out — so
only its own /history endpoint could ever read them. skill_router was a CRUD
API over `th_user_custom_skills`, a table nothing read: DeepAgent's Agent
Skills come from SKILL_DIRS on disk (see agent/deep_agent._build_backend).
"""

from .apple_router import router as apple_router
from .public_router import router as public_router
from .indicator_router import router as indicator_router

from .user_router import router as user_router
from .file_router import router as file_router
from .session_share_router import router as session_share_router
from .sharing_router import router as sharing_router
# Without this line `from .routers import records_router` in server.py resolves to
# the SUBMODULE (Python's fallback for a missing attribute on a package), the import
# raises nothing, and `app.include_router(<module>)` then dies with
# `AttributeError: ... has no attribute '_contains_router'` — aborting startup before
# uvicorn binds. The traceback was invisible: asyncio.run's task cleanup hangs on the
# scheduler, so the exception never got re-raised and the log just stopped.
from .records_router import router as records_router

public_router.include_router(apple_router)

__all__ = [
    "public_router",
    "indicator_router",
    "apple_router",
    "user_router",
    "file_router",
    "session_share_router",
    "sharing_router",
    "records_router",
]
