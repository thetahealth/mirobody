"""The FastAPI routers `Server.start` mounts, one per surface.

`apple_router` is also included into `public_router`, so the Apple Health
uploads answer under both /apple/* and the provider prefix.

    public_router         device providers: list, link, vendor OAuth, webhooks (/api/v1/pulse)
    apple_router          Apple Health uploads from a phone app (/apple)
    file_router           uploads, stored files, the data page (/files, /ws, /api/v1/data)
    user_router           settings and managed members (/api/user)
    session_share_router  a chat shared by link (/api/share)
    sharing_router        the care circle (/invitation)
    indicator_router      readings for the web client (/api/v1/health-indicators, /api/v1/data)
    records_router        the hosted platform's record shapes (/api/data, /api/standardize)
    journal_router        the journal (/api/v1/journal)
    genomics_router       genotypes and their exports (/api/v1/genomics)
    medication_router     medication plans (/api/v1/medications)
    data_export_router    the whole record as NDJSON (/api/user/data-export)
    setup_router          the first-run page's API (/api/setup)

Removed: `manage_router` (/api/v1/manage/*), `food_router` (/api/v1/food/*)
and `skill_router` (/api/skills/*).
All three were product surfaces with no caller left: the web client called
none of them, and nothing in the project did. food_router's
writes were also unreachable by design (it stored records as
`th_messages.message_type = 'food'`, which `get_chat_history` filters out) so
only its own /history endpoint could ever read them. skill_router was a CRUD
API over `th_user_custom_skills`, a table nothing read.
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
# `AttributeError: ... has no attribute '_contains_router'`: aborting startup before
# uvicorn binds. The traceback was invisible: asyncio.run's task cleanup hangs on the
# scheduler, so the exception never got re-raised and the log just stopped.
from .records_router import router as records_router
from .journal_router import router as journal_router
from .genomics_router import router as genomics_router
from .medication_router import router as medication_router
from .data_export_router import router as data_export_router
from .setup_router import router as setup_router

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
    "journal_router",
    "genomics_router",
    "medication_router",
    "data_export_router",
    "setup_router",
]
