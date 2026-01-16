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

__all__ = ["public_router", "manage_router", "session_share_router", "skill_router"]
