"""Identity: tokens, email codes, OAuth, care-circle sharing, user records.

Exports resolve lazily (PEP 562), and that is load-bearing, not style — the
same reason `mirobody/agent/__init__.py` is lazy.

This package's `email` module imports `mandrill` at module scope, `oauth_service`
and `user_service` reach the database, and all three live in the `[server]`
extra. The eager `from .email import MandrillEmailValidator` that used to sit
here therefore made **mandrill a hard requirement of the engine**, through a
chain nothing in the layering rules could see:

    mirobody/pulse/core/user.py
      -> mirobody.user                     (executes THIS __init__)
        -> mirobody.user.email
          -> mandrill

`mirobody/pulse/` is engine. So `pip install mirobody` plus a whoop payload —
no server, no database — raised ModuleNotFoundError, and 24 pulse tests failed
in any environment without the server extra. They passed in the dev venv, which
has everything, which is why this survived: it is only visible in a CLEAN
install, and CI never ran the suite at all.

import-linter cannot catch it either. A submodule import creates no graph edge
to the parent package, so `from mirobody.user.jwt import ...` looks like a leaf
import while actually executing everything listed here.
"""

from typing import TYPE_CHECKING

_EXPORTS = {
    "AbstractTokenValidator": "jwt",
    "JwtTokenValidator": "jwt",
    "MandrillEmailValidator": "email",
    "OAuthService": "oauth_service",
    "UserService": "user_service",
}

__all__ = [*_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .email import MandrillEmailValidator
    from .jwt import AbstractTokenValidator, JwtTokenValidator
    from .oauth_service import OAuthService
    from .user_service import UserService


def __getattr__(name: str):
    if name in _EXPORTS:
        import importlib

        module = importlib.import_module(f".{_EXPORTS[name]}", __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
