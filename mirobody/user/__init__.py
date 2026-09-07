"""Identity: who a person is, and whose record they may read.

    auth/             how they prove it: tokens, email codes, OAuth, passkeys
    user.py           the identity record, and the ONE lookup over it
    user_service.py   sign-in: pick a validator from auth/, land an account
    care_circle.py    who may read whose record
    account_merge.py  two sign-ins turn out to be one person
    profile.py        the health profile document generated from the record

Exports resolve lazily (PEP 562), and that is load-bearing, not style — the
same reason `mirobody/agent/__init__.py` is lazy.

This package's `auth.email` module imports `mandrill` at module scope, `auth.oauth_service`
and `user_service` reach the database, and all three live in the `[app]`
extra. The eager `from .email import MandrillEmailValidator` that used to sit
here therefore made **mandrill a hard requirement of the engine**, through a
chain nothing in the layering rules could see:

    mirobody/pulse/core/user.py
      -> mirobody.user                     (executes THIS __init__)
        -> mirobody.user.auth.email
          -> mandrill

`mirobody/pulse/` is engine. So `pip install mirobody` plus a whoop payload —
no server, no database — raised ModuleNotFoundError, and 24 pulse tests failed
in any environment without the server extra. They passed in the dev venv, which
has everything, which is why this survived: it is only visible in a CLEAN
install, and CI never ran the suite at all.

import-linter cannot catch it either. A submodule import creates no graph edge
to the parent package, so `from mirobody.user.auth.jwt import ...` looks like a leaf
import while actually executing everything listed here.
"""

from typing import TYPE_CHECKING

_EXPORTS = {
    "AbstractTokenValidator": "auth.jwt",
    "JwtTokenValidator": "auth.jwt",
    "MandrillEmailValidator": "auth.email",
    "OAuthService": "auth.oauth_service",
    "UserService": "user_service",
}

__all__ = [*_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .auth.email import MandrillEmailValidator
    from .auth.jwt import AbstractTokenValidator, JwtTokenValidator
    from .auth.oauth_service import OAuthService
    from .user_service import UserService


def __getattr__(name: str):
    if name in _EXPORTS:
        import importlib

        module = importlib.import_module(f".{_EXPORTS[name]}", __name__)
        return getattr(module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
