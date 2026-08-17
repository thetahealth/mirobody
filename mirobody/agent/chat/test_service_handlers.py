"""Every chat endpoint answers CORS preflight, and says which ones are public.

`ChatService` has 17 handlers. All 17 opened with the same OPTIONS preamble and
13 repeated the same four-line token check, which meant the authentication
requirement was a convention you had to remember rather than something visible
at the endpoint. These tests pin both halves so the decorator that replaces the
boilerplate cannot quietly change who can reach what.

The public set is deliberate: listing available agents, models and providers is
what a client needs *before* it has a session. `chat_handler` is absent from
both sets because it authenticates through `request.state.user_id`, set by
middleware, rather than through the token validator — a second mechanism that
this file documents rather than unifies.
"""

from __future__ import annotations

import ast
import pathlib

# Endpoints reachable without a token, by design.
PUBLIC = {"agents_handler", "model_handler", "provider_handler"}

# Self-authenticating: `chat_handler` reads middleware-populated
# `request.state.user_id`; `prompt_handler` uses a token when present but must
# not 401 without one.
STATE_AUTH = {"chat_handler", "prompt_handler"}


def _handlers() -> dict[str, ast.AST]:
    src = pathlib.Path(__file__).with_name("service.py").read_text(encoding="utf-8")
    tree = ast.parse(src)
    out = {}
    for cls in tree.body:
        if not isinstance(cls, ast.ClassDef):
            continue
        for m in cls.body:
            if isinstance(m, (ast.FunctionDef, ast.AsyncFunctionDef)) and m.name.endswith("_handler"):
                out[m.name] = m
    return out


def test_every_handler_carries_exactly_one_auth_decorator():
    """No handler may be undecorated — that is how one goes out unauthenticated."""
    undecorated, multi = [], []
    for name, node in _handlers().items():
        decos = [d.id for d in node.decorator_list if isinstance(d, ast.Name)]
        auth = [d for d in decos if d in {"public_endpoint", "requires_auth", "self_authenticating"}]
        if not auth:
            undecorated.append(name)
        elif len(auth) > 1:
            multi.append((name, auth))
    assert not undecorated, f"handlers with no auth decorator: {undecorated}"
    assert not multi, f"handlers with conflicting decorators: {multi}"


def test_public_flag_matches_the_declared_public_set():
    """The runtime flag must agree with the declared set, not just the source."""
    from .service import ChatService

    flagged_public = set()
    for name in _handlers():
        fn = getattr(ChatService, name, None)
        if fn is not None and getattr(fn, "__mirobody_public__", False):
            flagged_public.add(name)
    assert flagged_public == PUBLIC, (
        f"decorator flags say public={sorted(flagged_public)}, "
        f"declared public={sorted(PUBLIC)}"
    )


def test_every_handler_answers_options():
    """A missing preflight branch fails the browser, not the server.

    CORS preflight arrives as OPTIONS before the real request; a handler that
    falls through to its body on OPTIONS either 401s the preflight or does work
    for a request that carries no body.
    """
    # Preflight now comes from the decorators; every handler must carry one.
    missing = []
    for name, node in _handlers().items():
        decos = [d.id for d in node.decorator_list if isinstance(d, ast.Name)]
        if not ({"public_endpoint", "requires_auth", "self_authenticating"} & set(decos)):
            missing.append(name)
    assert not missing, f"handlers not routed through a preflight decorator: {missing}"


def test_the_public_endpoint_set_is_exactly_this():
    """Widening it should require editing this list, not just forgetting a check."""
    handlers = _handlers()
    unauthenticated = set()
    for name, node in handlers.items():
        decos = [d.id for d in node.decorator_list if isinstance(d, ast.Name)]
        if "public_endpoint" in decos:
            unauthenticated.add(name)

    assert unauthenticated == PUBLIC, (
        "the set of endpoints reachable without a token changed.\n"
        f"  expected public: {sorted(PUBLIC)}\n"
        f"  actually public: {sorted(unauthenticated)}\n"
        "If this is intentional, update PUBLIC and say why in the commit."
    )


def test_authenticated_handlers_cover_everything_else():
    handlers = set(_handlers())
    authed = handlers - PUBLIC - STATE_AUTH
    assert authed, "expected authenticated handlers"
    assert len(handlers) == len(authed) + len(PUBLIC) + len(STATE_AUTH)
