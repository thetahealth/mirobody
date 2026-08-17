"""Behavioural tests for the web-client serving in htdoc.py.

Each test pins one of the failure modes that motivated the rewrite from
literal per-file routes + SPA whitelist to `app.frontend()`:

- deep links the old whitelist missed (`/welcome`, `/chat/:sessionId`,
  `/indicator/new`, client-side `/auth/*` callbacks) 404'd on refresh;
- a naive catch-all would have shadowed API routes registered after it;
- `__/auth/handler` and `__/auth/iframe` are extensionless and must render
  as HTML, and the handler must keep its `{{POST_BODY}}` templating.

The app is assembled the same way `Server.start()` does it: plain starlette
Routes at construction, routers included afterwards, `add_htdoc_routes` last.
"""

import pytest


from fastapi import APIRouter, FastAPI
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from mirobody.server.htdoc import add_htdoc_routes

HTML = {"Accept": "text/html,application/xhtml+xml"}
JSON = {"Accept": "application/json"}


@pytest.fixture
def frontend_dir(tmp_path):
    frontend = tmp_path / "frontend"
    (frontend / "assets").mkdir(parents=True)
    (frontend / "index.html").write_text("<html>shell</html>")
    (frontend / "assets" / "app-C4fe9hM.js").write_text("console.log(1)")

    auth = frontend / "__" / "auth"
    auth.mkdir(parents=True)
    (auth / "handler").write_text("<html>handler {{POST_BODY}}</html>")
    (auth / "iframe").write_text("<html>iframe</html>")
    (auth / "handler.js").write_text("// helper js")
    return frontend


@pytest.fixture
def client(frontend_dir):
    routes = [
        Route(
            "/mirobody.json",
            endpoint=lambda request: JSONResponse({"__IS_GOOGLE_LOGIN_ON__": True}),
            methods=["GET", "HEAD"],
        )
    ]
    app = FastAPI(routes=routes)

    router = APIRouter(prefix="/api/share")

    @router.get("/list")
    async def share_list():
        return {"items": []}

    app.include_router(router)
    add_htdoc_routes(app, str(frontend_dir))
    return TestClient(app)


def test_api_route_beats_frontend(client):
    r = client.get("/api/share/list", headers=HTML)
    assert r.status_code == 200
    assert r.json() == {"items": []}


def test_construction_time_route_beats_frontend(client):
    r = client.get("/mirobody.json")
    assert r.status_code == 200
    assert r.json() == {"__IS_GOOGLE_LOGIN_ON__": True}
    # A deployment flipping a login flag must propagate on next load.
    assert r.headers["cache-control"] == "no-cache"


def test_root_serves_shell_no_cache(client):
    r = client.get("/", headers=HTML)
    assert r.status_code == 200
    assert "shell" in r.text
    assert r.headers["cache-control"] == "no-cache"


@pytest.mark.parametrize(
    "path",
    [
        # Everything the old whitelist covered...
        "/login", "/mcplogin", "/chat", "/drive", "/home", "/share/abc123",
        # ...and everything it missed (docs/frontend-shipping.md route inventory).
        "/welcome", "/chat/f00-session-id", "/developer",
        "/indicator", "/indicator/new", "/auth/oauth-callback",
    ],
)
def test_deep_links_serve_shell(client, path):
    r = client.get(path, headers=HTML)
    assert r.status_code == 200
    assert "shell" in r.text


def test_non_navigation_request_gets_real_404(client):
    # An API client (no text/html in Accept) must never receive the shell.
    assert client.get("/chat/f00", headers=JSON).status_code == 404
    # <script src> requests send Accept: */* — a missing asset stays a 404.
    assert client.get("/assets/gone.js", headers={"Accept": "*/*"}).status_code == 404


def test_unmatched_api_prefix_is_404_even_for_browsers(client):
    for path in ("/api/nonexistent", "/mcp/oops", "/auth/session/oops"):
        r = client.get(path, headers=HTML)
        assert r.status_code == 404, path
        assert "shell" not in r.text


def test_hashed_assets_are_immutable(client):
    r = client.get("/assets/app-C4fe9hM.js")
    assert r.status_code == 200
    assert r.headers["cache-control"] == "public, max-age=31536000, immutable"
    assert "etag" in r.headers  # StaticFiles gives revalidation for free


def test_auth_handler_templates_post_body(client):
    r = client.post("/__/auth/handler", content=b"firebase-payload")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/html")
    assert "handler firebase-payload" in r.text


def test_auth_helpers_render_as_html(client):
    # Extensionless files StaticFiles would have served as text/plain.
    for path in ("/__/auth/handler", "/__/auth/iframe"):
        r = client.get(path)
        assert r.headers["content-type"].startswith("text/html"), path
    assert client.get("/__/auth/handler.js").headers["content-type"].startswith(
        "application/javascript"
    )


def test_missing_frontend_dir_is_a_noop(tmp_path):
    # pip install ships no frontend/ — the server runs API + MCP only.
    app = FastAPI()
    add_htdoc_routes(app, "")
    add_htdoc_routes(app, str(tmp_path / "nonexistent"))
    client = TestClient(app)
    assert client.get("/", headers=HTML).status_code == 404
