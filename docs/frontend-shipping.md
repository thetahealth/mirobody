# Shipping & Serving the Frontend

How the built web client should be distributed with this package and served at
runtime. Research done 2026-08-17 against comparable open-source Python servers
that ship a SPA; includes the concrete gaps/bugs the current setup has. The
frontend source lives in a separate repo (`mirobody-web-rebuild`); this repo
only ever receives its **build output** (`npm run build:opensource` → `dist/`).

> **Implemented 2026-08-17** — the serving half of this document is done:
> `htdoc.py` now uses `app.frontend()` (mounted last in `Server.start()`),
> the whitelist and the per-file in-memory routes are gone, cache headers and
> API-prefix guards are in, and `frontend/` was refreshed from a fresh
> `build:opensource` (66 → 93 asset files; the previous build predated
> AuthShell and the design-token sweep). `mirobody/server/test_htdoc.py` pins
> the behaviour. Two deviations from the recommendation below, both deliberate:
>
> 1. **The wheel still ships no frontend.** The survey's common thread
>    (build-hook it into the wheel) contradicts a decision this repo already
>    made and recorded in `pyproject.toml` — excluding the client took the
>    wheel from ~57 MB (8 MB of it JS) to engine + data only. Reversing that
>    is a product call, not a serving fix; the section below stays as the
>    playbook if it is ever taken.
> 2. **`/auth` is not blanket-guarded.** The guard list below includes
>    `/auth`, but the client router owns callback routes under `/auth` (an
>    identity gateway 302s to one with tokens in the URL fragment). Only the
>    backend-owned `/auth/session` and `/auth/webauthn` subtrees get guards.
>
> Bug #2 below (double-`share` path) is also fixed — `/api/share/deactivate`
> is the route, the old double path stays as an undocumented alias. Gap #1 is
> closed by removing the WeChat code outright (see below); #4
> (`uri_prefix` half-applied) remains open.

## Current state (and why it needs to change)

- `frontend/` is committed at the repo root and explicitly excluded from the
  wheel (`MANIFEST.in`: `recursive-exclude frontend *`) — `pip install
  mirobody` ships **no UI**.
- `mirobody/server/htdoc.py` walks the directory at startup, reads **every file
  into memory**, and registers one literal `Route` per file. No ETag/Range
  support, restart required to pick up new files.
- The SPA fallback is a hard-coded whitelist (`/login /mcplogin /chat /drive
  /home /share/{share_id} /`). Any other client-side route 404s on direct
  navigation or refresh — see “Route inventory” below for what it already
  misses today.
- htdoc routes are folded into `FastAPI(routes=...)` at construction, **before**
  the `app.include_router(...)` calls in `server.py`. Starlette matches in
  registration order, so naively replacing the whitelist with a catch-all
  `Route("/{path:path}")` would shadow every API router registered after it.
  Any fix must be order-independent or registered last.

## How comparable projects do it

| Project | Built frontend in git? | In the wheel? | Serving | SPA fallback |
|---|---|---|---|---|
| open-webui | no (source only; `build/` gitignored) | yes — Hatch build hook runs `npm run build`, `force-include` into the package | `SPAStaticFiles` (StaticFiles subclass, `html=True`) mounted at `/` after routers | catch 404 → re-serve `index.html`, except `.js` paths stay 404 |
| Chainlit | no (dist gitignored) | yes — Hatch build hook + wheel `artifacts` | hand-written catch-all `GET /{full_path:path}` rendering a templated index | catch-all registered last; specific routes win by order |
| Gradio | no | yes — Hatch `artifacts`, `build_frontend.sh` | dedicated `/static`, `/assets`, `/svelte` routes + templated index | n/a (no arbitrary deep links) |
| Langflow | no (`langflow/frontend/` gitignored, copied in at release) | yes | **FastAPI ≥ 0.138 native `app.frontend("/", directory=…, fallback="index.html")`** | `app.frontend()` matches only after all path operations — order-independent by construction; a reserved `/api/{_path:path}` route forces real JSON 404s |
| Streamlit | no (`lib/streamlit/static` gitignored, built by Makefile) | yes — package_data | `StaticFiles(html=True)` subclass | 404 → `index.html` except reserved suffixes; blocks `//`/path traversal; `no-cache` for HTML, `public, immutable` for hashed assets |

Common thread: **build output is never committed to git; a build hook (or the
release pipeline) produces it and packages it inside the wheel; serving is
StaticFiles (or `app.frontend()`) plus a real catch-all fallback.**

Sources: open-webui `pyproject.toml`/`hatch_build.py`/`main.py`; Chainlit
`backend/pyproject.toml`/`server.py`; Gradio `pyproject.toml`/`routes.py`;
Langflow `main.py`/`.gitignore`; Streamlit
`web/server/starlette/starlette_static_routes.py` (all on GitHub `main`).

## Recommendation

### Distribution

Move the built assets inside the package — `mirobody/frontend/` — so the wheel
is self-contained:

```toml
# pyproject.toml
[tool.setuptools.package-data]
"mirobody" = ["frontend/**/*", ...existing entries...]
```

```text
# MANIFEST.in — replace `recursive-exclude frontend *` with:
recursive-include mirobody/frontend *
```

Release pipeline step (before `python -m build`): build the frontend repo with
`npm ci && npm run build:opensource`, then rsync `dist/` →
`mirobody/mirobody/frontend/`. Whether to also keep committing the output to
git is a trade-off: committing gives byte-level reviewability of what ships;
not committing (what all five surveyed projects chose) keeps the repo clean.

### Serving

Preferred (FastAPI ≥ 0.138, already unpinned here):

```python
def add_htdoc_routes(app: FastAPI, frontend_dir: str) -> None:
    if not frontend_dir or not Path(frontend_dir).is_dir():
        return  # pip-installed-without-frontend stays supported

    # Real 404s (not the SPA shell) for unmatched API prefixes.
    for prefix in ("/api", "/mcp", "/auth", "/oauth", "/files", "/invitation"):
        @app.api_route(f"{prefix}/{{_path:path}}", methods=["GET", "HEAD"],
                       include_in_schema=False)
        async def _not_found(_path: str):
            raise HTTPException(status_code=404)

    # Keep the `__/auth/handler` POST-body-templating special case as an
    # explicit route (path operations always beat the frontend fallback).

    app.frontend("/", directory=frontend_dir, fallback="index.html")
```

`app.frontend()` matches only after all path operations regardless of
registration order — this is what removes both the whitelist and the
construction-order hazard. If staying on older FastAPI, use the Streamlit
pattern instead: a `StaticFiles(html=True)` subclass whose `get_response()`
catches 404 and re-serves `index.html` unless the path has an asset extension
or a reserved API prefix — and mount it **after** every `include_router` call.

Either way, set cache headers explicitly: `Cache-Control: no-cache` for
`*.html` / `mirobody.json`, `public, max-age=31536000, immutable` for hashed
`assets/*`.

### Route inventory the fallback must cover

Current frontend client-side routes (from `src/router/index.jsx`, 2026-08-17):
`/`, `/welcome`, `/login`, `/mcplogin`, `/auth/wechat/callback`,
`/share/:shareSessionId`, `/chat`, `/chat/:sessionId`, `/drive`, `/home`,
`/developer`, `/indicator`, `/indicator/new`. The current whitelist misses
`/welcome`, `/auth/wechat/callback`, `/chat/:sessionId`, `/developer`,
`/indicator*` — with a real catch-all this list stops needing maintenance.

## Known gaps & bugs (recorded 2026-08-17)

1. **REMOVED (2026-08-17): WeChat login is gone from this package.** The
   opensource frontend supports email + Firebase (Google/Apple) login only.
   Deleted here: `mirobody/user/wechat.py` (`WeChatOpenValidator`),
   `mirobody/user/auth_wechat.py` (the `auth_wechat` identity helpers),
   `POST /wechat/verify` and its handler, the `wechat_open_*` credentials and
   `Config.get_wechat_open_options()`, the `health_app_user.wechat_openid`
   column and index, and the `auth_wechat` repoint branch in `merge_accounts`.
   The client's QR panel implements a *gateway* flow (`/wechat/start`,
   `/wechat/bridge/poll`) that this backend never had and will not grow.

   **Correcting what this entry used to say.** It claimed a007's gateway
   consumed `find_or_create_wechat_user` / `WeChatOpenValidator` *from this
   package*, so deleting them "would break that deployment". That was a
   misreport. `a007-holywell/backend_py/mcp_server/wechat_gateway.py` does
   import those names, but they resolve against a007's OWN vendored copy:
   `a007-holywell/backend_py/mirobody/` is 580 files tracked in a007's own
   git, including its own `user/wechat.py` and `user/auth_wechat.py`, and a007
   does not declare `mirobody` as an external dependency. There is no
   consumption of this package's WeChat code by that deployment, and nothing
   in any repo on disk even mounts that gateway (`make_routes` has no caller).

   Anything still needing this code has it in git history, and a007 has its
   own copy.

2. **`POST /api/share/share/deactivate` double-`share` path**
   (`session_share_router.py`: router prefix `/api/share` + route
   `"/share/deactivate"`). The cdm chat-client calls `/api/share/deactivate`
   and gets a 404. Fix the route to `"/deactivate"` (optionally keep the old
   path as an alias during transition).
3. **SPA whitelist 404s** — covered above; fixed structurally by the catch-all.
4. **`uri_prefix` is only applied to the manually-built route group**
   (`OAuthService`/`UserService`/`McpService`/`ChatService`…), not to the
   `APIRouter`s attached via `include_router` (`apple`, `file`, `user`,
   `share`, `pulse`, `manage`, `invitation`). A deployment setting
   `HTTP_URI_PREFIX` gets a half-prefixed API.
