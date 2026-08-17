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
>    `/auth`, but `/auth/wechat/callback` is a *client-side* route (the WeChat
>    gateway 302s to it with tokens in the URL fragment). Only the
>    backend-owned `/auth/session` and `/auth/webauthn` subtrees get guards.
>
> Bug #2 below (double-`share` path) is also fixed — `/api/share/deactivate`
> is the route, the old double path stays as an undocumented alias. Gap #1 is
> closed by decision (WeChat web login is out of scope here, see below); #4
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

1. **RESOLVED by decision (2026-08-17): WeChat web login is out of scope for
   this deployment.** The opensource frontend supports email + Firebase
   (Google/Apple) login only. The client's QR panel implements the *gateway*
   flow (`/wechat/start`, `/wechat/bridge/poll`), which lives in the
   proprietary a007-mirovital deployment and is specced in the
   `mirobody-web-rebuild` docs — this backend never had it and will not grow
   it. What made this a bug was `Server.__init__` auto-injecting
   `__IS_WECHAT_LOGIN_ON__: true` into `mirobody.json` whenever the
   `wechat_open_*` credentials (which serve `POST /wechat/verify`, an
   unrelated Open Platform flow) were configured — the panel rendered and
   then broke. That injection is removed; the flag is now only ever what a
   deployment sets explicitly in `MIROBODY_WEB_CONFIG`, so the panel stays
   hidden here and gateway-running deployments opt in. NOTE for a007: if its
   flag relied on the auto-injection, it must add `__IS_WECHAT_LOGIN_ON__:
   true` (and `__WECHAT_APP_ID__` if ever used) to its `MIROBODY_WEB_CONFIG`
   when it next syncs this package. `POST /wechat/verify` and the
   `mirobody/user` WeChat account code stay: a007's gateway consumes
   `find_or_create_wechat_user`/`WeChatOpenValidator` from this package
   (documented in mirobody-web-rebuild/CLAUDE.md) — deleting them here would
   break that deployment, so removing them needs an explicit go-ahead plus a
   check of a007's imports, not a frontend decision.
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
