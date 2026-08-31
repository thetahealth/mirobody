# The bundled web client

How the web client is distributed and served, and what you need to know to
replace it with your own.

## The API is the contract, the client is one consumer of it

`frontend/` holds **build output, not source** — this repository never receives
the client's source, and does not need to. Everything the bundled client does
goes through the same public HTTP and MCP endpoints your own code can call,
documented at [docs.mirobody.ai](https://docs.mirobody.ai/).

That is the point rather than a limitation: if you want a different UI, build it
against the same surface. Nothing the shipped client can reach is private to it.

## `pip install mirobody` ships no UI

The client is excluded from the wheel (`MANIFEST.in`:
`recursive-exclude frontend *`). Including it took the wheel to roughly 57 MB,
8 MB of that JavaScript, for something a library consumer never loads — and
`pip install mirobody` is a library, deliberately: two packages, 52 MB, the
vocabulary layer on numpy.

The UI comes with the Docker application (`git clone && ./deploy.sh`), which
serves `frontend/` from the checkout. A server started without that directory
works normally and simply has no UI mounted.

## Serving

`mirobody/server/htdoc.py` mounts the directory with
`app.frontend("/", directory=dir, fallback="index.html")` (FastAPI ≥ 0.138),
and three properties follow from that:

**Files are read from disk per request.** Sync a new build into `frontend/` and
it is live immediately — no restart. (The predecessor read the whole tree into
memory at startup, so a synced build did nothing until the process bounced.)

**The SPA fallback is a real catch-all, for navigation requests only.** Any
non-API path a browser navigates to gets `index.html`, so a client-side route
survives a refresh or a direct link without anyone maintaining a whitelist. A
non-navigation request (an API client probing an unknown path) still gets a 404
rather than a page of HTML. A custom frontend inherits this for free.

**Backend-owned prefixes answer 404, not the shell.** A GET under `/api`,
`/mcp`, `/oauth`, `/oauth2`, `/invitation`, `/apple`, `/google`, `/email`,
`/personal`, `/auth/session`, `/auth/webauthn` or `/.well-known` that matched
nothing is a mistake, and returning the SPA shell would turn it into a
confusing 200. Note that `/auth` is **not** guarded as a whole: the client owns
callback routes under it, because an identity gateway 302s to one with tokens
in the URL fragment. Only the two backend-owned subtrees are.

`app.frontend()` matches only after every path operation, regardless of
registration order, so a route you add later always wins over the fallback.

### Cache headers

Set by middleware, since `app.frontend()` exposes no header hook. A response
that already carries `Cache-Control` (streaming endpoints do) is left alone.

| Path | Header |
| --- | --- |
| `/assets/*` (hashed filenames), status 200 | `public, max-age=31536000, immutable` |
| `text/html` responses, and `/mirobody.json` | `no-cache` |

## Replacing the client

Point `frontend/` at your own build output. The requirements are only:

* an `index.html` at its root, for the fallback to serve;
* hashed asset filenames under `assets/` if you want the immutable caching;
* client-side routes that avoid the backend-owned prefixes above.

Rebuilding the bundled client from its own repository:

```bash
npm run build:opensource
rm -rf ../mirobody/frontend/assets && rsync -a --delete dist/ ../mirobody/frontend/
```

Clearing `assets/` first is deliberate — `rsync --delete` alone will not remove
chunks whose hashed names no longer appear in the new build.
