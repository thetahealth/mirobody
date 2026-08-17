"""
Backing services for the management API (`server/routers/manage_router.py`).

The three modules here have exactly one caller each — that router — and they
came from `pulse/core`, where being "shared infrastructure" was never true of
them. `auth.py` was the sharp case: it is a FastAPI `Depends` dependency, and
`fastapi` ships in the `[server]` extra, not the base engine dependencies. So
`mirobody.pulse.core.auth` could not be imported on a plain `pip install
mirobody` at all — an engine module that the engine cannot load. Verified by
blocking `fastapi` at the import hook: `mirobody.pulse` imports fine, that
module raises. Here, importing FastAPI is simply legal.

`ManageDatabaseService` did NOT come along: `pulse/manager.py` uses it too, so
it is genuinely shared and stays in `pulse/core/database.py`. `service.py`
imports it from there — server depending on the engine, which is the allowed
direction (the import-linter contract forbids only the reverse).
"""
