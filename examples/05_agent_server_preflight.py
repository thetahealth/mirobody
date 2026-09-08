"""Check whether this machine can run the full agent server — before you start it.

    pip install 'mirobody[app]'
    python examples/05_agent_server_preflight.py

Examples 01–04 need nothing but the package. This one covers ③ Answers, which is
a different proposition: the chat server, the MCP endpoint over HTTP, and the
agents need PostgreSQL, Redis, a model key and a JWT secret.

Rather than have you discover that one failure at a time from a traceback, this
reports every prerequisite at once and says exactly what to do about each. It
changes nothing and connects to nothing you have not configured.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import socket

OK, MISSING = "  ok  ", "MISSING"


def _mark(ok: bool) -> str:
    return OK if ok else MISSING


def _port_open(host: str, port: int, timeout: float = 0.6) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


print(__doc__.strip().splitlines()[0])
print("=" * 74)

rows: list[tuple[str, bool, str]] = []

# ── 1. the [app] extra ────────────────────────────────────────────────────
for mod, why in (("langchain", "the agent loop"),
                 ("deepagents", "middleware, skills, virtual filesystem"),
                 ("fastapi", "the HTTP surface")):
    rows.append((f"python: {mod}", importlib.util.find_spec(mod) is not None,
                 f"{why} — pip install 'mirobody[app]'"))

# ── 2. services ──────────────────────────────────────────────────────────────
# Defaults match what this repo's compose.yaml maps onto the host
# (pg → 127.0.0.1:18062, redis → 127.0.0.1:18069) — the stack the fix-it
# column tells you to start. A bare-metal Postgres/Redis is checked with
# PG_PORT=5432 / REDIS_PORT=6379 in the environment.
pg_host = os.environ.get("PG_HOST", "localhost")
pg_port = int(os.environ.get("PG_PORT", "18062"))
rd_host = os.environ.get("REDIS_HOST", "localhost")
rd_port = int(os.environ.get("REDIS_PORT", "18069"))

rows.append((f"postgres {pg_host}:{pg_port}", _port_open(pg_host, pg_port),
             "docker compose up -d pg   (schema is created on first start)"))
rows.append((f"redis {rd_host}:{rd_port}", _port_open(rd_host, rd_port),
             ("docker compose up -d redis   — or skip it: `mirobody dev` runs "
              "without Redis (in-process memory)")))

# ── 3. secrets ───────────────────────────────────────────────────────────────
model_keys = ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GOOGLE_API_KEY",
              "OPENROUTER_API_KEY", "DASHSCOPE_API_KEY")
present = [k for k in model_keys if os.environ.get(k)]
rows.append(("a model API key", bool(present),
             "set one of: " + ", ".join(model_keys[:3]) + ", …"))
rows.append(("JWT_KEY", bool(os.environ.get("JWT_KEY")),
             "openssl rand -hex 32   — or let `mirobody dev` generate one per run"))
rows.append(("CONFIG_ENCRYPTION_KEY", bool(os.environ.get("CONFIG_ENCRYPTION_KEY")),
             "openssl rand -hex 32   — or let `mirobody dev` generate one per run"))

# ── 4. config file ───────────────────────────────────────────────────────────
env_name = (os.environ.get("ENV") or "").strip()
cfg = f"config.{env_name}.yaml" if env_name else None
rows.append(("ENV + config.{ENV}.yaml", bool(cfg and os.path.isfile(cfg)),
             ("echo 'ENV=localdb' > .env, then create config.localdb.yaml — or skip "
              "the file entirely: `mirobody dev` configures itself in memory")))

# ── 5. docker, for the one-command path ──────────────────────────────────────
rows.append(("docker (optional)", shutil.which("docker") is not None,
             "only needed for ./deploy.sh; a local Postgres/Redis works too"))

width = max(len(n) for n, _, _ in rows)
for name, ok, hint in rows:
    print(f"[{_mark(ok)}] {name:<{width}}   {'' if ok else hint}")

blocking = [n for n, ok, _ in rows if not ok and not n.endswith("(optional)")]
print("=" * 74)
if blocking:
    print(f"{len(blocking)} prerequisite(s) missing: {', '.join(blocking)}")
    # Four of the seven are things `mirobody dev` produces for you: Redis
    # (optional there), both secrets (generated per run) and the config file
    # (built in memory — `config.yaml` is not in the wheel, so on a
    # `pip install` there is no file to find).
    dev_handles = {"JWT_KEY", "CONFIG_ENCRYPTION_KEY", "ENV + config.{ENV}.yaml"}
    dev_handles |= {n for n in blocking if n.startswith("redis ")}
    left = [n for n in blocking if n not in dev_handles]
    print("\nTwo paths past them:")
    print("    mirobody dev --pg-url postgres://user:pw@localhost:5432/mirobody")
    print("      one process, no config file, no Redis needed, secrets generated per run.")
    print(f"      still needs: {', '.join(left) if left else 'nothing else'}")
    print("    ./deploy.sh")
    print("      the Docker path: writes .env and config.localdb.yaml, starts")
    print("      Postgres, Redis and Mirobody together.")
else:
    print("Everything needed is present. Start the server with:\n")
    print("    mirobody serve\n")
    print("Then:")
    print("    http://localhost:18060          the web client")
    print("    http://localhost:18060/mcp      the MCP endpoint for Claude Desktop / Cursor")
    print("    http://localhost:18060/docs     the REST API")

if present:
    print(f"\n(model keys detected: {', '.join(present)})")
