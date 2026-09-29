"""Check whether this machine can run the full agent server — before you start it.

    pip install 'mirobody[app]'
    python examples/05_agent_server_preflight.py

Examples 01–04 need nothing but the package. This one covers ③ Answer, which is
a different proposition: the chat server, the MCP endpoint over HTTP, and the
agents need PostgreSQL, a model key and a JWT secret.

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
# The default matches the host port in compose.yaml. Set PG_HOST/PG_PORT when
# checking a database installed directly on the host or on a different port.
pg_host = os.environ.get("PG_HOST", "localhost")
pg_port = int(os.environ.get("PG_PORT", "18062"))

rows.append((f"postgres {pg_host}:{pg_port}", _port_open(pg_host, pg_port),
             "docker compose up -d pg   (schema is created on first start)"))

# ── 3. secrets ───────────────────────────────────────────────────────────────
# The five one-key providers (config.yaml's table); any one runs every surface.
model_keys = ("OPENROUTER_API_KEY", "DASHSCOPE_API_KEY", "GOOGLE_API_KEY",
              "OPENAI_API_KEY", "DEEPSEEK_API_KEY")
present = [k for k in model_keys if os.environ.get(k)]
rows.append(("a model API key", bool(present),
             "set one of: " + ", ".join(model_keys[:3]) + ", …"))
rows.append(("JWT_KEY", bool(os.environ.get("JWT_KEY")),
             "openssl rand -hex 32   — or let `mirobody dev` generate one per run"))
rows.append(("CONFIG_ENCRYPTION_KEY", bool(os.environ.get("CONFIG_ENCRYPTION_KEY")),
             "openssl rand -hex 32   — or let `mirobody dev` generate one per run"))

# ── 4. docker, for the one-command path ──────────────────────────────────────
rows.append(("docker (optional)", shutil.which("docker") is not None,
             "only needed for ./deploy.sh; a local Postgres works too"))

width = max(len(n) for n, _, _ in rows)
for name, ok, hint in rows:
    print(f"[{_mark(ok)}] {name:<{width}}   {'' if ok else hint}")

blocking = [n for n, ok, _ in rows if not ok and not n.endswith("(optional)")]
print("=" * 74)
if blocking:
    print(f"{len(blocking)} prerequisite(s) missing: {', '.join(blocking)}")
    # The development command generates both secrets for that run.
    dev_handles = {"JWT_KEY", "CONFIG_ENCRYPTION_KEY"}
    left = [n for n in blocking if n not in dev_handles]
    print("\nTwo paths past them:")
    print("    mirobody dev --pg-url postgres://user:pw@localhost:5432/mirobody")
    print("      one process, in-memory config, secrets generated per run.")
    print(f"      still needs: {', '.join(left) if left else 'nothing else'}")
    print("    ./deploy.sh")
    print("      the Docker path: writes .env, starts Postgres, server and worker.")
else:
    print("Everything needed is present. Start the server with:\n")
    print("    mirobody serve\n")
    print("Then:")
    print("    http://localhost:18060          the web client")
    print("    http://localhost:18060/mcp      the MCP endpoint for Claude Desktop / Cursor")
    print("    http://localhost:18060/docs     the REST API")

if present:
    print(f"\n(model keys detected: {', '.join(present)})")
