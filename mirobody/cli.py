"""Console entry point — ``mirobody <command>``.

Installed via ``[project.scripts]`` so a plain ``pip install mirobody`` gets a
runnable command (deployments use ``python -m mirobody`` — see __main__.py).

Commands:

* ``mirobody parse <file>``             — the engine's party trick: lab report
  in, standardized LOINC table out. One LLM key, no database, no server.
  Requires the ``[parse]`` extra.
* ``mirobody resolve <terms...>``       — offline indicator-name resolution
  against the shipped bundles. Needs NOTHING: no key, no config, no network.
* ``mirobody dev [--pg-url URL]``       — the same server in ONE command, with
  no config file, no Redis requirement and generated dev secrets. `config.yaml`
  is not in the wheel, so this is the only shape in which
  ``pip install 'mirobody[app]'`` alone can start something.
* ``mirobody serve [config.yaml ...]``  — the full HTTP server (chat, MCP,
  API). Requires the ``[app]`` extra; checked up front with a plain message
  instead of a traceback from deep inside an import chain.
* ``mirobody worker [config.yaml ...]`` — the background task worker
  (IndicatorSync, ProfileRefresh queues).
"""

from __future__ import annotations

import argparse
import unicodedata
import asyncio
import importlib.util
import os
import sys


def _require_extra(command: str, extra: str, marker: str, what: str) -> None:
    """Fail fast, and legibly, when *command*'s optional extra isn't installed.

    Every command past ``resolve`` imports a stack the default install does not
    carry, and each of those import chains dies several modules deep with an
    opaque ``ModuleNotFoundError``. Checking one marker dependency up front and
    naming the pip command is the entire fix.

    ``pip install mirobody`` is ② Standardize — ``resolve``, units, lexical,
    numpy and nothing else. That is deliberate: it is the surface other
    software depends ON, and it used to drag 93 packages and 245 MB behind it.
    """
    if importlib.util.find_spec(marker) is None:
        sys.exit(
            f"mirobody {command} needs {what}, which is an optional extra:\n"
            "\n"
            f"    pip install 'mirobody[{extra}]'\n"
            "\n"
            "The default install is the vocabulary layer — `mirobody resolve`\n"
            "works with no extras, no key and no network."
        )


def _cmd_serve(args: argparse.Namespace) -> None:
    _require_extra("serve", "app", "langchain", "the server and agent layer")
    from mirobody.server import Server

    asyncio.run(Server.start(yaml_files=args.configs, fastapi_routers=[]))


#: What `mirobody dev` needs that a git checkout gets from `config.yaml` and a
#: deployment gets from `./deploy.sh`. Written as a YAML overlay in MEMORY, not
#: to a file: `config.yaml` is not in the wheel (checked — no `config.y*ml`
#: member), so `pip install 'mirobody[app]' && mirobody serve` has no
#: configuration at all, and that is the actual reason "one command" did not
#: work. Writing a config into someone's working directory is a side effect a
#: `dev` command has no business having.
_DEV_CONFIG = """
PRODUCTION: false

HTTP_SERVER_NAME: mirobody-dev
HTTP_HOST: {host}
HTTP_PORT: {port}

PG_HOST: {pg_host}
PG_PORT: {pg_port}
PG_USER: {pg_user}
PG_PASSWORD: {pg_password}
PG_DBNAME: {pg_dbname}
PG_SCHEMA: public
PG_MIN_CONNECTION: 1
PG_MAX_CONNECTION: 4
PG_TIMEOUT: 10

REDIS_HOST: {redis_host}
REDIS_PORT: {redis_port}
REDIS_DATABASE: 0

JWT_KEY: {jwt_key}
CONFIG_ENCRYPTION_KEY: {config_key}

# Package-relative, which `utils/plugin_dirs.resolve_plugin_dir` turns into the
# dotted name — so these resolve from site-packages, not just from a checkout.
MCP_TOOL_DIRS:
  - mirobody/agent/tools
AGENT_DIRS:
  - mirobody/agent
PROVIDER_DIRS:
  - mirobody/pulse/providers
PROMPTS:
  - agent/prompts/mirobody.jinja

AGENT_CHECKPOINTER: false
"""


def _pg_from_url(url: str) -> dict[str, str]:
    """`postgres://user:pw@host:port/db` -> the `PG_*` keys `Config` reads.

    Accepts the two spellings every Postgres tool prints (`postgres://` and
    `postgresql://`) because a user pastes whichever one their client gave them.
    """
    from urllib.parse import unquote, urlparse

    u = urlparse(url)
    if u.scheme not in ("postgres", "postgresql"):
        sys.exit(f"mirobody dev: --pg-url must be postgres:// or postgresql://, got {u.scheme or url!r}")
    if not u.hostname or not (u.path or "").strip("/"):
        sys.exit(f"mirobody dev: --pg-url needs a host and a database name: {url!r}")
    return {
        "pg_host": u.hostname,
        "pg_port": str(u.port or 5432),
        "pg_user": unquote(u.username or ""),
        "pg_password": unquote(u.password or ""),
        "pg_dbname": u.path.strip("/"),
    }


def _cmd_dev(args: argparse.Namespace) -> None:
    """One command, one process, no config file — the local-development path.

    `serve` is the deployment shape: it reads `config.{ENV}.yaml`, expects
    Redis, and expects the secrets to already exist. Every one of those is a
    prerequisite `examples/05_agent_server_preflight.py` reports as MISSING on
    a fresh machine, and four of them are things a developer should not have to
    produce by hand to see the thing run.

    What this does NOT change: Redis stays optional because it already was —
    `RedisConfig.get_async_client` returns None when it cannot ping, and
    `Server` logs "local memory mode" and carries on. `dev` just stops treating
    that as a failure worth blocking on.
    """
    _require_extra("dev", "app", "langchain", "the server and agent layer")

    import io
    import secrets

    pg_url = args.pg_url or os.environ.get("PG_URL") or os.environ.get("DATABASE_URL")
    if not pg_url:
        sys.exit(
            "mirobody dev needs a Postgres to write to:\n"
            "\n"
            "    mirobody dev --pg-url postgres://user:pw@localhost:5432/mirobody\n"
            "\n"
            "or set PG_URL / DATABASE_URL. The schema is created on first start.\n"
            "No database at hand? `docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=pw \\\n"
            "    -e POSTGRES_DB=mirobody pgvector/pgvector:pg17` — pgvector, not plain\n"
            "postgres: `schema/00_init_schema.sql` creates a vector column."
        )

    # Ephemeral by default, and said out loud. A dev secret that persists is a
    # dev secret that reaches production in someone's shell history.
    #
    # Put into the ENVIRONMENT, not only into the overlay, and that is not a
    # workaround: `Config.__init__` builds its `FernetEncrypter` from
    # `get_fernet_key("CONFIG_ENCRYPTION_KEY")` BEFORE it loads any YAML, so a
    # value supplied in config can never satisfy it — the run just logs
    # "CONFIG_ENCRYPTION_KEY is not set" at ERROR and encrypts with a
    # publicly-known key. `LOG_ENCRYPTION_KEY` reads the same way.
    generated = []
    for name, nbytes in (("JWT_KEY", 32), ("CONFIG_ENCRYPTION_KEY", 16), ("LOG_ENCRYPTION_KEY", 16)):
        if not os.environ.get(name):
            os.environ[name] = secrets.token_hex(nbytes)
            generated.append(name)
    jwt_key = os.environ["JWT_KEY"]
    config_key = os.environ["CONFIG_ENCRYPTION_KEY"]

    overlay = _DEV_CONFIG.format(
        host=args.host, port=args.port,
        redis_host=args.redis_host, redis_port=args.redis_port,
        jwt_key=jwt_key, config_key=config_key,
        **_pg_from_url(pg_url),
    )

    print(f"mirobody dev — http://{args.host}:{args.port}")
    if generated:
        print(f"  generated for this run only: {', '.join(generated)}")
        print("  sessions and encrypted config values do NOT survive a restart.")
        print("  set them in the environment to keep them.")
    print(f"  postgres: {_pg_from_url(pg_url)['pg_host']}:{_pg_from_url(pg_url)['pg_port']}"
          f"/{_pg_from_url(pg_url)['pg_dbname']}")
    print(f"  redis:    {args.redis_host}:{args.redis_port} (optional — falls back to in-process memory)")
    print()

    from mirobody.server import Server

    # The overlay goes LAST so it wins over any file the user also passed.
    asyncio.run(Server.start(
        yaml_files=[*args.configs, io.StringIO(overlay)],
        fastapi_routers=[],
    ))


def _cmd_worker(args: argparse.Namespace) -> None:
    _require_extra("worker", "app", "langchain", "the server and agent layer")
    from mirobody.server import Worker

    asyncio.run(Worker.start(yaml_files=args.configs))


def _width(text: str) -> int:
    """Terminal COLUMNS, not characters.

    `f"{term:<{n}}"` pads by `len()`, and every CJK character occupies two
    columns in every terminal. So `血红蛋白` was billed as 4 and drawn as 8, and
    the LOINC column drifted four places right on exactly the rows that make the
    point — this command's whole pitch is that four languages land on one code,
    and it showed that as a table which did not line up.
    """
    return sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in text)


def _pad(text: str, width: int) -> str:
    return text + " " * max(0, width - _width(text))


def _cmd_resolve(args: argparse.Namespace) -> None:
    from mirobody.engine import get_resolver

    resolver = get_resolver()   # first call pays the bundle load (~seconds)
    width = max(_width(t) for t in args.terms)
    for term in args.terms:
        r = resolver.resolve(term)
        if r.resolved:
            loinc = f"LOINC {r.loinc}" if r.loinc else "(no LOINC axis row)"
            print(f"  {_pad(term, width)}  {loinc:<16}  {r.canonical}"
                  + (f"   [{r.candidates} candidates]" if r.candidates > 1 else ""))
        else:
            print(f"  {_pad(term, width)}  unresolved — not in the lexical index "
                  "(the full semantic pipeline may still resolve it)")


def _cmd_parse(args: argparse.Namespace) -> None:
    """Read a document into standardized readings. Needs one vision-capable key.

    The no-key case gets the same treatment as the missing extra in
    `_require_extra`, and for the same reason. It used to surface as a
    twenty-line traceback ending in a `ValueError` from four frames inside
    `unified_file_extract` — the message was correct and nobody would read it
    there. `parse` is the second command the README hands a new user, right
    after `resolve`, which needs no key at all; being told which environment
    variable to set is the entire content of the failure.
    """
    _require_extra("parse", "parse", "pypdfium2", "the document extraction stack")

    from mirobody.engine import parse_file

    if not os.path.isfile(args.file):
        sys.exit(f"mirobody parse: no such file: {args.file}")

    try:
        readings = asyncio.run(parse_file(args.file, resolve_names=not args.no_resolve))
    except ValueError as e:
        if "vision provider" not in str(e).lower():
            raise
        sys.exit(
            "mirobody parse reads the document with a vision-capable model, so it "
            "needs one API key:\n"
            "\n"
            "    export OPENROUTER_API_KEY=...     # or GOOGLE_API_KEY,\n"
            "                                      # DASHSCOPE_API_KEY, VOLCENGINE_API_KEY\n"
            "\n"
            "`mirobody resolve` needs no key and no network — try that first if you "
            "only want to see indicator resolution."
        )
    if not readings:
        print("No indicator measurements found in the document.")
        return
    for rd in readings:
        res = rd.resolution
        if res and res.resolved:
            tail = f"LOINC {res.loinc:<10} {res.canonical}" if res.loinc else res.canonical
        elif res:
            tail = "unresolved"
        else:
            tail = ""
        val = f"{rd.value} {rd.unit}".strip()
        print(f"  {rd.name:<24.24s}  {val:<16.16s}  {tail}")
    n_res = sum(1 for r in readings if r.resolution and r.resolution.resolved)
    print(f"\n{len(readings)} readings · {n_res} resolved to standard codes · offline lexical index")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="mirobody",
        description="Mirobody — the AI-native health data engine.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_serve = sub.add_parser("serve", help="run the HTTP server (requires the [app] extra)")
    p_serve.add_argument("configs", nargs="*", help="extra config YAML files, layered over config.yaml")
    p_serve.set_defaults(func=_cmd_serve)

    p_dev = sub.add_parser(
        "dev",
        help="run the server in one process with no config file (requires the [app] extra)",
    )
    p_dev.add_argument("configs", nargs="*", help="extra config YAML files, layered UNDER the dev defaults")
    p_dev.add_argument("--pg-url", default="", help="postgres://user:pw@host:port/db (or PG_URL / DATABASE_URL)")
    p_dev.add_argument("--host", default="127.0.0.1", help="bind address (default: loopback only)")
    p_dev.add_argument("--port", type=int, default=18090, help="HTTP port (default: 18090)")
    p_dev.add_argument("--redis-host", default="127.0.0.1", help="Redis host; unreachable is fine")
    p_dev.add_argument("--redis-port", type=int, default=6379, help="Redis port; unreachable is fine")
    p_dev.set_defaults(func=_cmd_dev)

    p_worker = sub.add_parser("worker", help="run the background task worker")
    p_worker.add_argument("configs", nargs="*", help="extra config YAML files, layered over config.yaml")
    p_worker.set_defaults(func=_cmd_worker)

    p_parse = sub.add_parser("parse", help="parse a health document into standardized indicators (requires the [parse] extra and one LLM key)")
    p_parse.add_argument("file", help="path to a lab report (pdf/png/jpg/txt/csv)")
    p_parse.add_argument("--no-resolve", action="store_true", help="skip offline code resolution")
    p_parse.set_defaults(func=_cmd_parse)

    p_resolve = sub.add_parser("resolve", help="resolve indicator names to standard codes — fully offline, no key needed")
    p_resolve.add_argument("terms", nargs="+", help="indicator names in any supported language")
    p_resolve.set_defaults(func=_cmd_resolve)

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
