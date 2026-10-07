"""Console entry point: ``mirobody <command>``.

Installed via ``[project.scripts]`` so a plain ``pip install mirobody`` gets a
runnable command (deployments use ``python -m mirobody``: see __main__.py).

Commands:

* ``mirobody parse <file>``             the engine's party trick: lab report
  in, standardized LOINC table out. One LLM key, no database, no server.
  Requires the ``[parse]`` extra.
* ``mirobody resolve <terms...>``       offline indicator-name resolution
  against the shipped bundles. Needs NOTHING: no key, no config, no network.
  ``--symptom`` / ``--condition`` code a complaint or a diagnosis on ICPC-3
  instead of a lab analyte on LOINC.
* ``mirobody device-bundle [--out PATH]``  the device vocabulary (catalogue,
  labels, crosswalks) as one digested JSON file, for a client that codes
  health-store batches without Python. Needs nothing, like ``resolve``.
* ``mirobody dev [--pg-url URL]``       the same server in ONE command, with
  no config file and generated dev secrets. `config.yaml`
  is not in the wheel, so this is the only shape in which
  ``pip install 'mirobody[app]'`` alone can start something.
* ``mirobody serve [config.yaml ...]``  the full HTTP server (chat, MCP,
  API). Requires the ``[app]`` extra; checked up front with a plain message
  instead of a traceback from deep inside an import chain.
* ``mirobody worker [config.yaml ...]``: the background task worker
  (IndicatorSync, ProfileRefresh queues).
* ``mirobody doctor [config.yaml ...]``, which LLM provider each surface
  (chat, vision, structured extraction, text, embeddings) would select with
  the current configuration, and what to set where one has none. Needs no
  database, but it reads the configuration layer, so it needs ``[app]`` or
  ``[parse]``.
* ``mirobody fetch cpic --version vX.Y.Z`` downloads and validates a CPIC data
  extract without executing the upstream SQL or requiring a database.
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

    ``pip install mirobody`` is ② Translate: ``resolve``, units, lexical,
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
#: to a file: `config.yaml` is not in the wheel (checked, no `config.y*ml`
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

JWT_KEY: {jwt_key}
CONFIG_ENCRYPTION_KEY: {config_key}

# Package-relative, which `utils/plugin_dirs.resolve_plugin_dir` turns into the
# dotted name — so these resolve from site-packages, not just from a checkout.
MCP_TOOL_DIRS:
  - mirobody/agent/tools
AGENT_DIRS:
  - mirobody/agent
PROVIDER_DIRS:
  - mirobody/collect/providers
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
    """One command, one process, no config file: the local-development path.

    `serve` reads deployment config and expects secrets to already exist.
    This path generates them for a local process and uses the supplied Postgres.
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
            "No database at hand? `docker run -d -p 5432:5432 -e POSTGRES_USER=user -e POSTGRES_PASSWORD=pw \\\n"
            "    -e POSTGRES_DB=mirobody pgvector/pgvector:pg17` — pgvector, not plain\n"
            "postgres: `schema/00_prolog.sql` creates a vector column."
        )

    # Ephemeral by default, and said out loud: a dev secret that persists is a
    # dev secret that reaches production in someone's shell history. It goes
    # into the ENVIRONMENT, not only the overlay, because `Config.__init__`
    # builds its `FernetEncrypter` from `get_fernet_key("CONFIG_ENCRYPTION_KEY")`
    # BEFORE it loads any YAML, so a value supplied in config can never satisfy
    # it: the run logs "CONFIG_ENCRYPTION_KEY is not set" at ERROR and encrypts
    # with a publicly-known key. `LOG_ENCRYPTION_KEY` reads the same way.
    generated = []
    for name, nbytes in (("JWT_KEY", 32), ("CONFIG_ENCRYPTION_KEY", 16), ("LOG_ENCRYPTION_KEY", 16)):
        if not os.environ.get(name):
            os.environ[name] = secrets.token_hex(nbytes)
            generated.append(name)
    jwt_key = os.environ["JWT_KEY"]
    config_key = os.environ["CONFIG_ENCRYPTION_KEY"]

    overlay = _DEV_CONFIG.format(
        host=args.host, port=args.port,
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


def _cmd_doctor(args: argparse.Namespace) -> None:
    """The provider self-check, on demand. Exit status 1 when NO surface has a
    provider, so a deploy script can gate on it; a partial deployment (a key
    with no embedding model, say) exits 0 with the gap named in the table."""
    # `dotenv` rather than a stack of its own: the configuration layer is what
    # this reads, and it arrives with either [app] or [parse]. Without the
    # guard a default install answered `mirobody doctor` with a traceback out
    # of `utils/config/config.py`, which is the failure the guard exists for.
    _require_extra("doctor", "app", "dotenv", "the configuration stack")

    from mirobody.utils.config import Config
    from mirobody.utils.config.doctor import format_report, provider_report

    async def configure() -> None:
        await Config.init(yaml_filenames=args.configs)
        # The setup page's choice, as the server applies it at boot: a
        # deployment set up in the browser has no key in .env, and this
        # reported "no model" for it. Without a database it says nothing,
        # and does not wait the driver's 75 s to say it.
        try:
            from mirobody.utils.config import settings

            await asyncio.wait_for(settings.apply(), timeout=5)
        except Exception:
            pass

    asyncio.run(configure())
    rows = provider_report()
    print(format_report(rows))
    if not any(r.provider for r in rows):
        sys.exit(1)
    if args.probe:
        _require_extra("doctor --probe", "app", "langchain_openai", "the agent stack")
        from mirobody.agent.probe import format_probes, probe_surfaces

        results = asyncio.run(probe_surfaces())
        print(format_probes(results))
        if not all(r.passed for r in results):
            sys.exit(1)


def _cmd_fetch_cpic(args: argparse.Namespace) -> None:
    from pathlib import Path

    from mirobody.translate.cpic_extract import fetch_extract

    directory = Path(args.dir or os.environ.get("CPIC_DIR") or "~/.mirobody/cpic")
    try:
        version, digest, installed = fetch_extract(args.version, directory)
    except (OSError, ValueError) as exc:
        status = getattr(exc, "code", None)
        suffix = f" status={status}" if isinstance(status, int) else ""
        sys.exit(f"CPIC fetch failed: {type(exc).__name__}{suffix}; no extract installed")
    print(f"CPIC {version} installed at {installed}; source sha256={digest}")


def _cmd_migrate_observations(args: argparse.Namespace) -> None:
    """Move the retired `th_series_data` history into the observation model,
    and drop it once every row is proven moved. Bounded and safe to re-run;
    see `collect/migrate_observations.py`."""
    _require_extra("migrate-observations", "app", "sqlalchemy", "the database layer")
    from mirobody.utils.config import Config
    from mirobody.collect.migrate_observations import RETIRED, migrate

    asyncio.run(Config.init(yaml_filenames=args.configs))
    counts = asyncio.run(migrate(batch=args.batch, user_id=args.user or None, repair=args.repair,
                                 write_undecrypted=args.write_undecrypted, verify_only=args.verify_only))
    if not counts["present"]:
        print("nothing to migrate: this database has no retired th_series_data")
        return
    rejected = ", ".join(f"{k}={v}" for k, v in sorted(counts["rejected"].items())) or "none"
    print(
        f"read {counts['read']} rows in {counts['batches']} batch(es): wrote {counts['written']} observations "
        f"({counts['coded']} coded), {counts['skipped']} already present, rejected {rejected}"
    )
    if counts["undecrypted"]:
        kept = "written without them" if args.write_undecrypted else "not migrated"
        print(
            f"{counts['undecrypted']} comment(s) did not decrypt under this connection's key, so their unit, "
            f"reference range and method are unknown; those rows were {kept}. Check PG_ENCRYPTION_KEY and re-run"
            + ("." if args.write_undecrypted else "; if that key is lost for good, re-run with --write-undecrypted.")
        )
    if counts["differs"]:
        print(
            f"{counts['differs']} row(s) differ from the reading already stored under the same name, time and "
            "source (an earlier run wrote it from a comment that did not decrypt, or two old names fold to one)."
            + ("" if args.repair else " Re-run with --repair to correct the ones nobody has changed since.")
        )
    if counts["missing"]:
        print(
            f"{counts['missing']} row(s) are not in the observation model and were not written (--verify-only): "
            "never migrated, or erased after an earlier migration. Run without --verify-only to write them, "
            f"or drop {RETIRED} yourself if they are readings that were erased."
        )
    if counts["dropped"]:
        print(f"every row is in the observation model; {RETIRED} is dropped")
    elif counts["left"] is None:
        print(f"{RETIRED} is kept: this run stopped before the last row (--batch); run it again")
    else:
        print(f"{RETIRED} is kept with {counts['left']} row(s) not proven moved. Re-run after fixing the above,"
              " or drop it yourself if those rows are not worth keeping")


def _cmd_migrate_genotypes(args: argparse.Namespace) -> None:
    """Publish 1.5.1 raw genotype rows without inventing normalized calls."""
    _require_extra("migrate-genotypes", "app", "sqlalchemy", "the database layer")
    from mirobody.collect.migrate_genotypes import migrate
    from mirobody.utils.config import Config

    asyncio.run(Config.init(yaml_filenames=args.configs))
    counts = asyncio.run(migrate(user_id=args.user or None, max_users=args.max_users))
    print(
        f"migrated {counts['users']} user(s), {counts['rows']} raw genotype row(s); "
        f"skipped {counts['skipped_active']} already active and "
        f"{counts['skipped_invalid']} invalid site(s)"
    )


def _cmd_recode(args: argparse.Namespace) -> None:
    """Replay the coding of every stored observation under the installed
    vocabulary and the current rules and aliases; see `observations.recode`."""
    _require_extra("recode", "app", "sqlalchemy", "the database layer")
    from mirobody.utils.config import Config
    from mirobody.utils import execute_query
    from mirobody.collect.observations import recode

    async def run() -> None:
        await Config.init(yaml_filenames=args.configs)
        if args.user:
            users = [args.user]
        else:
            rows = await execute_query("SELECT DISTINCT user_id FROM th_observation ORDER BY 1", {}, log_sql=False) or []
            users = [str(r["user_id"]) for r in rows]
        scanned = changed = 0
        for uid in users:
            report = await recode(uid)
            scanned += report.scanned
            changed += report.changed
        print(f"{len(users)} person(s): scanned {scanned} observations, recoded {changed}")

    asyncio.run(run())


def _width(text: str) -> int:
    """Terminal COLUMNS, not characters.

    `f"{term:<{n}}"` pads by `len()`, and every CJK character occupies two
    columns in every terminal. So `血红蛋白` was billed as 4 and drawn as 8, and
    the LOINC column drifted four places right on exactly the rows that make the
    point, this command's whole pitch is that four languages land on one code,
    and it showed that as a table which did not line up.
    """
    return sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in text)


def _pad(text: str, width: int) -> str:
    return text + " " * max(0, width - _width(text))


def _complaint_axis_hint(term: str) -> str:
    """When LOINC has no answer but ICPC-3 does, the reason names the axis that owns the term.

    A curated complaint ("发烧") or diagnosis ("高血压") is not a lab analyte, so
    the LOINC branch abstains. The same word is coded on ICPC-3's complaint (S) or
    diagnosis (D) axis; the hint names which, with its code and the flag that
    reaches it. Empty when neither axis codes the term, so a pure miss keeps its
    one-line message unchanged.
    """
    from mirobody.translate import resolve_condition, resolve_symptom

    for fn, flag, kind in ((resolve_symptom, "--symptom", "complaint"),
                           (resolve_condition, "--condition", "diagnosis")):
        r = fn(term)
        if r.coded:
            return f"not a lab analyte. This is a {kind}: ICPC-3 {r.code} ({r.display}). Ask with {flag}."
    return ""


def _print_icpc3(terms: list[str], width: int, *, condition: bool) -> None:
    """The complaint (`--symptom`) and diagnosis (`--condition`) axes, on ICPC-3.

    Same shape as the LOINC branch: the term, then the code and its ICPC-3 display.
    An abstention prints its `reason` in the register of the LOINC branch's
    "unresolved: ..." line, because a complaint ICPC-3 cannot place is a decision a
    person can close, not a miss to guess past.
    """
    from mirobody.translate import resolve_condition, resolve_symptom

    code = resolve_condition if condition else resolve_symptom
    for term in terms:
        r = code(term)
        if r.coded:
            print(f"  {_pad(term, width)}  {f'ICPC-3 {r.code}':<16}  {r.display}")
        else:
            print(f"  {_pad(term, width)}  {r.outcome}: {r.reason}")


def _cmd_resolve(args: argparse.Namespace) -> None:
    width = max(_width(t) for t in args.terms)
    if args.symptom or args.condition:
        _print_icpc3(args.terms, width, condition=args.condition)
        return
    from mirobody.engine import get_resolver

    try:
        resolver = get_resolver()   # first call pays the bundle load (~seconds)
    except RuntimeError as e:
        # A clone without `git lfs pull` has pointer stubs: one line, not a traceback.
        raise SystemExit(f"mirobody resolve: {e}") from None
    for term in args.terms:
        r = resolver.resolve(term)
        if r.resolved:
            loinc = f"LOINC {r.loinc}" if r.loinc else "(no LOINC axis row)"
            print(f"  {_pad(term, width)}  {loinc:<16}  {r.canonical}"
                  + (f"   [{r.candidates} candidates]" if r.candidates > 1 else ""))
        else:
            why = _complaint_axis_hint(term) or (
                "not in the lexical index, and no code is given rather than a guessed one")
            print(f"  {_pad(term, width)}  unresolved: {why}")


def _cmd_mcp(args: argparse.Namespace) -> None:
    """The stdio MCP server over the shipped vocabularies (no database, no key)."""
    from mirobody.mcp.stdio import main as serve_stdio

    raise SystemExit(serve_stdio([]))


def _cmd_device_bundle(args: argparse.Namespace) -> None:
    import json

    from mirobody.translate import device_bundle

    data = device_bundle.dumps()
    if not args.out:
        sys.stdout.buffer.write(data)
        return
    with open(args.out, "wb") as f:
        f.write(data)
    digest = json.loads(data)["digest"]
    print(f"wrote {args.out} ({len(data)} bytes, {digest})", file=sys.stderr)


def _cmd_parse(args: argparse.Namespace) -> None:
    """Read a document into standardized readings. Needs one vision-capable key.

    The no-key case gets the same treatment as the missing extra in
    `_require_extra`, and for the same reason. It used to surface as a
    twenty-line traceback ending in a `ValueError` from four frames inside
    `unified_file_extract`: the message was correct and nobody would read it
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
    except (ValueError, RuntimeError) as e:
        # A provider problem is one sentence for the person at the terminal,
        # not a traceback: no key at all (the registry's message names the
        # five and where to get them), a key whose model cannot read images
        # (a 404/400 from the gateway, naming UTILS_VISION_MODEL), or a
        # document nothing could read.
        message = str(e)
        if "none of these keys is set" in message:
            message += (
                "\n\nPut ONE key in the .env next to compose.yaml (or export it) — config.llm.yaml "
                "says which model each key selects. `mirobody resolve` needs no key and no network."
            )
        sys.exit(f"mirobody parse: {message}")
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
    days = {r.collected for r in readings if r.collected}
    if days:
        print(f"collected {' · '.join(sorted(days))}, as the document prints it")


def _cmd_import(args: argparse.Namespace) -> None:
    """Read a vendor's own export file into standardized readings.

    No database, no server, no key, no extra: `zipfile`, `xml.etree` and the
    decode tables are all stdlib or this package, so a bare `pip install
    mirobody` can read an export. That is the point. A deployment that wants
    live sync still registers a developer app with the vendor; a person who
    wants their own data out of their own phone does not.
    """
    import json
    from datetime import datetime

    from mirobody.kernel import decoders
    from mirobody.kernel.decoders import apple_export

    if not os.path.exists(args.file):
        sys.exit(f"mirobody import: no such file: {args.file}")

    counts = apple_export.Counts()
    seen: dict[str, list[float]] = {}
    span: list[int] = []
    unknown: dict[str, int] = {}
    out = open(args.out, "w", encoding="utf-8") if args.out else None
    try:
        for kind, item in apple_export.iter_items(args.file, counts):
            facts = decoders.decode("apple", kind, item, args.tz)
            if not facts:
                if kind:
                    unknown[kind] = unknown.get(kind, 0) + 1
                continue
            for f in facts:
                seen.setdefault(f.metric_key, []).append(f.value_num or 0.0)
                span.append(f.effective_start_ms)
                if out:
                    out.write(json.dumps(f.__dict__, ensure_ascii=False) + "\n")
    except (OSError, FileNotFoundError) as e:
        sys.exit(f"mirobody import: {e}")
    finally:
        if out:
            out.close()

    if not seen:
        print(f"Read {counts.records} records; none of them decoded to a known indicator.")
        return
    for metric in sorted(seen):
        values = seen[metric]
        print(f"  {metric:<36.36s}  {len(values):>7} readings   {min(values):g} … {max(values):g}")
    days = ""
    if span:
        first = datetime.fromtimestamp(min(span) / 1000).date()
        last = datetime.fromtimestamp(max(span) / 1000).date()
        days = f" · {first} … {last}"
    print(f"\n{sum(len(v) for v in seen.values())} readings · {len(seen)} indicators{days}")
    if unknown:
        total = sum(unknown.values())
        names = ", ".join(sorted(unknown)[:3])
        more = f" and {len(unknown) - 3} more" if len(unknown) > 3 else ""
        print(f"{total} records skipped: {names}{more}. Nothing was dropped in silence.")
    if counts.clinical_files:
        print(f"{counts.clinical_files} clinical records are in this export; this release does not read them.")
    if args.out:
        print(f"Facts written to {args.out}")


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
    p_dev.set_defaults(func=_cmd_dev)

    p_worker = sub.add_parser("worker", help="run the background task worker")
    p_worker.add_argument("configs", nargs="*", help="extra config YAML files, layered over config.yaml")
    p_worker.set_defaults(func=_cmd_worker)

    p_doctor = sub.add_parser("doctor", help="show which LLM provider each surface selects with the current config, and what is missing (requires the [app] or [parse] extra)")
    p_doctor.add_argument("configs", nargs="*", help="extra config YAML files, layered over config.yaml")
    p_doctor.add_argument("--probe", action="store_true", help="also send one real request per surface (a tool call, a schema-bound answer, an image) and report what the model did")
    p_doctor.set_defaults(func=_cmd_doctor)

    p_fetch = sub.add_parser("fetch", help="download a versioned public data asset")
    fetch_sub = p_fetch.add_subparsers(dest="asset", required=True)
    p_fetch_cpic = fetch_sub.add_parser("cpic", help="install a validated CPIC extract without executing SQL")
    p_fetch_cpic.add_argument("--version", default="latest", help="exact vX.Y.Z tag or latest")
    p_fetch_cpic.add_argument("--dir", default="", help="CPIC extract directory (or CPIC_DIR)")
    p_fetch_cpic.set_defaults(func=_cmd_fetch_cpic)

    p_parse = sub.add_parser("parse", help="parse a health document into standardized indicators (requires the [parse] extra and one LLM key)")
    p_parse.add_argument("file", help="path to a lab report (pdf/png/jpg/txt/csv)")
    p_parse.add_argument("--no-resolve", action="store_true", help="skip offline code resolution")
    p_parse.set_defaults(func=_cmd_parse)

    p_import = sub.add_parser(
        "import",
        help="read a vendor export file (Apple Health export.zip) — no key, no database, no extra",
    )
    p_import.add_argument("vendor", choices=["apple"], help="which vendor's export this is")
    p_import.add_argument("file", help="path to export.zip, export.xml, or the unpacked directory")
    p_import.add_argument("--tz", default="UTC", help="fallback timezone; Apple records carry their own offset")
    p_import.add_argument("--out", default="", help="write decoded facts to this file as JSON lines")
    p_import.set_defaults(func=_cmd_import)

    p_resolve = sub.add_parser("resolve", help="resolve indicator names to standard codes — fully offline, no key needed")
    p_resolve.add_argument("terms", nargs="+", help="indicator names in any supported language")
    axis = p_resolve.add_mutually_exclusive_group()
    axis.add_argument("--symptom", action="store_true",
                      help="treat each term as a complaint and code it on ICPC-3 (an S code)")
    axis.add_argument("--condition", action="store_true",
                      help="treat each term as a diagnosis and code it on ICPC-3 (a D code)")
    p_resolve.set_defaults(func=_cmd_resolve)

    p_bundle = sub.add_parser(
        "device-bundle",
        help="write the device vocabulary (catalogue, labels, crosswalks) as one JSON file — offline, no extra",
    )
    p_bundle.add_argument("--out", default="", help="write to this file instead of stdout")
    p_bundle.set_defaults(func=_cmd_device_bundle)
    p_mcp = sub.add_parser("mcp", help="run the stdio MCP server over the offline vocabularies (no key, no database)")
    p_mcp.set_defaults(func=_cmd_mcp)

    p_migrate = sub.add_parser(
        "migrate-observations",
        help="move the retired th_series_data history into the observation model (requires the [app] extra)",
    )
    p_migrate.add_argument("configs", nargs="*", help="extra config YAML files, layered over config.yaml")
    p_migrate.add_argument("--batch", type=int, default=2000, help="rows per batch (default: 2000)")
    p_migrate.add_argument("--user", default="", help="migrate one person only")
    p_migrate.add_argument("--repair", action="store_true",
                           help="amend a stored reading that differs from its old row, when nobody has changed it since")
    p_migrate.add_argument("--verify-only", action="store_true",
                           help="write nothing: mark the rows already moved and count the rest")
    p_migrate.add_argument("--write-undecrypted", action="store_true",
                           help="also write rows whose comment does not decrypt (their unit, range and method are lost)")
    p_migrate.set_defaults(func=_cmd_migrate_observations)

    p_genotypes = sub.add_parser(
        "migrate-genotypes",
        help="move 1.5.1 raw genotypes into an active set (requires the [app] extra)",
    )
    p_genotypes.add_argument("configs", nargs="*", help="extra config YAML files, layered over config.yaml")
    p_genotypes.add_argument("--user", default="", help="migrate one person only")
    p_genotypes.add_argument("--max-users", type=int, default=1000, help="maximum users per run")
    p_genotypes.set_defaults(func=_cmd_migrate_genotypes)

    p_recode = sub.add_parser(
        "recode",
        help="recode stored observations under the installed vocabulary, rules and aliases (requires the [app] extra)",
    )
    p_recode.add_argument("configs", nargs="*", help="extra config YAML files, layered over config.yaml")
    p_recode.add_argument("--user", default="", help="recode one person only")
    p_recode.set_defaults(func=_cmd_recode)

    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
