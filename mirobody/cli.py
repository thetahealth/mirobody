"""Console entry point — ``mirobody <command>``.

Installed via ``[project.scripts]`` so a plain ``pip install mirobody`` gets a
runnable command (deployments use ``python -m mirobody`` — see __main__.py).

Commands:

* ``mirobody parse <file>``             — the engine's party trick: lab report
  in, standardized LOINC table out. One LLM key, no database, no server.
* ``mirobody resolve <terms...>``       — offline indicator-name resolution
  against the shipped bundles. Needs NOTHING: no key, no config, no network.
* ``mirobody serve [config.yaml ...]``  — the full HTTP server (chat, MCP,
  API). Requires the ``[agents]`` extra; checked up front with a plain message
  instead of a traceback from deep inside an import chain.
* ``mirobody worker [config.yaml ...]`` — the background task worker
  (IndicatorSync, ProfileRefresh queues).
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.util
import sys


def _require_agents_extra(command: str) -> None:
    """Fail fast, and legibly, when the agent layer isn't installed.

    Both ``serve`` and ``worker`` import ``mirobody.server``, which wires the
    chat/agent stack; without the ``[agents]`` extra that import chain dies
    several modules deep with an opaque ``ModuleNotFoundError: langchain``.
    Check the one marker dependency here and say what to run instead.
    """
    if importlib.util.find_spec("langchain") is None:
        sys.exit(
            f"mirobody {command} needs the agent layer, which is an optional extra:\n"
            "\n"
            "    pip install 'mirobody[agents]'\n"
            "\n"
            "The default install is the data ENGINE as a library "
            "(collect + standardize); the chat server is the layer on top."
        )


def _cmd_serve(args: argparse.Namespace) -> None:
    _require_agents_extra("serve")
    from mirobody.server import Server

    asyncio.run(Server.start(yaml_files=args.configs, fastapi_routers=[]))


def _cmd_worker(args: argparse.Namespace) -> None:
    _require_agents_extra("worker")
    from mirobody.server import Worker

    asyncio.run(Worker.start(yaml_files=args.configs))


def _cmd_resolve(args: argparse.Namespace) -> None:
    from mirobody.engine import get_resolver

    resolver = get_resolver()   # first call pays the bundle load (~seconds)
    width = max(len(t) for t in args.terms)
    for term in args.terms:
        r = resolver.resolve(term)
        if r.resolved:
            loinc = f"LOINC {r.loinc}" if r.loinc else "(no LOINC axis row)"
            print(f"  {term:<{width}}  {loinc:<16}  {r.canonical}"
                  + (f"   [{r.candidates} candidates]" if r.candidates > 1 else ""))
        else:
            print(f"  {term:<{width}}  unresolved — not in the lexical index "
                  "(the full semantic pipeline may still resolve it)")


def _cmd_parse(args: argparse.Namespace) -> None:
    from mirobody.engine import parse_file

    readings = asyncio.run(parse_file(args.file, resolve_names=not args.no_resolve))
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

    p_serve = sub.add_parser("serve", help="run the HTTP server (requires the [agents] extra)")
    p_serve.add_argument("configs", nargs="*", help="extra config YAML files, layered over config.yaml")
    p_serve.set_defaults(func=_cmd_serve)

    p_worker = sub.add_parser("worker", help="run the background task worker")
    p_worker.add_argument("configs", nargs="*", help="extra config YAML files, layered over config.yaml")
    p_worker.set_defaults(func=_cmd_worker)

    p_parse = sub.add_parser("parse", help="parse a health document into standardized indicators (needs one LLM key)")
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
