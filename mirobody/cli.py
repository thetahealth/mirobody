"""Console entry point — ``mirobody <command>``.

Installed via ``[project.scripts]`` so a plain ``pip install mirobody`` gets a
runnable command (deployments use ``python -m mirobody`` — see __main__.py).

Commands:

* ``mirobody parse <file>``             — the engine's party trick: lab report
  in, standardized LOINC table out. One LLM key, no database, no server.
  Requires the ``[parse]`` extra.
* ``mirobody resolve <terms...>``       — offline indicator-name resolution
  against the shipped bundles. Needs NOTHING: no key, no config, no network.
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

    ``pip install mirobody`` is ② Translate — ``resolve``, units, lexical,
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
