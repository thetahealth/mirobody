"""A stdio MCP server over the shipped vocabularies: `mirobody-mcp`.

    uvx --from mirobody mirobody-mcp        # or: python -m mirobody.mcp.stdio

Runs on `pip install mirobody` alone, the library and numpy: this module is
the standard library plus the engine, and a lookup reads only the data inside
the package. No database, no network, no user record. The one exception is
`standardize_report`, which reads a document with a model and so needs the
`[parse]` extra and a model key; without them it says so instead of failing.

The HTTP server (`mcp/service.py`) serves a person's record and needs a
database; this one serves the vocabularies to any client, and the three tools
both offer have one body (`translate.terminology`).
"""

from __future__ import annotations

import contextlib
import json
import logging
import sys
from typing import Any, TextIO

logger = logging.getLogger(__name__)

#: Newest first, the same revisions `mcp/service.py` speaks. 2026-07-28 has no
#: handshake; `initialize` negotiates among the rest.
PROTOCOL_VERSIONS = ("2026-07-28", "2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05")
_LATEST_HANDSHAKE = "2025-11-25"
_META_PROTOCOL_VERSION = "io.modelcontextprotocol/protocolVersion"
_META_SERVER_INFO = "io.modelcontextprotocol/serverInfo"
#: Every list is the same for every caller, so a client may cache it.
_LIST_CACHE = {"ttlMs": 300_000, "cacheScope": "public"}

CAPABILITIES = {
    "tools": {"listChanged": False},
    "prompts": {"listChanged": False},
    "resources": {"listChanged": False, "subscribe": False},
}

INSTRUCTIONS = (
    "Mirobody turns health data as a person or a lab prints it into standard codes, offline. "
    "Before explaining a lab result, pass each row to standardize_reading with its value and unit: "
    "the unit takes part in the choice of code. An Observation without a coding is an honest "
    "'unknown': say that it is unknown, never supply a code from memory. Complaints and diagnoses "
    "in a person's own words go to standardize_complaint (ICPC-3). No tool here reads anyone's "
    "record or leaves the machine, except standardize_report, which sends the document to the "
    "configured model provider to be read."
)

_READINGS_ITEM = {
    "type": "object",
    "properties": {
        "name": {"type": "string", "description": "The test name exactly as printed, any language."},
        "value": {"type": "string", "description": "The result as printed: 13.5, <5, 阴性, Positive."},
        "unit": {"type": "string", "description": "The unit as printed: g/dL, mmol/L, 次/分. Omit when none."},
    },
    "required": ["name"],
    "additionalProperties": False,
}

TOOLS: list[dict[str, Any]] = [
    {
        "name": "standardize_reading",
        "title": "Standardize lab readings",
        "description": (
            "Turn lab or vital-sign readings as printed (any language) into FHIR Observations with their "
            "LOINC code and UCUM unit, offline and without a key. The unit takes part in choosing the code "
            "(cholesterol in mmol/L and in mg/dL are different codes), so pass it. An Observation without "
            "`coding` means no code was chosen, and `_mirobody.rejected_reason` says why when a code was "
            "set aside. A blood pressure such as 120/80 mmHg is two readings (systolic, diastolic). Pass "
            "one reading (name, value, unit) or up to 200 as `readings`."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {**_READINGS_ITEM["properties"], "readings": {"type": "array", "items": _READINGS_ITEM, "maxItems": 200}},
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "standardize_complaint",
        "title": "Standardize a symptom or diagnosis",
        "description": (
            "Classify a complaint or a diagnosis in a person's own words (头疼, sore throat, 高血压) on ICPC-3, "
            "offline. kind=symptom for what someone feels, kind=condition for a named diagnosis; the two use "
            "different ICPC-3 components and must not be mixed. The words stay in `code.text`; without a "
            "`coding` the vocabulary could not place them, and `_mirobody.reason` says why. The ICPC-3 display "
            "names are English."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "The complaint or diagnosis as the person wrote it."},
                "kind": {"type": "string", "enum": ["symptom", "condition"], "default": "symptom"},
            },
            "required": ["text"],
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "standardize_report",
        "title": "Standardize a lab report file",
        "description": (
            "Read a lab report (PDF, image, spreadsheet, text) and return every reading as a FHIR Observation "
            "with its LOINC code. Reading the document takes a model: this sends it to the model provider "
            "configured on this machine, and needs `pip install 'mirobody[parse]'` and one model key. "
            "Coding is local. Pass `path` for a local file or `text` for report text."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "A local file path readable by this server."},
                "text": {"type": "string", "description": "Report text, instead of a file."},
            },
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": True},
    },
    {
        "name": "resolve_indicator",
        "title": "Look up LOINC codes by name",
        "description": (
            "Resolve indicator names to LOINC codes, offline. Same code from two names means the same test. "
            "Unresolved is an honest no: report it unmatched, never invent a code. A panel name answers with "
            "the panel's code (blood pressure gives 85354-9); a family of tests (血脂, lipid panel) is refused. "
            "Prefer standardize_reading when a value and unit are known."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"names": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 200}},
            "required": ["names"],
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "normalize_unit",
        "title": "Normalize units to UCUM",
        "description": (
            "Normalize units as printed (mg/dL, 毫摩尔每升, 次/分) to canonical UCUM and their LOINC property "
            "family. An empty `ucum` means unrecognized. The family does not decide convertibility; "
            "convert_unit does."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {"units": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 200}},
            "required": ["units"],
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
    {
        "name": "convert_unit",
        "title": "Convert a value between units",
        "description": (
            "Convert one value between units before comparing readings recorded differently. Crossing mass "
            "and substance concentration (mg/dL and mmol/L) needs the reading's LOINC code for the molar "
            "mass. A null `converted` is an answer: the units measure different things, so report the "
            "readings separately."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "value": {"type": "number"},
                "from_unit": {"type": "string"},
                "to_unit": {"type": "string"},
                "loinc_code": {"type": "string", "description": "Needed only between mass and substance concentration."},
            },
            "required": ["value", "from_unit", "to_unit"],
            "additionalProperties": False,
        },
        "annotations": {"readOnlyHint": True, "openWorldHint": False},
    },
]

PROMPTS: list[dict[str, Any]] = [
    {
        "name": "read_lab_report",
        "title": "Read a lab report without guessing",
        "description": "Explain a lab report row by row, each row's identity checked against LOINC first.",
        "arguments": [{"name": "report", "description": "The report text, or leave empty and attach it.", "required": False}],
    },
    {
        "name": "compare_readings",
        "title": "Compare readings from different labs",
        "description": "Decide whether two readings are the same test, and compare them in one unit.",
        "arguments": [
            {"name": "first", "description": "A reading as printed, e.g. 总胆固醇 5.2 mmol/L", "required": True},
            {"name": "second", "description": "Another, e.g. Cholesterol 201 mg/dL", "required": True},
        ],
    },
    {
        "name": "log_symptoms",
        "title": "Code what someone says they feel",
        "description": "Split a description into complaints and code each on ICPC-3, keeping the words.",
        "arguments": [{"name": "text", "description": "What the person wrote.", "required": True}],
    },
    {
        "name": "standardize_document",
        "title": "Standardize a report file",
        "description": "Turn a lab report file into coded Observations (needs [parse] and a model key).",
        "arguments": [{"name": "path", "description": "A local file path.", "required": True}],
    },
]

RESOURCES: list[dict[str, Any]] = [
    {
        "uri": "mirobody://bundle",
        "name": "bundle",
        "title": "Vocabularies and licences",
        "description": "The vocabulary releases this server answers from, and their notices.",
        "mimeType": "application/json",
    },
    {
        "uri": "mirobody://catalog",
        "name": "catalog",
        "title": "Indicator catalogue",
        "description": "Every standard device and app indicator Mirobody knows, with its LOINC code and unit.",
        "mimeType": "text/tab-separated-values",
    },
]


class ToolError(ValueError):
    """Arguments a tool cannot use; reported inside the result, not as a protocol error."""


# --- the tools -----------------------------------------------------------------


def _readings(args: dict[str, Any]) -> list[dict[str, Any]]:
    if "readings" in args and "name" in args:
        raise ToolError("give one reading (name, value, unit) or `readings`, not both")
    rows = args.get("readings") if "readings" in args else [args]
    if not isinstance(rows, list) or not rows:
        raise ToolError("`readings` must be a non-empty list")
    out = []
    for row in rows:
        if not isinstance(row, dict) or not str(row.get("name") or "").strip():
            raise ToolError("every reading needs a `name`")
        out.append(row)
    return out


def _tool_standardize_reading(args: dict[str, Any]) -> dict[str, Any]:
    from mirobody.engine.observation import standardize_reading

    observations = [
        standardize_reading(str(r["name"]), r.get("value"), r.get("unit")) for r in _readings(args)
    ]
    coded = sum(1 for o in observations if o["code"].get("coding"))
    return {"observations": observations, "coded": coded, "of": len(observations)}


def _tool_standardize_complaint(args: dict[str, Any]) -> dict[str, Any]:
    from mirobody.translate.terminology import standardize_complaint

    out = standardize_complaint(str(args.get("text") or ""), str(args.get("kind") or "symptom"))
    if out.get("success") is False:
        raise ToolError(out["error"])
    return out


def _tool_standardize_report(args: dict[str, Any]) -> dict[str, Any]:
    import asyncio

    path, text = args.get("path"), args.get("text")
    if bool(path) == bool(text):
        raise ToolError("give `path` or `text`, one of them")
    try:
        import mirobody.utils.llm  # noqa: F401  (the [parse] extra's model clients)
    except ImportError as e:
        raise ToolError(
            "reading a document needs the [parse] extra: pip install 'mirobody[parse]', then set one model "
            f"key (OPENAI_API_KEY, OPENROUTER_API_KEY, ...). Missing: {e.name}"
        ) from e
    from mirobody.engine import parse_file, parse_text
    from mirobody.engine.observation import observation

    try:
        readings = asyncio.run(parse_file(str(path)) if path else parse_text(str(text)))
    except (OSError, RuntimeError) as e:
        raise ToolError(str(e)) from e
    observations = []
    for r in readings:
        o = observation(r.name, r.value, r.unit, r.resolution)
        if r.collected:
            o["effectiveDateTime"] = r.collected
        observations.append(o)
    coded = sum(1 for o in observations if o["code"].get("coding"))
    return {
        "observations": observations,
        "coded": coded,
        "of": len(observations),
        "note": "The document was read by the configured model provider; the codes were chosen locally.",
    }


def _terminology(fn_name: str):
    def call(args: dict[str, Any]) -> dict[str, Any]:
        from mirobody.translate import terminology

        if fn_name == "convert_unit":
            out = terminology.convert_unit(args.get("value"), args.get("from_unit", ""), args.get("to_unit", ""),
                                           args.get("loinc_code", ""))
        elif fn_name == "resolve_indicators":
            out = terminology.resolve_indicators(args.get("names"))
        else:
            out = terminology.normalize_units(args.get("units"))
        if out.get("success") is False:
            raise ToolError(out["error"])
        return out
    return call


HANDLERS = {
    "standardize_reading": _tool_standardize_reading,
    "standardize_complaint": _tool_standardize_complaint,
    "standardize_report": _tool_standardize_report,
    "resolve_indicator": _terminology("resolve_indicators"),
    "normalize_unit": _terminology("normalize_units"),
    "convert_unit": _terminology("convert_unit"),
}


# --- prompts and resources --------------------------------------------------------


def _prompt_text(name: str, args: dict[str, Any]) -> str:
    if name == "read_lab_report":
        report = str(args.get("report") or "").strip()
        body = f"\n\nThe report:\n{report}" if report else ""
        return (
            "Read this lab report for me. First copy every row exactly as printed (name, value, unit, "
            "reference range) without translating. Pass all rows to standardize_reading in one call. "
            "Explain only rows that came back with a LOINC coding; list the rest as not identified rather "
            "than guessing what they measure. Point out values outside their printed reference range." + body
        )
    if name == "compare_readings":
        return (
            f"Are these the same test, and how do they compare?\n1. {args.get('first', '')}\n2. {args.get('second', '')}\n\n"
            "Pass both to standardize_reading. They are the same test only if the LOINC codes match, or are "
            "the mass and molar forms of one analyte. Convert with convert_unit (give the loinc_code) before "
            "comparing numbers; never scale a value by hand."
        )
    if name == "log_symptoms":
        return (
            f"Here is what I wrote about how I feel: {args.get('text', '')}\n\n"
            "Split it into separate complaints in my own words, skipping anything I said I do NOT have. Pass "
            "each to standardize_complaint (kind=condition for a diagnosis I was given). Show my words beside "
            "the ICPC-3 name, and keep the ones that could not be placed, saying so."
        )
    return (
        f"Standardize the lab report at {args.get('path', '')} with standardize_report, then summarize which "
        "readings were coded, which were not, and anything outside its reference range."
    )


def _resource_text(uri: str) -> tuple[str, str]:
    from importlib import resources

    res = resources.files("mirobody").joinpath("res")
    if uri == "mirobody://catalog":
        return "text/tab-separated-values", res.joinpath("catalog", "metrics.tsv").read_text(encoding="utf-8")
    if uri == "mirobody://bundle":
        import mirobody
        from mirobody import translate
        from mirobody.kernel.metrics import TERMINOLOGY_VERSION
        from mirobody.units import essence

        notices = {
            "loinc": res.joinpath("loinc", "fhir_loinc_bundle.NOTICE").read_text(encoding="utf-8"),
            "icpc3": res.joinpath("icpc3", "icpc3.NOTICE").read_text(encoding="utf-8"),
            "ucum": res.joinpath("ucum", "ucum-essence.NOTICE").read_text(encoding="utf-8"),
        }
        body = {
            "mirobody": mirobody.__version__,
            "loinc": mirobody.BUNDLE_VERSION,
            "icpc3": translate.release(),
            "ucum": essence.UCUM_VERSION,
            "catalogue": TERMINOLOGY_VERSION,
            "notices": notices,
        }
        return "application/json", json.dumps(body, ensure_ascii=False, indent=2)
    raise KeyError(uri)


# --- the protocol -----------------------------------------------------------------


class Server:
    """One client over one stdin/stdout pair. `handle` is pure over a message,
    so a test can drive it without a process."""

    def __init__(self) -> None:
        import mirobody

        self.server_info = {"name": "mirobody", "title": "Mirobody", "version": mirobody.__version__}
        self.negotiated = ""

    def handle(self, msg: Any) -> dict[str, Any] | None:
        if not isinstance(msg, dict) or msg.get("jsonrpc") != "2.0" or not isinstance(msg.get("method"), str):
            return _error(msg.get("id") if isinstance(msg, dict) else None, -32600, "Invalid Request")
        method, params, rid = msg["method"], msg.get("params") or {}, msg.get("id")
        if "id" not in msg:
            return None  # a notification: nothing is sent back
        try:
            result = self._dispatch(method, params if isinstance(params, dict) else {})
        except _RpcError as e:
            return _error(rid, e.code, e.message)
        except Exception as e:  # a bug; the type only, since the arguments are health data
            logger.error("mcp stdio: %s failed: error_type=%s", method, type(e).__name__)
            return _error(rid, -32603, f"Internal error: {type(e).__name__}")
        if isinstance(result, dict):
            result.setdefault("resultType", "complete")
            meta = result.setdefault("_meta", {})
            meta.setdefault(_META_SERVER_INFO, self.server_info)
            meta.setdefault(_META_PROTOCOL_VERSION, self._version_for(params))
        return {"jsonrpc": "2.0", "id": rid, "result": result}

    def _version_for(self, params: dict[str, Any]) -> str:
        meta = params.get("_meta") if isinstance(params, dict) else None
        requested = meta.get(_META_PROTOCOL_VERSION) if isinstance(meta, dict) else None
        if requested in PROTOCOL_VERSIONS:
            return requested
        return self.negotiated or PROTOCOL_VERSIONS[0]

    def _dispatch(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        if method == "initialize":
            requested = params.get("protocolVersion")
            handshake = [v for v in PROTOCOL_VERSIONS if v != "2026-07-28"]
            self.negotiated = requested if requested in handshake else _LATEST_HANDSHAKE
            return {"protocolVersion": self.negotiated, "capabilities": CAPABILITIES,
                    "serverInfo": self.server_info, "instructions": INSTRUCTIONS}
        if method == "server/discover":
            # The SDK's `DiscoverResult`: `supportedVersions` and `capabilities`;
            # the server's identity rides in `_meta`, as on every result.
            return {"supportedVersions": list(PROTOCOL_VERSIONS), "capabilities": CAPABILITIES,
                    "instructions": INSTRUCTIONS, **_LIST_CACHE}
        if method == "ping":
            return {}
        if method == "tools/list":
            return {"tools": TOOLS, **_LIST_CACHE}
        if method == "tools/call":
            return self._call(params)
        if method == "prompts/list":
            return {"prompts": PROMPTS, **_LIST_CACHE}
        if method == "prompts/get":
            name = params.get("name")
            if name not in {p["name"] for p in PROMPTS}:
                raise _RpcError(-32602, f"Unknown prompt: {name}")
            text = _prompt_text(name, params.get("arguments") or {})
            return {"description": next(p["description"] for p in PROMPTS if p["name"] == name),
                    "messages": [{"role": "user", "content": {"type": "text", "text": text}}]}
        if method == "resources/list":
            return {"resources": RESOURCES, **_LIST_CACHE}
        if method == "resources/templates/list":
            return {"resourceTemplates": [], **_LIST_CACHE}
        if method == "resources/read":
            uri = params.get("uri")
            try:
                mime, text = _resource_text(str(uri))
            except KeyError:
                raise _RpcError(-32602, f"Unknown resource: {uri}") from None
            return {"contents": [{"uri": uri, "mimeType": mime, "text": text}]}
        raise _RpcError(-32601, f"Method not found: {method}")

    def _call(self, params: dict[str, Any]) -> dict[str, Any]:
        name, args = params.get("name"), params.get("arguments") or {}
        handler = HANDLERS.get(name)
        if handler is None:
            raise _RpcError(-32602, f"Unknown tool: {name}")
        if not isinstance(args, dict):
            raise _RpcError(-32602, "arguments must be an object")
        # A tool must not write to stdout: that is the protocol stream. Config
        # initialisation on the document path prints a banner there.
        with contextlib.redirect_stdout(sys.stderr):
            try:
                out = handler(args)
            except ToolError as e:
                return {"content": [{"type": "text", "text": str(e)}], "isError": True}
        return {"content": [{"type": "text", "text": json.dumps(out, ensure_ascii=False, indent=2)}],
                "structuredContent": out, "isError": False}


class _RpcError(Exception):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code, self.message = code, message


def _error(rid: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": rid, "error": {"code": code, "message": message}}


def serve(stdin: TextIO | None = None, stdout: TextIO | None = None) -> None:
    """Read newline-delimited JSON-RPC from `stdin`, answer on `stdout`, until EOF."""
    stdin = stdin or sys.stdin
    stdout = stdout or sys.stdout
    server = Server()
    for line in stdin:
        line = line.strip()
        if not line:
            continue
        try:
            msg = json.loads(line)
        except json.JSONDecodeError:
            reply: Any = _error(None, -32700, "Parse error")
        else:
            if isinstance(msg, list):
                reply = [r for r in (server.handle(m) for m in msg) if r is not None] or None
            else:
                reply = server.handle(msg)
        if reply is not None:
            stdout.write(json.dumps(reply, ensure_ascii=False) + "\n")
            stdout.flush()


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if argv and argv[0] in ("-h", "--help"):
        print(__doc__.strip().split("\n\n")[0])
        return 0
    if argv and argv[0] == "--version":
        import mirobody

        print(mirobody.__version__)
        return 0
    logging.basicConfig(level=logging.WARNING, stream=sys.stderr)
    for stream in (sys.stdin, sys.stdout):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure:
            reconfigure(encoding="utf-8")
    serve()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
