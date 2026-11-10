"""JavaScript in process: QuickJS compiled to WebAssembly, run on wasmtime.

This is the engine under deepagents' `eval` interpreter, without its
middleware: that one reaches records by calling tools from JavaScript, and the
path removed in 1.5.8 had models rebuild rows from display tables. Here the
code gets the original rows as globals, as the Python container does. QuickJS
has no network, filesystem, process or module loader; the WebAssembly
instance is the isolation boundary, and every run gets a fresh one.
"""

from __future__ import annotations

import json
import logging

import quickjs_rs
from deepagents.backends.protocol import ExecuteResponse

logger = logging.getLogger(__name__)

#: Twice the interpreter's default: `data` and `results` are parsed inside.
_MEMORY = 128 * 1024 * 1024

#: console.* writes through one host function; `data` and `results` are
#: parsed from one JSON string, so a value never passes through Python
#: marshalling on its way in.
_PRELUDE = """
const __text = v => {
  if (typeof v === 'string') return v;
  try { const s = JSON.stringify(v); return s === undefined ? String(v) : s; } catch (e) { return String(v); }
};
const __write = (...args) => __emit(args.map(__text).join(' '));
globalThis.console = {log: __write, info: __write, warn: __write, error: __write};
const __inputs = JSON.parse(globalThis.__inputs_json);
delete globalThis.__inputs_json;
globalThis.data = __inputs.data;
globalThis.results = __inputs.results;
"""


def _forget(exc: BaseException) -> None:
    """Drop what an engine exception holds: its frames keep handles alive, and
    a handle collected on another thread raises (langchain_quickjs does the
    same)."""
    exc.__traceback__ = exc.__context__ = exc.__cause__ = None


def run(code: str, inputs: dict, *, timeout: float, max_output: int = 40_000) -> ExecuteResponse:
    """`code` with `data` and `results` from `inputs` as globals. The output
    is what console.* wrote, then the value of the last expression when it has
    one. Exit codes follow the container's: 0, 1 for an error, 124 for the
    deadline. The whole run stays on the calling thread: a quickjs_rs handle
    dropped on another thread raises."""
    output: list[str] = []
    size = 0
    truncated = False

    def emit(line) -> None:
        nonlocal size, truncated
        text = str(line) + "\n"
        room = max_output - size
        if room <= 0:
            truncated = True
            return
        output.append(text[:room])
        size += min(len(text), room)
        truncated |= len(text) > room

    runtime = quickjs_rs.Runtime(memory_limit=_MEMORY)
    try:
        context = runtime.new_context(timeout=timeout)
        try:
            context.register("__emit", emit)
            context.globals["__inputs_json"] = json.dumps(inputs, ensure_ascii=False, default=str)
            context.eval(_PRELUDE)
            try:
                value = context.eval(code)
            except quickjs_rs.JSError as exc:
                # A model writes `return x` at the top level about as often as
                # a final expression; QuickJS refuses it before running
                # anything, so the same code runs again as a function body.
                if "return not in a function" not in str(exc):
                    raise
                _forget(exc)
                value = context.eval(f"(() => {{\n{code}\n}})()")
            if value is not quickjs_rs.UNDEFINED and value is not None:
                emit(value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str))
            return ExecuteResponse("".join(output), 0, truncated)
        except quickjs_rs.TimeoutError as exc:
            _forget(exc)
            return ExecuteResponse("".join(output), 124, truncated)
        except quickjs_rs.MemoryLimitError as exc:
            _forget(exc)
            emit("Error: the calculation ran out of memory")
            return ExecuteResponse("".join(output), 1, truncated)
        except quickjs_rs.JSError as exc:
            emit(str(exc))
            _forget(exc)
            return ExecuteResponse("".join(output), 1, truncated)
        except quickjs_rs.MarshalError as exc:
            # Only the last expression's value is marshalled back; a function
            # or a symbol there is not an error in the calculation.
            _forget(exc)
            return ExecuteResponse("".join(output), 0, truncated)
        finally:
            context.close()
    except Exception as exc:
        logger.warning("javascript execution failed: error_type=%s", type(exc).__name__)
        _forget(exc)
        return ExecuteResponse(f"error: JavaScript executor unavailable ({type(exc).__name__})", 1)
    finally:
        runtime.close()
