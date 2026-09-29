
from typing import TYPE_CHECKING

# Lazy (PEP 562), matching `mirobody/collect/__init__.py`,
# `mirobody/agent/__init__.py` and `mirobody/collect/providers/__init__.py`.
# Importing `mirobody.mcp.tool` ran this __init__, which imported `.service`
# and with it psycopg_pool. Generating a tool's JSON Schema needs
# neither; only serving it over HTTP does. Every `from mirobody.mcp import X`
# keeps working; each export simply pays its own import cost at first use.
_EXPORTS = {
    'load_tools_from_module'         : 'tool',
    'load_tools_from_directory'      : 'tool',
    'load_tools_from_directories'    : 'tool',
    'call_tool'                      : 'tool',
    'get_global_tools'               : 'tool',
    'get_global_descriptions'        : 'tool',
    'McpService'                     : 'service',
}
__all__ = [*_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .service import McpService
    from .tool import call_tool, get_global_descriptions, get_global_tools, load_tools_from_directories, load_tools_from_directory, load_tools_from_module


def __getattr__(name: str):
    import importlib

    where = _EXPORTS.get(name)
    if where is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = importlib.import_module(f".{where}", __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
