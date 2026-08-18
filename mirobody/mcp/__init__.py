
from typing import TYPE_CHECKING

# Lazy (PEP 562), matching `mirobody/pulse/__init__.py`,
# `mirobody/agent/__init__.py` and `mirobody/pulse/providers/__init__.py`.
#
# Importing `mirobody.mcp.tool` ran this __init__, which imported `.service`
# and with it psycopg_pool and redis. Generating a tool's JSON Schema needs
# neither; only serving it over HTTP does.
#
# Every existing `from mirobody.mcp import X` keeps working unchanged; each export
# simply pays its own import cost at first use.
_EXPORTS = {
    'load_tools_from_module'         : 'tool',
    'load_tools_from_directory'      : 'tool',
    'load_tools_from_directories'    : 'tool',
    'call_tool'                      : 'tool',
    'call_global_tool'               : 'tool',
    'get_global_tool_count'          : 'tool',
    'get_global_tools'               : 'tool',
    'get_global_descriptions'        : 'tool',
    'get_global_functions'           : 'tool',
    'load_resources_from_directory'  : 'resource',
    'load_resources_from_directories': 'resource',
    'read_resource'                  : 'resource',
    'McpService'                     : 'service',
}
__all__ = [*_EXPORTS]

if TYPE_CHECKING:  # static analyzers resolve the real symbols
    from .resource import load_resources_from_directories, load_resources_from_directory, read_resource
    from .service import McpService
    from .tool import call_global_tool, call_tool, get_global_descriptions, get_global_functions, get_global_tool_count, get_global_tools, load_tools_from_directories, load_tools_from_directory, load_tools_from_module


def __getattr__(name: str):
    import importlib

    where = _EXPORTS.get(name)
    if where is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    module = importlib.import_module(f".{where}", __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
