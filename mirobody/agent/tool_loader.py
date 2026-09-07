"""
MCP tools → LangChain tools for the agent

Handles loading and configuration of tools from multiple sources:
- Global tools (from mirobody.mcp.tool)
- User-specific MCP tools
"""

import functools
import inspect
import logging

from langchain_core.tools import StructuredTool

from mirobody.kernel import meds, query
from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)

# Tool names reserved by the native deepagents harness. The agent gets these
# from middleware, so a same-named global MCP tool (from any source) would shadow
# or collide with the native one — filter them out here:
#   - FilesystemMiddleware provides ls/read_file/write_file/edit_file/glob/grep
#     over the CompositeBackend (multimodal read for pdf/image/...).
#
# `write_todos` is deliberately absent: deepagents 0.7 dropped TodoListMiddleware
# from its default stack and the agent does not add it back, so nothing provides
# that name natively and there is nothing to shadow. Re-add it here if
# TodoListMiddleware is ever wired back into `agent.MirobodyAgent._build_agent`.
_NATIVE_TOOL_BLOCKLIST = frozenset({
    "ls", "read_file", "write_file", "edit_file", "glob", "grep",  # FilesystemMiddleware
})


def mcp_args_schema(input_schema: dict) -> dict:
    """The tool's JSON schema for the model: the MCP ``inputSchema`` minus the
    injected ``user_info`` parameter, everything else verbatim."""
    props = {k: v for k, v in input_schema.get("properties", {}).items() if k != "user_info"}
    schema = {k: v for k, v in input_schema.items() if k not in ("properties", "required")}
    schema["properties"] = props
    required = [r for r in input_schema.get("required", []) if r != "user_info"]
    if required:
        schema["required"] = required
    return schema


def _accepted_params(func) -> tuple[set[str], bool]:
    """`(named parameters, does it take **kwargs)`, read once at load time.

    The second half is load-bearing: a tool whose parameters are a declared
    schema takes them as ``**kwargs``, so its only named parameter is the
    catch-all itself. Filtering against that name alone dropped every real
    argument and the tool ran on its defaults — a confident, wrong answer the
    model had no way to attribute to the wrapper.
    """
    try:
        params = inspect.signature(func).parameters
    except (ValueError, TypeError):
        return set(), True
    named = {n for n, p in params.items() if p.kind is not inspect.Parameter.VAR_KEYWORD}
    takes_kwargs = any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values())
    return named, takes_kwargs


def _filtered(kwargs: dict, valid: set[str], takes_kwargs: bool) -> dict:
    """Drop what the tool cannot accept (LangGraph passes extras such as
    ``context``); pass everything when the tool accepts ``**kwargs`` or when
    the signature could not be read."""
    if takes_kwargs or not valid:
        return kwargs
    return {k: v for k, v in kwargs.items() if k in valid}


#: Tools that answer with a `mirobody.kernel.tools.Envelope` and are therefore wired as
#: `content_and_artifact`. A name, not a duck-type check, because the decision
#: has to be made at LOAD time — `response_format` is a constructor argument.
_ENVELOPE_TOOLS = frozenset({query.TOOL_NAME, meds.TOOL_NAME})


def _envelope_wrapper(bound_method, user_info: dict):
    """An envelope-returning tool as LangChain's `(text, artifact)` pair.

    `bound_method` is the loaded MCP tool — a bound method of the service
    instance the registry already built, so the same object answers both
    surfaces. The MCP path keeps the plain dict (an MCP client has no artifact
    channel); the split lives here, in the chat adapter.

    Returns `None` when the service does not offer an envelope, so a tool named
    in `_ENVELOPE_TOOLS` that has been reimplemented without one degrades to the
    ordinary string path instead of failing at load.
    """
    service = getattr(bound_method, "__self__", None)
    if service is None or not hasattr(service, "envelope"):
        return None

    from .tools.health_indicators_service import render_compact

    # A medications answer names its columns per view; a readings answer
    # derives them from the row shape.
    columns_for = getattr(type(service), "input_schema", None) is meds.TOOL_SCHEMA

    async def wrapper(**kwargs):
        envelope = await service.envelope(user_info, **kwargs)
        columns = meds.VIEW_COLUMNS.get(str(kwargs.get("view") or meds.VIEW_PLAN)) if columns_for else None
        return render_compact(envelope, columns), envelope

    return wrapper


async def load_global_tools(
    user_id: str,
    token: str,
    session_id: str | None = None,
    allowed_tools: list[str] | None = None,
    disallowed_tools: list[str] | None = None
) -> list[StructuredTool]:
    """
    Load global tools and properly handle async functions.

    Args:
        user_id: User ID for authentication
        token: JWT token for authentication
        session_id: Session ID included in the `user_info` passed to tools
            (see USER_INFO INJECTION in tools/__init__.py)
        allowed_tools: List of allowed tool names (whitelist)
        disallowed_tools: List of disallowed tool names (blacklist)

    Returns:
        List of LangChain StructuredTool instances
    """
    from ..mcp.tool import get_global_tools

    existing_tools = get_global_tools()

    # Prepare user_info for tools that require authentication
    user_info = {
        "user_id": user_id,
        "token": token,
        "session_id": session_id,
        "success": True  # used for authentication
    }
    
    # Convert to LangChain tools
    langchain_tools = []
    tool_names = []
    
    if existing_tools:
        for tool_name, tool_info in existing_tools.items():
            try:
                tool_func = tool_info.get("instance")
                tool_description = tool_info.get("description", {})
                requires_auth = tool_info.get("auth", False)
                
                # Check if tool is allowed or disallowed
                if not tool_func:
                    continue
                
                if allowed_tools and tool_name not in allowed_tools:
                    continue

                if disallowed_tools and tool_name in disallowed_tools:
                    continue

                if tool_name in _NATIVE_TOOL_BLOCKLIST:
                    logger.debug(f"Tool {tool_name} blocked — provided by native deepagents middleware")
                    continue

                # Get original function for async check (before partial wrapping)
                original_func = tool_func
                
                # Inject user_info if required by the tool
                if requires_auth:
                    tool_func = functools.partial(tool_func, user_info=user_info)
                    logger.debug(f"Tool {tool_name} requires auth, injected user_info via partial")
                
                # Create a wrapper that filters parameters to only those the function accepts
                # This prevents errors from extra parameters (like 'context' that LangGraph might pass)
                if inspect.iscoroutinefunction(original_func):
                    def create_async_filter_wrapper(f):
                        valid_params, takes_kwargs = _accepted_params(f)
                        async def wrapper(**kwargs):
                            return await f(**_filtered(kwargs, valid_params, takes_kwargs))
                        return wrapper
                    tool_func = create_async_filter_wrapper(tool_func)
                else:
                    def create_sync_filter_wrapper(f):
                        valid_params, takes_kwargs = _accepted_params(f)
                        def wrapper(**kwargs):
                            return f(**_filtered(kwargs, valid_params, takes_kwargs))
                        return wrapper
                    tool_func = create_sync_filter_wrapper(tool_func)
                
                # The MCP schema — enums, bounds, defaults — reaches the chat model
                # as-is (langchain-core accepts a JSON-Schema dict as args_schema).
                # Rebuilding it as `Any | None` fields erased every enum, which is
                # why the model sent `aggregate: none` and a repair middleware had
                # to guess. Only `user_info` is removed: it is injected, not asked.
                args_schema = None
                input_schema = tool_description.get("inputSchema", {})
                if input_schema and "properties" in input_schema:
                    args_schema = mcp_args_schema(input_schema)

                # A tool that answers with a `tools.Envelope` returns
                # `(rendered text, envelope)` and is declared
                # `content_and_artifact`, so LangChain puts the text in the
                # ToolMessage and the envelope in its `artifact`. That is what
                # `RetryGovernanceMiddleware` reads: whether a retry can help is
                # a fact about the call, and parsing it back out of prose a
                # harness may truncate is not a way to know it.
                envelope_tool = False
                if tool_name in _ENVELOPE_TOOLS:
                    wrapped = _envelope_wrapper(original_func, user_info)
                    if wrapped is not None:
                        tool_func, envelope_tool = wrapped, True

                # Create StructuredTool with explicit args_schema
                if inspect.iscoroutinefunction(original_func):
                    logger.info(f"Tool {tool_name} is async, using coroutine handler")
                    lc_tool = StructuredTool.from_function(
                        coroutine=tool_func,
                        name=tool_description.get("name", tool_name),
                        description=tool_description.get("description", ""),
                        args_schema=args_schema,
                        **({"response_format": "content_and_artifact"} if envelope_tool else {}),
                    )
                else:
                    lc_tool = StructuredTool.from_function(
                        func=tool_func,
                        name=tool_description.get("name", tool_name),
                        description=tool_description.get("description", ""),
                        args_schema=args_schema,
                        **({"response_format": "content_and_artifact"} if envelope_tool else {}),
                    )
                
                langchain_tools.append(lc_tool)
                tool_names.append(tool_name)
            except Exception as e:
                logger.warning("Failed to load tool %s: error_type=%s", tool_name, type(e).__name__, exc_info=not is_driver_exception(e))
    
    tool_names_str = ", ".join(tool_names)
    logger.info(f"Loaded {len(langchain_tools)} global tools, tool names: {tool_names_str}")
    return langchain_tools
