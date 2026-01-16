"""
Tool Loader Module for DeepAgent

Handles loading and configuration of tools from multiple sources:
- Global tools (from mirobody.mcp.tool)
- User-specific MCP tools
"""

import functools
import inspect
import logging
from typing import Any, Optional

from langchain_core.tools import StructuredTool

logger = logging.getLogger(__name__)


async def load_global_tools(
    user_id: str,
    token: str,
    allowed_tools: Optional[list[str]] = None,
    disallowed_tools: Optional[list[str]] = None
) -> list[StructuredTool]:
    """
    Load global tools and properly handle async functions.
    
    Args:
        user_id: User ID for authentication
        token: JWT token for authentication
        allowed_tools: List of allowed tool names (whitelist)
        disallowed_tools: List of disallowed tool names (blacklist)
    
    Returns:
        List of LangChain StructuredTool instances
    """
    from mirobody.mcp.tool import get_global_tools
        
    existing_tools = get_global_tools()
    
    # Prepare user_info for tools that require authentication
    user_info = {
        "user_id": user_id,
        "token": token,
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
                        async def wrapper(**kwargs):
                            # Get valid parameters for the wrapped function
                            try:
                                valid_params = set(inspect.signature(f).parameters.keys())
                            except (ValueError, TypeError):
                                valid_params = set()
                            # Filter kwargs to only include valid parameters
                            filtered_kwargs = {k: v for k, v in kwargs.items() if k in valid_params or not valid_params}
                            return await f(**filtered_kwargs)
                        return wrapper
                    tool_func = create_async_filter_wrapper(tool_func)
                else:
                    def create_sync_filter_wrapper(f):
                        def wrapper(**kwargs):
                            # Get valid parameters for the wrapped function
                            try:
                                valid_params = set(inspect.signature(f).parameters.keys())
                            except (ValueError, TypeError):
                                valid_params = set()
                            # Filter kwargs to only include valid parameters
                            filtered_kwargs = {k: v for k, v in kwargs.items() if k in valid_params or not valid_params}
                            return f(**filtered_kwargs)
                        return wrapper
                    tool_func = create_sync_filter_wrapper(tool_func)
                
                # Build args_schema from tool_description's inputSchema
                # This avoids LangChain trying to infer schema from wrapper function
                args_schema = None
                input_schema = tool_description.get("inputSchema", {})
                if input_schema and "properties" in input_schema:
                    from pydantic import create_model, Field
                    from typing import Any as AnyType, Optional, List
                    
                    fields = {}
                    properties = input_schema.get("properties", {})
                    required = input_schema.get("required", [])
                    
                    # Exclude user_info from schema (it's injected, not passed by LLM)
                    for prop_name, prop_info in properties.items():
                        if prop_name == "user_info":
                            continue
                        
                        prop_description = prop_info.get("description", "")
                        is_required = prop_name in required
                        
                        # Use Optional[Any] for all types to avoid complex type handling
                        if is_required:
                            fields[prop_name] = (AnyType, Field(..., description=prop_description))
                        else:
                            fields[prop_name] = (Optional[AnyType], Field(default=None, description=prop_description))
                    
                    if fields:
                        args_schema = create_model(f"{tool_name}_Args", **fields)
                
                # Create StructuredTool with explicit args_schema
                if inspect.iscoroutinefunction(original_func):
                    logger.info(f"Tool {tool_name} is async, using coroutine handler")
                    lc_tool = StructuredTool.from_function(
                        coroutine=tool_func,
                        name=tool_description.get("name", tool_name),
                        description=tool_description.get("description", ""),
                        args_schema=args_schema,
                    )
                else:
                    lc_tool = StructuredTool.from_function(
                        func=tool_func,
                        name=tool_description.get("name", tool_name),
                        description=tool_description.get("description", ""),
                        args_schema=args_schema,
                    )
                
                langchain_tools.append(lc_tool)
                tool_names.append(tool_name)
            except Exception as e:
                logger.warning(f"Failed to load tool {tool_name}: {e}", exc_info=True)
    
    tool_names_str = ", ".join(tool_names)
    logger.info(f"Loaded {len(langchain_tools)} global tools, tool names: {tool_names_str}")
    return langchain_tools


async def load_mcp_tools(user_id: str, token: str) -> list[StructuredTool]:
    """
    Load user-specific MCP tools.
    
    Args:
        user_id: User ID for tool loading
        token: JWT token for authentication
        
    Returns:
        List of LangChain StructuredTool instances
    """
    langchain_tools = []
    
    try:
        from mirobody.chat.mcp_loader import load_user_mcp_tools
        
        logger.info(f"Loading user MCP tools for user_id: {user_id}")
        user_mcp_tools = await load_user_mcp_tools(
            user_id=user_id,
            jwt_token=token
        )
        
        if user_mcp_tools:
            langchain_tools.extend(user_mcp_tools)
            logger.info(f"Added {len(user_mcp_tools)} user MCP tools to DeepAgent")
            tool_names = [tool.name for tool in user_mcp_tools]
            tool_names_str = ", ".join(tool_names)
            logger.info(f"Loaded {len(user_mcp_tools)} user MCP tools, tool names: {tool_names_str}")
        else:
            logger.info("No user MCP tools configured or loaded")
    except Exception as e:
        logger.error(f"Failed to load user MCP tools: {e}", exc_info=True)
    
    return langchain_tools
