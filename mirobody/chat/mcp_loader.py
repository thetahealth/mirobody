"""
MCP (Model Context Protocol) Dynamic Tool Loader
Loads tools from external MCP servers based on user configuration
"""

import aiohttp, json, logging

from typing import List
from langchain_core.tools import StructuredTool

from .user_config import get_user_mcps

#-----------------------------------------------------------------------------

async def load_remote_mcp_tools(server_url: str, token: str) -> tuple[list[dict] | None, str | None]:
    headers = {}
    if token and isinstance(token, str):
        headers["Authorization"] = f"Bearer {token}"

    payload = {
        "id": 1,
        "method": "tools/list"
    }

    tools = []

    async with aiohttp.ClientSession() as session:
        async with session.post(url=server_url, headers=headers, json=payload) as response:
            if not response.ok:
                return None, f"Status code: {response.status}"

            response_json = await response.json()
            if not isinstance(response_json, dict) or \
                "result" not in response_json or not isinstance(response_json["result"], dict) or \
                "tools" not in response_json["result"] or not isinstance(response_json["result"]["tools"], list):

                return None, f"Invalid response: {json.dumps(response_json, ensure_ascii=False)}"

            for tool in response_json["result"]["tools"]:
                tools.append({
                    "type"      : "function",
                    "function"  : {
                        "name"          : tool["name"],
                        "description"   : tool["description"],
                        "parameters"    : tool["inputSchema"]
                    }
                })

    return tools, None

#-----------------------------------------------------------------------------

async def load_user_mcp_tools(user_id: str, jwt_token: str = "") -> List[StructuredTool]:
    """
    Load MCP tools for a specific user based on their configuration
    
    Args:
        user_id: User ID
        jwt_token: JWT token for authentication (if needed)
    
    Returns:
        List of loaded MCP tools
    """

    try:
        # Get user's MCP configuration
        user_mcps, err = await get_user_mcps(user_id)
        if err:
            logging.error(f"Failed to get user MCP config: {err}")
            return []

        if not user_mcps or not isinstance(user_mcps, dict):
            logging.info(f"No MCP configuration found for user: {user_id}")
            return []

        all_tools = []

        # Load tools from each configured MCP server
        for server_name, config in user_mcps.items():
            if not isinstance(config, dict):
                logging.warning(f"Invalid config for server {server_name}")
                continue

            # Check if enabled
            if not config.get("enabled", True):
                logging.info(f"Skipping disabled MCP server: {server_name}")
                continue

            url = config.get("url", "").strip()
            if not url:
                logging.warning(f"No URL configured for MCP server: {server_name}")
                continue

            # Get token (from config or use jwt_token as fallback)
            token = config.get("token", "").strip()
            if not token and jwt_token:
                token = jwt_token

            logging.info(f"Loading tools from MCP server '{server_name}': {url} (token: {'provided' if token else 'none'})")

            tools, err = load_remote_mcp_tools(url, token)
            if err:
                logging.warning(f"No tools loaded from '{server_name}' at {url}: {err}")
            elif tools:
                tool_names = [getattr(t, 'schema', {}).get('function', {}).get('name', 'unknown') for t in tools]
                logging.info(f"Loaded {len(tools)} tools from '{server_name}': {tool_names}")
                all_tools.extend(tools)

        logging.info(f"Total MCP tools loaded for user {user_id}: {len(all_tools)}")
        return all_tools

    except Exception as e:
        logging.error(f"Error loading user MCP tools: {str(e)}", exc_info=True)
        return []

#-----------------------------------------------------------------------------
