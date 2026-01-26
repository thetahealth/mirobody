"""
Prompt Builder Module for DeepAgent

Handles dynamic system prompt construction with tool descriptions,
time information, and user context.
"""

import logging
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from jinja2 import Environment

logger = logging.getLogger(__name__)


async def build_system_prompt(
    base_prompt: str,
    language: str,
    user_id: str,
    langchain_tools: list,
    model_client: Any,
    agent_name: str,
    user_name: str,
    timezone: str = "UTC"
) -> str | list:
    """
    Build dynamic system prompt with tool descriptions, time, and user info.

    - Base prompt template
    - Tool descriptions
    - Current time
    - User information
    
    Args:
        base_prompt: Base prompt template
        language: User language
        user_id: User ID
        langchain_tools: List of LangChain tools
        model_client: LLM client for model detection
        agent_name: Agent name for prompt context
        user_name: User name for prompt context
        timezone: User timezone (e.g., "Asia/Shanghai", "America/New_York")
        
    Returns:
        System prompt string, or list with cache control for Claude
    """
    # Build tool descriptions
    tool_prompts = []
    for tool in langchain_tools:
        if hasattr(tool, 'description') and tool.description:
            tool_desc = f"**{tool.name}**: {tool.description}"
            tool_prompts.append(tool_desc)
    
    tools_description = "\n\n---\n\n".join(tool_prompts) if tool_prompts else ""
    if tools_description:
        tools_description += "\n\n---\n\n"
    
    current_time = datetime.now(ZoneInfo(timezone)).strftime("%A, %B %d, %Y, at %I:00 %p %Z (UTC%z)")
    
    # Build prompt with Jinja2 template rendering
    try:
        template = Environment(enable_async=True).from_string(base_prompt)
        rendered_prompt = await template.render_async(
            agent_name=agent_name,
            user_name=user_name,
            current_time=current_time,
            language=language if language else "en",
            tools_description=tools_description,
            user_info={"user_id": user_id},
        )
    except Exception as e:
        logger.warning(f"Failed to render prompt template: {e}, using base prompt")
        rendered_prompt = base_prompt
    
    # Add Claude cache control if using Claude model
    model_name = getattr(model_client, "model", "").lower()
    if "claude" in model_name:
        return [
            {
                "type": "text",
                "text": rendered_prompt,
                "cache_control": {"type": "ephemeral"},
            }
        ]
    else:
        return rendered_prompt
