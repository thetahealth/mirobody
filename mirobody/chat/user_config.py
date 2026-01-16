import json, logging

from typing import Any

from ..utils import execute_query

#-----------------------------------------------------------------------------
# User's MCP servers.

async def get_user_mcps(user_id: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        records = await execute_query(
            "SELECT mcp FROM theta_ai.user_mcp_config WHERE user_id = :user_id",
            {"user_id": user_id}
        )

    except Exception as e:
        logging.error(str(e), exc_info=True)
        return None, str(e)
    
    #-----------------------------------------------------
    
    default_mcps = {}

    if records and isinstance(records, list):
            prompts = records[0].get("mcp", default_mcps)
            return prompts, None
    
    return default_mcps, None


async def _set_user_mcps(user_id: str, mcps: dict[str, Any]) -> str | None:
    if not isinstance(mcps, dict):
        return "mcps must be a dictionary."
    
    try:
        mcp_json = json.dumps(mcps, ensure_ascii=False, separators=(',', ':'))
        
        await execute_query(
            """
            INSERT INTO theta_ai.user_mcp_config (user_id, mcp, created_at, updated_at)
            VALUES (:user_id, :mcp, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) 
            DO UPDATE SET 
                mcp = :mcp,
                updated_at = CURRENT_TIMESTAMP
            """,
            {"user_id": user_id, "mcp": mcp_json}
        )

    except Exception as e:
        return str(e)
    
    return None

#-----------------------------------------------------------------------------

async def set_user_mcp(user_id: str, name: str, url: str, token: str = "", enabled: int | bool = True, order: int = 0) -> str | None:
    if not isinstance(name, str):
        return "Invalid name."
    
    name = name.strip()
    if not name:
        return "Empty name."
    
    if not isinstance(url, str):
        return "Invalid url."
    
    url = url.strip()
    if not url:
        return "Empty url."
    
    # token is optional, can be empty string
    if token and not isinstance(token, str):
        return "Invalid token."
    
    #-----------------------------------------------------
    
    existing_config, err = await get_user_mcps(user_id)
    if err:
        return err

    if existing_config is None:
        existing_config = {}

    mcp_config = {
        "url": url,
        "enabled": True if enabled else False,
        "order": order if isinstance(order, int) else 0
    }
    
    # Only add token if provided
    if token and isinstance(token, str):
        mcp_config["token"] = token.strip()
    
    existing_config[name] = mcp_config
    
    return await _set_user_mcps(user_id, existing_config)


async def delete_user_mcp(user_id: str, name: str) -> str | None:
    if not isinstance(name, str):
        return "Invalid name."
    
    name = name.strip()
    if not name:
        return "Empty name."
    
    #-----------------------------------------------------

    existing_config, err = await get_user_mcps(user_id)
    if err:
        return err
    
    if not existing_config or name not in existing_config:
        return None
    
    del existing_config[name]

    return await _set_user_mcps(user_id, existing_config)

#-----------------------------------------------------------------------------
# User's prompts.

async def get_user_prompts(user_id: str) -> tuple[dict[str, Any] | None, str | None]:
    try:
        records = await execute_query(
            "SELECT prompt FROM theta_ai.user_agent_prompt WHERE user_id=:user_id",
            {"user_id": user_id}
        )
        
    except Exception as e:
        return None, str(e)
    
    #-----------------------------------------------------
    
    default_prompts = {}

    if records and isinstance(records, list):
            prompts = records[0].get("prompt", default_prompts)
            return prompts, None
    
    return default_prompts, None


async def _set_user_prompts(user_id: str, prompts: dict[str, Any]) -> str | None:
    if not isinstance(prompts, dict):
        return "prompts must be a dictionary."

    try:
        prompts_json = json.dumps(prompts, ensure_ascii=False, separators=(',', ':'))
        
        await execute_query(
            """
            INSERT INTO theta_ai.user_agent_prompt (user_id, prompt, created_at, updated_at)
            VALUES (:user_id, :prompts, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (user_id) 
            DO UPDATE SET 
                prompt = :prompts,
                updated_at = CURRENT_TIMESTAMP
            """,
            {"user_id": user_id, "prompts": prompts_json}
        )

    except Exception as e:
        return str(e)
    
    return None


async def get_user_prompt_by_name(user_id: str, prompt_name: str) -> tuple[str | None, str | None]:
    if not isinstance(user_id, str):
        return None, "Invalid user id."
    
    user_id = user_id.strip()
    if not user_id:
        return None, "Empty user id."

    if not isinstance(prompt_name, str):
        return None, "Invalid prompt name."
    
    prompt_name = prompt_name.strip()
    if not prompt_name:
        return None, "Empty prompt name."
    
    #-----------------------------------------------------

    prompts, err = await get_user_prompts(user_id)
    if err:
        return None, err
    
    if not prompts:
        return None, "No user defined prompts."
    
    #-----------------------------------------------------
    # ('test_user_1', '{"health": {"prompt": "You are a helpful assistant", "temperature": 0.7}}')

    obj = prompts.get(prompt_name)
    if not obj:
        return None, "Not found."
    
    if not isinstance(obj, dict):
        return None, "Invalid prompt record."
    
    prompt = obj.get("prompt")
    if not isinstance(prompt, str):
        return None, "Invalid prompt value."

    return prompt, None


async def set_user_prompt(user_id: str, name: str, prompt: str, order: int = 0) -> str | None:
    if not isinstance(name, str):
        return "Invalid name."
    
    name = name.strip()
    if not name:
        return "Empty name."
    
    if not isinstance(prompt, str):
        return "Invalid prompt."
    
    prompt = prompt.strip()
    if not prompt:
        return "Empty prompt."

    #-----------------------------------------------------
    
    existing_prompts, err = await get_user_prompts(user_id)
    if err:
        return err

    if existing_prompts is None:
        existing_prompts = {}

    existing_prompts[name] = {
        "prompt": prompt,
        "order": order if isinstance(order, int) else 0
    }
    
    return await _set_user_prompts(user_id, existing_prompts)


async def delete_user_prompt(user_id: str, name: str) -> str | None:
    if not isinstance(name, str):
        return "Invalid name."
    
    name = name.strip()
    if not name:
        return "Empty name."
    
    #-----------------------------------------------------

    existing_prompts, err = await get_user_prompts(user_id)
    if err:
        return err
    
    if not existing_prompts or name not in existing_prompts:
        return None
    
    del existing_prompts[name]

    return await _set_user_prompts(user_id, existing_prompts)

#-----------------------------------------------------------------------------
