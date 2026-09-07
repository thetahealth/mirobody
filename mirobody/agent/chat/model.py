from typing import Any
from pydantic import BaseModel, Field

#-----------------------------------------------------------------------------


class ChatFileObject:
    def __init__(
        self,
        file_key    : str = "",
        file_type   : str = "",
        file_name   : str = "",
        file_url    : str = "",
        file_size   : int = 0,

        duration    : int = 0,  # For audio/video files, in millisecond.
        storage_key : str = "",
        url         : str = ""
    ):
        self.file_key   = file_key
        self.file_type  = file_type
        self.file_name  = file_name
        self.file_url   = file_url
        self.file_size  = file_size

        self.duration   = duration
        self.storage_key= storage_key
        self.url        = url

#-----------------------------------------------------------------------------

class ChatStreamRequest:
    """Chat stream request from a client.

    All parameters are passed explicitly from the API request — no implicit
    context — for thread safety and testability.

    `agent`, `enable_mcp`, `group_id` and `reference_task_id` are ACCEPTED AND
    IGNORED: nothing reads them, but the shipped web client still sends
    `agent` and `group_id` and `chat_handler` rejects unknown fields, so
    removing them would turn a working client into a -4 on every message.
    They stay as compatibility fields until the clients stop sending them
    (`internal/frontend-single-agent-2026-09-05.md`). There is one agent;
    `provider` picks the model.
    """
    
    def __init__(
        self,
        question            : str = "",
        query_user_id       : str = "",
        agent               : str = "",
        provider            : str = "",
        enable_mcp          : int = 1,

        session_id          : str = "",
        question_id         : str = "",
        trace_id            : str = "",
        reference_task_id   : str = "",

        file_list           : list[ChatFileObject] | None = None,
        prompt_name         : str = "",

        user_id             : str = "",
        user_name           : str = "",
        group_id            : str = "",
        
        token               : str = "",
        language            : str = "",
        timezone            : str = "",

        scene               : str | None = None
    ):
        self.question       = question
        self.query_user_id  = query_user_id
        self.agent          = agent
        self.provider       = provider
        self.enable_mcp     = enable_mcp

        self.session_id     = session_id
        self.question_id    = question_id
        self.msg_id         = question_id
        self.trace_id       = trace_id
        self.reference_task_id = reference_task_id

        self.file_list      = file_list
        self.prompt_name    = prompt_name

        self.user_id        = user_id
        self.user_name      = user_name
        self.group_id       = group_id
        
        self.token          = token
        self.language       = language
        self.timezone       = timezone

        self.scene          = scene
        
        # files_data: Downloaded file content (set by HTTP adapter to avoid re-downloading)
        # List of dicts with 'content' (bytes), 'filename', 'content_type', 's3_key'
        self.files_data     : list[dict[str, Any]] | None = None

#-----------------------------------------------------------------------------


def has_attachment(file_list: Any) -> bool:
    """True when this turn really carries a file the agent can reach.

    Truthiness of `file_list` is not the same question. `[{}]` and `"x"` are
    both truthy and neither names a file: `/uploads/` would be empty and
    `_attachment_reminder` would return None, while the guard had already let
    the turn through and the stand-in question had already promised the model
    an attachment. `file_key` is what the whole upload path keys on, so it is
    what counts here. Entries arrive as dicts from the JSON body; the
    `ChatFileObject` branch is for callers that build the request in Python.
    """
    if not isinstance(file_list, (list, tuple)):
        return False
    for f in file_list:
        if isinstance(f, dict):
            if f.get("file_key"):
                return True
        elif getattr(f, "file_key", ""):
            return True
    return False

#-----------------------------------------------------------------------------

class UserInfo(BaseModel):
    user_id: str = Field(..., description="User ID")
    user_name: str = Field(..., description="User Name")

#-----------------------------------------------------------------------------
