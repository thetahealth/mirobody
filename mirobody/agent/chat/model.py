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
    """
    Chat stream request from frontend.
    
    All parameters should be explicitly passed from the API request,
    avoiding implicit context dependencies for thread safety and testability.
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

class UserInfo(BaseModel):
    user_id: str = Field(..., description="User ID")
    user_name: str = Field(..., description="User Name")

#-----------------------------------------------------------------------------
