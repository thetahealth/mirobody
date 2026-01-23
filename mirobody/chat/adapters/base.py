"""
Base adapter class for chat protocols

Defines the interface and common logic that all protocol adapters share.
Subclasses implement protocol-specific details (HTTP SSE, WebSocket, etc.).

All user_id parameters are consistently typed as str throughout.
"""

import asyncio, json, logging

from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator

from mirobody.chat.unified_chat_service import UnifiedChatService
from mirobody.chat.message import create_message
from mirobody.chat.task import get_reference_task_detail

from ..message import (
    save_message,
    get_last_message,
    chat_list_setter_hollywell
)
from ..model import ChatStreamRequest

from ...utils import get_query_user_id

#-----------------------------------------------------------------------------
# Constants
#-----------------------------------------------------------------------------

class PERMISSION_ENUM:
    """Permission type identifiers"""
    chat = "chat"


class PERMISSION_LEVEL_ENUM:
    """Permission level values"""
    no_permission = 0
    read = 1
    write = 2


class CHUNK_TYPE_ENUMS:
    """Chunk type identifiers for streaming responses"""
    
    reply = "reply"
    queryTitle = "queryTitle"
    queryArguments = "queryArguments"
    queryDetail = "queryDetail"
    thinking = "thinking"
    report = "report"
    image = "image"
    food_snap = "food_snap"
    costStatistics = "costStatistics"

    non_streaming_types = {
        "food_snap" : 1,
        "report"    : 1
    }
    
    @classmethod
    def get_streaming_types(cls):
        """Types that should be forwarded to frontend"""
        return [cls.reply, cls.queryTitle, cls.queryArguments, cls.queryDetail, cls.thinking, cls.costStatistics, cls.image]
    
    @classmethod
    def get_thinking_types(cls):
        """Types considered as 'thinking' content"""
        return [cls.thinking, cls.queryDetail, cls.queryArguments, cls.queryTitle]

#-----------------------------------------------------------------------------
# Base Adapter
#-----------------------------------------------------------------------------

class ChatProtocolAdapter(ABC):
    """
    Abstract base class for chat protocol adapters.
    
    Provides common logic for all protocols:
    - Permission validation
    - Message saving (user question, assistant response)
    - Message history retrieval
    - Chat extraction logging
    
    Subclasses must implement:
    - handle_request(): Protocol-specific request handling
    - stream_output(): Protocol-specific output formatting
    - get_session_id(): Protocol-specific session ID extraction
    
    Note: All user_id parameters are typed as str for consistency.
    """
    
    def __init__(self, chat_service: UnifiedChatService):
        """
        Initialize adapter with the chat service.
        
        Args:
            chat_service: Instance of UnifiedChatService
        """
        self.chat_service: UnifiedChatService = chat_service
        self.scene: str = "web"  # Default scene, subclasses can override
    
    # =========================================================================
    # Abstract methods - must be implemented by subclasses
    # =========================================================================
    
    @abstractmethod
    async def handle_request(
        self, 
        params: ChatStreamRequest, 
        **kwargs
    ) -> AsyncGenerator[str, None]:
        """
        Main entry point for handling a chat request.
        
        Args:
            params: Chat request parameters (includes user_id as str)
            **kwargs: Additional protocol-specific arguments
            
        Yields:
            Protocol-specific formatted output (e.g., SSE strings for HTTP)
        """
        pass
    
    @abstractmethod
    async def stream_output(
        self,
        chunks: AsyncGenerator[dict[str, Any], None],
        context: dict[str, Any]
    ) -> AsyncGenerator[str, None]:
        """
        Convert unified output chunks to protocol-specific format.
        
        Args:
            chunks: Raw chunks from UnifiedChatService
            context: Request context for background processing
            
        Yields:
            Protocol-specific formatted output
        """
        pass
    
    @abstractmethod
    def get_session_id(self, params: ChatStreamRequest) -> str:
        """
        Extract session_id for database storage.
        
        Different protocols may use different session ID sources:
        - HTTP: Uses params.session_id
        - WebSocket: May use trace_id
        
        Args:
            params: Chat request parameters
            
        Returns:
            Session ID string
        """
        pass
    
    # =========================================================================
    # Common methods - shared by all protocols
    # =========================================================================
    
    def get_scene(self) -> str:
        """
        Get the scene identifier for this adapter.
        
        Returns:
            Scene string (e.g., 'web' for HTTP)
        """
        return self.scene
    
    #-------------------------------------------------------------------------

    async def validate_permissions(
        self,
        params: ChatStreamRequest,
        user_id: str,
        **kwargs
    ) -> bool:
        """
        Validate help-ask permissions.
        
        In help-ask scenarios, user A can query user B's data if they have
        the appropriate chat permissions.
        
        Args:
            params: Chat request parameters
            user_id: Current user ID (str)
            **kwargs: Additional arguments (e.g., token for HTTP)
            
        Returns:
            True if permissions are valid, False otherwise
        """        
        # If not help-ask scenario (querying own data), always allow
        if not params.query_user_id or params.query_user_id == user_id:
            return True
        
        # Validate help-ask permissions
        permission_kwargs = {"permission": [PERMISSION_ENUM.chat]}
        if "token" in kwargs:
            permission_kwargs["token"] = kwargs["token"]
            
        permission_result = await get_query_user_id(
            params.query_user_id, 
            user_id,
            **permission_kwargs
        )
        
        if not permission_result.get("success"):
            logging.error(f"Permission validation failed: {permission_result.get('error')}")
            return False
            
        permissions = permission_result.get("permissions", {})
        if permissions.get(PERMISSION_ENUM.chat, 0) < PERMISSION_LEVEL_ENUM.read:
            logging.error(f"Insufficient chat permissions for user {user_id} to query {params.query_user_id}")
            return False
        
        return True
    
    #-------------------------------------------------------------------------

    async def save_user_question(
        self,
        params: ChatStreamRequest,
        user_id: str
    ) -> str:
        """
        Save user question to database.
        
        Args:
            params: Chat request parameters
            user_id: User ID (str)
            
        Returns:
            The saved message ID
        """        
        return await save_message(
            user_id=user_id,
            query_user_id=params.query_user_id or user_id,
            content=params.question,
            role='user',
            session_id=self.get_session_id(params),
            scene=self.get_scene(),
            agent=params.agent,
            message_type="text",
            msg_id=params.msg_id,
            provider=params.provider
        )
    
    #-------------------------------------------------------------------------

    async def save_assistant_response(
        self,
        reply_id: str,
        params: ChatStreamRequest,
        user_id: str,
        content: Any,
        question_msg_id: str
    ) -> str:
        """
        Save assistant response to database.
        
        Args:
            reply_id: Response message ID
            params: Chat request parameters
            user_id: User ID (str)
            content: Response content (element_list)
            question_msg_id: ID of the question this responds to
            
        Returns:
            The saved message ID
        """        
        return await save_message(
            user_id=user_id,
            query_user_id=params.query_user_id or user_id,
            content=content,
            role='assistant',
            session_id=self.get_session_id(params),
            scene=self.get_scene(),
            agent=params.agent,
            msg_id=reply_id,
            question_id=question_msg_id,
            message_type="text",
            provider=params.provider  
        )
    
    #-------------------------------------------------------------------------

    async def get_message_history(
        self,
        user_id: str,
        query_user_id: str | None = None,
        session_id: str | None = None,
        scene: str | None = None
    ) -> list:
        """
        Get message history for a session.
        
        Processes raw messages to:
        1. Extract file information from JSON content
        2. Filter consecutive assistant messages
        3. Remove trailing assistant message for web scene
        
        Args:
            user_id: User ID (str)
            query_user_id: Query user ID for help-ask scenarios
            session_id: Session ID
            scene: Scene identifier (uses adapter's scene if not provided)
            
        Returns:
            Processed list of messages
        """
        effective_scene = scene or self.get_scene()
        messages = await get_last_message(user_id, query_user_id, session_id, scene=effective_scene)
        
        # Process messages to extract files from JSON content
        new_messages = []
        for m in messages:
            content = m.get("content", "")
            role = m.get("role", "user")
            
            try:
                data = json.loads(content)
                if role == "user" and "files" in data:
                    files = data["files"]
                    new_msg = dict(
                        role="user",
                        files=[dict(
                            s3_key=f["file_key"],
                            file_name=f["filename"],
                            file_type=f["type"]
                        ) for f in files],
                        type="file"
                    )
                    new_messages.append(new_msg)
                else:
                    new_messages.append(m)
            except (json.JSONDecodeError, TypeError):
                new_messages.append(m)
        
        # Filter out consecutive assistant messages, keeping only the last one in each sequence
        filtered_messages = []
        for i, msg in enumerate(new_messages):
            if msg.get("role") != "assistant":
                filtered_messages.append(msg)
            else:
                # Keep this assistant message if it's the last or next is not assistant
                if i == len(new_messages) - 1 or new_messages[i + 1].get("role") != "assistant":
                    filtered_messages.append(msg)
                else:
                    logging.debug(f"Skipping consecutive assistant message at index {i}")
        
        return filtered_messages
    
    #-------------------------------------------------------------------------

    async def log_chat_extraction(
        self,
        params: ChatStreamRequest,
        user_id: str,
        msg_id: str
    ):
        """
        Log chat for extraction (fire-and-forget).
        
        Only logs for self-queries (not help-ask scenarios).
        
        Args:
            params: Chat request parameters
            user_id: User ID (str)
            msg_id: Message ID
        """
        if params.question and (params.query_user_id == user_id):
            asyncio.create_task(
                chat_list_setter_hollywell(
                    user_id=user_id,
                    msg_id=msg_id,
                    question=params.question
                )
            )
    
    #-------------------------------------------------------------------------
    async def prepare_unified_input(
        self,
        params: ChatStreamRequest,
        user_id: int,
        messages: list,
    ) -> dict[str, Any]:
        """
        Prepare input for UnifiedChatService (common logic)
        """

        messages = messages + [dict(role="user", content=params.question)] 

        return {
            'user_id': str(user_id),
            'query_user_id': params.query_user_id or str(user_id),
            'content': params.question,
            'agent': params.agent,
            'messages': messages,
            'enable_mcp': bool(params.enable_mcp),
            'trace_id': params.trace_id,
            'session_id': self.get_session_id(params),
            'group_id': params.group_id,
            'file_list': params.file_list,
            'files_data': getattr(params, 'files_data', None),  # Pass downloaded file content
            'prompt_name': params.prompt_name,
            'token': params.token or '',
            'language': params.language or '',
            'timezone': params.timezone or '',
            'provider': params.provider
        }
    
    #-------------------------------------------------------------------------
    async def _handle_reference_task(self, reference_task_id, params: ChatStreamRequest, user_id: int, need_create: bool = True):
        """
        Handle today task reference if specified
        """
        task_detail, task_recommend_question = await get_reference_task_detail(reference_task_id, user_id)
        if need_create and task_recommend_question:
            if task_recommend_question:
                content = json.dumps([dict(type="reply", content=task_recommend_question)], ensure_ascii=False)
                await create_message(
                    user_id, params.trace_id, content,
                    params.agent, query_user_id=params.query_user_id, 
                    role='assistant', reference_task_id=reference_task_id
                )

                return [
                    dict(role="user", content=f"<system-reminder>User has performed a data input action, and here is the result:\n{task_detail}</system-reminder>"),
                    dict(role="assistant", content=task_recommend_question)
                ]
            else:
                return []
        else:
            return [
                dict(role="user", content=f"<system-reminder>User has performed a data input action, and here is the result:\n{task_detail}</system-reminder>")
            ] if task_detail else []

#-----------------------------------------------------------------------------
