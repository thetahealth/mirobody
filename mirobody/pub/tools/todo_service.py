"""
Todo Service - MCP Tools for task management.

Provides todo/task list management capability:
- write_todos: Create and manage a structured task list

Todo data is persisted in workspace at /todos/todo_list.json
"""

import json
import logging
from typing import Any, Dict, List, Literal, Optional

from typing_extensions import TypedDict

from .files_utils import (
    WRITE_TODOS_TOOL_DESCRIPTION,
    func_description,
    get_backend,
    validate_user_info,
)

logger = logging.getLogger(__name__)

# Path for storing todo list in workspace
TODO_FILE_PATH = "/todos/todo_list.json"


class Todo(TypedDict):
    """A single todo item with content and status."""

    content: str
    """The content/description of the todo item."""

    status: Literal["pending", "in_progress", "completed"]
    """The current status of the todo item."""


class TodoService:
    """
    Todo Service - MCP Tools for task management.

    Provides task list management capabilities with persistence.
    All methods receive user_info parameter (auto-injected by MCP framework).

    user_info structure:
        {
            "user_id": str,      # User identifier (required)
            "session_id": str,   # Session identifier for workspace isolation (optional)
            "token": str,        # Optional auth token
            ...
        }

    Note:
        If session_id is not provided in user_info, the default "mcp" session is used.
        This provides a shared workspace per user for all MCP tool calls.
        Todo data is stored at /todos/todo_list.json
    """

    TODO_FILE_PATH = TODO_FILE_PATH

    def __init__(self):
        self.name = "Todo Service"
        self.version = "1.0.0"
        logger.info("TodoService initialized")

    def _coerce_json_value(self, value: Any, max_depth: int = 3) -> tuple[Any, bool]:
        """
        Best-effort JSON coercion for tool inputs that may be string-encoded multiple times.

        Returns:
            Tuple of (coerced_value, had_parse_error)
        """
        current = value

        for _ in range(max_depth):
            if not isinstance(current, str):
                return current, False

            candidate = current.strip()
            if not candidate:
                return candidate, True

            try:
                current = json.loads(candidate)
                continue
            except (json.JSONDecodeError, TypeError):
                if len(candidate) >= 2 and candidate[0] == candidate[-1] and candidate[0] in {'"', "'"}:
                    current = candidate[1:-1]
                    continue
                return candidate, True

        return current, False

    def _validate_todos(self, todos: Any) -> tuple[bool, str, List[Todo]]:
        """
        Validate and normalize todo list structure.

        Returns:
            Tuple of (is_valid, error_message, validated_todos)
        """
        todos, parse_error = self._coerce_json_value(todos)

        if isinstance(todos, dict) and "todos" in todos:
            todos, nested_parse_error = self._coerce_json_value(todos["todos"])
            parse_error = parse_error or nested_parse_error

        if parse_error and not isinstance(todos, list):
            return False, "todos must be a list (received unparseable string)", []
        if not isinstance(todos, list):
            return False, "todos must be a list", []

        validated_todos: List[Todo] = []
        valid_statuses = {"pending", "in_progress", "completed"}

        for i, todo in enumerate(todos):
            if not isinstance(todo, dict):
                return False, f"Todo item {i} must be a dictionary", []

            content = todo.get("content")
            status = todo.get("status")

            if not content or not isinstance(content, str):
                return False, f"Todo item {i} must have a non-empty 'content' string", []

            if status not in valid_statuses:
                return False, f"Todo item {i} has invalid status '{status}'. Must be one of: {valid_statuses}", []

            validated_todos.append(Todo(content=content, status=status))

        return True, "", validated_todos

    # =========================================================================
    # MCP Tools
    # =========================================================================

    @func_description(WRITE_TODOS_TOOL_DESCRIPTION)
    async def write_todos(
        self,
        todos: List[Dict[str, Any]],
        user_info: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> str:
        is_valid, user_id, error = validate_user_info(user_info)
        if not is_valid:
            logger.warning(f"write_todos auth failed: {error}")
            return f"Error: {error}"

        # Validate todos structure
        is_valid, error, validated_todos = self._validate_todos(todos)
        if not is_valid:
            return f"Error: {error}"

        try:
            backend = get_backend(user_info)

            logger.info(f"Writing {len(validated_todos)} todo(s) for user: {user_id}")

            # Persist to storage
            todos_data = [{"content": t["content"], "status": t["status"]} for t in validated_todos]
            json_content = json.dumps(todos_data, ensure_ascii=False, indent=2)
            await backend.awrite(self.TODO_FILE_PATH, json_content)

            return f"Updated todo list to {validated_todos}"

        except Exception as e:
            logger.error(f"write_todos failed: {e}")
            return f"Error: {e}"
