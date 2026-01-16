"""
PostgreSQL Store Implementation for DeepAgent Backend.

This module provides a PostgreSQL-backed implementation of LangGraph's BaseStore
interface, enabling persistent storage for DeepAgent's files and state across
multiple conversation threads.

Table Structure:
- namespace: Full namespace string (e.g., "deep_agent/session_id/user_id")
- key: Item key/identifier
- value: JSONB data
- session_id: Extracted from namespace for efficient session queries
- user_id: Extracted from namespace for efficient user queries
- created_at, updated_at: Timestamps

The session_id and user_id fields are automatically extracted from the namespace
tuple for efficient querying and data management.
"""

import json
import logging
from typing import Any, Optional, Iterable

from langgraph.store.base import BaseStore, Item, Op, Result

from mirobody.utils.db import execute_query

logger = logging.getLogger(__name__)


class PostgresLangGraphStore(BaseStore):
    """
    PostgreSQL implementation of LangGraph BaseStore.
    
    Stores files and state in the theta_ai.deep_agent_store table,
    using namespace tuples for isolation and organization.
    
    Namespace format: ("deep_agent", session_id, user_id)
    
    The store automatically extracts session_id and user_id from the namespace
    for efficient querying, while maintaining full namespace compatibility with
    LangGraph's BaseStore interface.
    """
    
    def __init__(self):
        """Initialize the PostgreSQL store."""
        super().__init__()
        logger.info("PostgresLangGraphStore initialized")
    
    @staticmethod
    def _namespace_to_string(namespace: tuple[str, ...]) -> str:
        """Convert namespace tuple to string for storage."""
        return "/".join(namespace)
    
    @staticmethod
    def _string_to_namespace(namespace_str: str) -> tuple[str, ...]:
        """Convert stored namespace string back to tuple."""
        return tuple(namespace_str.split("/"))
    
    @staticmethod
    def _extract_session_user_from_namespace(namespace: tuple[str, ...]) -> tuple[Optional[str], Optional[str]]:
        """
        Extract session_id and user_id from namespace tuple.
        
        Expected format: ("deep_agent", session_id, user_id)
        
        Args:
            namespace: Namespace tuple
            
        Returns:
            Tuple of (session_id, user_id), with None for missing values
            
        Examples:
            ("deep_agent", "session123", "user456") -> ("session123", "user456")
            ("deep_agent", "session123") -> ("session123", None)
            ("deep_agent",) -> (None, None)
        """
        session_id = namespace[1] if len(namespace) > 1 else None
        user_id = namespace[2] if len(namespace) > 2 else None
        
        logger.debug(f"Extracted from namespace {namespace}: session_id={session_id}, user_id={user_id}")
        
        return session_id, user_id
    
    @staticmethod
    def _sanitize_for_postgres_json(data: Any, max_depth: int = 100, current_depth: int = 0) -> Any:
        """
        Comprehensive data sanitization for PostgreSQL JSON/JSONB storage.
        
        Handles multiple edge cases:
        1. Control characters (\u0000-\u001F) - PostgreSQL doesn't support these in JSON
        2. Invalid UTF-8 sequences - causes encoding errors
        3. Special float values (NaN, Infinity) - not valid in JSON standard
        4. Excessive string lengths - prevents memory issues
        5. Deep recursion - prevents stack overflow
        6. Non-serializable types - converts to safe representations
        
        Args:
            data: Data structure to sanitize
            max_depth: Maximum recursion depth (default: 100)
            current_depth: Current recursion level (internal use)
            
        Returns:
            Sanitized data structure safe for PostgreSQL JSON/JSONB
        """
        import math
        
        # Recursion depth protection
        if current_depth > max_depth:
            logger.warning(f"Max recursion depth ({max_depth}) reached during sanitization")
            return "[TRUNCATED: max depth exceeded]"
        
        # Handle None
        if data is None:
            return None
        
        # Handle dictionaries
        if isinstance(data, dict):
            cleaned = {}
            for k, v in data.items():
                try:
                    # Keys must be strings, sanitize them too
                    clean_key = PostgresLangGraphStore._sanitize_string(str(k))
                    clean_value = PostgresLangGraphStore._sanitize_for_postgres_json(
                        v, max_depth, current_depth + 1
                    )
                    cleaned[clean_key] = clean_value
                except Exception as e:
                    logger.warning(f"Failed to sanitize dict key '{k}': {e}")
                    continue
            return cleaned
        
        # Handle lists/tuples
        elif isinstance(data, (list, tuple)):
            cleaned = []
            for item in data:
                try:
                    clean_item = PostgresLangGraphStore._sanitize_for_postgres_json(
                        item, max_depth, current_depth + 1
                    )
                    cleaned.append(clean_item)
                except Exception as e:
                    logger.warning(f"Failed to sanitize list item: {e}")
                    continue
            return cleaned
        
        # Handle strings
        elif isinstance(data, str):
            return PostgresLangGraphStore._sanitize_string(data)
        
        # Handle numbers
        elif isinstance(data, (int, float)):
            return PostgresLangGraphStore._sanitize_number(data)
        
        # Handle booleans (must come after numbers since bool is subclass of int)
        elif isinstance(data, bool):
            return data
        
        # Handle bytes
        elif isinstance(data, bytes):
            try:
                # Try to decode as UTF-8
                return data.decode('utf-8', errors='ignore')
            except Exception:
                # Fallback to safe representation
                return f"[BYTES: {len(data)} bytes]"
        
        # Handle other types (datetime, objects, etc.)
        else:
            try:
                # Try to convert to string
                return str(data)
            except Exception as e:
                logger.warning(f"Failed to convert type {type(data)} to string: {e}")
                return f"[OBJECT: {type(data).__name__}]"
    
    @staticmethod
    def _sanitize_string(text: str, max_length: int = 50_000_000) -> str:
        """
        Sanitize a string for PostgreSQL JSON/JSONB storage.
        
        Removes:
        - NULL character (\u0000)
        - Other control characters (\u0001-\u001F, except \t, \n, \r)
        - Invalid UTF-8 sequences
        
        Args:
            text: String to sanitize
            max_length: Maximum allowed string length (default: 50MB)
            
        Returns:
            Sanitized string safe for PostgreSQL
        """
        if not text:
            return text
        
        # Truncate excessive length
        if len(text) > max_length:
            logger.warning(f"String truncated from {len(text)} to {max_length} characters")
            text = text[:max_length] + "...[TRUNCATED]"
        
        # Remove control characters except tab, newline, carriage return
        # PostgreSQL JSON doesn't like \u0000-\u001F except \t, \n, \r
        cleaned_chars = []
        removed_count = 0
        
        for char in text:
            code = ord(char)
            # Keep: tab(9), newline(10), carriage return(13), and normal characters (>=32)
            if code == 9 or code == 10 or code == 13 or code >= 32:
                cleaned_chars.append(char)
            else:
                # Skip control characters
                removed_count += 1
        
        if removed_count > 0:
            logger.debug(f"Removed {removed_count} control characters from string")
        
        cleaned = ''.join(cleaned_chars)
        
        # Ensure valid UTF-8 by encoding and decoding with error handling
        try:
            cleaned = cleaned.encode('utf-8', errors='ignore').decode('utf-8', errors='ignore')
        except Exception as e:
            logger.warning(f"UTF-8 sanitization failed: {e}")
        
        return cleaned
    
    @staticmethod
    def _sanitize_number(num: float | int) -> float | int | None:
        """
        Sanitize numeric values for JSON storage.
        
        JSON standard doesn't support NaN, Infinity, -Infinity.
        PostgreSQL will reject these values.
        
        Args:
            num: Number to sanitize
            
        Returns:
            Sanitized number or None for invalid values
        """
        import math
        
        if isinstance(num, float):
            if math.isnan(num):
                logger.debug("Converted NaN to None")
                return None
            elif math.isinf(num):
                if num > 0:
                    logger.debug("Converted Infinity to large number")
                    return 1.7976931348623157e+308  # sys.float_info.max
                else:
                    logger.debug("Converted -Infinity to large negative number")
                    return -1.7976931348623157e+308
        
        return num
    
    async def get(
        self, 
        namespace: tuple[str, ...], 
        key: str
    ) -> Optional[Item]:
        """
        Retrieve an item from the store.
        
        Args:
            namespace: Namespace tuple (e.g., ("deep_agent", session_id, user_id))
            key: Item key/identifier
            
        Returns:
            Item object if found, None otherwise
        """
        namespace_str = self._namespace_to_string(namespace)
        
        query = """
            SELECT key, value, created_at, updated_at
            FROM theta_ai.deep_agent_store
            WHERE namespace = :namespace AND key = :key
        """
        
        try:
            rows = await execute_query(
                query=query,
                params={"namespace": namespace_str, "key": key}
            )
            
            if not rows or len(rows) == 0:
                return None
            
            row = rows[0]
            
            # Parse value - it might be stored as JSON string or dict
            value = row.get("value")
            if isinstance(value, str):
                value = json.loads(value)
            
            # Create Item object
            return Item(
                value=value,
                key=key,
                namespace=namespace,
                created_at=row.get("created_at"),
                updated_at=row.get("updated_at")
            )
            
        except Exception as e:
            logger.error(f"Failed to get item {namespace}/{key}: {e}", exc_info=True)
            return None
    
    async def put(
        self,
        namespace: tuple[str, ...],
        key: str,
        value: dict[str, Any]
    ) -> None:
        """
        Store or update an item.
        
        Args:
            namespace: Namespace tuple
            key: Item key
            value: Item value (must be JSON-serializable dict)
        """
        namespace_str = self._namespace_to_string(namespace)
        session_id, user_id = self._extract_session_user_from_namespace(namespace)
        
        query = """
            INSERT INTO theta_ai.deep_agent_store 
                (namespace, key, value, session_id, user_id, created_at, updated_at)
            VALUES (:namespace, :key, :value, :session_id, :user_id, NOW(), NOW())
            ON CONFLICT (namespace, key)
            DO UPDATE SET 
                value = :value,
                session_id = :session_id,
                user_id = :user_id,
                updated_at = NOW()
        """
        
        try:
            # Comprehensive data sanitization for PostgreSQL JSON/JSONB
            # Handles: NULL chars, control chars, invalid UTF-8, NaN/Infinity, etc.
            cleaned_value = self._sanitize_for_postgres_json(value)
            
            # Serialize value to JSON string
            value_json = json.dumps(cleaned_value, default=str, ensure_ascii=False)
            
            await execute_query(
                query=query,
                params={
                    "namespace": namespace_str,
                    "key": key,
                    "value": value_json,
                    "session_id": session_id,
                    "user_id": user_id
                }
            )
            
            logger.debug(f"Stored item: {namespace_str}/{key} (session={session_id}, user={user_id})")
            
        except Exception as e:
            logger.error(f"Failed to put item {namespace}/{key}: {e}", exc_info=True)
            raise
    
    async def delete(
        self,
        namespace: tuple[str, ...],
        key: str
    ) -> None:
        """
        Delete an item from the store.
        
        Args:
            namespace: Namespace tuple
            key: Item key to delete
        """
        namespace_str = self._namespace_to_string(namespace)
        
        query = """
            DELETE FROM theta_ai.deep_agent_store
            WHERE namespace = :namespace AND key = :key
        """
        
        try:
            await execute_query(
                query=query,
                params={"namespace": namespace_str, "key": key}
            )
            
            logger.debug(f"Deleted item: {namespace_str}/{key}")
            
        except Exception as e:
            logger.error(f"Failed to delete item {namespace}/{key}: {e}", exc_info=True)
            raise
    
    async def search(
        self,
        namespace_prefix: tuple[str, ...]
    ) -> list[Item]:
        """
        Search for items matching a namespace prefix.
        
        Args:
            namespace_prefix: Namespace prefix to search for
            
        Returns:
            List of matching Item objects
        """
        namespace_str = self._namespace_to_string(namespace_prefix)
        
        # Use LIKE for prefix matching
        query = """
            SELECT namespace, key, value, created_at, updated_at
            FROM theta_ai.deep_agent_store
            WHERE namespace LIKE :namespace_pattern
            ORDER BY namespace, key
        """
        
        try:
            rows = await execute_query(
                query=query,
                params={"namespace_pattern": f"{namespace_str}%"}
            )
            
            if not rows:
                return []
            
            items = []
            for row in rows:
                # Parse value
                value = row.get("value")
                if isinstance(value, str):
                    value = json.loads(value)
                
                # Reconstruct namespace tuple
                namespace = self._string_to_namespace(row.get("namespace"))
                
                item = Item(
                    value=value,
                    key=row.get("key"),
                    namespace=namespace,
                    created_at=row.get("created_at"),
                    updated_at=row.get("updated_at")
                )
                items.append(item)
            
            logger.debug(f"Found {len(items)} items with prefix {namespace_str}")
            return items
            
        except Exception as e:
            logger.error(f"Failed to search namespace {namespace_prefix}: {e}", exc_info=True)
            return []
    
    async def list_namespaces(
        self,
        prefix: Optional[tuple[str, ...]] = None,
        suffix: Optional[tuple[str, ...]] = None,
        max_depth: Optional[int] = None,
        limit: int = 100,
        offset: int = 0
    ) -> list[tuple[str, ...]]:
        """
        List available namespaces.
        
        Args:
            prefix: Optional namespace prefix filter
            suffix: Optional namespace suffix filter (not implemented)
            max_depth: Maximum namespace depth (not implemented)
            limit: Maximum number of results
            offset: Result offset for pagination
            
        Returns:
            List of namespace tuples
        """
        # Build base query
        query = """
            SELECT DISTINCT namespace
            FROM theta_ai.deep_agent_store
        """
        
        params = {}
        conditions = []
        
        if prefix:
            prefix_str = self._namespace_to_string(prefix)
            conditions.append("namespace LIKE :prefix_pattern")
            params["prefix_pattern"] = f"{prefix_str}%"
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += f" ORDER BY namespace LIMIT {limit} OFFSET {offset}"
        
        try:
            rows = await execute_query(query=query, params=params)
            
            namespaces = [
                self._string_to_namespace(row.get("namespace"))
                for row in rows
            ]
            
            return namespaces
            
        except Exception as e:
            logger.error(f"Failed to list namespaces: {e}", exc_info=True)
            return []
    
    async def get_by_session(self, session_id: str) -> list[Item]:
        """
        Retrieve all items for a specific session.
        
        Uses the session_id index for efficient querying.
        
        Args:
            session_id: Session identifier
            
        Returns:
            List of Item objects belonging to the session
        """
        query = """
            SELECT namespace, key, value, created_at, updated_at
            FROM theta_ai.deep_agent_store
            WHERE session_id = :session_id
            ORDER BY namespace, key
        """
        
        try:
            rows = await execute_query(
                query=query,
                params={"session_id": session_id}
            )
            
            if not rows:
                return []
            
            items = []
            for row in rows:
                # Parse value
                value = row.get("value")
                if isinstance(value, str):
                    value = json.loads(value)
                
                # Reconstruct namespace tuple
                namespace = self._string_to_namespace(row.get("namespace"))
                
                item = Item(
                    value=value,
                    key=row.get("key"),
                    namespace=namespace,
                    created_at=row.get("created_at"),
                    updated_at=row.get("updated_at")
                )
                items.append(item)
            
            logger.info(f"Found {len(items)} items for session {session_id}")
            return items
            
        except Exception as e:
            logger.error(f"Failed to get items for session {session_id}: {e}", exc_info=True)
            return []
    
    async def get_by_user(self, user_id: str) -> list[Item]:
        """
        Retrieve all items for a specific user.
        
        Uses the user_id index for efficient querying.
        
        Args:
            user_id: User identifier
            
        Returns:
            List of Item objects belonging to the user
        """
        query = """
            SELECT namespace, key, value, created_at, updated_at
            FROM theta_ai.deep_agent_store
            WHERE user_id = :user_id
            ORDER BY namespace, key
        """
        
        try:
            rows = await execute_query(
                query=query,
                params={"user_id": user_id}
            )
            
            if not rows:
                return []
            
            items = []
            for row in rows:
                # Parse value
                value = row.get("value")
                if isinstance(value, str):
                    value = json.loads(value)
                
                # Reconstruct namespace tuple
                namespace = self._string_to_namespace(row.get("namespace"))
                
                item = Item(
                    value=value,
                    key=row.get("key"),
                    namespace=namespace,
                    created_at=row.get("created_at"),
                    updated_at=row.get("updated_at")
                )
                items.append(item)
            
            logger.info(f"Found {len(items)} items for user {user_id}")
            return items
            
        except Exception as e:
            logger.error(f"Failed to get items for user {user_id}: {e}", exc_info=True)
            return []
    
    async def delete_by_session(self, session_id: str) -> int:
        """
        Delete all items for a specific session.
        
        Useful for session cleanup and data management.
        
        Args:
            session_id: Session identifier
            
        Returns:
            Number of items deleted
        """
        query = """
            DELETE FROM theta_ai.deep_agent_store
            WHERE session_id = :session_id
        """
        
        try:
            result = await execute_query(
                query=query,
                params={"session_id": session_id}
            )
            
            # Note: execute_query returns rows, for DELETE we need rowcount
            # This assumes execute_query can handle DELETE and return affected rows
            logger.info(f"Deleted session data for session {session_id}")
            return 0  # Would need to modify execute_query to return rowcount
            
        except Exception as e:
            logger.error(f"Failed to delete session {session_id}: {e}", exc_info=True)
            raise
    
    async def delete_by_user(self, user_id: str) -> int:
        """
        Delete all items for a specific user.
        
        Useful for user data cleanup and GDPR compliance.
        
        Args:
            user_id: User identifier
            
        Returns:
            Number of items deleted
        """
        query = """
            DELETE FROM theta_ai.deep_agent_store
            WHERE user_id = :user_id
        """
        
        try:
            result = await execute_query(
                query=query,
                params={"user_id": user_id}
            )
            
            logger.info(f"Deleted all data for user {user_id}")
            return 0  # Would need to modify execute_query to return rowcount
            
        except Exception as e:
            logger.error(f"Failed to delete user data {user_id}: {e}", exc_info=True)
            raise
    
    def batch(self, ops: Iterable[Op]) -> list[Result]:
        """
        Execute multiple operations synchronously in a single batch.
        
        Note: Since all underlying methods are async, this method uses asyncio
        to run them synchronously. For better performance, use abatch() instead.
        
        Args:
            ops: Iterable of operations to execute
            
        Returns:
            List of results corresponding to each operation
        """
        import asyncio
        
        # Run async batch method synchronously
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # If we're already in an async context, we can't use run()
                # In this case, we'll need to handle it differently
                # For now, log a warning and return empty results
                logger.warning("batch() called from async context, use abatch() instead")
                return []
            else:
                return loop.run_until_complete(self.abatch(ops))
        except RuntimeError:
            # No event loop, create a new one
            return asyncio.run(self.abatch(ops))
    
    async def abatch(self, ops: Iterable[Op]) -> list[Result]:
        """
        Execute multiple operations asynchronously in a single batch.
        
        Args:
            ops: Iterable of operations to execute
            
        Returns:
            List of results corresponding to each operation
        """
        # Convert to list to allow multiple iterations
        ops_list = list(ops)
        results = []
        
        for op in ops_list:
            try:
                if op.op == "get":
                    result = await self.get(op.namespace, op.key)
                    results.append(result)
                elif op.op == "put":
                    await self.put(op.namespace, op.key, op.value)
                    results.append(None)
                elif op.op == "delete":
                    await self.delete(op.namespace, op.key)
                    results.append(None)
                else:
                    logger.warning(f"Unknown operation type: {op.op}")
                    results.append(None)
            except Exception as e:
                logger.error(f"Error executing batch operation {op.op}: {e}", exc_info=True)
                results.append(None)
        
        return results
