"""
PostgreSQL Backend for DeepAgent

Provides persistent file storage using PostgreSQL with intelligent file parsing.
"""

from .postgres_backend import PostgresBackend
from .file_parser import FileParser

__all__ = ["PostgresBackend", "FileParser", "create_postgres_backend"]


def create_postgres_backend(
    session_id: str,
    user_id: str,
    file_parser=None,
    cache_ttl: int = 300,
    cache_maxsize: int = 100,
):
    """
    Create a PostgresBackend instance with automatic FileParser initialization.
    
    Args:
        session_id: Session ID for namespace isolation
        user_id: User ID for namespace isolation
        file_parser: FileParser instance (optional, auto-created if None)
        cache_ttl: Cache TTL in seconds (default: 300)
        cache_maxsize: Maximum cache entries (default: 100)
        
    Returns:
        PostgresBackend instance
        
    Example:
        >>> from mirobody.pub.agents.deep.backends import create_postgres_backend
        >>> 
        >>> # Auto-create FileParser
        >>> backend = create_postgres_backend(
        ...     session_id="session123",
        ...     user_id="user456"
        ... )
        >>> 
        >>> # Or provide custom FileParser
        >>> from mirobody.pub.agents.deep.backends import FileParser
        >>> parser = FileParser()
        >>> backend = create_postgres_backend(
        ...     session_id="session123",
        ...     user_id="user456",
        ...     file_parser=parser
        ... )
    """
    from .postgres_store import PostgresLangGraphStore
    
    # Auto-create FileParser if not provided
    if file_parser is None:
        file_parser = FileParser()
    
    store = PostgresLangGraphStore()
    
    return PostgresBackend(
        session_id=session_id,
        user_id=user_id,
        store=store,
        file_parser=file_parser,
        cache_ttl=cache_ttl,
        cache_maxsize=cache_maxsize,
    )
