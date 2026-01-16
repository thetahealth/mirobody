"""
PostgreSQL Backend for DeepAgent

Provides persistent file storage using PostgreSQL with intelligent file parsing.
"""

from .postgres_backend import PostgresBackend

__all__ = ["PostgresBackend", "create_postgres_backend"]


def create_postgres_backend(
    session_id: str,
    user_id: str,
    file_parser=None,
    cache_ttl: int = 300,
    cache_maxsize: int = 100,
):
    """
    Create a PostgresBackend instance.
    
    Args:
        session_id: Session ID for namespace isolation
        user_id: User ID for namespace isolation
        file_parser: FileParser instance for intelligent parsing (optional)
        cache_ttl: Cache TTL in seconds (default: 300)
        cache_maxsize: Maximum cache entries (default: 100)
        
    Returns:
        PostgresBackend instance
        
    Example:
        >>> from mirobody.pub.agents.deep.backends import create_postgres_backend
        >>> from mirobody.pub.agents.deep.files.file_parsers import FileParser
        >>> 
        >>> file_parser = FileParser(llm_client="gpt-4o")
        >>> backend = create_postgres_backend(
        ...     session_id="session123",
        ...     user_id="user456",
        ...     file_parser=file_parser
        ... )
    """
    from .postgres_store import PostgresLangGraphStore
    
    store = PostgresLangGraphStore()
    
    return PostgresBackend(
        session_id=session_id,
        user_id=user_id,
        store=store,
        file_parser=file_parser,
        cache_ttl=cache_ttl,
        cache_maxsize=cache_maxsize,
    )
