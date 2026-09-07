"""One `execute_query` for every Postgres access.

The contract every caller relies on:

* ``:name`` bound parameters, through SQLAlchemy ``text()``;
* a statement that returns rows (SELECT, ``… RETURNING``) gives ``list[dict]``;
* DML without a result set gives ``{"record_count": n}``;
* ``params`` is a dict (bind once) or a list of dicts (executemany).

Where the ENGINE comes from is the one thing that differs between deployments,
so it is injected: `use_engines(provider)` installs a callable
``(db_config) -> AsyncEngine``. The reference server installs nothing and gets
the default — ``global_config().get_postgresql(db_config).get_async_engine()``;
a consumer with its own DSN, ``search_path`` GUC and encryption key installs
its own at startup, and every module that imported `execute_query` keeps
working. Engines are cached here per ``db_config`` name; `reset_engines()`
forgets them after their owner has disposed them.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

_engines: dict[str, Any] = {}
_provider: Callable[[str], Any] | None = None


def use_engines(provider: Callable[[str], Any] | None) -> None:
    """Install how an engine is obtained for a ``db_config`` name (``None``
    restores the reference server's default). Forgets cached engines."""
    global _provider
    _provider = provider
    _engines.clear()


def reset_engines() -> None:
    """Forget the cached engines — call after disposing them."""
    _engines.clear()


def _default_provider(db_config: str) -> Any:
    from .config import global_config

    config = global_config()
    if not config:
        raise ValueError("no configuration found")
    return config.get_postgresql(db_config).get_async_engine()


def engine_for(db_config: str = "") -> Any:
    """The async engine for ``db_config``, built once per name.

    The check-then-set is safe only because every provider is synchronous, so
    asyncio cannot switch coroutines mid-block; a provider that awaits would
    need a lock here.
    """
    engine = _engines.get(db_config)
    if engine is None:
        engine = (_provider or _default_provider)(db_config)
        _engines[db_config] = engine
    return engine


def _summarize_sql(query: str, max_len: int = 512) -> str:
    text = " ".join(query.split())
    return text if len(text) <= max_len else text[:max_len] + "..."


async def execute_query(
    query: str,
    params: dict | list[dict] | None = None,
    db_config: str = "",
    trace_id: str = "",
    log_sql: bool = True,
    **kwargs,
):
    if not query:
        raise ValueError("SQL script cannot be empty")

    engine = engine_for(db_config)
    start = time.perf_counter()
    try:
        # Imported here rather than at module scope so that importing
        # `mirobody.utils.db` — which `mirobody.utils` re-exports from — does
        # not make SQLAlchemy a requirement of code that never opens a connection.
        from sqlalchemy import text

        # `engine.begin()` commits on success, rolls back on exception and
        # closes either way; no manual commit/rollback/close belongs here.
        async with engine.begin() as conn:
            # params=list[dict] is SQLAlchemy executemany; dict/None binds once.
            cur = await conn.execute(text(query), params)
            # Branch on whether the cursor HAS a result set, never on the SQL
            # prefix — `WITH … UPDATE`, `UPDATE … RETURNING` break the latter.
            if cur.returns_rows:
                ret: list[dict] | dict = [dict(row._mapping) for row in cur.fetchall()]
            elif isinstance(params, list):
                # rowcount under executemany is per-driver unreliable; the input
                # batch size is what the caller actually submitted.
                ret = {"record_count": len(params)}
            else:
                ret = {"record_count": cur.rowcount}

        if log_sql:
            extra: dict[str, Any] = {
                "records": len(ret) if isinstance(ret, list) else ret["record_count"],
                "time_cost": round((time.perf_counter() - start) * 1e3, 2),
            }
            if trace_id:
                extra["trace_id"] = trace_id
            logged_query = _summarize_sql(query)  # the statement text: placeholders, no values
            logger.info(logged_query, extra=extra, stacklevel=2)
        return ret

    except Exception as e:
        # Counts and a type only — never the parameter VALUES, which are the row
        # being written; the traceback and `stacklevel` name the statement, and
        # the INFO line above already carried its text when `log_sql` is on.
        extra = {
            "error_type": type(e).__name__,
            "param_count": len(params) if isinstance(params, list) else (len(params) if params else 0),
            "time_cost": round((time.perf_counter() - start) * 1e3, 2),
        }
        if trace_id:
            extra["trace_id"] = trace_id
        logger.error("execute_query failed", extra=extra, stacklevel=2, exc_info=True)
        raise
