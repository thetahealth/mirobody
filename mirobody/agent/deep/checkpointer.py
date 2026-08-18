"""LangGraph Postgres checkpointer — the agent's conversation memory.

This replaces a hand-rolled replay layer. Before this module, every turn
rebuilt the agent's message list by parsing the persisted UI chunk list
(``element_list``: the `reply`/`thinking`/`queryTitle`/`queryArguments`/
`queryDetail` dicts the SSE stream emits) back into LangChain
``AIMessage``/``ToolMessage`` objects — 424 lines of it, in what is now
``base/history_replay.py``. That is precisely what a checkpointer does, except
the round trip was
lossy by construction: the wire format is shaped for a UI, so `thinking` never
survived it, tool arguments came back as re-parsed JSON strings, and anything
the stream did not model was simply gone.

Now the graph is compiled with ``checkpointer=`` and invoked with
``thread_id = session_id``, so LangGraph persists the REAL message objects and
supplies the history itself. The caller passes only the NEW turn's message
(verified: turn 2 hands in 1 message and the model sees 3; a fresh thread_id
sees only its own).

``th_messages`` keeps its job and loses one: it remains the durable, queryable
projection that ``/api/history`` and session sharing render, but it is no
longer fed back into the agent. That split — checkpointer for execution state,
own table for the readable transcript — is the standard production shape.

Lifecycle: the pool/saver are process singletons (the graph itself is rebuilt
per request); ``close_checkpointer`` is for shutdown.
"""

from __future__ import annotations

import logging

from psycopg_pool import AsyncConnectionPool

logger = logging.getLogger(__name__)

_pool: AsyncConnectionPool | None = None
_saver = None
# Set once the first attempt fails, so a broken setup costs the connect timeout
# ONCE rather than on every turn. The realistic failure is permanent for the
# life of the process (missing dependency, or a DB role without DDL rights);
# a database that is genuinely down takes the whole app with it anyway, since
# the turn cannot be persisted to th_messages either. Restart to retry.
_unavailable = False

_CONNECT_TIMEOUT_SECONDS = 5


def _build_pool() -> AsyncConnectionPool:
    """A dedicated pool for the checkpoint tables.

    It mirrors ``PostgreSQLConfig.get_async_pool`` (same conninfo shape, same
    ``search_path``) but is built here rather than reused, for two reasons:
    ``autocommit=True`` — ``AsyncPostgresSaver.setup()`` runs DDL and the shared
    pool does not enable it — and no ``app.encryption_key`` option, which the
    checkpoint tables never need. ``PostgreSQLConfig.schema`` already has
    ``public`` appended, and libpq's ``options`` is whitespace-delimited so the
    comma-joined value must stay space-free.
    """
    from ...utils.config import global_config

    pg = global_config().get_postgresql()
    return AsyncConnectionPool(
        f"host={pg.host} port={pg.port} dbname={pg.database}",
        min_size=1,
        max_size=max(pg.maxconn // 2, 2),
        open=False,
        # Fail FAST rather than at the pool's 30s default. Measured against an
        # unreachable database: the first chat turn stalled the full 30 seconds
        # before degrading. Memory is a nice-to-have on any single turn, so a
        # few seconds is the most it may cost before we give up on it.
        timeout=_CONNECT_TIMEOUT_SECONDS,
        kwargs={
            "user": pg.user,
            "password": pg.password,
            "autocommit": True,
            "connect_timeout": _CONNECT_TIMEOUT_SECONDS,
            "options": f"-c search_path={pg.schema}",
        },
    )


async def get_checkpointer():
    """Process-wide ``AsyncPostgresSaver``, created on first use.

    Returns ``None`` if the checkpointer cannot be created (missing optional
    dependency or an unreachable database). A None checkpointer compiles a
    stateless graph: the turn still answers, it just has no cross-turn memory —
    which is strictly better than failing the request outright.
    """
    global _pool, _saver, _unavailable
    if _saver is not None:
        return _saver
    if _unavailable:
        return None

    # Opt-out switch. `setup()` below issues CREATE TABLE, which is consistent
    # with how this project already provisions its schema at startup
    # (`server/bootstrap.create_schema`, same DB user, and compose.yaml's PG
    # even runs `CREATE EXTENSION vector`) — but a deployment whose agent DB
    # role is DDL-less, or that wants to stage the rollout, sets
    # `AGENT_CHECKPOINTER: false` and gets the pre-checkpointer behaviour
    # (stateless turns) with no code change.
    from ...utils.config import safe_read_cfg

    if (safe_read_cfg("AGENT_CHECKPOINTER", "true") or "true").strip().lower() in ("false", "0", "off", "no"):
        logger.info("agent checkpointer disabled by AGENT_CHECKPOINTER; turns will be stateless")
        _unavailable = True
        return None

    try:
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
    except ImportError:
        logger.warning(
            "langgraph-checkpoint-postgres is not installed; the agent will run "
            "without cross-turn memory. Install the [agents] extra."
        )
        _unavailable = True
        return None

    try:
        _pool = _build_pool()
        await _pool.open()
        _saver = AsyncPostgresSaver(_pool)
        # Creates the checkpoint tables if absent; a no-op once they exist.
        await _saver.setup()
        logger.info("agent checkpointer ready")
        return _saver
    except Exception:
        logger.warning("checkpointer unavailable; running without cross-turn memory", exc_info=True)
        if _pool is not None:
            try:
                await _pool.close()
            except Exception:
                pass
        _pool = None
        _saver = None
        _unavailable = True
        return None


async def delete_thread(session_id: str) -> None:
    """Erase the agent's copy of one conversation.

    ``thread_id == session_id``, so this is the checkpointer half of "delete
    this conversation". Without it, ``chat/session.delete_session`` would clear
    ``th_messages``/``th_sessions`` while the agent's own copy of the same turns
    — health questions and the tool results answering them — survived
    indefinitely under the session id. A user who deletes a conversation must
    have it deleted, not merely hidden from the history endpoint.

    Best-effort: a failure here is logged, never raised, so it cannot block the
    user-visible deletion that already succeeded.
    """
    if not session_id:
        return
    saver = await get_checkpointer()
    if saver is None:
        return
    try:
        await saver.adelete_thread(str(session_id))
    except Exception:
        logger.warning("could not delete checkpoint thread %s", session_id, exc_info=True)


async def close_checkpointer() -> None:
    """Close the singleton pool. Call on process shutdown."""
    global _pool, _saver, _unavailable
    _saver = None
    _unavailable = False
    if _pool is not None:
        await _pool.close()
        _pool = None
