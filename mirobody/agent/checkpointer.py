"""LangGraph Postgres checkpointer: the agent's conversation memory.

This replaces a hand-rolled replay layer. Before this module, every turn
rebuilt the agent's message list by parsing the persisted transcript (the
blocks the SSE stream emits) back into LangChain ``AIMessage``/``ToolMessage``
objects: 424 lines of it, since deleted along with the agent that needed it.
That is precisely what a checkpointer does, except the round trip was lossy by
construction: the wire format is shaped for a UI, so reasoning never survived
it, tool arguments came back as re-parsed JSON strings, and anything the stream
did not model was simply gone.

Now the graph is compiled with ``checkpointer=`` and invoked with
``thread_id = thread_for(owner, session_id)``, so LangGraph persists the REAL message objects and
supplies the history itself. The caller passes only the NEW turn's message
(verified: turn 2 hands in 1 message and the model sees 3; a fresh thread_id
sees only its own).

``th_messages`` keeps its job and loses one: it remains the durable, queryable
projection that ``/api/history`` and session sharing render, but it is no
longer fed back into the agent. That split (checkpointer for execution state,
own table for the readable transcript) is the standard production shape.

Lifecycle: the saver and its pool are process singletons (the graph itself is
rebuilt per request), made by the first turn that needs them and kept for the
life of the process.
"""

from __future__ import annotations

import asyncio
import logging

from psycopg_pool import AsyncConnectionPool

from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)

_saver = None
# Set once the first attempt fails, so a broken setup costs the connect timeout
# ONCE rather than on every turn. The realistic failure is permanent for the
# life of the process (missing dependency, or a DB role without DDL rights);
# a database that is genuinely down takes the whole app with it anyway, since
# the turn cannot be persisted to th_messages either. Restart to retry.
_unavailable = False
# Turns that arrive while the first one builds the saver wait for it: without
# this, each built its own pool, and a turn could take a saver whose setup()
# had not finished, or whose pool a failed setup was about to close.
_starting = asyncio.Lock()

_CONNECT_TIMEOUT_SECONDS = 5


def _build_pool() -> AsyncConnectionPool:
    """A dedicated pool for the checkpoint tables.

    It mirrors ``PostgreSQLConfig.get_async_pool`` (same conninfo shape, same
    ``search_path``) but is built here rather than reused, for two reasons:
    ``autocommit=True`` (``AsyncPostgresSaver.setup()`` runs DDL and the shared
    pool does not enable it) and no ``app.encryption_key`` option, which the
    checkpoint tables never need. ``PostgreSQLConfig.schema`` already has
    ``public`` appended, and libpq's ``options`` is whitespace-delimited so the
    comma-joined value must stay space-free.
    """
    from mirobody.utils.config import global_config

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
    stateless graph: the turn still answers, it just has no cross-turn memory,
    which is strictly better than failing the request outright.
    """
    global _saver, _unavailable
    if _saver is not None or _unavailable:
        return _saver
    async with _starting:
        if _saver is None and not _unavailable:
            # Published only once `setup()` has succeeded.
            _saver = await _start()
            _unavailable = _saver is None
    return _saver


async def _start():
    """A saver whose tables exist, or None when there can be none."""
    # Opt-out switch. `setup()` below issues CREATE TABLE, which is consistent
    # with how this project already provisions its schema at startup
    # (`server/bootstrap.create_schema`, same DB user, and compose.yaml's PG
    # even runs `CREATE EXTENSION vector`), but a deployment whose agent DB
    # role is DDL-less, or that wants to stage the rollout, sets
    # `AGENT_CHECKPOINTER: false` and gets the pre-checkpointer behaviour
    # (stateless turns) with no code change.
    from mirobody.utils.config import safe_read_cfg

    if (safe_read_cfg("AGENT_CHECKPOINTER", "true") or "true").strip().lower() in ("false", "0", "off", "no"):
        logger.info("agent checkpointer disabled by AGENT_CHECKPOINTER; turns will be stateless")
        return None

    try:
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
    except ImportError:
        logger.warning(
            "langgraph-checkpoint-postgres is not installed; the agent will run "
            "without cross-turn memory. Install the [app] extra."
        )
        return None

    pool = None
    try:
        pool = _build_pool()
        await pool.open()
        saver = AsyncPostgresSaver(pool)
        # Creates the checkpoint tables if absent; a no-op once they exist.
        await saver.setup()
    except Exception as e:
        logger.warning("checkpointer unavailable; running without cross-turn memory: error_type=%s",
                       type(e).__name__, exc_info=not is_driver_exception(e))
        if pool is not None:
            try:
                await pool.close()
            except Exception as close_error:
                logger.debug("checkpoint pool close failed: error_type=%s", type(close_error).__name__)
        return None
    logger.info("agent checkpointer ready")
    return saver


def thread_for(owner_id: str, session_id: str) -> str:
    """The checkpoint thread of one person's conversation.

    Keyed on the session's OWNER as well as its id, because the id arrives from
    the client: keyed on the id alone, anyone who posted another person's
    session id resumed that person's checkpoint. Measured 2026-09-28 on the
    seeded stack: a second account sent a known `session_id` and the model
    repeated the first account's LDL and HDL back to it, and whatever it wrote
    became part of the first account's next turn. The owner is the person
    asking, not the record's subject, so the conversation someone has about
    their mother is theirs, not hers. `90_retire.sql` re-keys threads written
    before this under their first message's sender.
    """
    return f"{owner_id}:{session_id}"


async def delete_thread(thread_id: str) -> None:
    """Erase the agent's copy of one conversation (`thread_for`).

    This is the checkpointer half of "delete this conversation". Without it, ``chat/session.delete_session`` would clear
    ``th_messages``/``th_sessions`` while the agent's own copy of the same turns
    (health questions and the tool results answering them) survived indefinitely
    under the session id. A user who deletes a conversation must
    have it deleted, not merely hidden from the history endpoint.

    Best-effort: a failure here is logged, never raised, so it cannot block the
    user-visible deletion that already succeeded.
    """
    if not thread_id:
        return
    saver = await get_checkpointer()
    if saver is None:
        return
    try:
        await saver.adelete_thread(str(thread_id))
    except Exception as e:
        logger.warning("could not delete a checkpoint thread: error_type=%s", type(e).__name__)
