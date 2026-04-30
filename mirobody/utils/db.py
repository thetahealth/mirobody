import logging, time

from sqlalchemy import text

from .config import global_config

#-----------------------------------------------------------------------------

global_engines  = {}


def _summarize_for_log(v, max_len: int = 512, max_rows: int = 5):
    """Truncate a value so an error log line can't blow up to MBs or leak
    full row contents (e.g. plaintext fed to encrypt_content)."""
    if isinstance(v, str):
        return v if len(v) <= max_len else v[:max_len] + "..."
    if isinstance(v, list):
        if len(v) > max_rows:
            return f"[{len(v)} rows]"
        return [_summarize_for_log(x, max_len, max_rows) for x in v]
    if isinstance(v, dict):
        return {k: _summarize_for_log(val, max_len, max_rows) for k, val in v.items()}
    return v


async def execute_query(
    query       : str,
    params      : dict | list[dict] | None = None,
    db_config   : str = "",
    trace_id    : str = "",
    log_sql     : bool = True,
    **kwargs
):
    # Check SQL statement.
    if not query:
        raise ValueError("SQL script cannot be empty")

    # Engine cache: this check-then-set is safe only because every call below
    # (global_config / get_postgresql / get_async_engine) is synchronous, so
    # asyncio cannot switch coroutines mid-block. If any of them ever becomes
    # async, two coroutines could both miss the cache and create duplicate
    # engines (the loser leaks its connection pool) — add a lock at that point.
    if db_config in global_engines:
        engine = global_engines[db_config]
    else:
        config = global_config()
        if not config:
            raise ValueError("no configuration found")

        engine = config.get_postgresql(db_config).get_async_engine()
        global_engines[db_config] = engine

    #-----------------------------------------------------

    start_time = time.perf_counter()
    try:
        # async with engine.begin() handles commit on success, rollback on
        # exception, and close in both cases. Don't reintroduce manual
        # commit/rollback/close here.
        async with engine.begin() as conn:
            # params=list[dict] triggers SQLAlchemy executemany; dict/None binds once.
            cur = await conn.execute(text(query), params)

            # Decide branch by whether the cursor actually has a result set,
            # not by lexically matching the SQL prefix — that breaks for
            # `WITH ... UPDATE`, `UPDATE ... RETURNING`, etc.
            if cur.returns_rows:
                # SELECT or DML...RETURNING — both return list[dict].
                # Callers expecting a single row should do `result[0]` after a
                # truthy check (empty list is falsy). This avoids silently
                # dropping extra rows from bulk INSERT/UPDATE/DELETE...RETURNING.
                ret = [dict(row._mapping) for row in cur.fetchall()]

            elif isinstance(params, list):
                # cur.rowcount under executemany is per-driver unreliable —
                # some report only the last execution. Use the input batch
                # size, which is what the caller actually submitted.
                ret = {"record_count": len(params)}

            else:
                ret = {"record_count": cur.rowcount}

        end_time = time.perf_counter()
        extra = {
            "records"   : len(ret) if isinstance(ret, list) else ret["record_count"],
            "time_cost" : round((end_time-start_time)*1e3, 2)
        }
        if trace_id:
            extra["trace_id"] = trace_id

        if log_sql:
            logged_query = " ".join(query.split())
            if len(logged_query) > 512:
                logged_query = logged_query[:512] + "..."
            logging.info(logged_query, extra=extra, stacklevel=2)

        return ret

    except Exception as e:
        end_time = time.perf_counter()
        extra = {
            "sql"       : _summarize_for_log(" ".join(query.split())),
            "params"    : _summarize_for_log(params),
            "time_cost" : round((end_time-start_time)*1e3, 2)
        }
        if trace_id:
            extra["trace_id"] = trace_id

        logging.error(str(e), extra=extra, stacklevel=2, exc_info=True)

        raise


#-----------------------------------------------------------------------------
