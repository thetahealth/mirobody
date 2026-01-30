import logging, time, re

from sqlalchemy import text

from .config import global_config

#-----------------------------------------------------------------------------

global_engines  = {}

async def execute_query(
    query       : str,
    params      : dict | None = None,
    db_config   : str = "",
    fieldList   : list | None = None,
    trace_id    : str = "",
    **kargs
):
    # Check SQL statement.
    if not query:
        raise ValueError("SQL script cannot be empty")
    
    if not isinstance(db_config, str):
        db_config = ""
    
    global global_engines
    if db_config in global_engines:
        # Get existing engine.
        engine = global_engines[db_config]
    else:
        # Check database configuration.
        config = global_config()
        if not config:
            raise ValueError("no configuration found")
    
        # Init a new engine.
        engine = config.get_postgresql(db_config).get_async_engine()

        # Record this engine.
        global_engines[db_config] = engine

    #-----------------------------------------------------

    lower_query = query.strip().lower()
    field_list_size = 0 if fieldList is None else len(fieldList)

    start_time = time.time()
    conn = None
    try:
        conn = await engine.connect()
        cur = await conn.execute(text(query), fieldList if field_list_size > 0 else params)

        if lower_query.startswith("insert"):
            # Insert many rows.
            if field_list_size > 0:
                ret = {"record_count": field_list_size}

            # Insert one row.
            elif "returning" in lower_query:
                row = cur.fetchone()
                if row is not None:
                    ret = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
                else:
                    ret = {}

            else:
                ret = {"record_count": cur.rowcount}

        elif not re.match("^(update|delete|insert|create|drop|alter|truncate).*", lower_query):

            rows = cur.fetchall()
            sqlDesc = cur.keys()

            result_data = []
            for row in rows:
                if hasattr(row, "_mapping"):  # SQLAlchemy 1.4+ Row objects
                    result_data.append(dict(row._mapping))

                elif hasattr(row, "keys"):  # Row-like objects
                    result_data.append({k: row[k] for k in row.keys()})

                else:  # Tuple/list-like results with column position
                    row_dict = {}
                    for i, col_name in enumerate(sqlDesc):
                        if i < len(row):
                            row_dict[col_name] = row[i]
                    result_data.append(row_dict)

            ret = result_data

        else:
            ret = {"record_count": cur.rowcount}

        await conn.commit()

        emd_time = time.time()
        extra = {
            "records"   : len(ret) if isinstance(ret, list) else cur.rowcount,
            "time_cost" : round((emd_time-start_time)*1e3, 2)
        }
        if trace_id:
            extra["trace_id"] = trace_id

        logged_query = " ".join(query.split())
        if len(logged_query) > 512:
            logged_query = logged_query[:512] + "..."
        logging.info(logged_query, extra=extra, stacklevel=2)

        return ret

    except Exception as e:
        if conn:
            await conn.rollback()

        emd_time = time.time()
        extra = {
            "sql"       : " ".join(query.split()),
            "params"    : params,
            "field_list": fieldList,
            "time_cost" : round((emd_time-start_time)*1e3, 2)
        }
        if trace_id:
            extra["trace_id"] = trace_id

        logging.error(str(e), extra=extra, stacklevel=2)

        raise

    finally:
        if conn:
            await conn.close()


#-----------------------------------------------------------------------------
