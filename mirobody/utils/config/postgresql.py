from __future__ import annotations

import logging, time
import psycopg, psycopg_pool, psycopg.abc
import sqlalchemy, sqlalchemy.event, sqlalchemy.ext, sqlalchemy.ext.asyncio

from typing import Any, Self


#-----------------------------------------------------------------------------

class LoggedCursor(psycopg.Cursor):
    def execute(
        self,
        query: psycopg.abc.Query,
        params: psycopg.abc.Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None
    ) -> Self:
        start_time = time.time()
        cur = super().execute(query, params, prepare=prepare, binary=binary)
        end_time = time.time()

        logging.info(
            " ".join(str(query).split()),
            extra = {
                "time_cost" : round((end_time-start_time)*1e3, 2),
                "params"    : params,
                "records"   : cur.rowcount
            },
            stacklevel = 2
        )

        return self


# NOTE: there is no sync `LoggedConnection` counterpart to
# `LoggedAsyncConnection` below. One existed and was referenced by nothing:
# `get_async_pool` passes `connection_class=LoggedAsyncConnection`, while the
# sync `get_pool` passes no connection_class at all, so the sync class only
# ever wired `LoggedCursor` for callers that did not exist. `LoggedCursor`
# itself stays — it is the default `cursor_factory` of `get_client`.

class LoggedAsyncCursor(psycopg.AsyncCursor):
    async def execute(
        self,
        query: psycopg.abc.Query,
        params: psycopg.abc.Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None
    ) -> Self:
        start_time = time.time()
        cur = await super().execute(query, params, prepare=prepare, binary=binary)
        end_time = time.time()

        logging.info(
            " ".join(str(query).split()),
            extra = {
                "time_cost" : round((end_time-start_time)*1e3, 2),
                "params"    : params,
                "records"   : cur.rowcount
            },
            stacklevel = 2
        )

        return self


class LoggedAsyncConnection(psycopg.AsyncConnection):
    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)
        self.cursor_factory = LoggedAsyncCursor

#-----------------------------------------------------------------------------

def before_sqlarchemy_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    conn.info.setdefault("query_start_time", []).append(time.time())


def after_async_sqlarchemy_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    start_time = conn.info["query_start_time"].pop(-1)
    time_cost = round((time.time()-start_time)*1e3, 2)

    logging.info(
        " ".join(statement.split()),
        extra = {
            "time_cost" : time_cost,
            "params"    : parameters,
            "records"   : cursor.rowcount
        },
        stacklevel = 9
    )

def after_sqlarchemy_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    start_time = conn.info["query_start_time"].pop(-1)
    time_cost = round((time.time()-start_time)*1e3, 2)

    logging.info(
        " ".join(statement.split()),
        extra = {
            "time_cost" : time_cost,
            "params"    : parameters,
            "records"   : cursor.rowcount
        },
        stacklevel = 8
    )

#-----------------------------------------------------------------------------

class PostgreSQLConfig:
    def __init__(
        self,
        user    : str,
        password: str,
        database: str,
        host    : str,
        port    : int = 0,
        schema  : str = "",
        minconn : int = 0,
        maxconn : int = 0,
        timeout : int = 0,
        encrypt_key: str = ""
    ):
        self.host       = host if host else "127.0.0.1"
        self.port       = port if port > 0 else 5432
        self.user       = user
        self.password   = password
        self.database   = database
        self.minconn    = minconn if minconn > 0 else 1
        self.maxconn    = maxconn if maxconn > 0 else (10 if self.minconn < 5 else self.minconn*2)
        self.timeout    = timeout if timeout > 0 else 10
        self.encrypt_key= encrypt_key

        if not schema:
            schemas = []
        else:
            schemas = schema.split(",")

        if "public" not in schemas:
            schemas.append("public")
        self.schema = ",".join(schemas)


    def print(self):
        print(f"pg              : {self.host}:{self.port}/{self.database}:{self.schema}")

    # -----------------------------------------------------

    async def get_async_client(self, cursor_factory: psycopg.AsyncCursor | None = LoggedAsyncCursor):
        return await psycopg.AsyncConnection.connect(
            host    = self.host,
            port    = self.port,
            dbname  = self.database,
            user    = self.user,
            password= self.password,
            options = f"-c search_path={self.schema} -c app.encryption_key={self.encrypt_key}",
            cursor_factory = cursor_factory
        )


    def get_client(self, cursor_factory: psycopg.Cursor | None = LoggedCursor):
        return psycopg.connect(
            host    = self.host,
            port    = self.port,
            dbname  = self.database,
            user    = self.user,
            password= self.password,
            options = f"-c search_path={self.schema} -c app.encryption_key={self.encrypt_key}",
            cursor_factory = cursor_factory
        )

    #-----------------------------------------------------

    async def get_async_pool(self) -> psycopg_pool.AsyncConnectionPool[Any]:
        pool = psycopg_pool.AsyncConnectionPool(
            f"host={self.host} port={self.port} dbname={self.database}",
            connection_class= LoggedAsyncConnection,
            open            = False,
            min_size        = self.minconn,
            max_size        = self.maxconn,
            kwargs          = dict(
                user    = self.user,
                password= self.password,
                options = f"-c search_path={self.schema} -c app.encryption_key={self.encrypt_key}",
            ),
        )
        await pool.open()

        return pool


    def get_pool(self) -> psycopg_pool.ConnectionPool[Any]:
        return psycopg_pool.ConnectionPool(
            f"host={self.host} port={self.port} dbname={self.database}",
            min_size= self.minconn,
            max_size= self.maxconn,
            kwargs  = dict(
                user    = self.user,
                password= self.password,
                options = f"-c search_path={self.schema} -c app.encryption_key={self.encrypt_key}",
            ),
        )
    
    #-----------------------------------------------------

    def get_async_engine(self) -> sqlalchemy.ext.asyncio.AsyncEngine:
        url = sqlalchemy.URL.create(
            drivername = "postgresql+psycopg",
            username   = self.user,
            password   = self.password,
            host       = self.host,
            port       = self.port,
            database   = self.database,
        )
        async_engine = sqlalchemy.ext.asyncio.create_async_engine(
            url,
            connect_args= {
                "options": f"-c search_path={self.schema} -c app.encryption_key={self.encrypt_key}",
            },
            poolclass   = sqlalchemy.AsyncAdaptedQueuePool,
            pool_size   = self.maxconn
        )

        # sqlalchemy.event.listen(async_engine.sync_engine, "before_cursor_execute", before_sqlarchemy_cursor_execute)
        # sqlalchemy.event.listen(async_engine.sync_engine, "after_cursor_execute", after_async_sqlarchemy_cursor_execute)

        return async_engine


    def get_engine(self) -> sqlalchemy.Engine:
        url = sqlalchemy.URL.create(
            drivername = "postgresql+psycopg",
            username   = self.user,
            password   = self.password,
            host       = self.host,
            port       = self.port,
            database   = self.database,
        )
        engine = sqlalchemy.create_engine(
            url,
            connect_args= {
                "options": f"-c search_path={self.schema} -c app.encryption_key={self.encrypt_key}"
            },
            poolclass   = sqlalchemy.QueuePool,
            pool_size   = self.maxconn
        )

        sqlalchemy.event.listen(engine, "before_cursor_execute", before_sqlarchemy_cursor_execute)
        sqlalchemy.event.listen(engine, "after_cursor_execute", after_sqlarchemy_cursor_execute)

        return engine


#-----------------------------------------------------------------------------
