from __future__ import annotations

import logging
import time
import psycopg
import psycopg_pool
import psycopg.abc
import sqlalchemy
import sqlalchemy.ext
import sqlalchemy.ext.asyncio

from typing import Any, Self

logger = logging.getLogger(__name__)


#-----------------------------------------------------------------------------

class LoggedAsyncCursor(psycopg.AsyncCursor):
    """The server pool's cursor: each statement at DEBUG, with its duration and
    row count. Never its parameters: the pool serves sign-in, account merge and
    profile updates, and `update_user_name` binds the person's name."""

    async def execute(
        self,
        query: psycopg.abc.Query,
        params: psycopg.abc.Params | None = None,
        *,
        prepare: bool | None = None,
        binary: bool | None = None
    ) -> Self:
        start = time.perf_counter()
        await super().execute(query, params, prepare=prepare, binary=binary)
        logger.debug(
            " ".join(str(query).split()),
            extra={"duration_ms": round((time.perf_counter() - start) * 1e3, 2), "row_count": self.rowcount},
            stacklevel=2,
        )
        return self


class LoggedAsyncConnection(psycopg.AsyncConnection):
    def __init__(self, *args, **kargs):
        super().__init__(*args, **kargs)
        self.cursor_factory = LoggedAsyncCursor

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

    #-----------------------------------------------------

    async def get_async_pool(self) -> psycopg_pool.AsyncConnectionPool[Any]:
        pool = psycopg_pool.AsyncConnectionPool(
            f"host={self.host} port={self.port} dbname={self.database}",
            connection_class= LoggedAsyncConnection,
            open            = False,
            min_size        = self.minconn,
            max_size        = self.maxconn,
            # A connection killed by a Postgres restart is found here and
            # replaced, not handed to a request that then answers 500.
            check           = psycopg_pool.AsyncConnectionPool.check_connection,
            kwargs          = {
                "user": self.user,
                "password": self.password,
                "options": f"-c search_path={self.schema} -c app.encryption_key={self.encrypt_key}",
            },
        )
        await pool.open()

        return pool

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
            pool_size   = self.maxconn,
            # After a Postgres restart every pooled connection is dead, and
            # without this the first request to draw each one answered 500.
            pool_pre_ping = True,
        )

        return async_engine

#-----------------------------------------------------------------------------
