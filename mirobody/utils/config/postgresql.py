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

        logger.info(
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
            pool_size   = self.maxconn
        )

        return async_engine

#-----------------------------------------------------------------------------
