from __future__ import annotations

import logging
import redis
import redis.asyncio

logger = logging.getLogger(__name__)


#-----------------------------------------------------------------------------

class RedisConfig:
    def __init__(
        self,
        host    : str = "",
        port    : int = 0,
        password: str = "",
        database: int = 0,
        minconn : int = 0,
        maxconn : int = 0,
        timeout : int = 0,
        ssl                 : bool = False,
        ssl_check_hostname  : bool = False,
        ssl_cert_reqs       : str = "",
    ):
        self.host       = host if host else "127.0.0.1"
        self.port       = port if port > 0 else 6379
        self.password   = password
        self.database   = database
        
        self.minconn    = minconn if minconn > 0 else 1
        self.maxconn    = maxconn if maxconn > 0 else (10 if self.minconn < 10 else self.minconn*2)
        self.timeout    = timeout if timeout > 0 else 300

        self.ssl                = ssl
        self.ssl_check_hostname = ssl_check_hostname
        self.ssl_cert_reqs      = ssl_cert_reqs if ssl_cert_reqs else "none"


    def print(self):
        print(f"redis           : {self.host}:{self.port}/{self.database}")

    # -----------------------------------------------------
    # redis.asyncio.Redis handles connection pooling itself; every caller in
    # the project goes through get_async_client(). The sync client and the two
    # bare ConnectionPool builders that used to sit below it had no callers.

    async def get_async_client(self) -> redis.asyncio.Redis | None:
        client = await redis.asyncio.Redis(
            host                = self.host,
            port                = self.port,
            db                  = self.database,
            password            = self.password,
            ssl                 = self.ssl,
            ssl_check_hostname  = self.ssl_check_hostname,
            ssl_cert_reqs       = self.ssl_cert_reqs,
            decode_responses    = True,
            socket_timeout      = self.timeout,
            max_connections     = self.maxconn,
        )

        # Check it beforehand.
        try:
            if not await client.ping():
                await client.aclose()

                logger.error(f"Failed to ping Redis server '{self.host}:{self.port}' asynchronously.")
                return None

        except Exception as e:
            logger.error(str(e), extra={"host": self.host, "port": self.port})
            return None

        return client

#-----------------------------------------------------------------------------
