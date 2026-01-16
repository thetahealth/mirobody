import logging
import redis, redis.asyncio

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
    # redis.Redis handles connection pooling automatically, thus
    #   use getAioClient() or getClient() as possible as you can.

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

                logging.error(f"Failed to ping Redis server '{self.host}:{self.port}' asynchronously.")
                return None
            
        except Exception as e:
            logging.error(str(e), extra={"host": self.host, "port": self.port})
            return None

        return client


    def get_client(self) -> redis.Redis | None:
        client = redis.Redis(
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
        if not client.ping():
            client.close()

            logging.error(f"Failed to ping Redis server '{self.host}:{self.port}'.")
            return None

        return client

    #-----------------------------------------------------

    def get_async_pool(self) -> redis.asyncio.ConnectionPool:
        return redis.asyncio.ConnectionPool(
            connection_class    = redis.asyncio.SSLConnection if self.ssl else redis.asyncio.Connection,
            host                = self.host,
            port                = self.port,
            db                  = self.database,
            password            = self.password,
            ssl_check_hostname  = self.ssl_check_hostname,
            ssl_cert_reqs       = self.ssl_cert_reqs,
            decode_responses    = True,
            socket_timeout      = self.timeout,
        )

    def get_pool(self) -> redis.ConnectionPool:
        return redis.ConnectionPool(
            connection_class    = redis.SSLConnection if self.ssl else redis.Connection,
            host                = self.host,
            port                = self.port,
            db                  = self.database,
            password            = self.password,
            ssl_check_hostname  = self.ssl_check_hostname,
            ssl_cert_reqs       = self.ssl_cert_reqs,
            decode_responses    = True,
            socket_timeout      = self.timeout,
        )

#-----------------------------------------------------------------------------
