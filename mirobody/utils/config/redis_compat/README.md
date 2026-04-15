# redis_compat

A drop-in replacement for `redis.asyncio.Redis` that runs in-process — no Redis server required. Covers the most commonly used Redis commands.

## Architecture

```
RedisCompat (client.py)               # In-process client, API-compatible with redis.asyncio.Redis
    ├── MemoryStore (store_memory.py) # Pure in-memory backend
    └── PgStore (store_pg.py)         # PostgreSQL-backed persistent backend

RedisCompatServer (server.py)        # RESP-protocol TCP server, wire-compatible with redis-cli
    ├── MemoryStore
    └── RESP codec (resp.py)

PubSub (pubsub.py)                   # Pub/Sub support for both TCP and in-process modes
```

## Quick Start

### In-process usage (as a redis.asyncio.Redis replacement)

```python
from mirobody.utils.config.redis_compat import RedisCompat

# In-memory mode
r = RedisCompat()

# PostgreSQL-backed mode
r = RedisCompat(pg_config=pg_cfg)

await r.set("key", "value")
await r.get("key")  # "value"
```

### Standalone TCP server

```bash
python -m mirobody.utils.config.redis_compat --host 127.0.0.1 --port 6379
```

Then connect with `redis-cli -p 6379`.

## Supported Commands

| Type | Commands |
|------|----------|
| String | `GET` `SET` `SETEX` `SETNX` `INCR` `DECR` `INCRBY` `DECRBY` `APPEND` |
| Hash | `HSET` `HGET` `HGETALL` `HDEL` `HEXISTS` `HKEYS` `HVALS` `HLEN` `HINCRBY` `HMSET` `HMGET` |
| Set | `SADD` `SREM` `SMEMBERS` `SISMEMBER` `SCARD` `SINTER` `SUNION` `SDIFF` |
| List | `LPUSH` `RPUSH` `LPOP` `RPOP` `LLEN` `LRANGE` `LINDEX` `BLPOP` `BRPOP` |
| Generic | `EXISTS` `DEL` `KEYS` `EXPIRE` `TTL` `PING` |
| Pub/Sub | `PUBLISH` `SUBSCRIBE` `UNSUBSCRIBE` |
| Scripting | `EVAL` (compare-and-delete pattern only) |

## Storage Backends

- **MemoryStore** — Pure in-memory with TTL expiration and BLPOP/BRPOP blocking support. Best for testing and single-process use.
- **PgStore** — Persists data to PostgreSQL (tables: `mirobody_runtime_kv`/`hash`/`set`/`list`), auto-creates schema on first connection. Best for persistence or multi-process sharing.
