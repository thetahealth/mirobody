"""RESP-compatible TCP server (wire-compatible with redis-cli)."""

from __future__ import annotations

import asyncio

from .resp import (
    encode_array,
    encode_bulk_string,
    encode_error,
    encode_integer,
    encode_simple_string,
    read_resp,
)
from .store_memory import MemoryStore
from .pubsub import PubSub, Subscriber


class RedisCompatServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 6389):
        self.host = host
        self.port = port
        self.store = MemoryStore()
        self.pubsub = PubSub()

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        addr = writer.get_extra_info("peername")
        print(f"[+] client connected: {addr}")
        subscriber = Subscriber(writer=writer)

        try:
            while True:
                parts = await read_resp(reader)
                if parts is None:
                    break

                cmd = parts[0].upper()
                args = parts[1:]

                response = await self.dispatch(cmd, args, subscriber)
                if response is not None:
                    writer.write(response)
                    await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            self.pubsub.unsubscribe(subscriber)
            writer.close()
            print(f"[-] client disconnected: {addr}")

    async def dispatch(self, cmd: str, args: list[str], subscriber: Subscriber) -> bytes | None:
        match cmd:
            case "PING":
                return encode_simple_string(args[0] if args else "PONG")

            case "SET":
                if len(args) < 2:
                    return encode_error("wrong number of arguments for 'SET'")
                key, value = args[0], args[1]
                ex = None
                if len(args) >= 4 and args[2].upper() == "EX":
                    ex = int(args[3])
                await self.store.set(key, value, ex=ex)
                return encode_simple_string("OK")

            case "GET":
                if len(args) != 1:
                    return encode_error("wrong number of arguments for 'GET'")
                return encode_bulk_string(await self.store.get(args[0]))

            case "SETEX":
                if len(args) != 3:
                    return encode_error("wrong number of arguments for 'SETEX'")
                try:
                    ex = int(args[1])
                except ValueError:
                    return encode_error("value is not an integer or out of range")
                await self.store.set(args[0], args[2], ex=ex)
                return encode_simple_string("OK")

            case "SETNX":
                if len(args) != 2:
                    return encode_error("wrong number of arguments for 'SETNX'")
                return encode_integer(1 if await self.store.setnx(args[0], args[1]) else 0)

            case "INCR":
                if len(args) != 1:
                    return encode_error("wrong number of arguments for 'INCR'")
                try:
                    return encode_integer(await self.store.incr(args[0]))
                except ValueError as e:
                    return encode_error(str(e))

            case "DECR":
                if len(args) != 1:
                    return encode_error("wrong number of arguments for 'DECR'")
                try:
                    return encode_integer(await self.store.incr(args[0], by=-1))
                except ValueError as e:
                    return encode_error(str(e))

            case "INCRBY":
                if len(args) != 2:
                    return encode_error("wrong number of arguments for 'INCRBY'")
                try:
                    by = int(args[1])
                    return encode_integer(await self.store.incr(args[0], by=by))
                except ValueError as e:
                    return encode_error(str(e))

            case "DECRBY":
                if len(args) != 2:
                    return encode_error("wrong number of arguments for 'DECRBY'")
                try:
                    by = int(args[1])
                    return encode_integer(await self.store.incr(args[0], by=-by))
                except ValueError as e:
                    return encode_error(str(e))

            case "APPEND":
                if len(args) != 2:
                    return encode_error("wrong number of arguments for 'APPEND'")
                return encode_integer(await self.store.append(args[0], args[1]))

            case "DEL":
                if not args:
                    return encode_error("wrong number of arguments for 'DEL'")
                return encode_integer(await self.store.delete(*args))

            case "KEYS":
                pattern = args[0] if args else "*"
                keys = await self.store.keys(pattern)
                return encode_array([encode_bulk_string(k) for k in keys])

            case "EXISTS":
                if not args:
                    return encode_error("wrong number of arguments for 'EXISTS'")
                return encode_integer(await self.store.exists(*args))

            case "EXPIRE":
                if len(args) != 2:
                    return encode_error("wrong number of arguments for 'EXPIRE'")
                try:
                    seconds = int(args[1])
                except ValueError:
                    return encode_error("value is not an integer or out of range")
                return encode_integer(1 if await self.store.expire(args[0], seconds) else 0)

            case "TTL":
                if len(args) != 1:
                    return encode_error("wrong number of arguments for 'TTL'")
                return encode_integer(await self.store.ttl(args[0]))

            case "SADD":
                if len(args) < 2:
                    return encode_error("wrong number of arguments for 'SADD'")
                return encode_integer(await self.store.sadd(args[0], *args[1:]))

            case "SREM":
                if len(args) < 2:
                    return encode_error("wrong number of arguments for 'SREM'")
                return encode_integer(await self.store.srem(args[0], *args[1:]))

            case "SMEMBERS":
                if len(args) != 1:
                    return encode_error("wrong number of arguments for 'SMEMBERS'")
                return encode_array([encode_bulk_string(m) for m in await self.store.smembers(args[0])])

            case "SISMEMBER":
                if len(args) != 2:
                    return encode_error("wrong number of arguments for 'SISMEMBER'")
                return encode_integer(1 if await self.store.sismember(args[0], args[1]) else 0)

            case "SCARD":
                if len(args) != 1:
                    return encode_error("wrong number of arguments for 'SCARD'")
                return encode_integer(await self.store.scard(args[0]))

            case "SINTER":
                if not args:
                    return encode_error("wrong number of arguments for 'SINTER'")
                return encode_array([encode_bulk_string(m) for m in await self.store.sinter(*args)])

            case "SUNION":
                if not args:
                    return encode_error("wrong number of arguments for 'SUNION'")
                return encode_array([encode_bulk_string(m) for m in await self.store.sunion(*args)])

            case "SDIFF":
                if not args:
                    return encode_error("wrong number of arguments for 'SDIFF'")
                return encode_array([encode_bulk_string(m) for m in await self.store.sdiff(*args)])

            case "LPUSH":
                if len(args) < 2:
                    return encode_error("wrong number of arguments for 'LPUSH'")
                return encode_integer(await self.store.lpush(args[0], *args[1:]))

            case "RPUSH":
                if len(args) < 2:
                    return encode_error("wrong number of arguments for 'RPUSH'")
                return encode_integer(await self.store.rpush(args[0], *args[1:]))

            case "LPOP":
                if len(args) != 1:
                    return encode_error("wrong number of arguments for 'LPOP'")
                return encode_bulk_string(await self.store.lpop(args[0]))

            case "RPOP":
                if len(args) != 1:
                    return encode_error("wrong number of arguments for 'RPOP'")
                return encode_bulk_string(await self.store.rpop(args[0]))

            case "LLEN":
                if len(args) != 1:
                    return encode_error("wrong number of arguments for 'LLEN'")
                return encode_integer(await self.store.llen(args[0]))

            case "LRANGE":
                if len(args) != 3:
                    return encode_error("wrong number of arguments for 'LRANGE'")
                return encode_array([
                    encode_bulk_string(v)
                    for v in await self.store.lrange(args[0], int(args[1]), int(args[2]))
                ])

            case "LINDEX":
                if len(args) != 2:
                    return encode_error("wrong number of arguments for 'LINDEX'")
                return encode_bulk_string(await self.store.lindex(args[0], int(args[1])))

            case "BLPOP" | "BRPOP":
                if len(args) < 2:
                    return encode_error(f"wrong number of arguments for '{cmd}'")
                keys, timeout = args[:-1], float(args[-1])
                side = "left" if cmd == "BLPOP" else "right"
                # Try immediate pop
                for key in keys:
                    val = (await self.store.lpop(key)) if side == "left" else (await self.store.rpop(key))
                    if val is not None:
                        return encode_array([encode_bulk_string(key), encode_bulk_string(val)])
                # Block and wait
                if timeout == 0:
                    timeout = None  # infinite
                fut = self.store.add_list_waiter(keys, side)
                try:
                    key, val = await asyncio.wait_for(fut, timeout=timeout)
                    return encode_array([encode_bulk_string(key), encode_bulk_string(val)])
                except asyncio.TimeoutError:
                    return encode_array([])

            case "HSET" | "HMSET":
                if len(args) < 3 or len(args) % 2 == 0:
                    return encode_error(f"wrong number of arguments for '{cmd}'")
                mapping = dict(zip(args[1::2], args[2::2]))
                added = await self.store.hset(args[0], mapping)
                if cmd == "HMSET":
                    return encode_simple_string("OK")
                return encode_integer(added)

            case "HGET":
                if len(args) != 2:
                    return encode_error("wrong number of arguments for 'HGET'")
                return encode_bulk_string(await self.store.hget(args[0], args[1]))

            case "HGETALL":
                if len(args) != 1:
                    return encode_error("wrong number of arguments for 'HGETALL'")
                h = await self.store.hgetall(args[0])
                items: list[bytes] = []
                for k, v in h.items():
                    items.append(encode_bulk_string(k))
                    items.append(encode_bulk_string(v))
                return encode_array(items)

            case "HDEL":
                if len(args) < 2:
                    return encode_error("wrong number of arguments for 'HDEL'")
                return encode_integer(await self.store.hdel(args[0], *args[1:]))

            case "HEXISTS":
                if len(args) != 2:
                    return encode_error("wrong number of arguments for 'HEXISTS'")
                return encode_integer(1 if await self.store.hexists(args[0], args[1]) else 0)

            case "HKEYS":
                if len(args) != 1:
                    return encode_error("wrong number of arguments for 'HKEYS'")
                return encode_array([encode_bulk_string(k) for k in await self.store.hkeys(args[0])])

            case "HVALS":
                if len(args) != 1:
                    return encode_error("wrong number of arguments for 'HVALS'")
                return encode_array([encode_bulk_string(v) for v in await self.store.hvals(args[0])])

            case "HLEN":
                if len(args) != 1:
                    return encode_error("wrong number of arguments for 'HLEN'")
                return encode_integer(await self.store.hlen(args[0]))

            case "HMGET":
                if len(args) < 2:
                    return encode_error("wrong number of arguments for 'HMGET'")
                return encode_array([encode_bulk_string(await self.store.hget(args[0], f)) for f in args[1:]])

            case "HINCRBY":
                if len(args) != 3:
                    return encode_error("wrong number of arguments for 'HINCRBY'")
                try:
                    return encode_integer(await self.store.hincrby(args[0], args[1], int(args[2])))
                except ValueError:
                    return encode_error("value is not an integer or out of range")

            case "SUBSCRIBE":
                if not args:
                    return encode_error("wrong number of arguments for 'SUBSCRIBE'")
                self.pubsub.subscribe(subscriber, *args)
                msgs = []
                for i, ch in enumerate(args, 1):
                    msgs.append(encode_array([
                        encode_bulk_string("subscribe"),
                        encode_bulk_string(ch),
                        encode_integer(i),
                    ]))
                return b"".join(msgs)

            case "UNSUBSCRIBE":
                channels = args if args else list(subscriber.channels)
                self.pubsub.unsubscribe(subscriber, *args)
                msgs = []
                for ch in channels:
                    msgs.append(encode_array([
                        encode_bulk_string("unsubscribe"),
                        encode_bulk_string(ch),
                        encode_integer(len(subscriber.channels)),
                    ]))
                return b"".join(msgs) if msgs else encode_array([
                    encode_bulk_string("unsubscribe"),
                    encode_bulk_string(None),
                    encode_integer(0),
                ])

            case "PUBLISH":
                if len(args) != 2:
                    return encode_error("wrong number of arguments for 'PUBLISH'")
                count = self.pubsub.publish(args[0], args[1])
                return encode_integer(count)

            case "COMMAND":
                # redis-cli sends COMMAND DOCS on connect -- just return empty
                return encode_array([])

            case _:
                return encode_error(f"unknown command '{cmd}'")

    async def run(self):
        server = await asyncio.start_server(self.handle_client, self.host, self.port)
        print(f"RedisCompat listening on {self.host}:{self.port}")
        async with server:
            await server.serve_forever()
