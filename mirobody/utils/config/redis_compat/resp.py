"""RESP protocol encoding / decoding."""

from __future__ import annotations

import asyncio


def encode_simple_string(s: str) -> bytes:
    return f"+{s}\r\n".encode()


def encode_error(msg: str) -> bytes:
    return f"-ERR {msg}\r\n".encode()


def encode_integer(n: int) -> bytes:
    return f":{n}\r\n".encode()


def encode_bulk_string(s: str | None) -> bytes:
    if s is None:
        return b"$-1\r\n"
    data = s.encode()
    return f"${len(data)}\r\n".encode() + data + b"\r\n"


def encode_array(items: list[bytes]) -> bytes:
    header = f"*{len(items)}\r\n".encode()
    return header + b"".join(items)


_MAX_ARRAY_ELEMENTS = 64 * 1024       # 64K elements per command
_MAX_BULK_STRING    = 512 * 1024 * 1024  # 512 MB per bulk string


async def read_resp(reader: asyncio.StreamReader) -> list[str] | None:
    """Read one RESP command (an array of bulk strings) from the stream."""
    line = await reader.readline()
    if not line:
        return None
    line = line.strip()

    # Inline command (e.g. from telnet: "PING\r\n")
    if not line.startswith(b"*"):
        return line.decode().split()

    try:
        count = int(line[1:])
    except ValueError:
        return None
    if count < 0 or count > _MAX_ARRAY_ELEMENTS:
        return None

    parts: list[str] = []
    for _ in range(count):
        header = await reader.readline()
        if not header:
            return None
        try:
            length = int(header.strip()[1:])
        except ValueError:
            return None
        if length < 0 or length > _MAX_BULK_STRING:
            return None
        data = await reader.readexactly(length + 2)  # +2 for \r\n
        parts.append(data[:length].decode())
    return parts
