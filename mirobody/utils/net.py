"""Outbound URL safety and bounded fetching — the one implementation.

Every URL a caller hands in goes through `assert_public_url()` first: if any
address it resolves to is loopback, private, link-local, reserved or
unspecified, it is refused. Those ranges hold the database, the cloud
provider's metadata endpoint (169.254.169.254) and in-cluster services; a
server that fetches whatever it is told to is a pivot into all of them.

`fetch_bounded()` is the matching downloader: it counts bytes *as they
arrive* and disconnects the moment the cap is exceeded, instead of
``await resp.read()`` then a length check — which is no defence against a
response that claims 1 KB and sends 10 GB (Content-Length is the peer's word).

The judgement used to live inside one MCP-tool loader, for developer-typed
server URLs. The moment an API accepts caller-supplied image URLs there is a
second consumer with a much wider audience, and copying the check is how the
two drift.
"""

from __future__ import annotations

import ipaddress
import logging
import socket
from urllib.parse import urlparse

log = logging.getLogger(__name__)

DEFAULT_TIMEOUT_S = 15.0
MAX_REDIRECTS = 3
_REDIRECTS = frozenset({301, 302, 303, 307, 308})


#: Ranges `ipaddress` does not call private but that are not the public
#: internet either. `100.64.0.0/10` is RFC 6598 carrier-grade NAT, which is
#: also where several Kubernetes distributions put pod and service networks —
#: so it is reachable in-cluster while `is_private` returns False for it, the
#: one combination this guard exists to refuse.
_EXTRA_REFUSED = (
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("::ffff:100.64.0.0/106"),  # the same range, IPv4-mapped
)


class UnsafeUrlError(ValueError):
    """The URL cannot be fetched: wrong scheme, unresolvable host, or a host
    that resolves to a non-public address."""


class FetchTooLargeError(ValueError):
    """The response body exceeds the caller's byte cap."""


def assert_public_url(url: str, *, allow_private: bool = False) -> None:
    """``url`` must be http(s), its host must resolve, and **every** address it
    resolves to must be public.

    ``allow_private`` is for the local stack and tests only (a container
    reaching ``host.docker.internal``); no production path opens it.

    Every ``addrinfo`` is judged: one hostname can answer with a public A
    record *and* 127.0.0.1, and checking only the first is the bypass.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise UnsafeUrlError(f"url must be http(s) with a host: {url[:80]!r}")
    if allow_private:
        return
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        infos = socket.getaddrinfo(parsed.hostname, port)
    except OSError as exc:
        raise UnsafeUrlError(f"host does not resolve: {parsed.hostname} ({exc})") from None
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_unspecified:
            raise UnsafeUrlError(f"host resolves to a non-public address: {parsed.hostname}")
        if any(ip in net for net in _EXTRA_REFUSED):
            raise UnsafeUrlError(f"host resolves to a non-public address: {parsed.hostname}")


async def fetch_bounded(
    url: str,
    *,
    max_bytes: int,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    allow_private: bool = False,
) -> tuple[bytes, str]:
    """Fetch ``url`` and return ``(body, content_type)``; raise
    ``FetchTooLargeError`` past ``max_bytes``.

    Every redirect hop is re-checked with `assert_public_url` — checking only
    the initial URL does not stop the most common bypass, a public hostname
    that 302s to 169.254.169.254 — so automatic following is off and the hops
    are walked here.

    ``aiohttp`` is imported lazily: it is the one HTTP client this package
    depends on, and this module's *judgement* (`assert_public_url`) has no
    dependency at all.
    """
    import aiohttp
    from yarl import URL

    assert_public_url(url, allow_private=allow_private)
    current = url
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout_s)) as session:
        for _ in range(MAX_REDIRECTS + 1):
            async with session.get(current, allow_redirects=False) as resp:
                if resp.status in _REDIRECTS:
                    location = resp.headers.get("Location") or ""
                    current = str(URL(current).join(URL(location)))
                    assert_public_url(current, allow_private=allow_private)
                    continue
                resp.raise_for_status()
                content_type = (resp.headers.get("Content-Type") or "").split(";")[0].strip().lower()
                chunks: list[bytes] = []
                total = 0
                async for chunk in resp.content.iter_chunked(64 * 1024):
                    total += len(chunk)
                    if total > max_bytes:
                        raise FetchTooLargeError(f"response exceeds {max_bytes} bytes")
                    chunks.append(chunk)
                return b"".join(chunks), content_type
    raise UnsafeUrlError(f"too many redirects (>{MAX_REDIRECTS}) for {url[:80]!r}")
