"""One bounded GET, and one paginator, for every vendor API a provider pulls.

Each provider used to carry its own loop, and they disagreed: WHOOP's answered
a 401 with `[{}]`, a record of nothing that went on to be stored, and wrote the
next page's token into its caller's params; Oura's retried a 429 forever,
sleeping whatever `Retry-After` asked. Here a refused credential raises
`VendorAuthError` at once, a status that will not change raises `VendorError`
at once, and only a 429, a 5xx, a timeout or a dropped connection is retried,
`MAX_ATTEMPTS` times in all, never waiting longer than `MAX_WAIT_S`.
"""

from __future__ import annotations

import asyncio
from typing import Any

import aiohttp

MAX_ATTEMPTS = 4
MAX_WAIT_S = 60


class VendorAuthError(PermissionError):
    """The vendor refused the credential (401 or 403).

    A `PermissionError`, which is what the pull loop counts toward expiring a
    credential; any other failure is transient as far as the credential goes.
    """

    def __init__(self, status: int) -> None:
        super().__init__(f"vendor refused the credential: status={status}")
        self.status = status


class VendorError(RuntimeError):
    """The vendor answered with a status that is not data. The body is not
    kept: it can quote the request, and the request names a person."""

    def __init__(self, status: int) -> None:
        super().__init__(f"vendor answered status={status}")
        self.status = status


def _wait_s(retry_after: str | None, attempt: int) -> float:
    try:
        wait = float(retry_after) if retry_after else float(2 ** attempt)
    except ValueError:
        wait = float(2 ** attempt)
    return min(max(wait, 0.0), MAX_WAIT_S)


async def get_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    headers: dict[str, str],
    params: dict[str, Any] | None = None,
    timeout_s: float = 30,
) -> Any:
    """The JSON body of a GET, or an exception saying why there is none."""
    attempt = 1
    while True:
        try:
            async with session.get(
                url, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=timeout_s)
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                if resp.status in (401, 403):
                    raise VendorAuthError(resp.status)
                if (resp.status != 429 and resp.status < 500) or attempt == MAX_ATTEMPTS:
                    raise VendorError(resp.status)
                wait = _wait_s(resp.headers.get("Retry-After"), attempt)
        except (TimeoutError, aiohttp.ClientConnectionError):
            if attempt == MAX_ATTEMPTS:
                raise
            wait = float(2 ** attempt)
        await asyncio.sleep(wait)
        attempt += 1


async def get_pages(
    session: aiohttp.ClientSession,
    url: str,
    *,
    headers: dict[str, str],
    params: dict[str, Any],
    records_key: str,
    token_param: str,
    timeout_s: float = 30,
) -> list[dict[str, Any]]:
    """Every record of a paginated collection, following `next_token`.

    `records_key` is where a page keeps its records and `token_param` the
    query parameter that asks for the next page; both vendors answer the
    token itself as `next_token`. The caller's `params` are not modified.
    """
    records: list[dict[str, Any]] = []
    page_params = dict(params)
    while True:
        body = await get_json(session, url, headers=headers, params=page_params, timeout_s=timeout_s)
        page = body.get(records_key) if isinstance(body, dict) else None
        records.extend(r for r in page or [] if isinstance(r, dict))
        token = body.get("next_token") if isinstance(body, dict) else None
        if not token:
            return records
        page_params = {**params, token_param: token}
