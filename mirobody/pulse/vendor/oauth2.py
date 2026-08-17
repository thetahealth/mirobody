"""Shared OAuth2 token-endpoint helper for the device-brand vendor clients.

Port of the archived C++ ``src/health/vendor/oauth2.cpp``. Most of the direct
OAuth2 vendors (dexcom, oura, whoop, fitbit, polar, …) speak the standard
RFC 6749 token endpoint: a form-encoded POST with a ``grant_type`` and either
client credentials in the body or via HTTP Basic, returning a JSON body
``{access_token, refresh_token?, expires_in?}``. This centralizes that request
so each vendor's ``exchange_code()`` / ``refresh()`` is just "which URL, which
params, which auth style". Vendors with a non-standard envelope (e.g. Withings,
whose token reply is wrapped in ``{status, body:{…}}``) do their own.
"""

from __future__ import annotations

import aiohttp

from .base import TokenSet, VendorError

_TIMEOUT = aiohttp.ClientTimeout(total=30)


async def oauth2_token_request(
    token_url: str,
    params: dict[str, str],
    basic_user: str = "",
    basic_pass: str = "",
) -> TokenSet:
    """POST a standard OAuth2 token request (application/x-www-form-urlencoded)
    of ``params`` to ``token_url``.

    When ``basic_user`` is non-empty, client authentication is HTTP Basic
    (``basic_user:basic_pass``); otherwise the caller should include the client
    credentials among ``params``. Parses the standard JSON response into a
    :class:`TokenSet`.

    Raises :class:`VendorError` on transport error, a non-2xx status, or a
    response missing ``access_token``.
    """
    auth = aiohttp.BasicAuth(basic_user, basic_pass) if basic_user else None
    try:
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            async with session.post(token_url, data=params, auth=auth) as resp:
                text = await resp.text()
                if resp.status < 200 or resp.status >= 300:
                    raise VendorError(
                        f"oauth2 token endpoint {token_url} returned "
                        f"{resp.status}: {text[:300]}"
                    )
                try:
                    payload = await resp.json(content_type=None)
                except Exception as exc:
                    raise VendorError(
                        f"oauth2 token endpoint {token_url} returned "
                        f"non-JSON body: {text[:300]}"
                    ) from exc
    except VendorError:
        raise
    except Exception as exc:  # aiohttp transport errors
        raise VendorError(f"oauth2 token request to {token_url} failed: {exc}") from exc

    access_token = str(payload.get("access_token") or "")
    if not access_token:
        raise VendorError(
            f"oauth2 token endpoint {token_url} response has no access_token"
        )
    return TokenSet(
        access_token=access_token,
        refresh_token=str(payload.get("refresh_token") or ""),
        expires_in=int(payload.get("expires_in") or 0),
    )
