"""
Reusable OAuth2 client for providers.

Encapsulates the standard OAuth2 authorization-code flow:
  1. generate_authorization_url  build auth URL, store state in Postgres temporary state
  2. exchange_code_for_tokens    code → tokens, save credentials to DB
  3. get_valid_access_token      refresh a token near expiry, flag a refused one
  4. refresh_access_token        refresh_token grant

Providers use this via composition (not inheritance):
    self.oauth = OAuth2Client(client_id=..., ...)
    await self.oauth.generate_authorization_url(user_id, options)
"""

import json
import logging
import time
import uuid
from datetime import datetime, UTC
from typing import Any
from urllib.parse import urlencode, parse_qs

import aiohttp

from mirobody.collect.providers._platform.http import VendorError
from mirobody.utils.config import safe_read_cfg, global_config
from mirobody.kernel.ops import is_driver_exception

logger = logging.getLogger(__name__)


class RefreshRefused(PermissionError):
    """The token endpoint refused the refresh token (`invalid_grant`), or there
    is none to send: only the person linking again brings the account back."""

    def __init__(self) -> None:
        super().__init__("the refresh token was refused")


def _to_epoch_seconds(value: Any) -> int:
    """`expires_at` as epoch seconds; 0 (treat as expired) when unknown.

    The column is a TIMESTAMP holding UTC, so a row read back is a naive UTC
    datetime; a callback's freshly exchanged pair carries epoch seconds.
    """
    if isinstance(value, datetime):
        return int((value if value.tzinfo else value.replace(tzinfo=UTC)).timestamp())
    if isinstance(value, int | float):
        return int(value)
    return 0


class OAuth2Client:
    """Reusable OAuth2 client for providers"""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_url: str,
        auth_url: str,
        token_url: str,
        scopes: str,
        request_timeout: int = 30,
        refresh_extra_params: dict[str, str] | None = None,
    ):
        self.client_id = client_id
        self.client_secret = client_secret
        self.redirect_url = redirect_url
        self.auth_url = auth_url
        self.token_url = token_url
        self.scopes = scopes
        self.request_timeout = request_timeout
        # Extra params appended to refresh requests (e.g. Whoop needs scope)
        self.refresh_extra_params = refresh_extra_params or {}

        try:
            self.oauth_temp_ttl = int(safe_read_cfg("OAUTH_TEMP_TTL_SECONDS") or 900)
        except Exception:
            self.oauth_temp_ttl = 900

    # ------------------------------------------------------------------
    # Stage 1: Authorization URL
    # ------------------------------------------------------------------

    async def generate_authorization_url(
        self, user_id: str, options: dict[str, Any]
    ) -> dict[str, Any]:
        """Generate OAuth2 authorization URL and store state in Postgres temporary state."""
        if not self.client_id or not self.client_secret:
            raise ValueError("Missing OAuth2 client_id or client_secret")
        if not self.redirect_url:
            raise ValueError("Missing OAuth2 redirect_url")

        origin_return_url = options.get("return_url") or ""
        state_payload = {"s": str(uuid.uuid4()), "r": origin_return_url}
        state = urlencode(state_payload)

        try:
            cfg = global_config()
            ephemeral = cfg.get_ephemeral()
            await ephemeral.setex(f"oauth2:state:{state}", self.oauth_temp_ttl, user_id or "")
            await ephemeral.setex(f"oauth2:redir:{state}", self.oauth_temp_ttl, self.redirect_url)
        except Exception as e:
            logger.warning("OAuth state write failed: error_type=%s", type(e).__name__)
            raise RuntimeError("OAuth state is unavailable") from None

        params = {
            "client_id": self.client_id,
            "response_type": "code",
            "redirect_uri": self.redirect_url,
            "scope": self.scopes,
            "state": state,
        }
        authorization_url = f"{self.auth_url}?{urlencode(params)}"

        return {"link_web_url": authorization_url}

    # ------------------------------------------------------------------
    # Stage 2: Code → Tokens
    # ------------------------------------------------------------------

    async def exchange_code_for_tokens(
        self, code: str, state: str, db_service: Any, provider_slug: str
    ) -> dict[str, Any]:
        """Exchange authorization code for tokens, save to DB.

        Returns dict with keys: user_id, access_token, refresh_token,
        expires_at, return_url, provider_slug.
        """
        # Read state from Postgres temporary state
        cached_user_id = None
        redirect_uri = None
        return_url = None

        try:
            cfg = global_config()
            ephemeral = cfg.get_ephemeral()
            if state:
                cached_user_id = await ephemeral.take(f"oauth2:state:{state}")
                redirect_uri = await ephemeral.take(f"oauth2:redir:{state}")

            if isinstance(cached_user_id, bytes):
                cached_user_id = cached_user_id.decode("utf-8")
            if isinstance(redirect_uri, bytes):
                redirect_uri = redirect_uri.decode("utf-8")

            try:
                parsed = parse_qs(state or "")
                r_values = parsed.get("r")
                if r_values:
                    return_url = r_values[0]
            except Exception:
                return_url = None
        except Exception as e:
            logger.warning("OAuth state read failed: error_type=%s", type(e).__name__,
                           exc_info=not is_driver_exception(e))

        user_id = cached_user_id
        if not user_id:
            raise ValueError("Missing user_id for OAuth2 callback (state expired or invalid)")
        if not redirect_uri:
            raise ValueError("Missing redirect_uri for OAuth2 token exchange")

        # Exchange code for tokens
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
        }
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": redirect_uri,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.token_url, data=data, headers=headers,
                timeout=aiohttp.ClientTimeout(total=self.request_timeout)
            ) as resp:
                if resp.status != 200:
                    raise VendorError(resp.status)
                try:
                    token_json = json.loads(await resp.text())
                except ValueError:
                    raise RuntimeError("Token endpoint returned non-JSON body") from None

        access_token = token_json.get("access_token")
        refresh_token = token_json.get("refresh_token", "")
        expires_in = token_json.get("expires_in")

        if not access_token:
            raise RuntimeError("Invalid token response: missing access_token")

        expires_at = None
        if expires_in:
            try:
                expires_at = int(time.time()) + int(expires_in)
            except Exception:
                expires_at = None

        await db_service.save_oauth2_credentials(user_id, provider_slug, access_token, refresh_token, expires_at)
        logger.info("OAuth2 linked: provider=%s user_id=%s", provider_slug, user_id)

        return {
            "user_id": user_id,
            "access_token": access_token,
            "refresh_token": refresh_token,
            "expires_at": expires_at,
            "return_url": return_url,
            "provider_slug": provider_slug,
        }

    # ------------------------------------------------------------------
    # Token Management
    # ------------------------------------------------------------------

    async def get_valid_access_token(self, credentials: dict[str, Any], provider_slug: str, db_service: Any) -> str:
        """A usable access token for one linked account.

        `credentials` is the account as the pull loop holds it: `user_id`,
        `access_token`, `refresh_token`, `expires_at`. A token more than five
        minutes from expiry is used as it is; otherwise it is refreshed and the
        new pair saved. A refused refresh marks the link for reconnecting and
        raises `RefreshRefused`: the link stays, so the person is asked to
        reconnect instead of finding the provider gone. Any other failure
        (a timeout, a 5xx) raises as itself, and the next run tries again.
        """
        user_id = str(credentials["user_id"])
        access_token = credentials.get("access_token")
        if access_token and time.time() < _to_epoch_seconds(credentials.get("expires_at")) - 300:
            return access_token
        refresh_token = credentials.get("refresh_token")
        try:
            if not refresh_token:
                raise RefreshRefused()
            tokens = await self.refresh_access_token(refresh_token)
        except RefreshRefused:
            await db_service.mark_reconnect(user_id, provider_slug)
            logger.warning("OAuth2 refresh refused, reconnect needed: provider=%s user_id=%s",
                           provider_slug, user_id)
            raise
        new_access_token = tokens.get("access_token")
        if not new_access_token:
            raise RuntimeError("Token endpoint answered without an access_token")
        new_expires_at = int(time.time()) + int(tokens.get("expires_in", 86400))
        await db_service.save_oauth2_credentials(
            user_id, provider_slug, new_access_token, tokens.get("refresh_token", refresh_token), new_expires_at
        )
        return new_access_token

    async def refresh_access_token(self, refresh_token: str) -> dict[str, Any]:
        """The refresh_token grant. `invalid_grant` raises `RefreshRefused`;
        any other refusal raises `VendorError`, without the vendor's body."""
        data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            **self.refresh_extra_params,
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.token_url, data=data,
                timeout=aiohttp.ClientTimeout(total=self.request_timeout)
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                try:
                    body = json.loads(await resp.text())
                except ValueError:
                    body = None
                if isinstance(body, dict) and body.get("error") == "invalid_grant":
                    raise RefreshRefused()
                raise VendorError(resp.status)
