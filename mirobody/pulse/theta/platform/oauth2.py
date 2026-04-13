"""
Reusable OAuth2 client for Theta providers.

Encapsulates the standard OAuth2 authorization-code flow:
  1. generate_authorization_url  — build auth URL, store state in Redis
  2. exchange_code_for_tokens    — code → tokens, save credentials to DB
  3. get_valid_access_token      — auto-refresh expired tokens
  4. refresh_access_token        — refresh_token grant

Providers use this via composition (not inheritance):
    self.oauth = ThetaOAuth2Client(client_id=..., ...)
    await self.oauth.generate_authorization_url(user_id, options, db_service)
"""

import json
import logging
import time
import uuid
from typing import Any, Dict, Optional
from urllib.parse import urlencode, parse_qs

import aiohttp

from mirobody.pulse.core import LinkType
from mirobody.utils.config import safe_read_cfg, global_config


class ThetaOAuth2Client:
    """Reusable OAuth2 client for Theta providers"""

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        redirect_url: str,
        auth_url: str,
        token_url: str,
        scopes: str,
        request_timeout: int = 30,
        refresh_extra_params: Optional[Dict[str, str]] = None,
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
        self, user_id: str, options: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate OAuth2 authorization URL and store state in Redis."""
        if not self.client_id or not self.client_secret:
            raise ValueError("Missing OAuth2 client_id or client_secret")
        if not self.redirect_url:
            raise ValueError("Missing OAuth2 redirect_url")

        origin_return_url = options.get("return_url") or ""
        state_payload = {"s": str(uuid.uuid4()), "r": origin_return_url}
        state = urlencode(state_payload)

        try:
            cfg = global_config()
            redis_config = cfg.get_redis()
            redis_client = await redis_config.get_async_client()
            await redis_client.setex(f"oauth2:state:{state}", self.oauth_temp_ttl, user_id or "")
            await redis_client.setex(f"oauth2:redir:{state}", self.oauth_temp_ttl, self.redirect_url)
            await redis_client.aclose()
        except Exception as e:
            logging.warning(f"Failed to write oauth2 temp data to Redis: {e}")

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
    ) -> Dict[str, Any]:
        """Exchange authorization code for tokens, save to DB.

        Returns dict with keys: user_id, access_token, refresh_token,
        expires_at, return_url, provider_slug.
        """
        # Read state from Redis
        cached_user_id = None
        redirect_uri = None
        return_url = None

        try:
            cfg = global_config()
            redis_config = cfg.get_redis()
            redis_client = await redis_config.get_async_client()
            if state:
                cached_user_id = await redis_client.get(f"oauth2:state:{state}")
                redirect_uri = await redis_client.get(f"oauth2:redir:{state}")
                await redis_client.delete(f"oauth2:state:{state}")
                await redis_client.delete(f"oauth2:redir:{state}")
            await redis_client.aclose()

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
            logging.warning(f"Failed to read oauth2 temp data from Redis: {e}")

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
                raw_text = await resp.text()
                if resp.status != 200:
                    raise RuntimeError(f"Token exchange failed ({resp.status}): {raw_text}")
                try:
                    token_json = json.loads(raw_text)
                except Exception:
                    raise RuntimeError("Token endpoint returned non-JSON body")

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

        # Save credentials
        success = await db_service.save_oauth2_credentials(
            user_id, provider_slug, access_token, refresh_token, expires_at
        )
        if not success:
            raise RuntimeError("Failed to save OAuth2 credentials")

        logging.info(f"OAuth2 tokens saved for provider {provider_slug}, user {user_id}")

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

    async def get_valid_access_token(
        self, user_id: str, provider_slug: str, db_service: Any
    ) -> Optional[str]:
        """Get valid access token, auto-refresh if expired (5 min buffer)."""
        creds = await db_service.get_user_credentials(user_id, provider_slug, LinkType.OAUTH2)
        if not creds:
            return None

        access_token = creds.get("access_token")
        refresh_token = creds.get("refresh_token")
        expires_at = creds.get("expires_at", 0)

        # Check expiry with 5 min buffer
        if expires_at and time.time() < expires_at - 300:
            return access_token

        # Refresh token
        if not refresh_token:
            logging.warning(f"No refresh token for {provider_slug} user {user_id}, re-auth needed")
            return None

        try:
            new_tokens = await self.refresh_access_token(refresh_token)
        except Exception as e:
            logging.error(f"Token refresh failed for {provider_slug} user {user_id}: {e}")
            return None

        new_access_token = new_tokens.get("access_token")
        new_refresh_token = new_tokens.get("refresh_token", refresh_token)
        new_expires_in = new_tokens.get("expires_in", 86400)
        new_expires_at = int(time.time()) + int(new_expires_in)

        await db_service.save_oauth2_credentials(
            user_id, provider_slug, new_access_token, new_refresh_token, new_expires_at
        )

        return new_access_token

    async def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        """Refresh access token using refresh_token grant."""
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
                if resp.status != 200:
                    error_text = await resp.text()
                    raise RuntimeError(f"Token refresh failed ({resp.status}): {error_text}")
                return await resp.json()
