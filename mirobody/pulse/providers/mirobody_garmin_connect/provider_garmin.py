"""
Garmin Provider

Garmin OAuth data provider with complete authentication and data pulling functionality
"""

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, UTC
from typing import Any, Optional
from urllib.parse import parse_qs, urlencode

from requests_oauthlib import OAuth1Session

from mirobody.pulse.base import ProviderInfo
from mirobody.pulse.core import LinkType, ProviderStatus
from mirobody.pulse.core.push_service import push_service
from mirobody.pulse.ingest.models.requests import (
    FormatDataInput,
    StandardPulseData,
    StandardPulseMetaInfo,
    StandardPulseRecord,
)
from mirobody.pulse.providers.platform.base import BasePullProvider
from mirobody.pulse.providers.platform.normalize import records_from_facts
from mirobody.kernel import vendors
from mirobody.utils import execute_query
from mirobody.utils.config import safe_read_cfg, global_config
from ....utils.tasks import spawn
from mirobody.utils.log import secret_fingerprint

logger = logging.getLogger(__name__)

SECONDS_TO_MILLISECONDS = 1000


class GarminProvider(BasePullProvider):
    """Garmin Provider - Garmin OAuth Data Integration"""

    def __init__(self):
        super().__init__()
        # Load configuration from safe_read_cfg
        self.client_id = safe_read_cfg("GARMIN_CLIENT_ID")
        self.client_secret = safe_read_cfg("GARMIN_CLIENT_SECRET")
        self.redirect_url = safe_read_cfg("GARMIN_REDIRECT_URL")

        self.request_token_url = (
                safe_read_cfg("GARMIN_TOKEN_URL")
                or "https://connectapi.garmin.com/oauth-service/oauth/request_token"
        )
        self.auth_url = (
                safe_read_cfg("GARMIN_AUTH_URL")
                or "https://connect.garmin.com/oauthConfirm/"
        )
        self.access_token_url = (
                safe_read_cfg("GARMIN_ACCESS_TOKEN_URL")
                or "https://connectapi.garmin.com/oauth-service/oauth/access_token"
        )

        self.api_base_url = (
                safe_read_cfg("GARMIN_API_BASE_URL")
                or "https://apis.garmin.com/wellness-api/rest"
        )

        try:
            self.oauth_temp_ttl = int(safe_read_cfg("OAUTH_TEMP_TTL_SECONDS") or 900)
        except Exception:
            self.oauth_temp_ttl = 900

        # Deprecated in-memory cache (kept for backward compatibility but no longer used)
        self._oauth_token_secret_cache = {}

        # Validate configuration
        if not self.client_id or not self.client_secret:
            logger.error("Garmin OAuth credentials not configured. Please set GARMIN_CLIENT_ID and GARMIN_CLIENT_SECRET")
        else:
            logger.info(f"Garmin OAuth configuration validated successfully, client_id:{self.client_id[:3]}, redirect_url:{self.redirect_url}")

    @classmethod
    def create_provider(cls, config: dict[str, Any]) -> Optional['GarminProvider']:
        """
        Factory method to create Garmin provider from config

        Required config keys:
        - GARMIN_CLIENT_ID
        - GARMIN_CLIENT_SECRET

        Returns:
            Provider instance if config is valid, None otherwise
        """
        try:
            from mirobody.utils.config import safe_read_cfg
            client_id = safe_read_cfg("GARMIN_CLIENT_ID")
            client_secret = safe_read_cfg("GARMIN_CLIENT_SECRET")
            # The vendor OAuth client_secret was in this line, at INFO, on every
            # provider init. Logging whether it is configured is the useful
            # part; the value never was.
            logger.info(
                "Garmin provider %s, secret %s",
                client_id, secret_fingerprint(client_secret),
            )
            if not client_id or not client_secret:
                logger.warning("Failed to create Garmin provider: unable to read config values")
                return None

            return cls()
        except Exception as e:
            logger.warning(f"Failed to create Garmin provider: {e}")
            return None

    def register_pull_task(self) -> bool:
        """
        Register pull task for Garmin provider
        """
        return False

    @property
    def info(self) -> ProviderInfo:
        """Get Provider information"""
        return ProviderInfo(
            slug="theta_garmin",
            name="Garmin Connect",
            description="Garmin fitness and health data integration via OAuth",
            logo="https://static.thetahealth.ai/res/garmin.png",
            supported=True,
            auth_type=LinkType.OAUTH1,
            status=ProviderStatus.AVAILABLE,
        )

    async def link(self, request: Any) -> dict[str, Any]:
        """
        Link Garmin OAuth Provider - Stage 1: Generate OAuth authorization URL

        This method initiates the OAuth flow by generating an authorization URL
        that the user needs to visit to grant permission. After user authorization,
        the callback will be handled by the separate callback() method.

        Args:
            request: Link request containing user_id and options (redirect_url)

        Returns:
            Dict containing 'link_web_url' for user authorization

        Raises:
            RuntimeError: If OAuth configuration is invalid or token generation fails
        """
        user_id = request.user_id
        options = request.options or {}

        try:
            # Generate OAuth authorization URL (Stage 1 of OAuth flow)
            logger.info(f"Generating OAuth authorization URL for user: {user_id}")
            return await self._generate_authorization_url(user_id, options)

        except Exception as e:
            logger.error(f"Error linking Garmin provider: {str(e)}")
            raise RuntimeError(str(e))

    async def _generate_authorization_url(self, user_id: str, options: dict[str, Any]) -> dict[str, Any]:
        """
        Generate OAuth authorization URL for user to grant permission

        This method creates a request token with Garmin, stores the token secret
        in Redis cache, and builds the authorization URL that the user needs to visit.

        Args:
            user_id: User ID to associate with the OAuth flow
            options: Dict containing redirect_url for OAuth callback

        Returns:
            Dict containing 'link_web_url' for user authorization

        Raises:
            ValueError: If OAuth credentials are not configured
            RuntimeError: If request token generation fails
        """
        try:
            if not self.client_id or not self.client_secret:
                raise ValueError("Missing GARMIN_CLIENT_ID or GARMIN_CLIENT_SECRET configuration")

            # Create OAuth1Session for request token
            oauth = OAuth1Session(
                client_key=self.client_id,
                client_secret=self.client_secret,
                signature_method='HMAC-SHA1',
                signature_type='auth_header',
                verifier=None
            )

            # Get request token. OAuth1Session is synchronous `requests` —
            # awaiting it in a thread keeps this provider from freezing the
            # event loop (Oura/Whoop use aiohttp natively; Garmin is the one
            # provider still on OAuth1, which aiohttp does not speak).
            resp = await asyncio.to_thread(oauth.post, self.request_token_url)

            if resp.status_code != 200:
                raise RuntimeError(f"Failed to get request token: {resp.status_code} - {resp.text}")

            # Parse response
            params = parse_qs(resp.text)
            oauth_token = params['oauth_token'][0]
            oauth_token_secret = params['oauth_token_secret'][0]

            # Store oauth_token_secret in Redis keyed by oauth_token (TTL 15 minutes)
            try:
                cfg = global_config()
                redis_config = cfg.get_redis()
                redis_client = await redis_config.get_async_client()
                await redis_client.setex(
                    f"oauth:secret:{oauth_token}", self.oauth_temp_ttl, oauth_token_secret
                )
                # Optionally store user for cross-check (not strictly required)
                await redis_client.setex(
                    f"oauth:user:{oauth_token}", self.oauth_temp_ttl, user_id or ""
                )
                await redis_client.aclose()
            except Exception as e:
                logger.warning(f"Failed to write oauth temp data to Redis: {str(e)}")

            # Build authorization URL
            redirect_url = self.redirect_url
            if not redirect_url:
                raise ValueError("Missing GARMIN_REDIRECT_URL configuration")

            # Attach optional return_url to callback for round-trip
            return_url = options.get("return_url")
            if return_url:
                # append return_url as query to our callback
                # callback is handled by /api/v1/pulse/{platform}/{provider}/callback
                # here we embed return_url so it can be read back on callback
                if "?" in redirect_url:
                    redirect_uri_with_return = f"{redirect_url}&return_url={urlencode({'r': return_url})[2:]}"
                else:
                    redirect_uri_with_return = f"{redirect_url}?return_url={urlencode({'r': return_url})[2:]}"
            else:
                redirect_uri_with_return = redirect_url

            auth_params = {
                "oauth_token": oauth_token,
                "oauth_callback": redirect_uri_with_return
            }

            authorization_url = f"{self.auth_url}?{urlencode(auth_params)}"

            logger.info(f"Generated OAuth authorization URL for user {user_id}")

            return {
                "link_web_url": authorization_url
            }

        except Exception as e:
            logger.error(f"Error generating authorization URL: {str(e)}")
            raise

    async def callback(self, oauth_token: str, oauth_verifier: str) -> dict[str, Any]:
        """
        Handle OAuth callback - Stage 2: Exchange tokens and complete authentication

        This method processes the OAuth callback from Garmin, exchanges the temporary
        tokens for permanent access tokens, and saves the credentials to the database.
        The user_id is retrieved from Redis cache using the oauth_token as the key.

        Args:
            oauth_token: OAuth token received from Garmin callback
            oauth_verifier: OAuth verifier received from Garmin callback

        Returns:
            Dict containing provider_slug, access_token (truncated), and stage info

        Raises:
            RuntimeError: If token exchange fails or credentials cannot be saved
        """
        try:
            logger.info("Processing OAuth callback")
            credentials = {
                "oauth_token": oauth_token,
                "oauth_verifier": oauth_verifier
            }
            return await self._handle_oauth_callback(None, credentials)
        except Exception as e:
            logger.error(f"Error in OAuth callback: {str(e)}")
            raise RuntimeError(str(e))

    async def _handle_oauth_callback(self, user_id: str | None, credentials: dict[str, Any]) -> dict[str, Any]:
        """
        Internal method to handle OAuth callback and exchange tokens

        This method retrieves the user_id and oauth_token_secret from Redis cache,
        exchanges the temporary tokens for permanent access tokens with Garmin,
        and saves the credentials to the database.

        Args:
            user_id: User ID (ignored, will be retrieved from Redis)
            credentials: Dict containing oauth_token and oauth_verifier

        Returns:
            Dict with provider_slug, truncated access_token, and completion status

        Raises:
            ValueError: If required tokens are missing
            RuntimeError: If token exchange or credential saving fails
        """
        try:
            oauth_token = credentials.get("oauth_token")
            oauth_verifier = credentials.get("oauth_verifier")

            # Read oauth_token_secret and user_id from Redis by oauth_token (single source of truth)
            try:
                cfg = global_config()
                redis_config = cfg.get_redis()
                redis_client = await redis_config.get_async_client()
                oauth_token_secret = await redis_client.get(f"oauth:secret:{oauth_token}")
                cached_user_id = await redis_client.get(f"oauth:user:{oauth_token}")
                await redis_client.delete(f"oauth:secret:{oauth_token}")
                await redis_client.delete(f"oauth:user:{oauth_token}")
                await redis_client.aclose()
                if isinstance(oauth_token_secret, bytes):
                    oauth_token_secret = oauth_token_secret.decode("utf-8")
                if isinstance(cached_user_id, bytes):
                    cached_user_id = cached_user_id.decode("utf-8")
            except Exception as e:
                logger.warning(f"Failed to read oauth temp data from Redis: {str(e)}")
                oauth_token_secret = None
                cached_user_id = None

            # Always rely on Redis-stored user_id to avoid spoofed params
            user_id = cached_user_id
            if user_id:
                logger.info(f"Using user_id from Redis: {user_id}")

            if not oauth_token or not oauth_verifier:
                raise ValueError("Missing oauth_token or oauth_verifier in callback")

            if not oauth_token_secret:
                raise ValueError("Missing oauth_token_secret from stage 1")

            if not user_id:
                raise ValueError("Missing user_id for OAuth callback")

            # Create OAuth1Session for access token
            oauth = OAuth1Session(
                client_key=self.client_id,
                client_secret=self.client_secret,
                resource_owner_key=oauth_token,
                resource_owner_secret=oauth_token_secret,
                verifier=oauth_verifier
            )
            garmin_user_id = await asyncio.to_thread(self._get_user_id, oauth)

            # Get access token (sync requests — run in a thread, see above)
            resp = await asyncio.to_thread(oauth.post, self.access_token_url)

            if resp.status_code != 200:
                raise RuntimeError(f"Failed to get access token: {resp.status_code} - {resp.text}")

            # Parse access token response
            params = parse_qs(resp.text)
            access_token = params['oauth_token'][0]
            access_token_secret = params['oauth_token_secret'][0]

            # Save credentials to database using new OAuth1 method with user_name
            # Use Garmin user id retrieved via _get_user_id(oauth)
            success = await self.db_service.save_oauth1_credentials(
                user_id, self.info.slug, access_token, access_token_secret, user_name=garmin_user_id
            )

            if not success:
                raise RuntimeError("Failed to save OAuth credentials")

            # Redis keys already deleted above; no in-memory cleanup required

            logger.info(f"Successfully linked Garmin provider for user {user_id}")

            # Build credentials payload directly from freshly obtained tokens
            creds_payload: dict[str, Any] = {
                "access_token": access_token,
                "access_token_secret": access_token_secret,
                "user_id": user_id,
            }

            # Start an async task to pull data after successful link
            spawn(self._pull_and_push_for_user(creds_payload))

            return {
                "provider_slug": self.info.slug,
                "access_token": access_token[:20] + "...",
                "stage": "completed"
            }

        except Exception as e:
            logger.error(f"Error handling OAuth callback: {str(e)}")
            raise

    async def unlink(self, user_id: str) -> dict[str, Any]:
        """
        Unlink Garmin provider by deleting user registration

        Args:
            user_id: User ID

        Returns:
            Unlink result data
        """
        api_unlink_success = False
        api_error_message = None

        try:
            logger.info(f"Unlinking Garmin provider for user: {user_id}")

            # Get stored credentials using new OAuth method
            credentials = await self.db_service.get_user_credentials(user_id, self.info.slug, self.info.auth_type)
            if not credentials:
                await self.db_service.delete_user_theta_provider(user_id, self.info.slug)
                logger.warning(f"No stored credentials found for user {user_id}")
                return {"success": True, "message": "No credentials found; treated as unlinked"}

            # Use new OAuth1 format
            access_token = credentials.get("access_token")
            token_secret = credentials.get("access_token_secret")

            if not access_token or not token_secret:
                logger.warning(f"Invalid stored credentials for user {user_id}")
                # Will be removed from database in finally block
            else:
                # Create OAuth1Session for API calls
                oauth = OAuth1Session(
                    client_key=self.client_id,
                    client_secret=self.client_secret,
                    resource_owner_key=access_token,
                    resource_owner_secret=token_secret
                )

                # Call DELETE API to unlink user (sync requests — thread)
                unlink_url = f"{self.api_base_url}/user/registration"
                resp = await asyncio.to_thread(oauth.delete, unlink_url)

                if resp.status_code == 204:
                    api_unlink_success = True
                    logger.info(f"Successfully unlinked Garmin provider for user {user_id}")
                else:
                    api_error_message = f"Garmin API unlink failed: {resp.status_code} - {resp.text}"
                    logger.error(api_error_message)
                    # Raise on API unlink failure as requested
                    raise RuntimeError(api_error_message)

        except Exception as e:
            api_error_message = str(e)
            logger.error(f"Error unlinking Garmin provider: {str(e)}")

        # Always try to remove from database
        try:
            await self.db_service.delete_user_theta_provider(user_id, self.info.slug)

            if api_unlink_success:
                return {"success": True, "message": "Successfully unlinked from Garmin"}
            # After cleanup, propagate API failure
            raise RuntimeError(f"Failed to unlink from Garmin: {api_error_message}")

        except Exception as db_error:
            logger.error(f"Failed to remove from database: {str(db_error)}")
            raise RuntimeError(f"Failed to unlink provider: {api_error_message or 'Unknown error'}")

    async def format_data(self, fmt_input: FormatDataInput) -> StandardPulseData:
        """Garmin summaries → standard records, via ``mirobody.kernel.vendors.garmin``.

        The payload is ``{data_type: [summary, ...], ...}`` for one user (a
        webhook push or an active pull, already split per user). Every data
        type Garmin sends is decoded by the shared table; ``activityDetails``
        wraps an activity summary under ``summary``.
        """
        ctx = fmt_input.context
        request_id = self.generate_request_id()
        if not ctx.theta_user_id:
            logger.error("No theta_user_id found in format context")
            return self._create_empty_response(request_id, "")
        tz = ctx.user_timezone or "UTC"
        msg_id = ctx.msg_id or ""
        records: list[StandardPulseRecord] = []
        types: list[str] = []
        for key, items in fmt_input.payload.items():
            if key in ("theta_user_id", "msg_id") or not isinstance(items, list) or not items:
                continue
            types.append(key)
            for item in items:
                if key == "activityDetails":
                    item = item.get("summary") if isinstance(item, dict) else None
                    facts = vendors.decode("garmin", "activities", item, tz, source_record_id=msg_id) if item else []
                else:
                    facts = vendors.decode("garmin", key, item, tz, source_record_id=msg_id)
                records.extend(records_from_facts(facts, slug=self.info.slug, tz=tz, source_id=msg_id))
        if not types:
            return self._create_empty_response(request_id, ctx.theta_user_id)
        logger.info("Formatted %d Garmin records from %d data types", len(records), len(types))
        return StandardPulseData(
            metaInfo=StandardPulseMetaInfo(userId=ctx.theta_user_id, requestId=request_id, source="theta", timezone=tz),
            healthData=records,
            processingInfo={"provider": "theta_garmin", "data_types": types, "msg_id": msg_id, "user_timezone": tz},
        )

    def _get_api_endpoints_config(self, start_timestamp: int, end_timestamp: int) -> dict[str, str]:
        """
        Get API endpoints with correct parameter names for each endpoint.

        Different Garmin API endpoints require different parameter names:
        - Most endpoints use: uploadStartTimeInSeconds/uploadEndTimeInSeconds
        - activityDetails uses: summaryStartTimeInSeconds/summaryEndTimeInSeconds
        - Other endpoints may have different requirements

        Args:
            start_timestamp: Start timestamp in seconds
            end_timestamp: End timestamp in seconds

        Returns:
            Dict mapping data type to complete API URL
        """
        endpoints_config = {
            # Standard endpoints using uploadStartTimeInSeconds/uploadEndTimeInSeconds
            "sleeps": {
                "path": "/sleeps",
                "start_param": "uploadStartTimeInSeconds",
                "end_param": "uploadEndTimeInSeconds"
            },
            "dailies": {
                "path": "/dailies",
                "start_param": "uploadStartTimeInSeconds",
                "end_param": "uploadEndTimeInSeconds"
            },
            "bodyComps": {
                "path": "/bodyComps",
                "start_param": "uploadStartTimeInSeconds",
                "end_param": "uploadEndTimeInSeconds"
            },
            "userMetrics": {
                "path": "/userMetrics",
                "start_param": "uploadStartTimeInSeconds",
                "end_param": "uploadEndTimeInSeconds"
            },
            "hrv": {
                "path": "/hrv",
                "start_param": "uploadStartTimeInSeconds",
                "end_param": "uploadEndTimeInSeconds"
            },
            "stress": {
                "path": "/stressDetails",
                "start_param": "uploadStartTimeInSeconds",
                "end_param": "uploadEndTimeInSeconds"
            },
            "pulseOx": {
                "path": "/pulseOx",
                "start_param": "uploadStartTimeInSeconds",
                "end_param": "uploadEndTimeInSeconds"
            },
            "respiration": {
                "path": "/respiration",
                "start_param": "uploadStartTimeInSeconds",
                "end_param": "uploadEndTimeInSeconds"
            },
            "bloodPressures": {
                "path": "/bloodPressures",
                "start_param": "uploadStartTimeInSeconds",
                "end_param": "uploadEndTimeInSeconds"
            },
            "skinTemp": {
                "path": "/skinTemp",
                "start_param": "uploadStartTimeInSeconds",
                "end_param": "uploadEndTimeInSeconds"
            },
            "activities": {
                "path": "/activities",
                "start_param": "uploadStartTimeInSeconds",
                "end_param": "uploadEndTimeInSeconds"
            },
            "activityDetails": {
                "path": "/activityDetails",
                "start_param": "uploadStartTimeInSeconds",
                "end_param": "uploadEndTimeInSeconds"
            },
        }

        # Build complete URLs
        data_types = {}
        for data_type, config in endpoints_config.items():
            path = config["path"]
            start_param = config["start_param"]
            end_param = config["end_param"]

            url = f"{self.api_base_url}{path}?{start_param}={start_timestamp}&{end_param}={end_timestamp}"
            data_types[data_type] = url

        return data_types

    async def pull_from_vendor_api(self, access_token: str, token_secret: str, days: int | None = 1) -> list[dict[str, Any]]:
        """
        Pull data from Garmin API using OAuth credentials

        Args:
            access_token: OAuth access token
            token_secret: OAuth token secret
            days: Number of days to pull data for (default: 1 days for initial connection)

        Returns:
            List of raw data
        """
        try:
            logger.info("Starting Garmin data pull")

            if not access_token or not token_secret:
                raise ValueError("Access token and token secret are required")

            # Create OAuth session
            oauth = OAuth1Session(
                client_key=self.client_id,
                client_secret=self.client_secret,
                resource_owner_key=access_token,
                resource_owner_secret=token_secret
            )

            # Get user ID first. Everything below is synchronous `requests`
            # via OAuth1Session; each unit runs in a worker thread so a
            # multi-endpoint, multi-day pull does not stall every other
            # coroutine on this loop for its whole duration — which is what
            # happened when these were called inline in this `async def`.
            user_id = await asyncio.to_thread(self._get_user_id, oauth)

            all_data = []

            end_timestamp = int(datetime.now(UTC).timestamp())  # utc, timestamp in seconds
            start_timestamp = end_timestamp - (days * 24 * 60 * 60)  # N days ago

            logger.info(f"Pulling Garmin data for the last {days} days")

            # Split into 1-day batches if days > 1 due to API limitation (max 86400 seconds)
            if days > 1:
                logger.info(f"Splitting {days} days into daily batches due to API limitation")
                for day_offset in range(days):
                    batch_end = end_timestamp - (day_offset * 24 * 60 * 60)
                    batch_start = batch_end - (24 * 60 * 60)
                    logger.info(f"Pulling batch {day_offset + 1}/{days}: {batch_start} to {batch_end}")
                    
                    batch_data = await asyncio.to_thread(
                        self._pull_data_batch, oauth, user_id, batch_start, batch_end)
                    all_data.extend(batch_data)
            else:
                # Single day request
                batch_data = await asyncio.to_thread(
                    self._pull_data_batch, oauth, user_id, start_timestamp, end_timestamp)
                all_data.extend(batch_data)

            logger.info(f"Completed Garmin data pull: {len(all_data)} data sets retrieved")
            return all_data

        except Exception as e:
            logger.error(f"Error in Garmin data pull: {str(e)}")
            return []

    def _pull_data_batch(self, oauth: OAuth1Session, user_id: str, start_timestamp: int, end_timestamp: int) -> list[dict[str, Any]]:
        """
        Pull data for a single time batch (max 24 hours)
        
        Args:
            oauth: OAuth session
            user_id: Garmin user ID
            start_timestamp: Start timestamp in seconds
            end_timestamp: End timestamp in seconds
            
        Returns:
            List of raw data for this batch
        """
        batch_data = []
        
        # Get API endpoints with correct parameter names for each endpoint
        data_types = self._get_api_endpoints_config(start_timestamp, end_timestamp)

        for data_type, url in data_types.items():
            try:
                logger.info(f"Pulling {data_type} data from Garmin API")
                resp = oauth.get(url)

                if resp.status_code == 200:
                    data = resp.json()
                    # Normalize response for certain endpoints that wrap list in a key
                    if data_type == "epochs" and isinstance(data, dict) and "epochs" in data:
                        data = data.get("epochs")

                    raw_data = {
                        "user_id": user_id,
                        "data_type": data_type,
                        "data": data,
                        "timestamp": int(time.time() * SECONDS_TO_MILLISECONDS),
                        "api_url": url
                    }
                    batch_data.append(raw_data)
                    logger.info(f"Successfully pulled {data_type} data: {len(data) if isinstance(data, list) else 1} records")
                else:
                    logger.warning(f"Failed to pull {data_type} data: {resp.status_code} - {resp.text}")

            except Exception as e:
                logger.error(f"Error pulling {data_type} data: {str(e)}")
                continue
        
        return batch_data

    def _get_user_id(self, oauth: OAuth1Session) -> str:
        """Get Garmin user ID"""
        try:
            user_id_url = f"{self.api_base_url}/user/id"
            resp = oauth.get(user_id_url)

            if resp.status_code == 200:
                user_data = resp.json()
                user_id = user_data.get("userId", "")
                logger.info(f"Retrieved Garmin user ID: {user_id}")
                return str(user_id)
            logger.error(f"Failed to get user ID: {resp.status_code} - {resp.text}")
            return ""

        except Exception as e:
            logger.error(f"Error getting user ID: {str(e)}")
            return ""

    def _detect_data_format(self, raw_data: dict[str, Any]) -> str:
        is_active_pull_format = (
                "data" in raw_data and
                "theta_user_id" in raw_data and
                "data_type" in raw_data and
                isinstance(raw_data.get("data"), list)
        )

        return "active_pull" if is_active_pull_format else "webhook"

    def _split_webhook_data_by_user_id(self, raw_data: dict[str, Any]) -> tuple[dict[str, dict[str, list[dict]]], set[str]]:
        """

        Args:
            raw_data: {data_type: [items...]}

        Returns:
            Tuple of:
            - user_data_map: {external_user_id: {data_type: [items...]}}
            - external_user_ids: Set of external user IDs
        """
        user_data_map = {}  # {external_user_id: {data_type: [items...]}}
        external_user_ids = set()

        logger.info("Processing existing webhook format")

        for data_type, data_list in raw_data.items():
            if not isinstance(data_list, list):
                continue

            for item in data_list:
                if not isinstance(item, dict):
                    continue

                external_user_id = item.get("userId")
                if not external_user_id:
                    continue

                external_user_id = str(external_user_id)
                external_user_ids.add(external_user_id)

                if external_user_id not in user_data_map:
                    user_data_map[external_user_id] = {}

                if data_type not in user_data_map[external_user_id]:
                    user_data_map[external_user_id][data_type] = []

                user_data_map[external_user_id][data_type].append(item)

        return user_data_map, external_user_ids

    async def _handle_deregistration(self, raw_data: dict[str, Any]) -> None:
        """
        handle Garmin deregistration webhook

        Args:
            raw_data: Deregistration，including theta_user_id
        """
        deregistrations = raw_data.get("deregistrations", [])
        if not isinstance(deregistrations, list):
            logger.warning("Invalid deregistrations format")
            return

        # Try to get theta_user_id from raw_data (mapped by webhook upper layer)
        theta_user_id = raw_data.get("theta_user_id")

        for dereg in deregistrations:
            if not isinstance(dereg, dict):
                continue

            external_user_id = dereg.get("userId")
            if not external_user_id:
                continue

            try:
                logger.info(f"Processing deregistration for external user: {external_user_id}")

                if not theta_user_id:
                    user_mapping = await self._batch_map_external_to_theta_user_ids([str(external_user_id)])
                    theta_user_id = user_mapping.get(str(external_user_id))

                if not theta_user_id:
                    logger.warning(f"No theta_user_id found for external user: {external_user_id}")
                    continue

                await self.db_service.delete_user_theta_provider(theta_user_id, self.info.slug)
                logger.info(f"Successfully deregistered user {theta_user_id} (external: {external_user_id})")

            except Exception as e:
                logger.error(f"Error processing deregistration for {external_user_id}: {str(e)}")

    def _split_active_pull_data_by_user_id(self, raw_data: dict[str, Any]) -> tuple[dict[str, dict[str, list[dict]]], set[str]]:
        """Split active-pull format data by user.

        In active-pull format, theta_user_id is pre-injected by
        _pull_and_push_for_user and used directly as the grouping key
        (identity mapping in save_raw_data_to_db).

        Args:
            raw_data: {
                "data": [...],
                "theta_user_id": "system_user_1",
                "data_type": "sleeps",
                "api_url": "...",
                "timestamp": 1757738143556
            }

        Returns:
            Tuple of:
            - user_data_map: {theta_user_id: {data_type: [items...]}}
            - user_ids: Set of theta user IDs
        """
        user_data_map = {}
        external_user_ids = set()

        external_user_id = str(raw_data["theta_user_id"])
        data_type = raw_data["data_type"]
        data_list = raw_data["data"]

        logger.info(f"Processing active pull format for user {external_user_id}, data_type {data_type}")

        external_user_ids.add(external_user_id)

        if external_user_id not in user_data_map:
            user_data_map[external_user_id] = {}

        if data_type not in user_data_map[external_user_id]:
            user_data_map[external_user_id][data_type] = []

        if isinstance(data_list, list):
            for item in data_list:
                if isinstance(item, dict):
                    if "userId" not in item:
                        item["userId"] = external_user_id
                    user_data_map[external_user_id][data_type].append(item)
        else:
            logger.warning(f"Data field is not a list for user {external_user_id}, data_type {data_type}")

        return user_data_map, external_user_ids

    def _split_data_by_user_id(self, raw_data: dict[str, Any]) -> tuple[dict[str, dict[str, list[dict]]], set[str], str]:
        """

        Args:
            raw_data: ：
            1. Webhook: {data_type: [items...]}
            2. pull data: {
                "data": [...],
                "user_id": "1",
                "data_type": "sleeps",
                "api_url": "...",
                "timestamp": 1757738143556
            }

        Returns:
            Tuple of:
            - user_data_map: {user_id: {data_type: [items...]}}
            - user_ids: Set of user IDs
            - data_format: ("active_pull" or "webhook")
        """
        data_format = self._detect_data_format(raw_data)

        if data_format == "active_pull":
            user_data_map, user_ids = self._split_active_pull_data_by_user_id(raw_data)
            return user_data_map, user_ids, data_format
        user_data_map, user_ids = self._split_webhook_data_by_user_id(raw_data)
        return user_data_map, user_ids, data_format

    async def save_raw_data_to_db(self, raw_data: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Args:
            raw_data: ：
            1. Webhook: {
                'dailies': [{userId: 'a', summaryId: 'xxx', ...}, {userId: 'b', summaryId: 'yyy', ...}],
                'sleeps': [{userId: 'a', summaryId: 'zzz', ...}, {userId: 'b', summaryId: 'www', ...}]
            }
            2. pull : {
                "data": [...],
                "user_id": "theta_xxx",
                "data_type": "sleeps",
                "api_url": "...",
                "timestamp": 1757738143556
            }
            3. Deregistration: {
                "deregistrations": [
                    {"userId": "external_user_id", "userAccessToken": "token"}
                ]
            }

        Returns:
            raw_data_by_id:
        """
        try:
            if not isinstance(raw_data, dict):
                return []

            user_data_map, user_ids, data_format = self._split_data_by_user_id(raw_data)

            if not user_data_map:
                logger.warning("No valid data found")
                return []

            if data_format == "active_pull":
                user_id_to_theta_mapping = {user_id: user_id for user_id in user_ids}
                logger.info(f"Using direct mapping for active pull format: {len(user_ids)} users")
            else:
                user_id_to_theta_mapping = await self._batch_map_external_to_theta_user_ids(
                    list(user_ids)
                )
                logger.info(f"Mapped {len(user_id_to_theta_mapping)}/{len(user_ids)} external user IDs")

            final_result_list = []

            for user_id, user_data in user_data_map.items():
                theta_user_id = user_id_to_theta_mapping.get(user_id)

                if not theta_user_id:
                    logger.warning(f"Failed to map user ID {user_id} to theta user ID")
                    continue

                if not isinstance(user_data, dict):
                    logger.error(f"Invalid user_data type for user {user_id}: {type(user_data)}, expected dict")
                    continue

                user_result = user_data.copy()
                user_result["theta_user_id"] = theta_user_id

                data_key = next(
                    (item.get("summaryId") for data_list in user_result.values()
                     if isinstance(data_list, list)
                     for item in data_list
                     if isinstance(item, dict) and item.get("summaryId")),
                    None
                )
                if not data_key:
                    data_key = f"{user_id}_{int(time.time())}"
                user_result["msg_id"] = data_key

                try:
                    insert_sql = (
                        "INSERT INTO health_data_garmin "
                        "(create_at, update_at, is_del, msg_id, raw_data, theta_user_id, external_user_id) "
                        "VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, :is_del, :msg_id, :raw_data, :theta_user_id, :external_user_id) "
                        "ON CONFLICT (msg_id) DO NOTHING"
                    )

                    external_user_id = user_id

                    insert_params = {
                        "is_del": False,
                        "msg_id": data_key,
                        "raw_data": json.dumps(user_result, ensure_ascii=False),
                        "theta_user_id": theta_user_id,
                        "external_user_id": external_user_id,
                    }
                    await execute_query(query=insert_sql, params=insert_params)
                    logger.debug(f"Saved user data with msg_id: {data_key} for user: {theta_user_id}")

                    if "deregistrations" in user_result and isinstance(user_result.get("deregistrations"), list):
                        logger.info("Executing deregistration actions after saving")
                        await self._handle_deregistration(user_result)
                        continue

                except Exception as e:
                    logger.error(f"Error saving user data for {user_id}: {str(e)}")
                    continue

                final_result_list.append(user_result)

            logger.info(f"Successfully processed {len(final_result_list)} users with format: {data_format}")
            return final_result_list

        except Exception as e:
            logger.error(f"Error saving Garmin raw data: {str(e)}")
            return []

    async def _batch_map_external_to_theta_user_ids(self, external_user_ids: list[str]) -> dict[str, str]:
        """
        Args:
            external_user_ids: external user ID

        Returns:
            mapping: external_user_id -> theta_user_id
        """
        if not external_user_ids:
            return {}

        try:
            placeholders = ", ".join([f":username_{i}" for i in range(len(external_user_ids))])
            sql = (
                f"SELECT username, user_id FROM health_user_provider "
                f"WHERE username IN ({placeholders}) AND provider = :provider AND is_del = FALSE "
                f"ORDER BY username, update_at DESC"
            )

            params = {
                "provider": self.info.slug
            }
            for i, external_id in enumerate(external_user_ids):
                params[f"username_{i}"] = external_id

            result = await execute_query(query=sql, params=params)

            mapping = {}
            seen_usernames = set()

            if result:
                for row in result:
                    username = row["username"]
                    if username not in seen_usernames:
                        mapping[username] = row["user_id"]
                        seen_usernames.add(username)

            logger.info(f"Mapped {len(mapping)} out of {len(external_user_ids)} external user IDs to theta user IDs")

            unmapped = set(external_user_ids) - set(mapping.keys())
            if unmapped:
                logger.warning(f"Failed to map external user IDs: {unmapped}")

            return mapping

        except Exception as e:
            logger.error(f"Error in batch mapping external to theta user IDs: {str(e)}")
            return {}

    async def is_data_already_processed(self, raw_data: dict[str, Any]) -> bool:
        return False

    async def _pull_and_push_for_user(self, credentials: dict[str, Any]) -> bool:
        """
        Override base implementation to pull with OAuth1 credentials and push to platform.

        Args:
            credentials: Dict containing at least 'user_id'. Access tokens are loaded from DB.

        Returns:
            Whether successful
        """
        try:
            user_id = credentials.get("user_id")
            if not user_id:
                logger.error("[_pull_and_push_for_user] Missing user_id in credentials")
                return False
            access_token = credentials.get("access_token")
            token_secret = credentials.get("access_token_secret")
            if not access_token or not token_secret:
                logger.error(f"[_pull_and_push_for_user] Invalid credentials for user {user_id} - missing token or secret")
                return False

            # Wait a few seconds for newly issued OAuth tokens to become effective on Garmin servers
            logger.info(f"Waiting for OAuth tokens to become effective for user {user_id}")
            await asyncio.sleep(8)

            # Pull from vendor API
            raw_data_list = await self.pull_from_vendor_api(access_token, token_secret, days=7)
            if not raw_data_list:
                logger.info(f"No data pulled for user {user_id}")
                return True

            success_count = 0
            error_count = 0

            for raw_data in raw_data_list:
                try:
                    raw_data["user_id"] = user_id

                    # Optional: allow provider-level dedup gates
                    if await self.is_data_already_processed(raw_data):
                        continue

                    msg_id = str(uuid.uuid4())
                    push_success = await push_service.push_data(
                        platform="theta",
                        provider_slug=self.info.slug,
                        data=raw_data,
                        msg_id=msg_id,
                    )

                    if push_success:
                        success_count += 1
                    else:
                        error_count += 1
                        logger.error(f"Failed to push data for user {user_id} with msg_id {msg_id}")
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing data for user {user_id}: {str(e)}")
                    continue

            logger.info(f"Processed {success_count} records for user {user_id}; errors={error_count}")
            return error_count == 0

        except Exception as e:
            logger.error(f"Error in _pull_and_push_for_user: {str(e)}")
            return False
