"""
Whoop Provider

Whoop OAuth2 data provider with authentication and data pulling functionality
"""

import json
import logging
import time
import uuid
from datetime import datetime, timedelta, UTC
from typing import Any, Optional

import aiohttp

from mirobody.collect.base import ProviderInfo
from mirobody.collect.core import LinkType, ProviderStatus
from mirobody.collect.core.push_service import push_service
from mirobody.collect.ingest import FormatDataInput, StandardPulseData, StandardPulseMetaInfo, StandardPulseRecord
from mirobody.collect.providers._platform.base import BasePullProvider
from mirobody.collect.providers._platform.http import VendorError, get_json, get_pages
from mirobody.collect.providers._platform.oauth2 import OAuth2Client
from mirobody.collect.providers._platform.normalize import records_from_facts
from mirobody.kernel import decoders
from mirobody.kernel.ops import is_driver_exception
from mirobody.utils import execute_query
from mirobody.utils.config import safe_read_cfg
from mirobody.utils.tasks import spawn
from mirobody.utils.log import secret_fingerprint

logger = logging.getLogger(__name__)

#: WHOOP's collections, keyed by the decoder type each one holds.
COLLECTIONS: dict[str, str] = {
    "cycle": "/cycle",
    "sleep": "/activity/sleep",
    "workout": "/activity/workout",
    "recovery": "/recovery",
}


class WhoopProvider(BasePullProvider):
    """Whoop Provider - Whoop OAuth2 Data Integration"""

    def __init__(self):
        super().__init__()

        # Load configuration
        self.client_id = safe_read_cfg("WHOOP_CLIENT_ID")
        self.client_secret = safe_read_cfg("WHOOP_CLIENT_SECRET")
        self.redirect_url = safe_read_cfg("WHOOP_REDIRECT_URL")

        # OAuth2 endpoints (configurable, with defaults)
        self.auth_url = (
                safe_read_cfg("WHOOP_AUTH_URL")
                or "https://api.prod.whoop.com/oauth/oauth2/auth"
        )
        self.token_url = (
                safe_read_cfg("WHOOP_TOKEN_URL")
                or "https://api.prod.whoop.com/oauth/oauth2/token"
        )

        # API endpoints (configurable)
        self.api_base_url = (
                safe_read_cfg("WHOOP_API_BASE_URL")
                or "https://api.prod.whoop.com/developer/v2"
        )

        # Scopes
        self.scopes = (
                safe_read_cfg("WHOOP_SCOPES")
                or "offline read:recovery read:sleep read:cycles read:profile read:workout read:body_measurement"
        )

        try:
            self.request_timeout = int(safe_read_cfg("WHOOP_REQUEST_TIMEOUT") or 30)
        except (ValueError, TypeError):
            self.request_timeout = 30

        # OAuth2 client (encapsulates auth URL, token exchange, refresh)
        self.oauth = OAuth2Client(
            client_id=self.client_id or "",
            client_secret=self.client_secret or "",
            redirect_url=self.redirect_url or "",
            auth_url=self.auth_url,
            token_url=self.token_url,
            scopes=self.scopes,
            request_timeout=self.request_timeout,
            refresh_extra_params={"scope": self.scopes},  # Whoop requires scope on refresh
        )

        if not self.client_id or not self.client_secret:
            logger.error("Whoop OAuth credentials not configured. Please set WHOOP_CLIENT_ID and WHOOP_CLIENT_SECRET")
        else:
            logger.info(f"Whoop OAuth configuration validated successfully, client_id:{self.client_id[:3]}, redirect_url:{self.redirect_url}")

    @classmethod
    def create_provider(cls, config: dict[str, Any]) -> Optional['WhoopProvider']:
        """
        Factory method to create Whoop provider from config
        
        Required config keys:
        - WHOOP_CLIENT_ID
        - WHOOP_CLIENT_SECRET
        
        Returns:
            Provider instance if config is valid, None otherwise
        """
        try:
            # Verify config is accessible before creating instance
            from mirobody.utils.config import safe_read_cfg
            client_id = safe_read_cfg("WHOOP_CLIENT_ID")
            client_secret = safe_read_cfg("WHOOP_CLIENT_SECRET")
            # The vendor OAuth client_secret was in this line, at INFO, on every
            # provider init. Logging whether it is configured is the useful
            # part; the value never was.
            logger.info(
                "whoop provider %s, secret %s",
                client_id, secret_fingerprint(client_secret),
            )
            if not client_id or not client_secret:
                # Unset credentials are the usual self-hosted state, not a fault:
                # at WARNING this read as a failure on every boot.
                logger.info("Whoop not configured (WHOOP_CLIENT_ID / WHOOP_CLIENT_SECRET unset); provider off")
                return None

            return cls()
        except Exception as e:
            logger.warning(f"Failed to create Whoop provider: {e}")
            return None

    @property
    def info(self) -> ProviderInfo:
        """Get Provider information"""
        return ProviderInfo(
            slug="theta_whoop",
            name="Whoop",
            description="Whoop fitness and health data integration via OAuth2",
            logo="https://static.thetahealth.ai/res/whoop.png",
            supported=True,
            auth_type=LinkType.OAUTH2,
            status=ProviderStatus.AVAILABLE,
        )

    async def link(self, request: Any) -> dict[str, Any]:
        """Link Whoop OAuth2 Provider - generate OAuth2 authorization URL."""
        user_id = request.user_id
        options = request.options or {}

        try:
            logger.info(f"Generating OAuth2 authorization URL for user: {user_id}")
            return await self.oauth.generate_authorization_url(user_id, options)
        except Exception as e:
            logger.error(f"Error linking Whoop provider: {str(e)}")
            raise RuntimeError(str(e))

    async def callback(self, code: str, state: str) -> dict[str, Any]:
        """Handle OAuth2 callback - exchange authorization code for tokens."""
        try:
            logger.info("Processing OAuth2 callback")
            result = await self.oauth.exchange_code_for_tokens(
                code, state, self.db_service, self.info.slug
            )

            # Trigger immediate pull using unified path
            creds_payload: dict[str, Any] = {
                "user_id": result["user_id"],
                "access_token": result["access_token"],
                "refresh_token": result.get("refresh_token", ""),
            }
            spawn(self._pull_and_push_for_user(creds_payload))

            return {
                "provider_slug": self.info.slug,
                "stage": "completed",
                "return_url": result.get("return_url"),
            }
        except Exception as e:
            logger.error(f"Error in OAuth2 callback: {str(e)}")
            raise RuntimeError(str(e))

    async def unlink(self, user_id: str) -> dict[str, Any]:
        """
        Unlink Whoop provider by deleting user registration from database
        
        Args:
            user_id: User ID
            
        Returns:
            Unlink result data
        """
        try:
            logger.info(f"Unlinking Whoop provider for user: {user_id}")

            # Delete user registration from database
            await self.db_service.delete_user_theta_provider(user_id, self.info.slug)

            logger.info(f"Successfully unlinked Whoop provider for user {user_id}")
            return {"success": True, "message": "Successfully unlinked from Whoop"}

        except Exception as e:
            logger.error(f"Failed to unlink Whoop provider: {str(e)}")
            raise RuntimeError(f"Failed to unlink provider: {str(e)}")

    def _extract_external_user_id(self, saved_data: dict[str, Any]) -> str:
        """Extract Whoop numeric user ID from data records."""
        data_items = saved_data.get("data", [])
        if isinstance(data_items, list) and data_items:
            return str(data_items[0].get("user_id", ""))
        if isinstance(data_items, dict):
            return str(data_items.get("user_id", ""))
        return ""

    async def format_data(self, fmt_input: FormatDataInput) -> StandardPulseData:
        """WHOOP records → standard records, via ``mirobody.kernel.decoders.whoop``.

        The payload is ``{"data_type": ..., "data": [...], "timestamp": pulled_at_ms}``;
        body measurements have no time of their own and are filed at the
        pull instant.
        """
        ctx = fmt_input.context
        request_id = self.generate_request_id()
        if not ctx.theta_user_id:
            logger.error("No theta_user_id found in Whoop format context")
            return self._create_empty_response(request_id, "")
        payload = fmt_input.payload
        data_type = str(payload.get("data_type", "unknown"))
        items = payload.get("data") or []
        if not isinstance(items, list):
            items = [items]
        if not items:
            return self._create_empty_response(request_id, ctx.theta_user_id)
        tz = ctx.user_timezone or "UTC"
        msg_id = ctx.msg_id or ""
        pulled_at = int(payload.get("timestamp") or 0)
        records: list[StandardPulseRecord] = []
        for item in items:
            # WHOOP's own record id, never the per-pull msg_id: the id is part
            # of a reading's identity, so a msg_id stored a new copy per pull.
            record_id = str(item.get("id") or "") if isinstance(item, dict) else ""
            facts = decoders.decode("whoop", data_type, item, tz, pulled_at_ms=pulled_at, source_record_id=record_id)
            records.extend(records_from_facts(facts, slug=self.info.slug, tz=tz, source_id=record_id))
        logger.info("Formatted %d Whoop records from %d %s items", len(records), len(items), data_type)
        return StandardPulseData(
            metaInfo=StandardPulseMetaInfo(userId=ctx.theta_user_id, requestId=request_id, source="theta", timezone=tz),
            healthData=records,
            processingInfo={"provider": "theta_whoop", "data_type": data_type, "msg_id": msg_id, "user_timezone": tz},
        )

    async def pull_from_vendor_api(self, access_token: str, refresh_token: str, days: int | None = None) -> list[dict[str, Any]]:
        """The last `days` of each WHOOP collection, and the body measurement.

        One package per decoder type: `{"data_type", "data", "timestamp"}`.
        A collection record is the whole record, the same object WHOOP's by-id
        endpoint answers, so nothing is fetched twice. A collection that fails
        is skipped and the rest still arrive; a refused token raises
        `VendorAuthError` for the pull loop to count.
        """
        headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
        params: dict[str, Any] = {"limit": 25}
        if days:
            end = datetime.now(UTC)
            params["start"] = (end - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            params["end"] = end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        pulled_at = int(time.time() * 1000)
        packages: list[dict[str, Any]] = []
        async with aiohttp.ClientSession() as session:
            for data_type, path in COLLECTIONS.items():
                try:
                    records = await get_pages(
                        session, f"{self.api_base_url}{path}", headers=headers, params=params,
                        records_key="records", token_param="nextToken", timeout_s=self.request_timeout,
                    )
                except (VendorError, TimeoutError, aiohttp.ClientError) as e:
                    logger.warning("WHOOP collection skipped: data_type=%s error_type=%s",  # phi: ok decoder type name
                                   data_type, type(e).__name__)
                    continue
                if records:
                    packages.append({"data_type": data_type, "data": records, "timestamp": pulled_at})
            try:
                body = await get_json(session, f"{self.api_base_url}/user/measurement/body",
                                      headers=headers, timeout_s=self.request_timeout)
            except (VendorError, TimeoutError, aiohttp.ClientError) as e:
                logger.warning("WHOOP body measurement skipped: error_type=%s", type(e).__name__)
                body = None
            if isinstance(body, dict) and body:
                packages.append({"data_type": "body", "data": [body], "timestamp": pulled_at})
        return packages

    async def _handle_whoop_auth_failure(self, user_id: str, error_details: str) -> None:
        """Handle Whoop authentication failure by cleaning up invalid credentials."""
        try:
            logger.error(f"Whoop authentication failed for user {user_id}: {error_details}")

            # Remove invalid credentials from database
            await self.db_service.delete_user_theta_provider(user_id, self.info.slug)
            logger.info(f"Removed invalid Whoop credentials for user {user_id}")

            # Log guidance for user re-authorization
            logger.error(
                f"User {user_id} needs to re-authorize Whoop connection. "
                f"Refresh token has expired. Please have them complete the OAuth flow again."
            )

        except Exception as e:
            logger.error(f"Error handling Whoop auth failure for user {user_id}: {str(e)}")

    async def get_valid_access_token(self, user_id: str) -> str | None:
        """Get a valid access token for the user, refreshing if necessary."""
        token = await self.oauth.get_valid_access_token(user_id, self.info.slug, self.db_service)
        if not token:
            await self._handle_whoop_auth_failure(user_id, "Token refresh failed or no valid credentials")
        return token

    async def save_raw_data_to_db(self, raw_data: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Save full Whoop payload into health_data_whoop.
        We store the entire provider response as jsonb for auditing and reprocessing.
        """
        try:
            if not isinstance(raw_data, (dict, list)):
                return []

            theta_user_id = self._extract_theta_user_id(raw_data)
            external_user_id = self._extract_external_user_id(raw_data)

            # Generate a simple msg_id using timestamp
            msg_id = f"whoop_{theta_user_id}_{int(time.time())}" if theta_user_id else f"whoop_{int(time.time())}"

            insert_sql = (
                "INSERT INTO health_data_whoop "
                "(create_at, update_at, is_del, msg_id, raw_data, theta_user_id, external_user_id) "
                "VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, :is_del, :msg_id, :raw_data, :theta_user_id, :external_user_id)"
            )
            params = {
                "is_del": False,
                "msg_id": msg_id,
                "raw_data": json.dumps(raw_data, ensure_ascii=False),
                "theta_user_id": theta_user_id,
                "external_user_id": external_user_id,
            }
            await execute_query(query=insert_sql, params=params)

            # Add msg_id to returned data for consistency
            result_data = raw_data.copy() if isinstance(raw_data, dict) else {"data": raw_data}
            result_data["msg_id"] = msg_id
            return [result_data]
        except Exception as e:
            logger.error("WHOOP raw save failed: data_type=%s error_type=%s",
                         raw_data.get("data_type"), type(e).__name__, exc_info=not is_driver_exception(e))
            return []

    async def is_data_already_processed(self, raw_data: dict[str, Any]) -> bool:
        return False

    async def _pull_and_push_for_user(self, credentials: dict[str, Any]) -> bool:
        """
        Override: unified per-user pull + push using OAuth2 tokens.
        Accepts credentials dict which may contain access_token/refresh_token/user_id.
        """
        try:
            user_id = credentials.get("user_id") if isinstance(credentials, dict) else None
            if not user_id:
                logger.error("[whoop:_pull_and_push_for_user] Missing user_id in credentials")
                return False

            # Ensure we have a valid access token (handles refresh if needed)
            access_token = await self.get_valid_access_token(user_id)
            if not access_token:
                logger.error(f"[whoop:_pull_and_push_for_user] Unable to get valid access token for user {user_id}")
                return False

            # Get the latest credentials from database (may include updated refresh_token)
            latest_credentials = await self.db_service.get_user_credentials(user_id, self.info.slug, self.info.auth_type)
            if not latest_credentials:
                logger.error(f"[whoop:_pull_and_push_for_user] Unable to get latest credentials for user {user_id}")
                return False

            refresh_token = latest_credentials.get("refresh_token")
            if not refresh_token:
                logger.warning(f"[whoop:_pull_and_push_for_user] No refresh token available for user {user_id}")

            # Pull recent data
            raw_data_list = await self.pull_from_vendor_api(access_token, refresh_token, days=2)
            if not raw_data_list:
                logger.info(f"No recent whoop data for user {user_id}")
                return True

            success_count = 0
            error_count = 0
            for raw_data in raw_data_list:
                try:
                    # Inject system user ID (from credentials DB).
                    # No need to inject external user ID: _extract_external_user_id
                    # override reads it from data[0]["user_id"] at every call site.
                    raw_data["theta_user_id"] = user_id
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
                        logger.error(f"Failed to push whoop data for user {user_id} with msg_id {msg_id}")
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error processing whoop data for user {user_id}: {str(e)}")
                    continue

            logger.info(f"Processed whoop data for user {user_id}: success={success_count}, errors={error_count}")
            return error_count == 0
        except Exception as e:
            logger.error(f"Error in whoop _pull_and_push_for_user: {str(e)}")
            return False
