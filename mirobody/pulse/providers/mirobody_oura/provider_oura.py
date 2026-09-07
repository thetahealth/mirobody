"""
Oura Provider

Oura Ring OAuth2 data provider with authentication and data pulling functionality.
Supports sleep, activity, readiness, heart rate, SpO2, stress, and more.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta, UTC
from typing import Any, Optional

import aiohttp

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
from mirobody.pulse.providers.platform.oauth2 import OAuth2Client
from mirobody.pulse.providers.platform.normalize import records_from_facts
from mirobody.kernel import vendors
from mirobody.utils import execute_query
from mirobody.utils.config import safe_read_cfg
from ....utils.tasks import spawn

logger = logging.getLogger(__name__)


class OuraProvider(BasePullProvider):
    """Oura Provider — Oura Ring OAuth2 Data Integration"""

    # API constants
    API_BASE_URL = "https://api.ouraring.com"
    AUTH_URL = "https://cloud.ouraring.com/oauth/authorize"
    TOKEN_URL = "https://api.ouraring.com/oauth/token"
    DEFAULT_SCOPES = "personal daily heartrate workout session spo2"

    # Sandbox: no real token needed, any string works as Bearer token
    # e.g. curl -H "Authorization: Bearer test" https://api.ouraring.com/v2/sandbox/usercollection/sleep
    SANDBOX_API_PREFIX = "/v2/sandbox/usercollection"

    # Endpoints to pull. Where each document's time comes from is declared
    # once, in ``mirobody.kernel.vendors.oura.STRATEGY``.
    API_ENDPOINTS = [
        {"path": "/v2/usercollection/personal_info", "data_type": "personal_info", "paginated": False},
        {"path": "/v2/usercollection/sleep", "data_type": "sleep", "paginated": True},
        {"path": "/v2/usercollection/daily_sleep", "data_type": "daily_sleep", "paginated": True},
        {"path": "/v2/usercollection/daily_activity", "data_type": "daily_activity", "paginated": True},
        {"path": "/v2/usercollection/daily_readiness", "data_type": "daily_readiness", "paginated": True},
        {"path": "/v2/usercollection/heartrate", "data_type": "heartrate", "paginated": False},
        {"path": "/v2/usercollection/daily_spo2", "data_type": "daily_spo2", "paginated": True},
        {"path": "/v2/usercollection/daily_stress", "data_type": "daily_stress", "paginated": True},
        # Disabled: returns 401 — likely requires Oura Membership ($5.99/mo) subscription
        # {"path": "/v2/usercollection/daily_resilience", "data_type": "daily_resilience", "paginated": True},
        # {"path": "/v2/usercollection/daily_cardiovascular_age", "data_type": "daily_cardiovascular_age", "paginated": True},
        {"path": "/v2/usercollection/vo2_max", "data_type": "vo2_max", "paginated": True},
        {"path": "/v2/usercollection/workout", "data_type": "workout", "paginated": True},
        {"path": "/v2/usercollection/session", "data_type": "session", "paginated": True},
        {"path": "/v2/usercollection/sleep_time", "data_type": "sleep_time", "paginated": True},
    ]

    def __init__(self):
        super().__init__()

        # OAuth2 client (reusable across all OAuth2 providers)
        client_id = safe_read_cfg("OURA_CLIENT_ID")
        client_secret = safe_read_cfg("OURA_CLIENT_SECRET")
        redirect_url = safe_read_cfg("OURA_REDIRECT_URL")

        self.oauth = OAuth2Client(
            client_id=client_id or "",
            client_secret=client_secret or "",
            redirect_url=redirect_url or "",
            auth_url=self.AUTH_URL,
            token_url=self.TOKEN_URL,
            scopes=self.DEFAULT_SCOPES,
        )

        # Pull configuration
        self.backfill_days = 30
        self.request_timeout = 30

        if not client_id or not client_secret:
            logger.error("Oura OAuth credentials not configured. Please set OURA_CLIENT_ID and OURA_CLIENT_SECRET")
        else:
            logger.info(f"Oura OAuth configuration validated, client_id:{client_id[:3]}...")

    @classmethod
    def create_provider(cls, config: dict[str, Any]) -> Optional['OuraProvider']:
        """Factory method — return None if config insufficient"""
        try:
            client_id = safe_read_cfg("OURA_CLIENT_ID")
            client_secret = safe_read_cfg("OURA_CLIENT_SECRET")
            if not client_id or not client_secret:
                logger.info("OuraProvider disabled: missing OURA_CLIENT_ID or OURA_CLIENT_SECRET")
                return None
            return cls()
        except Exception as e:
            logger.warning(f"Failed to create Oura provider: {e}")
            return None

    @property
    def info(self) -> ProviderInfo:
        """Provider metadata"""
        return ProviderInfo(
            slug="theta_oura",
            name="Oura",
            description="Oura Ring sleep, activity, and readiness tracking via OAuth2",
            logo="https://static.thetahealth.ai/res/oura.png",
            supported=True,
            auth_type=LinkType.OAUTH2,
            status=ProviderStatus.AVAILABLE,
        )

    # =========================================================================
    # OAuth2 Flow — delegates to OAuth2Client
    # =========================================================================

    async def link(self, request: Any) -> dict[str, Any]:
        """Generate OAuth2 authorization URL"""
        return await self.oauth.generate_authorization_url(
            request.user_id, request.options or {}
        )

    async def callback(self, code: str, state: str) -> dict[str, Any]:
        """Exchange authorization code for tokens and trigger initial pull"""
        result = await self.oauth.exchange_code_for_tokens(
            code, state, self.db_service, self.info.slug
        )

        # Trigger initial data pull (backfill) asynchronously
        spawn(self._pull_and_push_for_user({
            "user_id": result["user_id"],
            "access_token": result["access_token"],
            "refresh_token": result["refresh_token"],
        }))

        return {
            "provider_slug": self.info.slug,
            "access_token": result["access_token"][:20] + "..." if result["access_token"] else "",
            "stage": "completed",
            "return_url": result.get("return_url"),
        }

    async def get_valid_access_token(self, user_id: str) -> str | None:
        """Get valid access token, auto-refresh if expired"""
        return await self.oauth.get_valid_access_token(
            user_id, self.info.slug, self.db_service
        )

    # =========================================================================
    # Data Pulling
    # =========================================================================

    def register_pull_task(self) -> bool:
        return True

    async def _pull_and_push_for_user(self, credentials: dict[str, Any]) -> bool:
        """Pull data for a single user and push to processing pipeline"""
        user_id = credentials.get("user_id") or credentials.get("theta_user_id", "")
        if not user_id:
            logger.error("No user_id in Oura credentials")
            return False

        try:
            # Always go through get_valid_access_token: it consults expires_at
            # and refreshes on the fly when needed. The previous `if not access_token`
            # guard only triggered for missing tokens — expired-but-present tokens
            # silently slipped through and hit Oura with a dead Bearer header.
            access_token = await self.get_valid_access_token(user_id)
            if not access_token:
                logger.error(f"No valid access token for Oura user {user_id}")
                return False

            refresh_token = credentials.get("refresh_token", "")

            # Determine pull range: backfill on first pull, 1 day otherwise
            last_pull = credentials.get("last_pull_at")
            days = self.backfill_days if not last_pull else 1

            raw_data_list = await self.pull_from_vendor_api(access_token, refresh_token, days=days)

            success_count = 0
            error_count = 0

            for raw_data in raw_data_list:
                try:
                    raw_data["theta_user_id"] = user_id
                    await push_service.push_data(
                        platform="theta",
                        provider_slug=self.info.slug,
                        data=raw_data,
                    )
                    success_count += 1
                except Exception as e:
                    error_count += 1
                    logger.error(f"Failed to push Oura data for user {user_id}: {e}")

            logger.info(f"Oura pull complete for user {user_id}: {success_count} success, {error_count} errors")
            return error_count == 0

        except Exception as e:
            logger.error(f"Oura pull_and_push failed for user {user_id}: {e}")
            return False

    async def pull_from_vendor_api(
        self, access_token: str, refresh_token: str, days: int | None = None
    ) -> list[dict[str, Any]]:
        """Fetch data from all Oura API endpoints"""
        pull_days = days or 1

        start_date = (datetime.now(UTC) - timedelta(days=pull_days)).strftime("%Y-%m-%d")
        end_date = datetime.now(UTC).strftime("%Y-%m-%d")

        headers = {"Authorization": f"Bearer {access_token}"}
        all_data = []

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.request_timeout)
        ) as session:
            for endpoint_config in self.API_ENDPOINTS:
                path = endpoint_config["path"]
                data_type = endpoint_config["data_type"]
                paginated = endpoint_config.get("paginated", True)

                # Heart rate uses datetime params
                if data_type == "heartrate":
                    params = {
                        "start_datetime": f"{start_date}T00:00:00+00:00",
                        "end_datetime": f"{end_date}T23:59:59+00:00",
                    }
                elif data_type == "personal_info":
                    params = {}
                else:
                    params = {"start_date": start_date, "end_date": end_date}

                try:
                    url = f"{self.API_BASE_URL}{path}"

                    if paginated:
                        data = await self._fetch_paginated_data(session, url, headers, params)
                    elif data_type == "personal_info":
                        data = await self._fetch_single_resource(session, url, headers)
                    else:
                        # heartrate returns flat list in "data" key
                        data = await self._fetch_list_data(session, url, headers, params)

                    if data:
                        all_data.append({
                            "data_type": data_type,
                            "data": data if isinstance(data, list) else [data],
                            "timestamp": int(time.time() * 1000),
                        })
                        logger.info(f"Fetched {len(data) if isinstance(data, list) else 1} {data_type} records")
                except Exception as e:
                    logger.error(f"Failed to fetch Oura {data_type}: {e}")

        return all_data

    async def _fetch_paginated_data(
        self, session: aiohttp.ClientSession, url: str,
        headers: dict, params: dict
    ) -> list[dict[str, Any]]:
        """Handle Oura pagination (next_token)"""
        all_records = []
        next_token = None

        while True:
            req_params = dict(params)
            if next_token:
                req_params["next_token"] = next_token

            async with session.get(url, headers=headers, params=req_params) as resp:
                if resp.status == 429:
                    retry_after = int(resp.headers.get("Retry-After", 60))
                    logger.warning(f"Oura rate limited, waiting {retry_after}s")
                    await asyncio.sleep(retry_after)
                    continue
                if resp.status == 401:
                    raise ValueError("Oura access token expired or invalid")
                if resp.status != 200:
                    logger.warning(f"Oura API error {resp.status} for {url}: {await resp.text()}")
                    break

                body = await resp.json()
                records = body.get("data", [])
                all_records.extend(records)

                next_token = body.get("next_token")
                if not next_token:
                    break

        return all_records

    async def _fetch_single_resource(
        self, session: aiohttp.ClientSession, url: str, headers: dict
    ) -> dict[str, Any] | None:
        """Fetch a single resource (e.g., personal_info)"""
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                logger.error(f"Oura API error {resp.status} for {url}")
                return None
            return await resp.json()

    async def _fetch_list_data(
        self, session: aiohttp.ClientSession, url: str,
        headers: dict, params: dict
    ) -> list[dict[str, Any]]:
        """Fetch list data without pagination (e.g., heartrate)"""
        async with session.get(url, headers=headers, params=params) as resp:
            if resp.status == 429:
                retry_after = int(resp.headers.get("Retry-After", 60))
                logger.warning(f"Oura rate limited, waiting {retry_after}s")
                await asyncio.sleep(retry_after)
                return await self._fetch_list_data(session, url, headers, params)
            if resp.status != 200:
                logger.warning(f"Oura API error {resp.status} for {url}: {await resp.text()}")
                return []
            body = await resp.json()
            return body.get("data", [])

    # =========================================================================
    # Raw Data Storage
    # =========================================================================

    async def save_raw_data_to_db(self, raw_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Save raw Oura data to health_data_oura table"""
        theta_user_id = self._extract_theta_user_id(raw_data)
        external_user_id = self._extract_external_user_id(raw_data)
        msg_id = f"oura_{theta_user_id}_{int(time.time())}"

        query = """
            INSERT INTO health_data_oura
            (create_at, update_at, is_del, msg_id, raw_data, theta_user_id, external_user_id)
            VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, false, :msg_id, :raw_data, :theta_user_id, :external_user_id)
        """
        params = {
            "msg_id": msg_id,
            "raw_data": json.dumps(raw_data, ensure_ascii=False),
            "theta_user_id": theta_user_id,
            "external_user_id": external_user_id,
        }
        await execute_query(query, params)

        raw_data["msg_id"] = msg_id
        return [raw_data]

    async def is_data_already_processed(self, raw_data: dict[str, Any]) -> bool:
        return False

    # =========================================================================
    # Data Formatting
    # =========================================================================

    async def format_data(self, fmt_input: FormatDataInput) -> StandardPulseData:
        """Oura documents → standard records, via ``mirobody.kernel.vendors.oura``.

        The payload is ``{"data_type": ..., "data": [...], "timestamp": pulled_at_ms}``;
        ``personal_info`` has no time of its own and is filed at the pull
        instant's local midnight.
        """
        ctx = fmt_input.context
        request_id = self.generate_request_id()
        payload = fmt_input.payload
        data_type = str(payload.get("data_type", "unknown"))
        items = payload.get("data") or []
        if not isinstance(items, list):
            items = [items]
        tz = ctx.user_timezone or "UTC"
        msg_id = ctx.msg_id or ""
        pulled_at = int(payload.get("timestamp") or 0)
        records: list[StandardPulseRecord] = []
        for item in items:
            facts = vendors.decode("oura", data_type, item, tz, pulled_at_ms=pulled_at, source_record_id=msg_id)
            records.extend(records_from_facts(facts, slug=self.info.slug, tz=tz, source_id=msg_id))
        logger.info("Formatted %d Oura records from %d %s items", len(records), len(items), data_type)
        return StandardPulseData(
            metaInfo=StandardPulseMetaInfo(userId=ctx.theta_user_id, requestId=request_id, source="theta", timezone=tz),
            healthData=records,
            processingInfo={"provider": "theta_oura", "data_type": data_type, "raw_count": len(items),
                            "mapped_count": len(records), "msg_id": msg_id},
        )
