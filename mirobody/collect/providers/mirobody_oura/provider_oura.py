"""
Oura Provider

Oura Ring OAuth2 data provider with authentication and data pulling functionality.
Supports sleep, activity, readiness, heart rate, SpO2, stress, and more.
"""

import json
import logging
import time
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
from mirobody.utils import execute_query
from mirobody.utils.config import safe_read_cfg
from mirobody.utils.tasks import spawn

logger = logging.getLogger(__name__)


class OuraProvider(BasePullProvider):
    """Oura Provider: Oura Ring OAuth2 Data Integration"""

    # API constants
    API_BASE_URL = "https://api.ouraring.com"
    AUTH_URL = "https://cloud.ouraring.com/oauth/authorize"
    TOKEN_URL = "https://api.ouraring.com/oauth/token"
    DEFAULT_SCOPES = "personal daily heartrate workout session spo2"

    #: The collections pulled, keyed by decoder type. Where each document's
    #: time comes from is declared once, in `decoders.oura.STRATEGY`.
    #: daily_resilience and daily_cardiovascular_age answer 401 without an
    #: Oura Membership, so they are not asked for.
    API_ENDPOINTS: dict[str, str] = {
        "personal_info": "/v2/usercollection/personal_info",
        "sleep": "/v2/usercollection/sleep",
        "daily_sleep": "/v2/usercollection/daily_sleep",
        "daily_activity": "/v2/usercollection/daily_activity",
        "daily_readiness": "/v2/usercollection/daily_readiness",
        "heartrate": "/v2/usercollection/heartrate",
        "daily_spo2": "/v2/usercollection/daily_spo2",
        "daily_stress": "/v2/usercollection/daily_stress",
        "vo2_max": "/v2/usercollection/vo2_max",
        "workout": "/v2/usercollection/workout",
    }

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
        """Factory method: return None if config insufficient"""
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
    # OAuth2 Flow: delegates to OAuth2Client
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
            # guard only triggered for missing tokens: expired-but-present tokens
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
        """The last `days` (one when unset) of every collection in `API_ENDPOINTS`.

        One package per decoder type: `{"data_type", "data", "timestamp"}`.
        Every collection but personal_info pages with `next_token`, heart rate
        included. A collection that fails is skipped and the rest still
        arrive; a refused token raises `VendorAuthError` for the pull loop to
        count.
        """
        now = datetime.now(UTC)
        start_date = (now - timedelta(days=days or 1)).strftime("%Y-%m-%d")
        end_date = now.strftime("%Y-%m-%d")
        headers = {"Authorization": f"Bearer {access_token}"}
        pulled_at = int(time.time() * 1000)
        packages: list[dict[str, Any]] = []
        async with aiohttp.ClientSession() as session:
            for data_type, path in self.API_ENDPOINTS.items():
                url = f"{self.API_BASE_URL}{path}"
                try:
                    if data_type == "personal_info":
                        body = await get_json(session, url, headers=headers, timeout_s=self.request_timeout)
                        data = [body] if isinstance(body, dict) and body else []
                    else:
                        params = (
                            {"start_datetime": f"{start_date}T00:00:00+00:00",
                             "end_datetime": f"{end_date}T23:59:59+00:00"}
                            if data_type == "heartrate"
                            else {"start_date": start_date, "end_date": end_date}
                        )
                        data = await get_pages(session, url, headers=headers, params=params, records_key="data",
                                               token_param="next_token", timeout_s=self.request_timeout)
                except (VendorError, TimeoutError, aiohttp.ClientError) as e:
                    logger.warning("Oura collection skipped: data_type=%s error_type=%s",  # phi: ok decoder type name
                                   data_type, type(e).__name__)
                    continue
                if data:
                    packages.append({"data_type": data_type, "data": data, "timestamp": pulled_at})
        return packages

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
        """Oura documents → standard records, via ``mirobody.kernel.decoders.oura``.

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
            # Oura's own document id, never the per-pull msg_id: the id is part
            # of a reading's identity, so a msg_id stored a new copy per pull.
            record_id = str(item.get("id") or "") if isinstance(item, dict) else ""
            facts = decoders.decode("oura", data_type, item, tz, pulled_at_ms=pulled_at, source_record_id=record_id)
            records.extend(records_from_facts(facts, slug=self.info.slug, tz=tz, source_id=record_id))
        logger.info("Formatted %d Oura records from %d %s items", len(records), len(items), data_type)
        return StandardPulseData(
            metaInfo=StandardPulseMetaInfo(userId=ctx.theta_user_id, requestId=request_id, source="theta", timezone=tz),
            healthData=records,
            processingInfo={"provider": "theta_oura", "data_type": data_type, "raw_count": len(items),
                            "mapped_count": len(records), "msg_id": msg_id},
        )
