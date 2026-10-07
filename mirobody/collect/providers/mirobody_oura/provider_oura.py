"""Oura: an OAuth 2.0 provider, pulled every hour."""

import logging
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import aiohttp

from mirobody.collect.base import ProviderInfo
from mirobody.collect.core import LinkType, ProviderStatus
from mirobody.collect.ingest import FormatDataInput, StandardPulseData, StandardPulseMetaInfo, StandardPulseRecord
from mirobody.collect.providers._platform.base import BasePullProvider
from mirobody.collect.providers._platform.http import VendorError, get_json, get_pages
from mirobody.collect.providers._platform.normalize import records_from_facts
from mirobody.collect.providers._platform.oauth2 import OAuth2Client
from mirobody.kernel import decoders
from mirobody.utils.config import safe_read_cfg
from mirobody.utils.tasks import spawn

logger = logging.getLogger(__name__)


class OuraProvider(BasePullProvider):
    """Oura's API v2, linked with OAuth 2.0."""

    API_BASE_URL = "https://api.ouraring.com"
    AUTH_URL = "https://cloud.ouraring.com/oauth/authorize"
    TOKEN_URL = "https://api.ouraring.com/oauth/token"
    DEFAULT_SCOPES = "personal daily heartrate workout session spo2"

    # Hourly: ten collections per person per run, against a rate limit of
    # 5,000 requests per five minutes.
    pull_interval_hours = 1.0
    backfill_days = 30
    raw_table = "health_data_oura"
    request_timeout = 30

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

    def __init__(self) -> None:
        super().__init__()
        self.client_id = safe_read_cfg("OURA_CLIENT_ID")
        self.client_secret = safe_read_cfg("OURA_CLIENT_SECRET")
        self.oauth = OAuth2Client(
            client_id=self.client_id or "",
            client_secret=self.client_secret or "",
            redirect_url=safe_read_cfg("OURA_REDIRECT_URL") or "",
            auth_url=self.AUTH_URL,
            token_url=self.TOKEN_URL,
            scopes=self.DEFAULT_SCOPES,
        )

    @classmethod
    def create_provider(cls, config: dict[str, Any]) -> "OuraProvider | None":
        """None unless OURA_CLIENT_ID and OURA_CLIENT_SECRET are set."""
        provider = super().create_provider(config)
        if provider is None:
            return None
        if not provider.client_id or not provider.client_secret:
            logger.info("Oura not configured (OURA_CLIENT_ID / OURA_CLIENT_SECRET unset); provider off")
            return None
        return provider

    @property
    def info(self) -> ProviderInfo:
        return ProviderInfo(
            slug="theta_oura",
            name="Oura",
            description="Oura Ring sleep, activity, and readiness tracking via OAuth2",
            logo="https://static.thetahealth.ai/res/oura.png",
            supported=True,
            auth_type=LinkType.OAUTH2,
            status=ProviderStatus.AVAILABLE,
        )

    async def link(self, request: Any) -> dict[str, Any]:
        """The authorization URL the person approves the link at."""
        return await self.oauth.generate_authorization_url(request.user_id, request.options or {})

    async def callback(self, code: str, state: str) -> dict[str, Any]:
        """Exchange the authorization code, store the tokens, start the backfill."""
        result = await self.oauth.exchange_code_for_tokens(code, state, self.db_service, self.info.slug)
        self._relinked(result["user_id"])
        spawn(self._pull_and_push_for_user({
            "user_id": result["user_id"],
            "access_token": result["access_token"],
            "refresh_token": result["refresh_token"],
            "expires_at": result.get("expires_at"),
        }, days=self.backfill_days))
        return {
            "provider_slug": self.info.slug,
            "stage": "completed",
            "return_url": result.get("return_url"),
        }

    async def pull_from_vendor_api(self, credentials: dict[str, Any], days: int) -> list[dict[str, Any]]:
        """The last `days` of every collection in `API_ENDPOINTS`.

        One package per decoder type: `{"data_type", "data", "timestamp"}`.
        Every collection but personal_info pages with `next_token`, heart rate
        included. A collection that fails is skipped and the rest still
        arrive; a refused token raises a `PermissionError` for the pull loop
        to count.
        """
        access_token = await self.oauth.get_valid_access_token(credentials, self.info.slug, self.db_service)
        now = datetime.now(UTC)
        start_date = (now - timedelta(days=days)).strftime("%Y-%m-%d")
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
        logger.info("Oura formatted: data_type=%s item_count=%d record_count=%d",  # phi: ok decoder type name
                    data_type, len(items), len(records))
        return StandardPulseData(
            metaInfo=StandardPulseMetaInfo(userId=ctx.theta_user_id, requestId=request_id, source="theta", timezone=tz),
            healthData=records,
            processingInfo={"provider": "theta_oura", "data_type": data_type, "raw_count": len(items),
                            "mapped_count": len(records), "msg_id": msg_id},
        )
