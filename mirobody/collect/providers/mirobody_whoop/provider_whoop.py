"""WHOOP: an OAuth 2.0 provider, pulled once a day."""

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
from mirobody.utils.log import secret_fingerprint
from mirobody.utils.tasks import spawn

logger = logging.getLogger(__name__)

#: WHOOP's collections, keyed by the decoder type each one holds.
COLLECTIONS: dict[str, str] = {
    "cycle": "/cycle",
    "sleep": "/activity/sleep",
    "workout": "/activity/workout",
    "recovery": "/recovery",
}


class WhoopProvider(BasePullProvider):
    """WHOOP's developer API v2, linked with OAuth 2.0."""

    pull_interval_hours = 24.0
    raw_table = "health_data_whoop"

    def __init__(self) -> None:
        super().__init__()
        self.client_id = safe_read_cfg("WHOOP_CLIENT_ID")
        self.client_secret = safe_read_cfg("WHOOP_CLIENT_SECRET")
        self.redirect_url = safe_read_cfg("WHOOP_REDIRECT_URL")
        self.auth_url = safe_read_cfg("WHOOP_AUTH_URL") or "https://api.prod.whoop.com/oauth/oauth2/auth"
        self.token_url = safe_read_cfg("WHOOP_TOKEN_URL") or "https://api.prod.whoop.com/oauth/oauth2/token"
        self.api_base_url = safe_read_cfg("WHOOP_API_BASE_URL") or "https://api.prod.whoop.com/developer/v2"
        self.scopes = (
            safe_read_cfg("WHOOP_SCOPES")
            or "offline read:recovery read:sleep read:cycles read:profile read:workout read:body_measurement"
        )
        try:
            self.request_timeout = int(safe_read_cfg("WHOOP_REQUEST_TIMEOUT") or 30)
        except (ValueError, TypeError):
            self.request_timeout = 30
        self.oauth = OAuth2Client(
            client_id=self.client_id or "",
            client_secret=self.client_secret or "",
            redirect_url=self.redirect_url or "",
            auth_url=self.auth_url,
            token_url=self.token_url,
            scopes=self.scopes,
            request_timeout=self.request_timeout,
            refresh_extra_params={"scope": self.scopes},  # WHOOP requires the scope on a refresh
        )

    @classmethod
    def create_provider(cls, config: dict[str, Any]) -> "WhoopProvider | None":
        """None unless WHOOP_CLIENT_ID and WHOOP_CLIENT_SECRET are set."""
        provider = super().create_provider(config)
        if provider is None:
            return None
        if not provider.client_id or not provider.client_secret:
            # Unset credentials are the usual self-hosted state, not a fault.
            logger.info("WHOOP not configured (WHOOP_CLIENT_ID / WHOOP_CLIENT_SECRET unset); provider off")
            return None
        logger.info("WHOOP configured: client_id=%s secret=%s",
                    provider.client_id, secret_fingerprint(provider.client_secret))
        return provider

    @property
    def info(self) -> ProviderInfo:
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
        """The authorization URL the person approves the link at."""
        return await self.oauth.generate_authorization_url(request.user_id, request.options or {})

    async def callback(self, code: str, state: str) -> dict[str, Any]:
        """Exchange the authorization code, store the tokens, start the backfill."""
        result = await self.oauth.exchange_code_for_tokens(code, state, self.db_service, self.info.slug)
        self._relinked(result["user_id"])
        spawn(self._pull_and_push_for_user({
            "user_id": result["user_id"],
            "access_token": result["access_token"],
            "refresh_token": result.get("refresh_token", ""),
            "expires_at": result.get("expires_at"),
        }, days=self.backfill_days))
        return {
            "provider_slug": self.info.slug,
            "stage": "completed",
            "return_url": result.get("return_url"),
        }

    async def unlink(self, user_id: str) -> dict[str, Any]:
        await self.db_service.delete_user_theta_provider(user_id, self.info.slug)
        logger.info("WHOOP unlinked: user_id=%s", user_id)
        return {"success": True, "message": "Successfully unlinked from Whoop"}

    def _extract_external_user_id(self, saved_data: dict[str, Any]) -> str:
        """WHOOP's numeric user id, which every record carries."""
        items = saved_data.get("data")
        first = items[0] if isinstance(items, list) and items else None
        return str(first.get("user_id") or "") if isinstance(first, dict) else ""

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
        logger.info("WHOOP formatted: data_type=%s item_count=%d record_count=%d",  # phi: ok decoder type name
                    data_type, len(items), len(records))
        return StandardPulseData(
            metaInfo=StandardPulseMetaInfo(userId=ctx.theta_user_id, requestId=request_id, source="theta", timezone=tz),
            healthData=records,
            processingInfo={"provider": "theta_whoop", "data_type": data_type, "msg_id": msg_id, "user_timezone": tz},
        )

    async def pull_from_vendor_api(self, credentials: dict[str, Any], days: int) -> list[dict[str, Any]]:
        """The last `days` of each WHOOP collection, and the body measurement.

        One package per decoder type: `{"data_type", "data", "timestamp"}`.
        A collection record is the whole record, the same object WHOOP's by-id
        endpoint answers, so nothing is fetched twice. A collection that fails
        is skipped and the rest still arrive; a refused token raises a
        `PermissionError` for the pull loop to count.
        """
        access_token = await self.oauth.get_valid_access_token(credentials, self.info.slug, self.db_service)
        headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
        end = datetime.now(UTC)
        params: dict[str, Any] = {
            "start": (end - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "end": end.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "limit": 25,
        }
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
