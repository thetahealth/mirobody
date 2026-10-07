"""Garmin: an OAuth 1.0a provider that pushes, and is pulled once after a link."""

import asyncio
import json
import logging
import time
from datetime import datetime, UTC
from typing import Any
from urllib.parse import parse_qs, urlencode

from requests_oauthlib import OAuth1Session

from mirobody.collect.base import ProviderInfo
from mirobody.collect.core import LinkType, ProviderStatus
from mirobody.collect.ingest import FormatDataInput, StandardPulseData, StandardPulseMetaInfo, StandardPulseRecord
from mirobody.collect.providers._platform.base import BasePullProvider
from mirobody.collect.providers._platform.http import VendorAuthError
from mirobody.collect.providers._platform.normalize import records_from_facts
from mirobody.kernel import decoders
from mirobody.kernel.ops import is_driver_exception
from mirobody.utils import execute_query
from mirobody.utils.config import safe_read_cfg, global_config
from mirobody.utils.tasks import spawn
from mirobody.utils.log import secret_fingerprint

logger = logging.getLogger(__name__)

SECONDS_TO_MILLISECONDS = 1000

#: The Health API's REST path for each summary type, keyed by the name a push
#: gives the same type: a pulled batch is filed under that name, so it reaches
#: the decoder row a pushed one does (`/respiration` answers what a push calls
#: `allDayRespiration`, `/pulseOx` what it calls `pulseox`).
PULL_PATHS: dict[str, str] = {
    "sleeps": "/sleeps",
    "dailies": "/dailies",
    "bodyComps": "/bodyComps",
    "userMetrics": "/userMetrics",
    "hrv": "/hrv",
    "stressDetails": "/stressDetails",
    "pulseox": "/pulseOx",
    "allDayRespiration": "/respiration",
    "bloodPressures": "/bloodPressures",
    "skinTemp": "/skinTemp",
    "activities": "/activities",
    "activityDetails": "/activityDetails",
}


def _text(value: Any) -> str:
    """Temporary state as text: the store may hand back bytes."""
    return value.decode("utf-8") if isinstance(value, bytes) else str(value or "")


class GarminProvider(BasePullProvider):
    """Garmin's Health API, linked with OAuth 1.0a.

    Garmin pushes its summaries to the webhook, so there is no scheduled
    pull: the one pull is the backfill after a link.
    """

    backfill_days = 7

    def __init__(self) -> None:
        super().__init__()
        self.client_id = safe_read_cfg("GARMIN_CLIENT_ID")
        self.client_secret = safe_read_cfg("GARMIN_CLIENT_SECRET")
        self.redirect_url = safe_read_cfg("GARMIN_REDIRECT_URL")
        self.request_token_url = (
            safe_read_cfg("GARMIN_TOKEN_URL") or "https://connectapi.garmin.com/oauth-service/oauth/request_token"
        )
        self.auth_url = safe_read_cfg("GARMIN_AUTH_URL") or "https://connect.garmin.com/oauthConfirm/"
        self.access_token_url = (
            safe_read_cfg("GARMIN_ACCESS_TOKEN_URL")
            or "https://connectapi.garmin.com/oauth-service/oauth/access_token"
        )
        self.api_base_url = safe_read_cfg("GARMIN_API_BASE_URL") or "https://apis.garmin.com/wellness-api/rest"
        try:
            self.oauth_temp_ttl = int(safe_read_cfg("OAUTH_TEMP_TTL_SECONDS") or 900)
        except (ValueError, TypeError):
            self.oauth_temp_ttl = 900

    @classmethod
    def create_provider(cls, config: dict[str, Any]) -> "GarminProvider | None":
        """None unless GARMIN_CLIENT_ID and GARMIN_CLIENT_SECRET are set."""
        provider = super().create_provider(config)
        if provider is None:
            return None
        if not provider.client_id or not provider.client_secret:
            # Unset credentials are the usual self-hosted state, not a fault.
            logger.info("Garmin not configured (GARMIN_CLIENT_ID / GARMIN_CLIENT_SECRET unset); provider off")
            return None
        logger.info("Garmin configured: client_id=%s secret=%s",
                    provider.client_id, secret_fingerprint(provider.client_secret))
        return provider

    def register_pull_task(self) -> bool:
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
        """OAuth 1.0a stage 1: a request token, and the URL the person approves it at.

        The request token's secret and the account it is for wait in
        temporary state under the token, for `callback`. `request.options`
        may carry `return_url`, where the browser goes once the link is made.
        Returns `{"link_web_url": ...}`.
        """
        if not self.client_id or not self.client_secret:
            raise ValueError("Missing GARMIN_CLIENT_ID or GARMIN_CLIENT_SECRET configuration")
        if not self.redirect_url:
            raise ValueError("Missing GARMIN_REDIRECT_URL configuration")
        user_id = request.user_id
        oauth = OAuth1Session(
            client_key=self.client_id,
            client_secret=self.client_secret,
            signature_method="HMAC-SHA1",
            signature_type="auth_header",
        )
        # OAuth1Session is synchronous `requests`, which aiohttp cannot
        # replace for OAuth 1.0a: a thread keeps it off the event loop.
        resp = await asyncio.to_thread(oauth.post, self.request_token_url)
        if resp.status_code != 200:
            raise RuntimeError(f"Garmin request token refused: status={resp.status_code}")
        params = parse_qs(resp.text)
        oauth_token = params["oauth_token"][0]

        try:
            ephemeral = global_config().get_ephemeral()
            await ephemeral.setex(f"oauth:secret:{oauth_token}", self.oauth_temp_ttl, params["oauth_token_secret"][0])
            await ephemeral.setex(f"oauth:user:{oauth_token}", self.oauth_temp_ttl, user_id or "")
        except Exception as e:
            logger.warning("Garmin OAuth state write failed: error_type=%s", type(e).__name__,
                           exc_info=not is_driver_exception(e))
            raise RuntimeError("Garmin OAuth state is unavailable") from None

        # The callback reads return_url back from its own query string.
        callback_url = self.redirect_url
        return_url = (request.options or {}).get("return_url")
        if return_url:
            separator = "&" if "?" in callback_url else "?"
            callback_url = f"{callback_url}{separator}{urlencode({'return_url': return_url})}"
        query = urlencode({"oauth_token": oauth_token, "oauth_callback": callback_url})
        return {"link_web_url": f"{self.auth_url}?{query}"}

    async def callback(self, oauth_token: str, oauth_verifier: str) -> dict[str, Any]:
        """OAuth 1.0a stage 2: trade the verified request token for an access token.

        The account the link belongs to, and the request token's secret, come
        from the temporary state stage 1 wrote under `oauth_token`, never from
        the query string, which the browser controls.
        """
        try:
            ephemeral = global_config().get_ephemeral()
            token_secret = _text(await ephemeral.take(f"oauth:secret:{oauth_token}"))
            user_id = _text(await ephemeral.take(f"oauth:user:{oauth_token}"))
        except Exception as e:
            logger.warning("Garmin OAuth state read failed: error_type=%s", type(e).__name__,
                           exc_info=not is_driver_exception(e))
            token_secret = user_id = ""
        if not token_secret or not user_id:
            raise ValueError("Garmin OAuth state expired or unknown")

        request_session = OAuth1Session(
            client_key=self.client_id,
            client_secret=self.client_secret,
            resource_owner_key=oauth_token,
            resource_owner_secret=token_secret,
            verifier=oauth_verifier,
        )
        # OAuth1Session is synchronous `requests`: a thread keeps it off the loop.
        resp = await asyncio.to_thread(request_session.post, self.access_token_url)
        if resp.status_code != 200:
            raise RuntimeError(f"Garmin access-token exchange failed: status={resp.status_code}")
        params = parse_qs(resp.text)
        access_token = params["oauth_token"][0]
        access_token_secret = params["oauth_token_secret"][0]

        # Asked with the request token, as this used to be, /user/id answers
        # nothing: the link was stored without Garmin's user id, and every push
        # for the person was then dropped as belonging to nobody.
        try:
            garmin_user_id = await asyncio.to_thread(
                self._get_user_id, self._oauth_session(access_token, access_token_secret))
        except VendorAuthError:
            garmin_user_id = ""
        await self.db_service.save_oauth1_credentials(
            user_id, self.info.slug, access_token, access_token_secret, user_name=garmin_user_id
        )
        logger.info("Garmin linked: user_id=%s", user_id)

        self._relinked(user_id)
        spawn(self._backfill_after_link({
            "user_id": user_id,
            "username": garmin_user_id,
            "access_token": access_token,
            "access_token_secret": access_token_secret,
        }))
        return {"provider_slug": self.info.slug, "stage": "completed"}

    async def _backfill_after_link(self, credentials: dict[str, Any]) -> None:
        """The one pull a Garmin link gets, `backfill_days` back.

        It waits first: a token Garmin has just issued is not accepted for a
        few seconds. If Garmin's user id was not to be had at link time, it is
        asked for again here and stored, since a push finds its account by it.
        """
        await asyncio.sleep(8)
        if not credentials.get("username"):
            oauth = self._oauth_session(credentials["access_token"], credentials["access_token_secret"])
            try:
                garmin_user_id = await asyncio.to_thread(self._get_user_id, oauth)
            except VendorAuthError:
                garmin_user_id = ""
            if garmin_user_id:
                await self.db_service.save_oauth1_credentials(
                    credentials["user_id"], self.info.slug, credentials["access_token"],
                    credentials["access_token_secret"], user_name=garmin_user_id,
                )
        await self._pull_and_push_for_user(credentials, days=self.backfill_days)

    def _oauth_session(self, access_token: str, token_secret: str) -> OAuth1Session:
        """A session signing as one linked account."""
        return OAuth1Session(
            client_key=self.client_id,
            client_secret=self.client_secret,
            resource_owner_key=access_token,
            resource_owner_secret=token_secret,
        )

    async def unlink(self, user_id: str) -> dict[str, Any]:
        """Revoke the registration at Garmin, then remove the link here.

        The local link goes either way: it is what the person asked to
        remove, and a token Garmin rejects cannot be revoked by trying again.
        A Garmin failure used to surface as a 500 after the row was already
        deleted, so the app said "failed" about a provider that was gone.
        """
        vendor_revoked = False
        try:
            credentials = await self.db_service.get_user_credentials(user_id, self.info.slug, self.info.auth_type)
            if credentials:
                oauth = self._oauth_session(credentials["access_token"], credentials["access_token_secret"])
                resp = await asyncio.to_thread(oauth.delete, f"{self.api_base_url}/user/registration")
                vendor_revoked = resp.status_code == 204
                if not vendor_revoked:
                    logger.warning("Garmin did not revoke the registration: user_id=%s status_code=%d",
                                   user_id, resp.status_code)
        except Exception as e:
            logger.error("Garmin revocation failed: user_id=%s error_type=%s", user_id, type(e).__name__,
                         exc_info=not is_driver_exception(e))

        await self.db_service.delete_user_theta_provider(user_id, self.info.slug)
        if vendor_revoked:
            return {"success": True, "vendor_revoked": True, "message": "Successfully unlinked from Garmin"}
        return {
            "success": True,
            "vendor_revoked": False,
            "message": (
                "Unlinked here. Garmin did not confirm the revocation; remove Mirobody under "
                "Garmin Connect > Settings > Connected Apps to finish."
            ),
        }

    async def format_data(self, fmt_input: FormatDataInput) -> StandardPulseData:
        """Garmin summaries → standard records, via ``mirobody.kernel.decoders.garmin``.

        The payload is ``{data_type: [summary, ...], ...}`` for one person, as
        `save_raw_data_to_db` returns it for a push or a pull. Every summary
        type is decoded by the shared table; ``activityDetails`` wraps an
        activity summary under ``summary``, and a deregistration decodes to
        nothing.
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
            if key in ("theta_user_id", "msg_id", "deregistrations") or not isinstance(items, list) or not items:
                continue
            types.append(key)
            for item in items:
                if not isinstance(item, dict):
                    continue
                # Garmin's own summaryId, never the per-pull msg_id: the id is
                # part of a reading's identity, so a msg_id stored a new copy
                # of every daily summary per pull.
                record_id = str(item.get("summaryId") or "")
                if key == "activityDetails":
                    summary = item.get("summary")
                    facts = (decoders.decode("garmin", "activities", summary, tz, source_record_id=record_id)
                             if isinstance(summary, dict) else [])
                else:
                    facts = decoders.decode("garmin", key, item, tz, source_record_id=record_id)
                records.extend(records_from_facts(facts, slug=self.info.slug, tz=tz, source_id=record_id))
        if not types:
            return self._create_empty_response(request_id, ctx.theta_user_id)
        logger.info("Garmin formatted: record_count=%d type_count=%d", len(records), len(types))
        return StandardPulseData(
            metaInfo=StandardPulseMetaInfo(userId=ctx.theta_user_id, requestId=request_id, source="theta", timezone=tz),
            healthData=records,
            processingInfo={"provider": "theta_garmin", "data_types": types, "msg_id": msg_id, "user_timezone": tz},
        )

    async def pull_from_vendor_api(self, credentials: dict[str, Any], days: int) -> list[dict[str, Any]]:
        """The last `days` of every summary type in `PULL_PATHS`, a day at a time.

        Garmin answers at most 86,400 seconds of uploads per request. One
        package per type per day: `{"user_id": Garmin's id, "data_type",
        "data", "timestamp"}`. OAuth1Session is synchronous `requests`, so
        each day runs in a worker thread rather than stalling the loop.
        """
        oauth = self._oauth_session(credentials["access_token"], credentials["access_token_secret"])
        garmin_user_id = await asyncio.to_thread(self._get_user_id, oauth)
        end = int(datetime.now(UTC).timestamp())
        packages: list[dict[str, Any]] = []
        for day in range(days):
            day_end = end - day * 86400
            packages += await asyncio.to_thread(self._pull_day, oauth, garmin_user_id, day_end - 86400, day_end)
        return packages

    def _pull_day(self, oauth: OAuth1Session, garmin_user_id: str, start: int, end: int) -> list[dict[str, Any]]:
        """One upload window of every summary type; a type that fails is skipped."""
        pulled_at = int(time.time() * SECONDS_TO_MILLISECONDS)
        packages: list[dict[str, Any]] = []
        for data_type, path in PULL_PATHS.items():
            url = f"{self.api_base_url}{path}?uploadStartTimeInSeconds={start}&uploadEndTimeInSeconds={end}"
            try:
                resp = oauth.get(url)
                if resp.status_code != 200:
                    logger.warning("Garmin summary skipped: data_type=%s status_code=%d",  # phi: ok summary type name
                                   data_type, resp.status_code)
                    continue
                data = resp.json()
            except Exception as e:
                logger.warning("Garmin summary skipped: data_type=%s error_type=%s",  # phi: ok summary type name
                               data_type, type(e).__name__, exc_info=not is_driver_exception(e))
                continue
            if isinstance(data, list) and data:
                packages.append({"user_id": garmin_user_id, "data_type": data_type, "data": data,
                                 "timestamp": pulled_at})
        return packages

    def _get_user_id(self, oauth: OAuth1Session) -> str:
        """Garmin's id for the account `oauth` signs as, or "" when Garmin
        does not say. A refused token raises `VendorAuthError`."""
        resp = oauth.get(f"{self.api_base_url}/user/id")
        if resp.status_code in (401, 403):
            raise VendorAuthError(resp.status_code)
        if resp.status_code != 200:
            logger.warning("Garmin user id unavailable: status_code=%d", resp.status_code)
            return ""
        return str((resp.json() or {}).get("userId") or "")

    async def save_raw_data_to_db(self, raw_data: dict[str, Any]) -> list[dict[str, Any]]:
        """Store a Garmin payload per person; return what `format_data` reads.

        Three shapes arrive. A pull: one `data_type` and its `data` list, with
        the account's `theta_user_id`, which only the pull loop sets (the
        webhook route strips it from a push) and Garmin's own id in `user_id`.
        A push: `{summary_type: [summary, ...]}` across many people, each
        summary naming its person by Garmin's `userId`. A deregistration push,
        which is a push whose type is `deregistrations`.

        Returns one `{summary_type: [...], "theta_user_id", "msg_id"}` per
        person saved.
        """
        if not isinstance(raw_data, dict):
            return []
        if raw_data.get("theta_user_id") and isinstance(raw_data.get("data"), list):
            batches = [(
                str(raw_data["theta_user_id"]),
                str(raw_data.get("user_id") or ""),
                {str(raw_data.get("data_type") or ""): raw_data["data"]},
            )]
        else:
            batches = await self._push_batches(raw_data)

        saved: list[dict[str, Any]] = []
        for theta_user_id, external_user_id, by_type in batches:
            row: dict[str, Any] = {**by_type, "theta_user_id": theta_user_id}
            row["msg_id"] = next(
                (str(item["summaryId"]) for items in by_type.values() for item in items
                 if isinstance(item, dict) and item.get("summaryId")),
                f"{external_user_id or theta_user_id}_{int(time.time())}",
            )
            try:
                await execute_query(
                    query=(
                        "INSERT INTO health_data_garmin "
                        "(create_at, update_at, is_del, msg_id, raw_data, theta_user_id, external_user_id) "
                        "VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, FALSE, :msg_id, :raw_data, "
                        ":theta_user_id, :external_user_id) ON CONFLICT (msg_id) DO NOTHING"
                    ),
                    params={
                        "msg_id": row["msg_id"],
                        "raw_data": json.dumps(row, ensure_ascii=False),
                        "theta_user_id": theta_user_id,
                        "external_user_id": external_user_id,
                    },
                )
            except Exception as e:
                logger.error("Garmin raw save failed: user_id=%s error_type=%s", theta_user_id,
                             type(e).__name__, exc_info=not is_driver_exception(e))
                continue
            if "deregistrations" in by_type:
                # The person removed Mirobody in Garmin Connect: the link goes.
                # The row is still returned: it is handled, and formats to nothing.
                await self.db_service.delete_user_theta_provider(theta_user_id, self.info.slug)
                logger.info("Garmin deregistration: user_id=%s", theta_user_id)
            saved.append(row)
        return saved

    async def _push_batches(self, raw_data: dict[str, Any]) -> list[tuple[str, str, dict[str, list[dict]]]]:
        """A push split per person: `(theta_user_id, garmin_user_id, {type: items})`.

        A summary whose Garmin `userId` matches no linked account is dropped:
        nothing in a push says whose account it is except that id.
        """
        by_person: dict[str, dict[str, list[dict]]] = {}
        for data_type, items in raw_data.items():
            if not isinstance(items, list):
                continue
            for item in items:
                if isinstance(item, dict) and item.get("userId"):
                    by_person.setdefault(str(item["userId"]), {}).setdefault(data_type, []).append(item)
        accounts = await self._batch_map_external_to_theta_user_ids(list(by_person))
        unmatched_count = len(by_person) - len(accounts)
        if unmatched_count:
            logger.warning("Garmin push for unlinked accounts: unmatched_count=%d", unmatched_count)
        return [(accounts[garmin_id], garmin_id, by_type)
                for garmin_id, by_type in by_person.items() if garmin_id in accounts]

    async def _batch_map_external_to_theta_user_ids(self, external_user_ids: list[str]) -> dict[str, str]:
        """Garmin user id -> account id, through the links' stored Garmin id.

        The newest live link wins when one Garmin account was linked twice.
        """
        if not external_user_ids:
            return {}
        placeholders = ", ".join(f":username_{i}" for i in range(len(external_user_ids)))
        params: dict[str, Any] = {"provider": self.info.slug}
        params.update({f"username_{i}": external_id for i, external_id in enumerate(external_user_ids)})
        try:
            rows = await execute_query(
                query=(
                    f"SELECT username, user_id FROM health_user_provider "
                    f"WHERE username IN ({placeholders}) AND provider = :provider AND is_del = FALSE "
                    f"ORDER BY username, update_at DESC"
                ),
                params=params,
            )
        except Exception as e:
            logger.error("Garmin account lookup failed: id_count=%d error_type=%s", len(external_user_ids),
                         type(e).__name__, exc_info=not is_driver_exception(e))
            return {}
        mapping: dict[str, str] = {}
        for row in rows or []:
            mapping.setdefault(row["username"], row["user_id"])
        return mapping
