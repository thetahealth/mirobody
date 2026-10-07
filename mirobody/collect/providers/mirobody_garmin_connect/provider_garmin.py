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

from mirobody.collect.base import ProviderInfo
from mirobody.collect.core import LinkType, ProviderStatus
from mirobody.collect.core.push_service import push_service
from mirobody.collect.ingest import FormatDataInput, StandardPulseData, StandardPulseMetaInfo, StandardPulseRecord
from mirobody.collect.providers._platform.base import BasePullProvider
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

        # Validate configuration
        if not self.client_id or not self.client_secret:
            logger.error("Garmin OAuth credentials not configured. Please set GARMIN_CLIENT_ID and GARMIN_CLIENT_SECRET")
        else:
            logger.info("Garmin OAuth configuration validated")

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
                # Unset credentials are the usual self-hosted state, not a fault:
                # at WARNING this read as a failure on every boot.
                logger.info("Garmin not configured (GARMIN_CLIENT_ID / GARMIN_CLIENT_SECRET unset); provider off")
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
        in Postgres temporary state cache, and builds the authorization URL that the user needs to visit.

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

            # Get request token. OAuth1Session is synchronous `requests`:
            # awaiting it in a thread keeps this provider from freezing the
            # event loop (Oura/Whoop use aiohttp natively; Garmin is the one
            # provider still on OAuth1, which aiohttp does not speak).
            resp = await asyncio.to_thread(oauth.post, self.request_token_url)

            if resp.status_code != 200:
                raise RuntimeError(f"Failed to get request token: status={resp.status_code}")

            # Parse response
            params = parse_qs(resp.text)
            oauth_token = params['oauth_token'][0]
            oauth_token_secret = params['oauth_token_secret'][0]

            # Store oauth_token_secret in Postgres temporary state keyed by oauth_token (TTL 15 minutes)
            try:
                cfg = global_config()
                ephemeral = cfg.get_ephemeral()
                await ephemeral.setex(
                    f"oauth:secret:{oauth_token}", self.oauth_temp_ttl, oauth_token_secret
                )
                # Optionally store user for cross-check (not strictly required)
                await ephemeral.setex(
                    f"oauth:user:{oauth_token}", self.oauth_temp_ttl, user_id or ""
                )
            except Exception as e:
                logger.warning("Garmin OAuth state write failed: error_type=%s", type(e).__name__)
                raise RuntimeError("Garmin OAuth state is unavailable") from None

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
            logger.error("Garmin authorization URL failed: error_type=%s", type(e).__name__)
            raise

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
        garmin_user_id = await asyncio.to_thread(
            self._get_user_id, self._oauth_session(access_token, access_token_secret))
        await self.db_service.save_oauth1_credentials(
            user_id, self.info.slug, access_token, access_token_secret, user_name=garmin_user_id
        )
        logger.info("Garmin linked: user_id=%s", user_id)

        spawn(self._pull_and_push_for_user({
            "user_id": user_id,
            "access_token": access_token,
            "access_token_secret": access_token_secret,
        }))
        return {"provider_slug": self.info.slug, "stage": "completed"}

    def _oauth_session(self, access_token: str, token_secret: str) -> OAuth1Session:
        """A session signing as one linked account."""
        return OAuth1Session(
            client_key=self.client_id,
            client_secret=self.client_secret,
            resource_owner_key=access_token,
            resource_owner_secret=token_secret,
        )

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
                oauth = self._oauth_session(access_token, token_secret)

                # Call DELETE API to unlink user (sync requests: thread)
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

        # The local link goes either way: it is what the person asked to remove,
        # and a token Garmin rejects cannot be revoked by trying again. A Garmin
        # failure used to surface as a 500 after the row was already deleted, so
        # the app said "failed" about a provider that was gone.
        try:
            await self.db_service.delete_user_theta_provider(user_id, self.info.slug)
        except Exception as db_error:
            logger.error(f"Failed to remove from database: {str(db_error)}")
            raise RuntimeError(f"Failed to unlink provider: {api_error_message or 'Unknown error'}") from db_error

        if api_unlink_success:
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
        logger.info("Formatted %d Garmin records from %d data types", len(records), len(types))
        return StandardPulseData(
            metaInfo=StandardPulseMetaInfo(userId=ctx.theta_user_id, requestId=request_id, source="theta", timezone=tz),
            healthData=records,
            processingInfo={"provider": "theta_garmin", "data_types": types, "msg_id": msg_id, "user_timezone": tz},
        )

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

            oauth = self._oauth_session(access_token, token_secret)

            # Get user ID first. Everything below is synchronous `requests`
            # via OAuth1Session; each unit runs in a worker thread so a
            # multi-endpoint, multi-day pull does not stall every other
            # coroutine on this loop for its whole duration, which is what
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

        for data_type, path in PULL_PATHS.items():
            url = (f"{self.api_base_url}{path}?uploadStartTimeInSeconds={start_timestamp}"
                   f"&uploadEndTimeInSeconds={end_timestamp}")
            try:
                logger.info(f"Pulling {data_type} data from Garmin API")
                resp = oauth.get(url)

                if resp.status_code == 200:
                    data = resp.json()
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
                await self.db_service.delete_user_theta_provider(theta_user_id, self.info.slug)
                logger.info("Garmin deregistration: user_id=%s", theta_user_id)
                continue
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
        if len(accounts) < len(by_person):
            logger.warning("Garmin push for unlinked accounts: unmatched_count=%d", len(by_person) - len(accounts))
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
                    # The account's id goes under its own key: `user_id` is
                    # Garmin's, and overwriting it made the batch look like a
                    # push, which filed every summary under "data".
                    raw_data["theta_user_id"] = user_id

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
