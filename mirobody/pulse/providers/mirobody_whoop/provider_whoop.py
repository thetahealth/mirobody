"""
Whoop Provider

Whoop OAuth2 data provider with authentication and data pulling functionality
"""

import asyncio
import json
import logging
import time
import uuid
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
from mirobody.utils.log import secret_fingerprint

logger = logging.getLogger(__name__)


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

        # Data pull configuration
        try:
            self.max_detail_records = int(safe_read_cfg("WHOOP_MAX_DETAIL_RECORDS") or 50)
        except (ValueError, TypeError):
            self.max_detail_records = 50

        try:
            self.concurrent_requests = int(safe_read_cfg("WHOOP_CONCURRENT_REQUESTS") or 5)
        except (ValueError, TypeError):
            self.concurrent_requests = 5

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
                logger.warning("Failed to create Whoop provider: unable to read config values")
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
            raise RuntimeError(str(e)) from e

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
                "access_token": result["access_token"][:20] + "...",
                "stage": "completed",
                "return_url": result.get("return_url"),
            }
        except Exception as e:
            logger.error(f"Error in OAuth2 callback: {str(e)}")
            raise RuntimeError(str(e)) from e

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
            raise RuntimeError(f"Failed to unlink provider: {str(e)}") from e

    def _extract_external_user_id(self, saved_data: dict[str, Any]) -> str:
        """Extract Whoop numeric user ID from data records."""
        data_items = saved_data.get("data", [])
        if isinstance(data_items, list) and data_items:
            return str(data_items[0].get("user_id", ""))
        if isinstance(data_items, dict):
            return str(data_items.get("user_id", ""))
        return ""

    async def format_data(self, fmt_input: FormatDataInput) -> StandardPulseData:
        """WHOOP records → standard records, via ``mirobody.kernel.vendors.whoop``.

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
            facts = vendors.decode("whoop", data_type, item, tz, pulled_at_ms=pulled_at, source_record_id=msg_id)
            records.extend(records_from_facts(facts, slug=self.info.slug, tz=tz, source_id=msg_id))
        logger.info("Formatted %d Whoop records from %d %s items", len(records), len(items), data_type)
        return StandardPulseData(
            metaInfo=StandardPulseMetaInfo(userId=ctx.theta_user_id, requestId=request_id, source="theta", timezone=tz),
            healthData=records,
            processingInfo={"provider": "theta_whoop", "data_type": data_type, "msg_id": msg_id, "user_timezone": tz},
        )

    async def pull_from_vendor_api(self, access_token: str, refresh_token: str, days: int | None = None) -> list[dict[str, Any]]:
        """
        Pull data from Whoop API using OAuth2 credentials.
        If days is provided, limit the collection endpoints to the last N days
        (aligned with pull_recent_data behavior); otherwise fetch full history.
        Implements three-layer data fetching strategy:
        1. Collection data (cycles, sleeps, workouts, recovery)
        2. Detailed data (by-ID endpoints)
        3. Static data (user profile, body measurements)
        """
        try:
            if days and days > 0:
                logger.info(f"Starting Whoop data pull (last {days} days)")
            else:
                logger.info("Starting comprehensive Whoop data pull")

            if not access_token:
                raise ValueError("Access token is required")

            headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
            all_raw_data = []

            async with aiohttp.ClientSession() as session:
                # Layer 1: Fetch all collection data
                logger.info("Layer 1: Fetching collection data")
                # Optional date range params for recent window
                collection_params = None
                if days and days > 0:
                    end_date = datetime.now(UTC)
                    start_date = end_date - timedelta(days=days)
                    collection_params = {
                        "start": start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "end": end_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "limit": 25,
                    }

                # Fetch cycles
                cycles_url = f"{self.api_base_url}/cycle"
                cycles = await self._fetch_paginated_data(session, cycles_url, headers, collection_params)
                logger.info(f"Fetched {len(cycles)} cycle records")

                # Fetch sleeps
                sleeps_url = f"{self.api_base_url}/activity/sleep"
                sleeps = await self._fetch_paginated_data(session, sleeps_url, headers, collection_params)
                logger.info(f"Fetched {len(sleeps)} sleep records")

                # Fetch workouts
                workouts_url = f"{self.api_base_url}/activity/workout"
                workouts = await self._fetch_paginated_data(session, workouts_url, headers, collection_params)
                logger.info(f"Fetched {len(workouts)} workout records")

                # Fetch recovery
                recovery_url = f"{self.api_base_url}/recovery"
                recoveries = await self._fetch_paginated_data(session, recovery_url, headers, collection_params)
                logger.info(f"Fetched {len(recoveries)} recovery records")

                # Layer 2: Fetch detailed data concurrently
                logger.info("Layer 2: Fetching detailed data with concurrent requests")

                # Prepare concurrent detail fetching
                detail_tasks = []

                # Cycle details
                if cycles:
                    detail_tasks.append(
                        self._fetch_detail_batch(
                            session, cycles, f"{self.api_base_url}/cycle/{{id}}",
                            "id", headers
                        )
                    )
                else:
                    detail_tasks.append(asyncio.create_task(asyncio.sleep(0)))  # Placeholder

                # Sleep details
                if sleeps:
                    detail_tasks.append(
                        self._fetch_detail_batch(
                            session, sleeps, f"{self.api_base_url}/activity/sleep/{{id}}",
                            "id", headers
                        )
                    )
                else:
                    detail_tasks.append(asyncio.create_task(asyncio.sleep(0)))

                # Workout details
                if workouts:
                    detail_tasks.append(
                        self._fetch_detail_batch(
                            session, workouts, f"{self.api_base_url}/activity/workout/{{id}}",
                            "id", headers
                        )
                    )
                else:
                    detail_tasks.append(asyncio.create_task(asyncio.sleep(0)))

                # Recovery by cycle
                if cycles:
                    detail_tasks.append(
                        self._fetch_detail_batch(
                            session, cycles, f"{self.api_base_url}/cycle/{{id}}/recovery",
                            "id", headers
                        )
                    )
                else:
                    detail_tasks.append(asyncio.create_task(asyncio.sleep(0)))

                # Execute all detail fetching concurrently
                start_time = time.time()
                results = await asyncio.gather(*detail_tasks, return_exceptions=True)
                elapsed = time.time() - start_time
                logger.info(f"Completed concurrent detail fetching in {elapsed:.2f} seconds")

                # Unpack results
                detailed_cycles = results[0] if not isinstance(results[0], Exception) and results[0] else []
                detailed_sleeps = results[1] if not isinstance(results[1], Exception) and results[1] else []
                detailed_workouts = results[2] if not isinstance(results[2], Exception) and results[2] else []
                cycle_recoveries = results[3] if not isinstance(results[3], Exception) and results[3] else []

                logger.info(f"Fetched details - Cycles: {len(detailed_cycles)}, Sleeps: {len(detailed_sleeps)}, "
                             f"Workouts: {len(detailed_workouts)}, Recoveries: {len(cycle_recoveries)}")

                # Layer 3: Fetch static data
                logger.info("Layer 3: Fetching static data")

                # Fetch user profile
                profile_url = f"{self.api_base_url}/user/profile/basic"
                profile = await self._fetch_paginated_data(session, profile_url, headers)
                logger.info("Fetched user profile data")

                # Fetch body measurements
                body_url = f"{self.api_base_url}/user/measurement/body"
                body_measurements = await self._fetch_paginated_data(session, body_url, headers)
                logger.info("Fetched body measurement data")

                # Package all data into raw data format
                timestamp = int(time.time() * 1000)
                user_id = ""  # Do not fetch whoop user id; keep empty

                # Add cycle data
                if detailed_cycles:
                    all_raw_data.append({
                        "user_id": user_id,
                        "data_type": "cycles",
                        "data": detailed_cycles,
                        "timestamp": timestamp,
                    })

                # Add sleep data (prefer detailed if available)
                if detailed_sleeps:
                    all_raw_data.append({
                        "user_id": user_id,
                        "data_type": "sleeps",
                        "data": detailed_sleeps,
                        "timestamp": timestamp,
                    })
                elif sleeps:
                    all_raw_data.append({
                        "user_id": user_id,
                        "data_type": "sleeps",
                        "data": sleeps,
                        "timestamp": timestamp,
                    })

                # Add workout data
                if detailed_workouts:
                    all_raw_data.append({
                        "user_id": user_id,
                        "data_type": "workouts",
                        "data": detailed_workouts,
                        "timestamp": timestamp,
                    })

                # Add recovery data
                if cycle_recoveries:
                    all_raw_data.append({
                        "user_id": user_id,
                        "data_type": "recoveries",
                        "data": cycle_recoveries,
                        "timestamp": timestamp,
                    })
                elif recoveries:
                    all_raw_data.append({
                        "user_id": user_id,
                        "data_type": "recoveries",
                        "data": recoveries,
                        "timestamp": timestamp,
                    })

                # Add user data
                if profile:
                    all_raw_data.append({
                        "user_id": user_id,
                        "data_type": "user_profile",
                        "data": profile,
                        "timestamp": timestamp,
                    })

                if body_measurements:
                    all_raw_data.append({
                        "user_id": user_id,
                        "data_type": "body_measurements",
                        "data": body_measurements,
                        "timestamp": timestamp,
                    })

            logger.info(f"Completed comprehensive Whoop data pull: {len(all_raw_data)} data packages")
            return all_raw_data

        except Exception as e:
            logger.error(f"Error in Whoop data pull: {str(e)}")
            return []

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

    async def _fetch_paginated_data(
            self,
            session: aiohttp.ClientSession,
            endpoint: str,
            headers: dict[str, str],
            params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Generic method to fetch paginated data from WHOOP API
        
        Args:
            session: aiohttp session
            endpoint: API endpoint URL
            headers: Request headers (should include Authorization)
            params: Optional query parameters
            
        Returns:
            List of all records from all pages
        """
        all_records = []
        next_token = None
        params = params or {}

        while True:
            # Add nextToken if available
            if next_token:
                params["nextToken"] = next_token

            # Retry logic for rate limiting
            retry_count = 0
            max_retries = 3
            data = {}

            while retry_count <= max_retries:
                try:
                    async with session.get(endpoint, headers=headers, params=params, timeout=aiohttp.ClientTimeout(total=self.request_timeout)) as resp:
                        if resp.status == 429:  # Rate limited
                            retry_after = int(resp.headers.get("Retry-After", "60"))
                            if retry_count < max_retries:
                                logger.warning(f"Rate limited on {endpoint}, retrying after {retry_after} seconds")
                                await asyncio.sleep(min(retry_after, 60))  # Cap at 60 seconds
                                retry_count += 1
                                continue
                            logger.error(f"Max retries exceeded for {endpoint} due to rate limiting")
                            break
                        elif resp.status == 401:
                            # Token should have been validated at entry point, 401 indicates auth failure
                            text = await resp.text()
                            logger.error(f"Authentication failed for {endpoint}: {resp.status} - {text}")
                            break
                        elif resp.status != 200:
                            text = await resp.text()
                            logger.error(f"Failed to fetch {endpoint}: {resp.status} - {text}")
                            break
                        else:
                            data = await resp.json()
                            break
                except TimeoutError:
                    logger.error(f"Timeout fetching {endpoint}")
                    if retry_count < max_retries:
                        retry_count += 1
                        await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                        continue
                    break
                except Exception as e:
                    logger.error(f"Error fetching {endpoint}: {str(e)}")
                    if retry_count < max_retries:
                        retry_count += 1
                        await asyncio.sleep(2 ** retry_count)
                        continue
                    break

            # Check if we successfully got data
            if retry_count > max_retries:
                break

            # Extract records and next token
            if "records" in data:
                records = data.get("records", [])
                all_records.extend(records)
                next_token = data.get("next_token")

                logger.info(f"Fetched {len(records)} records from {endpoint}, total: {len(all_records)}")

                # If no next token, we've reached the end
                if not next_token:
                    break
            else:
                # Non-paginated response, return as single item list
                all_records.append(data)
                break

        return all_records

    async def _fetch_detail_batch(
            self,
            session: aiohttp.ClientSession,
            items: list[dict],
            url_template: str,
            id_field: str,
            headers: dict[str, str],
    ) -> list[dict[str, Any]]:
        """
        Fetch detailed data for a batch of items concurrently
        
        Args:
            session: aiohttp session
            items: List of items containing IDs
            url_template: URL template with {id} placeholder
            id_field: Field name containing the ID
            headers: Request headers (should include Authorization)
            
        Returns:
            List of detailed records
        """
        semaphore = asyncio.Semaphore(self.concurrent_requests)

        async def fetch_one(item: dict) -> dict | None:
            async with semaphore:
                item_id = item.get(id_field)
                if not item_id:
                    return None

                url = url_template.format(id=item_id)
                try:
                    details = await self._fetch_paginated_data(
                        session, url, headers.copy()
                    )
                    return details[0] if details else None
                except Exception as e:
                    logger.error(f"Error fetching detail for {id_field}={item_id}: {str(e)}")
                    return None

        # Create tasks for concurrent execution
        tasks = [fetch_one(item) for item in items[:self.max_detail_records]]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out None values and exceptions
        detailed_records = []
        for result in results:
            if result and not isinstance(result, Exception):
                detailed_records.append(result)

        return detailed_records

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
            logger.info(f"whoop raw_data: {raw_data}")
            logger.error(f"Error saving Whoop raw data: {str(e)}")
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
                    # No need to inject external user ID — _extract_external_user_id
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
