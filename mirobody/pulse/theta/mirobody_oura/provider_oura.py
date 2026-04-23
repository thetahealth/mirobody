"""
Theta Oura Provider

Oura Ring OAuth2 data provider with authentication and data pulling functionality.
Supports sleep, activity, readiness, heart rate, SpO2, stress, and more.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import aiohttp

from mirobody.pulse.base import ProviderInfo
from mirobody.pulse.core import LinkType, ProviderStatus
from mirobody.pulse.core.indicators_info import StandardIndicator
from mirobody.pulse.core.push_service import push_service
from mirobody.pulse.data_upload.models.requests import (
    FormatDataInput,
    StandardPulseData,
    StandardPulseMetaInfo,
    StandardPulseRecord,
)
from mirobody.pulse.theta.platform.base import BaseThetaProvider
from mirobody.pulse.theta.platform.oauth2 import ThetaOAuth2Client
from mirobody.pulse.theta.platform.utils import ThetaDataFormatter, ThetaTimeUtils
from mirobody.utils import execute_query
from mirobody.utils.config import safe_read_cfg


class ThetaOuraProvider(BaseThetaProvider):
    """Theta Oura Provider — Oura Ring OAuth2 Data Integration"""

    # API constants
    API_BASE_URL = "https://api.ouraring.com"
    AUTH_URL = "https://cloud.ouraring.com/oauth/authorize"
    TOKEN_URL = "https://api.ouraring.com/oauth/token"
    DEFAULT_SCOPES = "personal daily heartrate workout session spo2"

    # Sandbox: no real token needed, any string works as Bearer token
    # e.g. curl -H "Authorization: Bearer test" https://api.ouraring.com/v2/sandbox/usercollection/sleep
    SANDBOX_API_PREFIX = "/v2/sandbox/usercollection"

    # Endpoints configuration
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

        self.oauth = ThetaOAuth2Client(
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
            logging.error("Oura OAuth credentials not configured. Please set OURA_CLIENT_ID and OURA_CLIENT_SECRET")
        else:
            logging.info(f"Oura OAuth configuration validated, client_id:{client_id[:3]}...")

        # Oura → StandardIndicator mapping
        # Format: "oura_field": StandardIndicator              — source unit == standard unit
        #         "oura_field": (StandardIndicator, "src_unit") — downstream auto-converts src → standard
        self.INDICATOR_MAPPING = {
            "sleep": {
                "total_sleep_duration": (StandardIndicator.DAILY_TOTAL_SLEEP_TIME, "s"),
                "time_in_bed": (StandardIndicator.SLEEP_IN_BED, "s"),
                "awake_time": (StandardIndicator.SLEEP_ANALYSIS_AWAKE, "s"),
                "deep_sleep_duration": (StandardIndicator.SLEEP_ANALYSIS_ASLEEP_DEEP, "s"),
                "light_sleep_duration": (StandardIndicator.SLEEP_ANALYSIS_ASLEEP_CORE, "s"),
                "rem_sleep_duration": (StandardIndicator.SLEEP_ANALYSIS_ASLEEP_REM, "s"),
                "efficiency": StandardIndicator.SLEEP_EFFICIENCY,
                "latency": StandardIndicator.SLEEP_LATENCY,
                "average_heart_rate": StandardIndicator.DAILY_HEART_RATE_AVG,
                "lowest_heart_rate": StandardIndicator.DAILY_HEART_RATE_MIN,
                "average_hrv": StandardIndicator.HRV_RMSSD,
                "average_breath": StandardIndicator.RESPIRATORY_RATE,
                "restless_periods": StandardIndicator.SLEEP_DISTURBANCES,
                "temperature_delta": StandardIndicator.TEMPERATURE_DELTA,
            },
            "daily_sleep": {
                "score": StandardIndicator.SLEEP_OVERALL_SCORE,
            },
            "daily_activity": {
                "steps": StandardIndicator.DAILY_STEPS,
                "active_calories": StandardIndicator.DAILY_CALORIES_ACTIVE,
                "total_calories": StandardIndicator.DAILY_CALORIES_TOTAL,
                "equivalent_walking_distance": StandardIndicator.DAILY_DISTANCE,
                "high_activity_time": (StandardIndicator.DAILY_ACTIVITY_INTENSITY_HIGH, "s"),
                "medium_activity_time": (StandardIndicator.DAILY_ACTIVITY_INTENSITY_MEDIUM, "s"),
                "low_activity_time": (StandardIndicator.DAILY_ACTIVITY_INTENSITY_LOW, "s"),
                "sedentary_time": (StandardIndicator.SEDENTARY_TIME, "s"),
                "resting_time": (StandardIndicator.RESTING_TIME, "s"),
                "score": StandardIndicator.DAILY_ACTIVITY_SCORE,
            },
            "daily_readiness": {
                "score": StandardIndicator.RECOVERY_SCORE,
                "temperature_deviation": StandardIndicator.TEMPERATURE_DELTA,
            },
            "heartrate": {
                "bpm": StandardIndicator.HEART_RATE,
            },
            "daily_spo2": {
                "spo2_percentage.average": StandardIndicator.BLOOD_OXYGEN,
            },
            "daily_stress": {
                "stress_high": StandardIndicator.STRESS_HIGH_DURATION,
                "recovery_high": StandardIndicator.RECOVERY_HIGH_DURATION,
            },
            "vo2_max": {
                "vo2_max": StandardIndicator.VO2_MAX,
            },
            "daily_cardiovascular_age": {
                "vascular_age": StandardIndicator.BODY_AGE,
            },
            "workout": {
                "calories": StandardIndicator.CALORIES_ACTIVE,
                "distance": StandardIndicator.DISTANCE,
            },
            "personal_info": {
                "weight": StandardIndicator.WEIGHT,
                "height": StandardIndicator.HEIGHT,
            },
        }

    @classmethod
    def create_provider(cls, config: Dict[str, Any]) -> Optional['ThetaOuraProvider']:
        """Factory method — return None if config insufficient"""
        try:
            client_id = safe_read_cfg("OURA_CLIENT_ID")
            client_secret = safe_read_cfg("OURA_CLIENT_SECRET")
            if not client_id or not client_secret:
                logging.info("OuraProvider disabled: missing OURA_CLIENT_ID or OURA_CLIENT_SECRET")
                return None
            return cls()
        except Exception as e:
            logging.warning(f"Failed to create Oura provider: {e}")
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
    # OAuth2 Flow — delegates to ThetaOAuth2Client
    # =========================================================================

    async def link(self, request: Any) -> Dict[str, Any]:
        """Generate OAuth2 authorization URL"""
        return await self.oauth.generate_authorization_url(
            request.user_id, request.options or {}
        )

    async def callback(self, code: str, state: str) -> Dict[str, Any]:
        """Exchange authorization code for tokens and trigger initial pull"""
        result = await self.oauth.exchange_code_for_tokens(
            code, state, self.db_service, self.info.slug
        )

        # Trigger initial data pull (backfill) asynchronously
        asyncio.create_task(self._pull_and_push_for_user({
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

    async def get_valid_access_token(self, user_id: str) -> Optional[str]:
        """Get valid access token, auto-refresh if expired"""
        return await self.oauth.get_valid_access_token(
            user_id, self.info.slug, self.db_service
        )

    # =========================================================================
    # Data Pulling
    # =========================================================================

    def register_pull_task(self) -> bool:
        return True

    async def _pull_and_push_for_user(self, credentials: Dict[str, Any]) -> bool:
        """Pull data for a single user and push to processing pipeline"""
        user_id = credentials.get("user_id") or credentials.get("theta_user_id", "")
        if not user_id:
            logging.error("No user_id in Oura credentials")
            return False

        try:
            access_token = credentials.get("access_token")
            if not access_token:
                access_token = await self.get_valid_access_token(user_id)
            if not access_token:
                logging.error(f"No valid access token for Oura user {user_id}")
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
                    logging.error(f"Failed to push Oura data for user {user_id}: {e}")

            logging.info(f"Oura pull complete for user {user_id}: {success_count} success, {error_count} errors")
            return error_count == 0

        except Exception as e:
            logging.error(f"Oura pull_and_push failed for user {user_id}: {e}")
            return False

    async def pull_from_vendor_api(
        self, access_token: str, refresh_token: str, days: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Fetch data from all Oura API endpoints"""
        pull_days = days or 1

        start_date = (datetime.now(timezone.utc) - timedelta(days=pull_days)).strftime("%Y-%m-%d")
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

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
                        logging.info(f"Fetched {len(data) if isinstance(data, list) else 1} {data_type} records")
                except Exception as e:
                    logging.error(f"Failed to fetch Oura {data_type}: {e}")

        return all_data

    async def _fetch_paginated_data(
        self, session: aiohttp.ClientSession, url: str,
        headers: dict, params: dict
    ) -> List[Dict[str, Any]]:
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
                    logging.warning(f"Oura rate limited, waiting {retry_after}s")
                    await asyncio.sleep(retry_after)
                    continue
                if resp.status == 401:
                    raise ValueError("Oura access token expired or invalid")
                if resp.status != 200:
                    logging.warning(f"Oura API error {resp.status} for {url}: {await resp.text()}")
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
    ) -> Optional[Dict[str, Any]]:
        """Fetch a single resource (e.g., personal_info)"""
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                logging.error(f"Oura API error {resp.status} for {url}")
                return None
            return await resp.json()

    async def _fetch_list_data(
        self, session: aiohttp.ClientSession, url: str,
        headers: dict, params: dict
    ) -> List[Dict[str, Any]]:
        """Fetch list data without pagination (e.g., heartrate)"""
        async with session.get(url, headers=headers, params=params) as resp:
            if resp.status == 429:
                retry_after = int(resp.headers.get("Retry-After", 60))
                logging.warning(f"Oura rate limited, waiting {retry_after}s")
                await asyncio.sleep(retry_after)
                return await self._fetch_list_data(session, url, headers, params)
            if resp.status != 200:
                logging.warning(f"Oura API error {resp.status} for {url}: {await resp.text()}")
                return []
            body = await resp.json()
            return body.get("data", [])

    # =========================================================================
    # Raw Data Storage
    # =========================================================================

    async def save_raw_data_to_db(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
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

    async def is_data_already_processed(self, raw_data: Dict[str, Any]) -> bool:
        return False

    # =========================================================================
    # Data Formatting
    # =========================================================================

    async def format_data_v2(self, fmt_input: FormatDataInput) -> StandardPulseData:
        """Transform Oura raw data → StandardPulseData"""
        ctx = fmt_input.context
        payload = fmt_input.payload
        data_type = payload.get("data_type", "unknown")
        data_items = payload.get("data", [])

        request_id = self.generate_request_id()

        if not isinstance(data_items, list):
            data_items = [data_items]

        processing_info = {
            "provider": "theta_oura",
            "data_type": data_type,
            "raw_count": len(data_items),
            "mapped_count": 0,
            "msg_id": ctx.msg_id or "",
        }

        records = []
        mapping = self.INDICATOR_MAPPING.get(data_type, {})

        if not mapping:
            logging.warning(f"No Oura indicator mapping for data_type: {data_type}")

        # Heart rate is a special case: each item is a single reading
        if data_type == "heartrate":
            records = self._process_heartrate_data(data_items, ctx, mapping)
            processing_info["mapped_count"] = len(records)
        else:
            for item in data_items:
                item_records = self._process_data_item(item, data_type, ctx, mapping)
                records.extend(item_records)
                processing_info["mapped_count"] += len(item_records)

        logging.info(
            f"Oura format_data: type={data_type}, raw={len(data_items)}, "
            f"mapped={processing_info['mapped_count']} for user {ctx.theta_user_id}"
        )

        return StandardPulseData(
            metaInfo=StandardPulseMetaInfo(
                userId=ctx.theta_user_id,
                requestId=request_id,
                source="theta",
                timezone=ctx.user_timezone,
            ),
            healthData=records,
            processingInfo=processing_info,
        )

    @staticmethod
    def _resolve_mapping_entry(entry):
        """Unpack mapping entry into (indicator_name, unit).

        Supported formats:
          - StandardIndicator              → (name, standard_unit)
          - (StandardIndicator, src_unit)  → (name, src_unit)  # downstream auto-converts
        """
        if isinstance(entry, tuple):
            indicator, src_unit = entry
            return indicator.value.name, src_unit
        # bare StandardIndicator
        return entry.value.name, entry.value.standard_unit

    def _process_data_item(
        self, item: Dict[str, Any], data_type: str,
        ctx: Any, mapping: Dict
    ) -> List[StandardPulseRecord]:
        """Process a single data item using indicator mapping"""
        records = []

        # Extract timestamp source based on data_type:
        # - Daily summary types have a "day" field ("2026-03-06") representing user's local date
        # - Sleep has "bedtime_start" with precise timezone-aware timestamp
        # - Other types fall back to "timestamp"
        DAILY_DATA_TYPES = {
            "daily_activity", "daily_sleep", "daily_readiness", "daily_spo2",
            "daily_stress", "daily_resilience", "daily_cardiovascular_age",
            "sleep_time", "vo2_max",
        }
        if data_type in DAILY_DATA_TYPES:
            time_str = item.get("day") or item.get("timestamp") or ""
        else:
            time_str = item.get("timestamp") or item.get("bedtime_start") or item.get("day") or ""
        user_tz = ctx.user_timezone or "UTC"
        timestamp = ThetaTimeUtils.parse_timestamp_with_smart_timezone(time_str, user_tz)
        if not timestamp:
            return records

        # For daily summary data, compute explicit startTime/endTime in user's local timezone
        # so downstream doesn't fall back to UTC-based record_time date extraction
        start_time_ms = None
        end_time_ms = None
        if data_type in DAILY_DATA_TYPES:
            try:
                from zoneinfo import ZoneInfo
                tz = ZoneInfo(user_tz)
                local_dt = datetime.fromtimestamp(timestamp / 1000, tz=tz)
                day_start = local_dt.replace(hour=0, minute=0, second=0, microsecond=0)
                day_end = local_dt.replace(hour=23, minute=59, second=59, microsecond=999000)
                start_time_ms = int(day_start.timestamp() * 1000)
                end_time_ms = int(day_end.timestamp() * 1000)
            except Exception:
                pass

        # Indicators that need seconds→minutes conversion (Oura API returns seconds)
        SECONDS_TO_MINUTES = {"stressHighDuration", "recoveryHighDuration"}

        for field_path, entry in mapping.items():
            value = self._extract_nested_value(item, field_path)
            if value is None:
                continue

            indicator_name, unit = self._resolve_mapping_entry(entry)

            # Convert seconds to minutes for duration indicators
            if indicator_name in SECONDS_TO_MINUTES and isinstance(value, (int, float)):
                value = round(value / 60, 1)

            records.append(StandardPulseRecord(
                source=ThetaDataFormatter.format_source_name(self.info.slug),
                type=indicator_name,
                timestamp=timestamp,
                unit=unit,
                value=value,
                timezone=ctx.user_timezone,
                source_id=ctx.msg_id or "",
                startTime=start_time_ms,
                endTime=end_time_ms,
            ))

        return records

    def _process_heartrate_data(
        self, data_items: List[Dict[str, Any]], ctx: Any, mapping: Dict
    ) -> List[StandardPulseRecord]:
        """Process heart rate time series — each item has bpm, source, timestamp"""
        records = []
        hr_entry = mapping.get("bpm")
        if not hr_entry:
            return records

        indicator_name, unit = self._resolve_mapping_entry(hr_entry)

        for item in data_items:
            bpm = item.get("bpm")
            ts_str = item.get("timestamp")
            if bpm is None or not ts_str:
                continue

            timestamp = ThetaTimeUtils.parse_timestamp_with_smart_timezone(ts_str, ctx.user_timezone or "UTC")
            if not timestamp:
                continue

            records.append(StandardPulseRecord(
                source=ThetaDataFormatter.format_source_name(self.info.slug),
                type=indicator_name,
                timestamp=timestamp,
                unit=unit,
                value=bpm,
                timezone=ctx.user_timezone,
                source_id=ctx.msg_id or "",
            ))

        return records

