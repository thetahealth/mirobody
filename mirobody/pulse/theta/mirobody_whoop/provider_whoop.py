"""
Theta Whoop Provider

Whoop OAuth2 data provider with authentication and data pulling functionality
"""

import asyncio
import json
import logging
import time
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from urllib.parse import urlencode

import aiohttp

from mirobody.pulse.base import ProviderInfo
from mirobody.pulse.core import LinkType, ProviderStatus
from mirobody.pulse.core.indicators_info import StandardIndicator
from mirobody.pulse.core.push_service import push_service
from mirobody.pulse.core.units import UNIT_CONVERSIONS
from mirobody.pulse.data_upload.models.requests import (
    StandardPulseData,
    StandardPulseMetaInfo,
    StandardPulseRecord,
)
from mirobody.pulse.theta.platform.base import BaseThetaProvider
from mirobody.pulse.theta.platform.utils import ThetaDataFormatter, ThetaTimeUtils
from mirobody.utils import execute_query
from mirobody.utils.config import safe_read_cfg, global_config


class ThetaWhoopProvider(BaseThetaProvider):
    """Theta Whoop Provider - Whoop OAuth2 Data Integration"""

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

        # OAuth temp TTL (seconds)
        try:
            self.oauth_temp_ttl = int(safe_read_cfg("OAUTH_TEMP_TTL_SECONDS") or 900)
        except Exception:
            self.oauth_temp_ttl = 900

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

        if not self.client_id or not self.client_secret:
            logging.error("Whoop OAuth credentials not configured. Please set WHOOP_CLIENT_ID and WHOOP_CLIENT_SECRET")
        else:
            logging.info(f"Whoop OAuth configuration validated successfully, client_id:{self.client_id[:3]}, redirect_url:{self.redirect_url}")

        # WHOOP to StandardIndicator mapping
        self.WHOOP_INDICATOR_MAPPING = {
            "sleep": {
                "score.stage_summary.total_in_bed_time_milli": (StandardIndicator.SLEEP_IN_BED.value.name, lambda x: x, StandardIndicator.SLEEP_IN_BED.value.standard_unit),
                "score.stage_summary.total_awake_time_milli": (
                StandardIndicator.SLEEP_ANALYSIS_AWAKE.value.name, lambda x: x, StandardIndicator.SLEEP_ANALYSIS_AWAKE.value.standard_unit),
                "score.stage_summary.total_light_sleep_time_milli": (
                StandardIndicator.SLEEP_ANALYSIS_ASLEEP_CORE.value.name, lambda x: x, StandardIndicator.SLEEP_ANALYSIS_ASLEEP_CORE.value.standard_unit),
                "score.stage_summary.total_slow_wave_sleep_time_milli": (
                StandardIndicator.SLEEP_ANALYSIS_ASLEEP_DEEP.value.name, lambda x: x, StandardIndicator.SLEEP_ANALYSIS_ASLEEP_DEEP.value.standard_unit),
                "score.stage_summary.total_rem_sleep_time_milli": (
                StandardIndicator.SLEEP_ANALYSIS_ASLEEP_REM.value.name, lambda x: x, StandardIndicator.SLEEP_ANALYSIS_ASLEEP_REM.value.standard_unit),
                "score.sleep_efficiency_percentage": (StandardIndicator.SLEEP_EFFICIENCY.value.name, lambda x: x, StandardIndicator.SLEEP_EFFICIENCY.value.standard_unit),
                "score.respiratory_rate": (StandardIndicator.RESPIRATORY_RATE.value.name, lambda x: x, StandardIndicator.RESPIRATORY_RATE.value.standard_unit),
                "score.sleep_performance_percentage": (StandardIndicator.SLEEP_PERFORMANCE.value.name, lambda x: x, StandardIndicator.SLEEP_PERFORMANCE.value.standard_unit),
                "score.sleep_consistency_percentage": (StandardIndicator.SLEEP_CONSISTENCY.value.name, lambda x: x, StandardIndicator.SLEEP_CONSISTENCY.value.standard_unit),
                "score.stage_summary.disturbance_count": (StandardIndicator.SLEEP_DISTURBANCES.value.name, lambda x: x, StandardIndicator.SLEEP_DISTURBANCES.value.standard_unit),
            },
            "cycle": {
                "score.average_heart_rate": (StandardIndicator.HEART_RATE.value.name, lambda x: x, StandardIndicator.HEART_RATE.value.standard_unit),
                "score.max_heart_rate": (StandardIndicator.HEART_RATE_MAX.value.name, lambda x: x, StandardIndicator.HEART_RATE_MAX.value.standard_unit),
                "score.kilojoule": (
                StandardIndicator.CALORIES_ACTIVE.value.name, lambda x: x * UNIT_CONVERSIONS["kcal"]["kJ"], StandardIndicator.CALORIES_ACTIVE.value.standard_unit),
                "score.strain": (StandardIndicator.STRAIN.value.name, lambda x: x, StandardIndicator.STRAIN.value.standard_unit),
            },
            "recovery": {
                "score.resting_heart_rate": (StandardIndicator.RESTING_HEART_RATE.value.name, lambda x: x, StandardIndicator.RESTING_HEART_RATE.value.standard_unit),
                "score.hrv_rmssd_milli": (StandardIndicator.HRV_RMSSD.value.name, lambda x: x, StandardIndicator.HRV_RMSSD.value.standard_unit),
                "score.spo2_percentage": (StandardIndicator.BLOOD_OXYGEN.value.name, lambda x: x, StandardIndicator.BLOOD_OXYGEN.value.standard_unit),
                "score.recovery_score": (StandardIndicator.RECOVERY_SCORE.value.name, lambda x: x, StandardIndicator.RECOVERY_SCORE.value.standard_unit),
                "score.skin_temp_celsius": (StandardIndicator.SKIN_TEMPERATURE.value.name, lambda x: x, StandardIndicator.SKIN_TEMPERATURE.value.standard_unit),
            },
            "workout": {
                "score.average_heart_rate": (StandardIndicator.HEART_RATE.value.name, lambda x: x, StandardIndicator.HEART_RATE.value.standard_unit),
                "score.max_heart_rate": (StandardIndicator.HEART_RATE_MAX.value.name, lambda x: x, StandardIndicator.HEART_RATE_MAX.value.standard_unit),
                "score.distance_meter": (StandardIndicator.DISTANCE.value.name, lambda x: x, StandardIndicator.DISTANCE.value.standard_unit),
                "score.kilojoule": (
                StandardIndicator.CALORIES_ACTIVE.value.name, lambda x: x * UNIT_CONVERSIONS["kcal"]["kJ"], StandardIndicator.CALORIES_ACTIVE.value.standard_unit),
                # Heart rate zones: milliseconds to minutes
                "score.zone_durations.zone_zero_milli": (
                StandardIndicator.WORKOUT_DURATION_LOW.value.name, lambda x: x / UNIT_CONVERSIONS["ms"]["min"], StandardIndicator.WORKOUT_DURATION_LOW.value.standard_unit),
                "score.zone_durations.zone_one_milli": (
                StandardIndicator.WORKOUT_DURATION_LOW.value.name, lambda x: x / UNIT_CONVERSIONS["ms"]["min"], StandardIndicator.WORKOUT_DURATION_LOW.value.standard_unit),
                "score.zone_durations.zone_two_milli": (
                StandardIndicator.WORKOUT_DURATION_MEDIUM.value.name, lambda x: x / UNIT_CONVERSIONS["ms"]["min"], StandardIndicator.WORKOUT_DURATION_MEDIUM.value.standard_unit),
                "score.zone_durations.zone_three_milli": (
                StandardIndicator.WORKOUT_DURATION_MEDIUM.value.name, lambda x: x / UNIT_CONVERSIONS["ms"]["min"], StandardIndicator.WORKOUT_DURATION_MEDIUM.value.standard_unit),
                "score.zone_durations.zone_four_milli": (
                StandardIndicator.WORKOUT_DURATION_HIGH.value.name, lambda x: x / UNIT_CONVERSIONS["ms"]["min"], StandardIndicator.WORKOUT_DURATION_HIGH.value.standard_unit),
                "score.zone_durations.zone_five_milli": (
                StandardIndicator.WORKOUT_DURATION_HIGH.value.name, lambda x: x / UNIT_CONVERSIONS["ms"]["min"], StandardIndicator.WORKOUT_DURATION_HIGH.value.standard_unit),
                "score.altitude_gain_meter": (StandardIndicator.ALTITUDE_GAIN.value.name, lambda x: x, StandardIndicator.ALTITUDE_GAIN.value.standard_unit),
                "score.altitude_change_meter": (StandardIndicator.ALTITUDE_CHANGE.value.name, lambda x: x, StandardIndicator.ALTITUDE_CHANGE.value.standard_unit),
            },
            "body": {
                "height_meter": (StandardIndicator.HEIGHT.value.name, lambda x: x, StandardIndicator.HEIGHT.value.standard_unit),
                "weight_kilogram": (StandardIndicator.WEIGHT.value.name, lambda x: x, StandardIndicator.WEIGHT.value.standard_unit),
                "max_heart_rate": (StandardIndicator.MAX_HEART_RATE_PROFILE.value.name, lambda x: x, StandardIndicator.MAX_HEART_RATE_PROFILE.value.standard_unit),
            },
        }

    @classmethod
    def create_provider(cls, config: Dict[str, Any]) -> Optional['ThetaWhoopProvider']:
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
            logging.info(f"whoop provider {client_id} {client_secret}")
            if not client_id or not client_secret:
                logging.warning("Failed to create Whoop provider: unable to read config values")
                return None

            return cls()
        except Exception as e:
            logging.warning(f"Failed to create Whoop provider: {e}")
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

    async def link(self, request: Any) -> Dict[str, Any]:
        """
        Link Whoop OAuth2 Provider - Stage 1: Generate OAuth2 authorization URL
        
        This method initiates the OAuth2 flow by generating an authorization URL
        that the user needs to visit to grant permission. After user authorization,
        the callback will be handled by the separate callback() method.
        
        Args:
            request: Link request containing user_id and options (redirect_url)
            
        Returns:
            Dict containing 'link_web_url' for user authorization
            
        Raises:
            RuntimeError: If OAuth2 configuration is invalid or URL generation fails
        """
        user_id = request.user_id
        options = request.options or {}

        try:
            # Generate OAuth2 authorization URL (Stage 1 of OAuth2 flow)
            logging.info(f"Generating OAuth2 authorization URL for user: {user_id}")
            return await self._generate_authorization_url(user_id, options)

        except Exception as e:
            logging.error(f"Error linking Whoop provider: {str(e)}")
            raise RuntimeError(str(e))

    async def _generate_authorization_url(self, user_id: str, options: Dict[str, Any]) -> Dict[str, Any]:
        """Stage 1: Generate OAuth2 authorization URL with robust redirect handling and debug logs"""
        try:
            if not self.client_id or not self.client_secret:
                raise ValueError("Missing WHOOP_CLIENT_ID or WHOOP_CLIENT_SECRET configuration")

            redirect_uri = self.redirect_url
            if not redirect_uri:
                raise ValueError("Missing WHOOP_REDIRECT_URL configuration")

            # Create state embedding return_url and store mapping in Redis
            origin_return_url = options.get("return_url") or ""
            # encode light wrapper as JSON then url-encode into state
            state_payload = {"s": str(uuid.uuid4()), "r": origin_return_url}
            state = urlencode(state_payload)
            try:
                cfg = global_config()
                redis_config = cfg.get_redis()
                redis_client = await redis_config.get_async_client()
                await redis_client.setex(f"oauth2:state:{state}", self.oauth_temp_ttl, user_id or "")
                await redis_client.setex(f"oauth2:redir:{state}", self.oauth_temp_ttl, redirect_uri)
                await redis_client.aclose()
            except Exception as e:
                logging.warning(f"Failed to write oauth2 temp data to Redis: {str(e)}")

            params = {
                "client_id": self.client_id,
                "response_type": "code",
                "redirect_uri": redirect_uri,
                "scope": self.scopes,
                "state": state,
            }
            authorization_url = f"{self.auth_url}?{urlencode(params)}"

            # Concise debug logs for OAuth stage 1 (no sensitive info)
            scopes_count = len((self.scopes or "").split())
            logging.info(f"[WHOOP][OAUTH2][STAGE1] auth_url prepared; scopes={scopes_count}")
            logging.info(f"Generated Whoop OAuth2 authorization URL for user {user_id}")
            return {"link_web_url": authorization_url}

        except Exception as e:
            logging.error(f"Error generating Whoop authorization URL: {str(e)}")
            raise

    async def callback(self, code: str, state: str) -> Dict[str, Any]:
        """
        Handle OAuth2 callback - Stage 2: Exchange authorization code for tokens
        
        This method processes the OAuth2 callback from Whoop, exchanges the authorization
        code for access tokens, and saves the credentials to the database.
        The user_id is retrieved from Redis cache using the state parameter as the key.
        
        Args:
            code: Authorization code received from Whoop callback
            state: State parameter received from Whoop callback
            
        Returns:
            Dict containing provider_slug, access_token (truncated), and stage info
            
        Raises:
            RuntimeError: If token exchange fails or credentials cannot be saved
        """
        try:
            logging.info("Processing OAuth2 callback")
            return await self._handle_oauth2_callback(None, code, state)
        except Exception as e:
            logging.error(f"Error in OAuth2 callback: {str(e)}")
            raise RuntimeError(str(e))

    async def _handle_oauth2_callback(self, user_id: str, code: str, state: Optional[str]) -> Dict[str, Any]:
        """Stage 2: Exchange authorization code for access token"""
        try:
            # Read state and redirect_uri from Redis
            cached_user_id = None
            redirect_uri = None
            return_url = None
            try:
                cfg = global_config()
                redis_config = cfg.get_redis()
                redis_client = await redis_config.get_async_client()
                if state:
                    cached_user_id = await redis_client.get(f"oauth2:state:{state}")
                    redirect_uri = await redis_client.get(f"oauth2:redir:{state}")
                    await redis_client.delete(f"oauth2:state:{state}")
                    await redis_client.delete(f"oauth2:redir:{state}")
                await redis_client.aclose()
                if isinstance(cached_user_id, bytes):
                    cached_user_id = cached_user_id.decode("utf-8")
                if isinstance(redirect_uri, bytes):
                    redirect_uri = redirect_uri.decode("utf-8")
                # parse return_url from encoded state
                try:
                    from urllib.parse import parse_qs
                    parsed = parse_qs(state or "")
                    r_values = parsed.get("r")
                    if r_values:
                        return_url = r_values[0]
                except Exception:
                    return_url = None
            except Exception as e:
                logging.warning(f"Failed to read oauth2 temp data from Redis: {str(e)}")

            # Always rely on Redis-stored user_id
            user_id = cached_user_id or user_id
            if not user_id:
                raise ValueError("Missing user_id for OAuth2 callback")
            if not redirect_uri:
                raise ValueError("Missing redirect_uri for OAuth2 token exchange")

            # Concise debug logs for OAuth stage 2 (do not log state/code)
            logging.info("[WHOOP][OAUTH2][STAGE2] callback received")

            # Exchange code for tokens using client_secret_post
            # Whoop requires client_secret_post (confirmed by API error message)
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            }
            data = {
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": redirect_uri,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            }

            # Debug: log credential info (safely)
            logging.info(f"[WHOOP][OAUTH2][STAGE2] Using token auth method: client_secret_post")
            logging.info(f"[WHOOP][OAUTH2][STAGE2] client_id length: {len(self.client_id) if self.client_id else 0}")
            logging.info(f"[WHOOP][OAUTH2][STAGE2] client_secret length: {len(self.client_secret) if self.client_secret else 0}")
            # Minimal request log (no secrets or URLs)
            safe_client_id = (self.client_id[:6] + "*") if self.client_id else ""
            logging.info(f"[WHOOP][OAUTH2][STAGE2] token request prepared; grant=authorization_code, client={safe_client_id}")

            async with aiohttp.ClientSession() as session:
                start_ts = time.time()
                async with session.post(self.token_url, data=data, headers=headers, timeout=aiohttp.ClientTimeout(total=self.request_timeout)) as resp:
                    elapsed_ms = int((time.time() - start_ts) * 1000)
                    raw_text = await resp.text()
                    # Minimal response log
                    logging.info(f"[WHOOP][OAUTH2][STAGE2] token response; status={resp.status}, elapsed_ms={elapsed_ms}")
                    if resp.status != 200:
                        raise RuntimeError(f"Failed to get access token: {resp.status} - {raw_text}")
                    try:
                        token_json = json.loads(raw_text)
                    except Exception:
                        raise RuntimeError("Token endpoint returned non-JSON body")

            access_token = token_json.get("access_token")
            refresh_token = token_json.get("refresh_token")
            expires_in = token_json.get("expires_in")  # seconds until expiration
            expires_at = None

            if not access_token:
                raise RuntimeError("Invalid token response from Whoop: missing access_token")

            if not refresh_token:
                # Be compatible with servers that don't return refresh_token
                logging.warning("[WHOOP][OAUTH2][STAGE2] Token response missing refresh_token; proceeding without refresh capability")
                refresh_token = ""

            # Calculate expires_at timestamp if expires_in is provided
            if expires_in:
                try:
                    expires_at = int(time.time()) + int(expires_in)
                    logging.info(f"[WHOOP][OAUTH2][STAGE2] Token expires in {expires_in} seconds (at timestamp {expires_at})")
                except Exception as e:
                    logging.warning(f"[WHOOP][OAUTH2][STAGE2] Failed to calculate expires_at: {str(e)}")
                    expires_at = None

            # Save OAuth2 credentials to database using OAuth2 method
            success = await self.db_service.save_oauth2_credentials(
                user_id, self.info.slug, access_token, refresh_token, expires_at
            )
            if not success:
                raise RuntimeError("Failed to save OAuth2 credentials")

            logging.info(f"Successfully linked Whoop provider for user {user_id}")

            # Construct credentials payload and trigger immediate pull using unified path
            creds_payload: Dict[str, Any] = {
                "user_id": user_id,
                "access_token": access_token,
                "refresh_token": refresh_token,
            }
            asyncio.create_task(self._pull_and_push_for_user(creds_payload))

            return {
                "provider_slug": self.info.slug,
                "access_token": access_token[:20] + "...",
                "stage": "completed",
                # Echo return_url out to router for 302 redirect
                "return_url": return_url,
            }

        except Exception as e:
            logging.error(f"Error handling Whoop OAuth2 callback: {str(e)}")
            raise

    async def unlink(self, user_id: str) -> Dict[str, Any]:
        """
        Unlink Whoop provider by deleting user registration from database
        
        Args:
            user_id: User ID
            
        Returns:
            Unlink result data
        """
        try:
            logging.info(f"Unlinking Whoop provider for user: {user_id}")

            # Delete user registration from database
            await self.db_service.delete_user_theta_provider(user_id, self.info.slug)

            logging.info(f"Successfully unlinked Whoop provider for user {user_id}")
            return {"success": True, "message": "Successfully unlinked from Whoop"}

        except Exception as e:
            logging.error(f"Failed to unlink Whoop provider: {str(e)}")
            raise RuntimeError(f"Failed to unlink provider: {str(e)}")

    async def format_data(self, raw_data: Dict[str, Any]) -> StandardPulseData:
        """Format Whoop raw data to StandardPulseData format"""
        start_time = time.time()

        try:
            request_id = self.generate_request_id()
            user_id = raw_data.get("user_id", "")
            data_content = raw_data.get("data", {})
            data_type = raw_data.get("data_type", "unknown")

            logging.info(f"Processing Whoop data for user: {user_id}, type: {data_type}, records: {len(data_content) if isinstance(data_content, list) else 1}")

            if not user_id:
                logging.error("No user_id found in Whoop data")
                return self._create_empty_response(request_id, "")

            # Get user timezone
            user_timezone = await self._get_user_timezone(user_id)
            logging.info(f"Using timezone {user_timezone} for user {user_id}")

            # Initialize processing_info with user_timezone
            processing_info = {
                "provider": "theta_whoop",
                "start_time": start_time,
                "processed_indicators": 0,
                "skipped_indicators": 0,
                "errors": [],
                "msg_id": raw_data.get("msg_id", ""),  # Extract msg_id for source_id
                "user_timezone": user_timezone,  # Add user timezone to processing_info
            }

            if not data_content:
                logging.info("No data content found in Whoop data")
                return self._create_empty_response(request_id, user_id)

            # Ensure data_content is a list
            if not isinstance(data_content, list):
                data_content = [data_content]

            meta_info = StandardPulseMetaInfo(userId=user_id, requestId=request_id, source="theta", timezone=user_timezone)

            health_records: List[StandardPulseRecord] = []

            # Process different data types
            if data_type == "sleeps":
                health_records.extend(self._process_sleep_data(data_content, processing_info))
            elif data_type == "cycles":
                health_records.extend(self._process_cycle_data(data_content, processing_info))
            elif data_type == "workouts":
                health_records.extend(self._process_workout_data(data_content, processing_info))
            elif data_type == "recoveries":
                health_records.extend(self._process_recovery_data(data_content, processing_info))
            elif data_type in ["body_measurements", "user_profile"]:
                health_records.extend(self._process_user_data(data_content, data_type, processing_info))
            else:
                logging.warning(f"Unknown Whoop data type: {data_type}")

            processing_info.update({
                "end_time": time.time(),
                "processing_duration_ms": int((time.time() - start_time) * 1000),
                "total_records": len(health_records),
                "success_rate": 1.0 if health_records else 0.0,
            })

            result = StandardPulseData(
                metaInfo=meta_info,
                healthData=health_records,
                processingInfo=processing_info,
            )

            logging.info(f"Formatted {len(health_records)} Whoop data records for user {user_id}")
            return result

        except Exception as e:
            processing_info.update({
                "end_time": time.time(),
                "processing_duration_ms": int((time.time() - start_time) * 1000),
                "fatal_error": str(e),
            })
            logging.error(f"Error formatting Whoop data: {str(e)}")
            request_id = self.generate_request_id()
            return self._create_empty_response(request_id, raw_data.get("user_id", ""))

    def _process_sleep_data(self, data: List[Dict], processing_info: Dict) -> List[StandardPulseRecord]:
        """Process Whoop sleep data using the mapping configuration.
        
        Expected record shape:
        {
          "start": "...",
          "score_state": "SCORED",
          "score": {
            "stage_summary": {...},
            "sleep_performance_percentage": ...,
            "sleep_consistency_percentage": ...,
            "sleep_efficiency_percentage": ...,
            "respiratory_rate": ...
          }
        }
        """
        records: List[StandardPulseRecord] = []

        for sleep in data:
            try:
                # Use sleep start time as timestamp
                start_time_str = sleep.get("start") or sleep.get("created_at")
                timestamp_ms = (
                    ThetaTimeUtils.parse_time_to_timestamp(start_time_str) if start_time_str else int(time.time() * 1000)
                )

                # Skip if not scored
                if sleep.get("score_state") != "SCORED":
                    continue

                # Use mapping to process indicators
                score = sleep.get("score", {})
                for field_path, (indicator_name, converter, unit) in self.WHOOP_INDICATOR_MAPPING["sleep"].items():
                    # Navigate nested fields
                    parts = field_path.split(".")
                    value = score
                    for field in parts[1:]:  # Skip "score" prefix
                        if isinstance(value, dict):
                            value = value.get(field)
                        else:
                            value = None
                            break

                    if value is not None:
                        record = StandardPulseRecord(
                            source=ThetaDataFormatter.format_source_name(self.info.slug),
                            type=indicator_name,
                            timestamp=timestamp_ms,
                            unit=unit,
                            value=float(converter(value)),
                            timezone=processing_info.get("user_timezone", "UTC"),
                            source_id=processing_info.get("msg_id", ""),
                        )
                        records.append(record)
                        processing_info["processed_indicators"] += 1

            except Exception as e:
                logging.error(f"Error processing Whoop sleep data: {str(e)}", exc_info=True)
                processing_info["errors"].append(f"Sleep data: {str(e)}")
                processing_info["skipped_indicators"] += 1

        return records

    def _process_cycle_data(self, data: List[Dict], processing_info: Dict) -> List[StandardPulseRecord]:
        """Process Whoop cycle data focusing on strain and energy metrics.
        
        Expected record shape:
        {
          "id": ...,
          "start": "...",
          "end": "...",
          "score": {
            "strain": ...,
            "kilojoule": ...,
            "average_heart_rate": ...,
            "max_heart_rate": ...
          }
        }
        """
        records: List[StandardPulseRecord] = []

        for cycle in data:
            try:
                # Use cycle start time as timestamp
                start_time_str = cycle.get("start")
                timestamp_ms = (
                    ThetaTimeUtils.parse_time_to_timestamp(start_time_str) if start_time_str else int(time.time() * 1000)
                )

                # Skip if not scored
                if cycle.get("score_state") != "SCORED":
                    continue

                # Use mapping to process indicators
                score = cycle.get("score", {})
                for field_path, (indicator_name, converter, unit) in self.WHOOP_INDICATOR_MAPPING["cycle"].items():
                    # Navigate nested fields
                    value = score
                    for field in field_path.split(".")[1:]:  # Skip "score" prefix
                        value = value.get(field) if isinstance(value, dict) else None

                    if value is not None:
                        record = StandardPulseRecord(
                            source=ThetaDataFormatter.format_source_name(self.info.slug),
                            type=indicator_name,
                            timestamp=timestamp_ms,
                            unit=unit,
                            value=float(converter(value)),
                            timezone=processing_info.get("user_timezone", "UTC"),
                            source_id=processing_info.get("msg_id", ""),
                        )
                        records.append(record)
                        processing_info["processed_indicators"] += 1

            except Exception as e:
                logging.error(f"Error processing Whoop cycle data: {str(e)}")
                processing_info["errors"].append(f"Cycle data: {str(e)}")
                processing_info["skipped_indicators"] += 1

        return records

    def _process_workout_data(self, data: List[Dict], processing_info: Dict) -> List[StandardPulseRecord]:
        """Process Whoop workout data including zones and performance metrics.
        
        Expected record shape:
        {
          "id": "...",
          "start": "...",
          "end": "...",
          "sport_name": "...",
          "score": {
            "strain": ...,
            "average_heart_rate": ...,
            "max_heart_rate": ...,
            "kilojoule": ...,
            "distance_meter": ...,
            "altitude_gain_meter": ...,
            "zone_durations": {
              "zone_zero_milli": ...,
              "zone_one_milli": ...,
              ...
            }
          }
        }
        """
        records: List[StandardPulseRecord] = []

        for workout in data:
            try:
                # Use workout start time as timestamp
                start_time_str = workout.get("start")
                timestamp_ms = (
                    ThetaTimeUtils.parse_time_to_timestamp(start_time_str) if start_time_str else int(time.time() * 1000)
                )

                # Skip if not scored
                if workout.get("score_state") != "SCORED":
                    continue

                # Sport type can be tracked through a separate indicator or logged
                sport_name = workout.get("sport_name")
                if sport_name:
                    # Log sport type for reference
                    logging.info(f"Workout type: {sport_name} at {timestamp_ms}")
                    # Could create a mapping of sport names to numeric codes if needed
                    # For now, we skip creating a record for sport_name

                # Aggregate heart rate zone durations first to avoid duplicate indicators
                # zone_zero + zone_one -> workoutDurationLow
                # zone_two + zone_three -> workoutDurationMedium
                # zone_four + zone_five -> workoutDurationHigh
                score = workout.get("score", {})
                zone_durations = score.get("zone_durations", {}) if isinstance(score, dict) else {}

                def _ms(val: Any) -> float:
                    try:
                        return float(val)
                    except Exception:
                        return 0.0

                low_ms = _ms(zone_durations.get("zone_zero_milli")) + _ms(zone_durations.get("zone_one_milli"))
                if low_ms > 0:
                    records.append(
                        StandardPulseRecord(
                            source=ThetaDataFormatter.format_source_name(self.info.slug),
                            type="workoutDurationLow",
                            timestamp=timestamp_ms,
                            unit="min",
                            value=float(low_ms / UNIT_CONVERSIONS["ms"]["min"]),
                            timezone=processing_info.get("user_timezone", "UTC"),
                            source_id=processing_info.get("msg_id", ""),
                        )
                    )
                    processing_info["processed_indicators"] += 1

                medium_ms = _ms(zone_durations.get("zone_two_milli")) + _ms(zone_durations.get("zone_three_milli"))
                if medium_ms > 0:
                    records.append(
                        StandardPulseRecord(
                            source=ThetaDataFormatter.format_source_name(self.info.slug),
                            type="workoutDurationMedium",
                            timestamp=timestamp_ms,
                            unit="min",
                            value=float(medium_ms / UNIT_CONVERSIONS["ms"]["min"]),
                            timezone=processing_info.get("user_timezone", "UTC"),
                            source_id=processing_info.get("msg_id", ""),
                        )
                    )
                    processing_info["processed_indicators"] += 1

                high_ms = _ms(zone_durations.get("zone_four_milli")) + _ms(zone_durations.get("zone_five_milli"))
                if high_ms > 0:
                    records.append(
                        StandardPulseRecord(
                            source=ThetaDataFormatter.format_source_name(self.info.slug),
                            type="workoutDurationHigh",
                            timestamp=timestamp_ms,
                            unit="min",
                            value=float(high_ms / UNIT_CONVERSIONS["ms"]["min"]),
                            timezone=processing_info.get("user_timezone", "UTC"),
                            source_id=processing_info.get("msg_id", ""),
                        )
                    )
                    processing_info["processed_indicators"] += 1

                # Use mapping to process indicators (skip raw zone_durations entries to avoid double-counting)
                score = workout.get("score", {})
                for field_path, (indicator_name, converter, unit) in self.WHOOP_INDICATOR_MAPPING["workout"].items():
                    if field_path.startswith("score.zone_durations."):
                        # Already aggregated above
                        continue
                    # Navigate nested fields
                    parts = field_path.split(".")
                    value = score
                    for field in parts[1:]:  # Skip "score" prefix
                        if isinstance(value, dict):
                            value = value.get(field)
                        else:
                            value = None
                            break

                    if value is not None:
                        record = StandardPulseRecord(
                            source=ThetaDataFormatter.format_source_name(self.info.slug),
                            type=indicator_name,
                            timestamp=timestamp_ms,
                            unit=unit,
                            value=float(converter(value)),
                            timezone=processing_info.get("user_timezone", "UTC"),
                            source_id=processing_info.get("msg_id", ""),
                        )
                        records.append(record)
                        processing_info["processed_indicators"] += 1

            except Exception as e:
                logging.error(f"Error processing Whoop workout data: {str(e)}")
                processing_info["errors"].append(f"Workout data: {str(e)}")
                processing_info["skipped_indicators"] += 1

        return records

    def _process_recovery_data(self, data: List[Dict], processing_info: Dict) -> List[StandardPulseRecord]:
        """Process Whoop recovery data including HRV and readiness metrics.
        
        Expected record shape:
        {
          "cycle_id": ...,
          "sleep_id": "...",
          "created_at": "...",
          "score": {
            "recovery_score": ...,
            "resting_heart_rate": ...,
            "hrv_rmssd_milli": ...,
            "spo2_percentage": ...,
            "skin_temp_celsius": ...
          }
        }
        """
        records: List[StandardPulseRecord] = []

        for recovery in data:
            try:
                # Use created_at as timestamp
                created_at_str = recovery.get("created_at")
                timestamp_ms = (
                    ThetaTimeUtils.parse_time_to_timestamp(created_at_str) if created_at_str else int(time.time() * 1000)
                )

                # Skip if not scored
                if recovery.get("score_state") != "SCORED":
                    continue

                # Use mapping to process indicators
                score = recovery.get("score", {})
                for field_path, (indicator_name, converter, unit) in self.WHOOP_INDICATOR_MAPPING["recovery"].items():
                    # Navigate nested fields
                    value = score
                    for field in field_path.split(".")[1:]:  # Skip "score" prefix
                        value = value.get(field) if isinstance(value, dict) else None

                    if value is not None:
                        record = StandardPulseRecord(
                            source=ThetaDataFormatter.format_source_name(self.info.slug),
                            type=indicator_name,
                            timestamp=timestamp_ms,
                            unit=unit,
                            value=float(converter(value)),
                            timezone=processing_info.get("user_timezone", "UTC"),
                            source_id=processing_info.get("msg_id", ""),
                        )
                        records.append(record)
                        processing_info["processed_indicators"] += 1

            except Exception as e:
                logging.error(f"Error processing Whoop recovery data: {str(e)}")
                processing_info["errors"].append(f"Recovery data: {str(e)}")
                processing_info["skipped_indicators"] += 1

        return records

    def _process_user_data(self, data: List[Dict], data_type: str, processing_info: Dict) -> List[StandardPulseRecord]:
        """Process Whoop user profile and body measurement data.
        
        For body measurements:
        {
          "height_meter": ...,
          "weight_kilogram": ...,
          "max_heart_rate": ...
        }
        
        For user profile (not converted to health records):
        {
          "user_id": ...,
          "email": "...",
          "first_name": "...",
          "last_name": "..."
        }
        """
        records: List[StandardPulseRecord] = []
        timestamp_ms = int(time.time() * 1000)

        for item in data:
            try:
                if data_type == "body_measurements":
                    # Use mapping to process body measurement indicators
                    for field_path, (indicator_name, converter, unit) in self.WHOOP_INDICATOR_MAPPING["body"].items():
                        value = item.get(field_path)

                        if value is not None:
                            record = StandardPulseRecord(
                                source=ThetaDataFormatter.format_source_name(self.info.slug),
                                type=indicator_name,
                                timestamp=timestamp_ms,
                                unit=unit,
                                value=float(converter(value)),
                                timezone=processing_info.get("user_timezone", "UTC"),
                                source_id=processing_info.get("msg_id", ""),
                            )
                            records.append(record)
                            processing_info["processed_indicators"] += 1

                elif data_type == "user_profile":
                    # User profile data is informational, log but don't create health records
                    logging.info(f"Received user profile data")

            except Exception as e:
                logging.error(f"Error processing Whoop {data_type}: {str(e)}")
                processing_info["errors"].append(f"{data_type}: {str(e)}")
                processing_info["skipped_indicators"] += 1

        return records

    async def pull_from_vendor_api(self, access_token: str, refresh_token: str, days: Optional[int] = None) -> List[Dict[str, Any]]:
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
                logging.info(f"Starting Whoop data pull (last {days} days)")
            else:
                logging.info("Starting comprehensive Whoop data pull")

            if not access_token:
                raise ValueError("Access token is required")

            headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
            all_raw_data = []

            async with aiohttp.ClientSession() as session:
                # Layer 1: Fetch all collection data
                logging.info("Layer 1: Fetching collection data")
                # Optional date range params for recent window
                collection_params = None
                if days and days > 0:
                    end_date = datetime.now(timezone.utc)
                    start_date = end_date - timedelta(days=days)
                    collection_params = {
                        "start": start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "end": end_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                        "limit": 25,
                    }

                # Fetch cycles
                cycles_url = f"{self.api_base_url}/cycle"
                cycles = await self._fetch_paginated_data(session, cycles_url, headers, collection_params)
                logging.info(f"Fetched {len(cycles)} cycle records")

                # Fetch sleeps
                sleeps_url = f"{self.api_base_url}/activity/sleep"
                sleeps = await self._fetch_paginated_data(session, sleeps_url, headers, collection_params)
                logging.info(f"Fetched {len(sleeps)} sleep records")

                # Fetch workouts
                workouts_url = f"{self.api_base_url}/activity/workout"
                workouts = await self._fetch_paginated_data(session, workouts_url, headers, collection_params)
                logging.info(f"Fetched {len(workouts)} workout records")

                # Fetch recovery
                recovery_url = f"{self.api_base_url}/recovery"
                recoveries = await self._fetch_paginated_data(session, recovery_url, headers, collection_params)
                logging.info(f"Fetched {len(recoveries)} recovery records")

                # Layer 2: Fetch detailed data concurrently
                logging.info("Layer 2: Fetching detailed data with concurrent requests")

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
                logging.info(f"Completed concurrent detail fetching in {elapsed:.2f} seconds")

                # Unpack results
                detailed_cycles = results[0] if not isinstance(results[0], Exception) and results[0] else []
                detailed_sleeps = results[1] if not isinstance(results[1], Exception) and results[1] else []
                detailed_workouts = results[2] if not isinstance(results[2], Exception) and results[2] else []
                cycle_recoveries = results[3] if not isinstance(results[3], Exception) and results[3] else []

                logging.info(f"Fetched details - Cycles: {len(detailed_cycles)}, Sleeps: {len(detailed_sleeps)}, "
                             f"Workouts: {len(detailed_workouts)}, Recoveries: {len(cycle_recoveries)}")

                # Layer 3: Fetch static data
                logging.info("Layer 3: Fetching static data")

                # Fetch user profile
                profile_url = f"{self.api_base_url}/user/profile/basic"
                profile = await self._fetch_paginated_data(session, profile_url, headers)
                logging.info(f"Fetched user profile data")

                # Fetch body measurements
                body_url = f"{self.api_base_url}/user/measurement/body"
                body_measurements = await self._fetch_paginated_data(session, body_url, headers)
                logging.info(f"Fetched body measurement data")

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

            logging.info(f"Completed comprehensive Whoop data pull: {len(all_raw_data)} data packages")
            return all_raw_data

        except Exception as e:
            logging.error(f"Error in Whoop data pull: {str(e)}")
            return []

    async def _refresh_access_token(self, session: aiohttp.ClientSession, refresh_token: str) -> Optional[Dict[str, Any]]:
        try:
            # Use client_secret_post for refresh token
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            }
            data = {
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": self.scopes,
            }
            logging.info("[WHOOP][OAUTH2][REFRESH] Using token auth method: client_secret_post")
            # Minimal refresh request log
            safe_client_id = (self.client_id[:6] + "*") if self.client_id else ""
            logging.info(f"[WHOOP][OAUTH2][REFRESH] token request prepared; client={safe_client_id}")
            async with session.post(self.token_url, data=data, headers=headers, timeout=aiohttp.ClientTimeout(total=self.request_timeout)) as resp:
                raw_text = await resp.text()
                logging.info(f"[WHOOP][OAUTH2][REFRESH] token response; status={resp.status}")

                if resp.status != 200:
                    # Log detailed error information
                    if resp.status == 400:
                        logging.error(f"[WHOOP][OAUTH2][REFRESH] Bad request (400) - likely refresh token expired or invalid: {raw_text}")
                    elif resp.status == 401:
                        logging.error(f"[WHOOP][OAUTH2][REFRESH] Unauthorized (401) - refresh token expired or client credentials invalid: {raw_text}")
                    elif resp.status == 403:
                        logging.error(f"[WHOOP][OAUTH2][REFRESH] Forbidden (403) - insufficient permissions: {raw_text}")
                    else:
                        logging.error(f"[WHOOP][OAUTH2][REFRESH] HTTP {resp.status} error: {raw_text}")
                    return None

                try:
                    token_data = json.loads(raw_text)
                    logging.info("[WHOOP][OAUTH2][REFRESH] Successfully refreshed access token")
                    return token_data
                except json.JSONDecodeError as e:
                    logging.error(f"[WHOOP][OAUTH2][REFRESH] Failed to parse token response as JSON: {str(e)}, response: {raw_text}")
                    return None

        except aiohttp.ClientError as e:
            logging.error(f"[WHOOP][OAUTH2][REFRESH] Network error during token refresh: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"[WHOOP][OAUTH2][REFRESH] Unexpected error during token refresh: {str(e)}")
            return None

    async def _handle_whoop_auth_failure(self, user_id: str, error_details: str) -> None:
        """Handle Whoop authentication failure by cleaning up invalid credentials."""
        try:
            logging.error(f"Whoop authentication failed for user {user_id}: {error_details}")

            # Remove invalid credentials from database
            await self.db_service.delete_user_theta_provider(user_id, self.info.slug)
            logging.info(f"Removed invalid Whoop credentials for user {user_id}")

            # Log guidance for user re-authorization
            logging.error(
                f"User {user_id} needs to re-authorize Whoop connection. "
                f"Refresh token has expired. Please have them complete the OAuth flow again."
            )

        except Exception as e:
            logging.error(f"Error handling Whoop auth failure for user {user_id}: {str(e)}")

    async def get_valid_access_token(self, user_id: str) -> Optional[str]:
        """
        Get a valid access token for the user, refreshing if necessary
        
        Args:
            user_id: User ID
            
        Returns:
            Valid access token or None if unable to get/refresh
        """
        try:
            # Get stored credentials
            credentials = await self.db_service.get_user_credentials(user_id, self.info.slug, self.info.auth_type)
            if not credentials:
                logging.error(f"No credentials found for user {user_id}")
                return None

            access_token = credentials.get("access_token")
            refresh_token = credentials.get("refresh_token")
            expires_at = credentials.get("expires_at")

            if not access_token:
                logging.error(f"No access token found for user {user_id}")
                return None

            # Check if token is expired
            current_time = int(time.time())

            # Convert expires_at to timestamp if it's a datetime object
            if expires_at:
                if isinstance(expires_at, datetime):
                    expires_at = int(expires_at.timestamp())
                else:
                    expires_at = int(expires_at)

            if expires_at and current_time < expires_at:
                # Token is still valid
                logging.info(f"Access token still valid for user {user_id}, expires in {expires_at - current_time} seconds")
                return access_token

            # Token is expired or no expiry info, try to refresh
            if not refresh_token:
                logging.error(f"No refresh token available for user {user_id}")
                return None

            logging.info(f"Access token expired for user {user_id}, attempting refresh")

            # Refresh the token
            async with aiohttp.ClientSession() as session:
                refreshed = await self._refresh_access_token(session, refresh_token)

            if not refreshed or "access_token" not in refreshed:
                logging.error(f"Failed to refresh token for user {user_id}")
                # Handle authentication failure - clean up invalid credentials
                await self._handle_whoop_auth_failure(user_id, "Refresh token failed or expired")
                return None

            # Extract new tokens and expiry
            new_access_token = refreshed["access_token"]
            new_refresh_token = refreshed.get("refresh_token", refresh_token)  # Use old refresh token if not provided
            expires_in = refreshed.get("expires_in")

            # Calculate new expires_at
            new_expires_at = None
            if expires_in:
                try:
                    new_expires_at = int(time.time()) + int(expires_in)
                    logging.info(f"New token expires in {expires_in} seconds (at timestamp {new_expires_at})")
                except Exception as e:
                    logging.warning(f"Failed to calculate expires_at: {str(e)}")

            # Save updated credentials
            save_success = await self.db_service.save_oauth2_credentials(
                user_id, self.info.slug, new_access_token, new_refresh_token, new_expires_at
            )

            if not save_success:
                logging.error(f"Failed to save refreshed credentials for user {user_id}")
                # Still return the new token even if save failed

            logging.info(f"Successfully refreshed token for user {user_id}")
            return new_access_token

        except Exception as e:
            logging.error(f"Error getting valid access token for user {user_id}: {str(e)}")
            return None

    async def _fetch_paginated_data(
            self,
            session: aiohttp.ClientSession,
            endpoint: str,
            headers: Dict[str, str],
            params: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
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
                                logging.warning(f"Rate limited on {endpoint}, retrying after {retry_after} seconds")
                                await asyncio.sleep(min(retry_after, 60))  # Cap at 60 seconds
                                retry_count += 1
                                continue
                            else:
                                logging.error(f"Max retries exceeded for {endpoint} due to rate limiting")
                                break
                        elif resp.status == 401:
                            # Token should have been validated at entry point, 401 indicates auth failure
                            text = await resp.text()
                            logging.error(f"Authentication failed for {endpoint}: {resp.status} - {text}")
                            break
                        elif resp.status != 200:
                            text = await resp.text()
                            logging.error(f"Failed to fetch {endpoint}: {resp.status} - {text}")
                            break
                        else:
                            data = await resp.json()
                            break
                except asyncio.TimeoutError:
                    logging.error(f"Timeout fetching {endpoint}")
                    if retry_count < max_retries:
                        retry_count += 1
                        await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                        continue
                    else:
                        break
                except Exception as e:
                    logging.error(f"Error fetching {endpoint}: {str(e)}")
                    if retry_count < max_retries:
                        retry_count += 1
                        await asyncio.sleep(2 ** retry_count)
                        continue
                    else:
                        break

            # Check if we successfully got data
            if retry_count > max_retries:
                break

            # Extract records and next token
            if "records" in data:
                records = data.get("records", [])
                all_records.extend(records)
                next_token = data.get("next_token")

                logging.info(f"Fetched {len(records)} records from {endpoint}, total: {len(all_records)}")

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
            items: List[Dict],
            url_template: str,
            id_field: str,
            headers: Dict[str, str],
    ) -> List[Dict[str, Any]]:
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

        async def fetch_one(item: Dict) -> Optional[Dict]:
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
                    logging.error(f"Error fetching detail for {id_field}={item_id}: {str(e)}")
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

    async def save_raw_data_to_db(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Save full Whoop payload into theta_ai.health_data_whoop.
        We store the entire provider response as jsonb for auditing and reprocessing.
        """
        try:
            if not isinstance(raw_data, (dict, list)):
                return []

            # Extract user_id from raw_data (temporarily used as theta_user_id and external_user_id)
            user_id = raw_data.get("user_id", "")

            # Generate a simple msg_id using timestamp
            msg_id = f"whoop_{user_id}_{int(time.time())}" if user_id else f"whoop_{int(time.time())}"

            insert_sql = (
                "INSERT INTO theta_ai.health_data_whoop "
                "(create_at, update_at, is_del, msg_id, raw_data, theta_user_id, external_user_id) "
                "VALUES (CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, :is_del, :msg_id, :raw_data, :theta_user_id, :external_user_id)"
            )
            params = {
                "is_del": False,
                "msg_id": msg_id,
                "raw_data": json.dumps(raw_data, ensure_ascii=False),
                "theta_user_id": user_id,
                "external_user_id": user_id,
            }
            await execute_query(query=insert_sql, params=params)

            # Add msg_id to returned data for consistency
            result_data = raw_data.copy() if isinstance(raw_data, dict) else {"data": raw_data}
            result_data["msg_id"] = msg_id
            return [result_data]
        except Exception as e:
            logging.info(f"whoop raw_data: {raw_data}")
            logging.error(f"Error saving Whoop raw data: {str(e)}")
            return []

    async def is_data_already_processed(self, raw_data: Dict[str, Any]) -> bool:
        return False

    async def _pull_and_push_for_user(self, credentials: Dict[str, Any]) -> bool:
        """
        Override: unified per-user pull + push using OAuth2 tokens.
        Accepts credentials dict which may contain access_token/refresh_token/user_id.
        """
        try:
            user_id = credentials.get("user_id") if isinstance(credentials, dict) else None
            if not user_id:
                logging.error("[whoop:_pull_and_push_for_user] Missing user_id in credentials")
                return False

            # Ensure we have a valid access token (handles refresh if needed)
            access_token = await self.get_valid_access_token(user_id)
            if not access_token:
                logging.error(f"[whoop:_pull_and_push_for_user] Unable to get valid access token for user {user_id}")
                return False

            # Get the latest credentials from database (may include updated refresh_token)
            latest_credentials = await self.db_service.get_user_credentials(user_id, self.info.slug, self.info.auth_type)
            if not latest_credentials:
                logging.error(f"[whoop:_pull_and_push_for_user] Unable to get latest credentials for user {user_id}")
                return False

            refresh_token = latest_credentials.get("refresh_token")
            if not refresh_token:
                logging.warning(f"[whoop:_pull_and_push_for_user] No refresh token available for user {user_id}")

            # Pull recent data
            raw_data_list = await self.pull_from_vendor_api(access_token, refresh_token, days=2)
            if not raw_data_list:
                logging.info(f"No recent whoop data for user {user_id}")
                return True

            success_count = 0
            error_count = 0
            for raw_data in raw_data_list:
                try:
                    raw_data["user_id"] = user_id
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
                        logging.error(f"Failed to push whoop data for user {user_id} with msg_id {msg_id}")
                except Exception as e:
                    error_count += 1
                    logging.error(f"Error processing whoop data for user {user_id}: {str(e)}")
                    continue

            logging.info(f"Processed whoop data for user {user_id}: success={success_count}, errors={error_count}")
            return error_count == 0
        except Exception as e:
            logging.error(f"Error in whoop _pull_and_push_for_user: {str(e)}")
            return False
