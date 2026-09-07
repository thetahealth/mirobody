"""
Base classes for providers
"""

import logging
import time
import uuid
from abc import abstractmethod
from typing import Any, Optional

from mirobody.kernel import connect
from mirobody.pulse import LinkRequest
from mirobody.pulse.base import Provider
from mirobody.pulse.core import LinkType
from mirobody.pulse.core.push_service import push_service
from mirobody.pulse.core.user import PlatformUserService
from mirobody.pulse.ingest.models.requests import (
    FormatDataContext,
    FormatDataInput,
    StandardPulseData,
    StandardPulseMetaInfo,
)
from mirobody.pulse.providers.platform.database_service import ProviderDatabaseService
from mirobody.utils import execute_query

logger = logging.getLogger(__name__)


class BasePullProvider(Provider):
    """
    Base class for providers

    Provides common functionality for providers
    """

    def __init__(self):
        self.db_service = ProviderDatabaseService()
        self.user_service = PlatformUserService()

    @classmethod
    def create_provider(cls, config: dict[str, Any]) -> Optional['BasePullProvider']:
        """
        Factory method to create provider instance from config
        
        Subclasses should override this method to:
        1. Check if required config keys exist
        2. Return None if config is insufficient
        3. Return provider instance if config is valid
        
        Default implementation: call no-arg constructor
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Provider instance if config is valid, None otherwise
        """
        try:
            return cls()
        except Exception as e:
            logger.warning(f"Failed to create provider {cls.__name__}: {e}")
            return None

    def register_pull_task(self) -> bool:
        """
        Register pull task for provider
        """
        return True

    async def link(self, request: LinkRequest) -> dict[str, Any]:
        user_id = request.user_id
        provider_slug = request.provider_slug
        auth_type = request.auth_type

        try:
            # OAuth types should use callback flow, not direct link
            if auth_type in (LinkType.OAUTH1, LinkType.OAUTH2, LinkType.OAUTH):
                raise ValueError(f"{auth_type} should use OAuth callback flow")
            
            # Validate credentials
            await self._validate_credentials(request.credentials)
            
            # Build credentials based on auth_type
            connect_info = request.credentials.get("connect_info")
            if auth_type == LinkType.CUSTOMIZED:
                if not connect_info:
                    raise ValueError("connect_info is required for customized auth type")
                username = ""
                password = ""
            elif auth_type == LinkType.PASSWORD:
                username = request.credentials.get("username", "")
                password = request.credentials.get("password", "")
                if not username or not password:
                    raise ValueError("Username and password are required for PASSWORD auth type")
            else:
                raise ValueError(f"Unsupported auth type: {auth_type}")
            
            # Save to database
            creds = self.db_service.Credentials(username=username, password=password, connect_info=connect_info)
            success = await self.db_service.save_user_theta_provider(user_id, provider_slug, auth_type, creds)
            
            if not success:
                raise RuntimeError(f"Failed to link provider {provider_slug}")
            
            logger.info(f"Successfully linked theta provider {provider_slug} ({auth_type.value}) for user {user_id}")
            result = {"provider_slug": provider_slug, "msg": "ok", "connected": True}
            if username:
                result["username"] = username
            return result

        except Exception as e:
            logger.error(f"Error linking theta provider {provider_slug}: {str(e)}")
            raise RuntimeError(str(e))

    async def unlink(self, user_id: str) -> dict[str, Any]:
        provider_slug = self.info.slug

        try:
            success = await self.db_service.delete_user_theta_provider(user_id, provider_slug)

            if success:
                logger.info(f"Successfully unlinked theta provider {provider_slug} for user {user_id}")
                return {"provider_slug": provider_slug}
            raise RuntimeError(f"Failed to unlink provider {provider_slug}")

        except Exception as e:
            logger.error(f"Error unlinking theta provider {provider_slug}: {str(e)}")
            raise RuntimeError(str(e))

    async def _validate_credentials(self, credentials: dict[str, Any]) -> None:
        """Reject credentials that cannot work, by raising. Default: accept.

        `credentials` is the LinkRequest's dict: `username`/`password` for
        `LinkType.PASSWORD`, `connect_info` for `LinkType.CUSTOMIZED`. Override
        to probe the vendor (pgsql opens a connection).
        """

    async def _get_user_timezone(self, user_id: str) -> str:
        try:
            user_info = await self.user_service.get_user_by_id(user_id)
            if user_info and user_info.get("tz"):
                user_timezone = user_info.get("tz").strip()
                if user_timezone:
                    logger.info(f"Retrieved user timezone from database: user_id={user_id}, timezone={user_timezone}")
                    return user_timezone

            logger.info(f"No timezone found for user {user_id}, using default UTC")
            return "UTC"

        except Exception as e:
            logger.warning(f"Failed to get user timezone for user {user_id}: {str(e)}, using default UTC")
            return "UTC"

    def _extract_theta_user_id(self, saved_data: dict[str, Any]) -> str:
        """Extract internal system user ID from saved data.

        Tries 'theta_user_id' (this platform's own convention) then
        'app_user_id' (Vital convention). Does NOT fall back to 'user_id'
        because 'user_id' means external/vendor user ID by convention.
        """
        return saved_data.get("theta_user_id", saved_data.get("app_user_id", ""))

    def _extract_external_user_id(self, saved_data: dict[str, Any]) -> str:
        """Extract vendor-side user ID from saved data.

        Default: reads top-level 'user_id' which by convention is the
        external/vendor user ID. Returns '' if absent.
        Override per provider when external ID lives deeper in the payload
        (e.g., Whoop reads from data[0]["user_id"] for webhook path).
        """
        return saved_data.get("user_id", "")

    def _extract_msg_id(self, saved_data: dict[str, Any]) -> str:
        """Extract message/request tracking ID from saved data."""
        return saved_data.get("msg_id", "")

    @staticmethod
    def _extract_nested_value(data: dict, field_path: str):
        """Navigate nested dict by dot-separated path: 'a.b.c' → data['a']['b']['c']"""
        value = data
        for key in field_path.split("."):
            if isinstance(value, dict):
                value = value.get(key)
            else:
                return None
        return value

    async def build_format_context(self, saved_data: dict[str, Any]) -> FormatDataContext:
        """Build pre-resolved context from saved raw data.

        Extracts identity fields via overridable hooks, resolves timezone
        from DB, and returns a complete FormatDataContext.  Caller
        (Platform.post_data) passes the result inside FormatDataInput so
        that format_data needs no DB access.
        """
        theta_user_id = self._extract_theta_user_id(saved_data)
        external_user_id = self._extract_external_user_id(saved_data)
        msg_id = self._extract_msg_id(saved_data)
        user_timezone = await self._get_user_timezone(theta_user_id)
        return FormatDataContext(
            theta_user_id=theta_user_id,
            external_user_id=external_user_id,
            user_timezone=user_timezone,
            msg_id=msg_id,
        )

    async def format_data(self, fmt_input: FormatDataInput) -> StandardPulseData:
        """Vendor payload -> `StandardPulseData`. The one thing a provider must do.

        `fmt_input.context` carries everything already resolved by the caller
        (`build_format_context`: internal user id, vendor user id, timezone,
        msg_id), so this method needs no database access and is a pure
        transformation — which is what makes it snapshot-testable against a
        recorded payload. `fmt_input.payload` is the vendor's data, untouched.

        (This used to be two methods, `format_data(raw)` and `format_data_v2(
        fmt_input)`, each detecting whether the subclass had overridden the other
        and forwarding — a migration that stopped halfway. Every provider now
        implements this signature and nothing else.)
        """
        raise NotImplementedError(f"{type(self).__name__} must implement format_data")

    def generate_request_id(self) -> str:
        """Generate request ID"""
        return str(uuid.uuid4())

    def _create_empty_response(self, request_id: str, user_id: str) -> StandardPulseData:
        meta_info = StandardPulseMetaInfo(userId=user_id or "", requestId=request_id, source="theta", timezone="UTC")

        return StandardPulseData(metaInfo=meta_info, healthData=[])

    # ========== Pull Related Methods ==========

    async def get_all_user_credentials(self) -> list[dict[str, Any]]:
        try:
            # Explicitly pass link_type based on current provider's authentication method
            link_type = self.info.auth_type
            if link_type == self.info.auth_type.SERVICE:
                return []
            return await self.db_service.get_all_user_credentials_for_provider(self.info.slug, link_type)
        except Exception as e:
            logger.warning(f"Error getting user credentials for provider {self.info.slug}: {str(e)}")
            return []

    @abstractmethod
    async def save_raw_data_to_db(self, raw_data: dict[str, Any]) -> list[dict[str, Any]]:
        pass

    @abstractmethod
    async def is_data_already_processed(self, raw_data: dict[str, Any]) -> bool:
        pass

    async def pull_and_push(self) -> bool:
        try:
            credentials = await self.get_all_user_credentials()
            if not credentials:
                logger.info(f"No users found for provider {self.info.slug}")
                return True

            success_count = 0
            error_count = 0

            for cred in credentials:
                try:
                    user_success = await self._pull_and_push_for_user(cred)
                    if user_success:
                        success_count += 1
                    else:
                        error_count += 1
                except Exception as e:
                    logger.error(f"Error processing user {cred['user_id']}: {str(e)}")
                    error_count += 1

            logger.info(
                f"Pull and push completed for provider {self.info.slug}: {success_count} success, {error_count} errors"
            )
            return error_count == 0

        except Exception as e:
            logger.error(f"Error in pull_and_push for provider {self.info.slug}: {str(e)}")
            return False


    #: When to stop trying a credential that keeps failing, and for how long.
    #: Three because a transient outage is one or two failures and a changed
    #: password is every one; thirty minutes because the person who fixes it
    #: does so by relinking, which resets the state anyway.
    DEBOUNCE = connect.DebouncePolicy(threshold=3, cooldown_ms=30 * 60 * 1000)

    def _now_ms(self) -> int:
        """The clock, as one overridable call. A back-off is a decision about
        elapsed time, and a test that cannot move time can only assert that
        nothing happens yet."""
        return int(time.time() * 1000)

    def _debounce_state(self, user_id: str) -> connect.Credential:
        states = self.__dict__.setdefault("_credential_states", {})
        key = str(user_id)
        if key not in states:
            states[key] = connect.Credential(provider=self.info.slug, subject_id=key)
        return states[key]

    def _record_pull_outcome(self, user_id: str, *, ok: bool, now_ms: int) -> None:
        states = self.__dict__.setdefault("_credential_states", {})
        cred = self._debounce_state(user_id)
        states[str(user_id)] = (
            connect.record_success(cred)
            if ok
            else connect.record_failure(cred, now_ms=now_ms, policy=self.DEBOUNCE)
        )

    async def _pull_and_push_for_user(self, credentials: dict[str, Any]) -> bool:
        """
        Execute pull and push for a single user

        Args:
            credentials: User credentials

        Returns:
            Whether successful
        """
        try:
            user_id = credentials["user_id"]
            username = credentials["username"]
            password = credentials["password"]

            # A credential whose password the person changed fails on every
            # tick, forever, at the loop's full rate — and some vendors count
            # that as an attack and lock the account the person still uses.
            # `mirobody.kernel.connect` is the state machine: consecutive
            # authorization failures expire the credential, and a retry waits
            # out a cooldown. Held in memory per worker, which is the right
            # scope for "do not hammer this account right now"; the durable
            # answer is the credential's own state, which relinking resets.
            cred = self._debounce_state(user_id)
            now_ms = self._now_ms()
            if not connect.may_attempt(cred, now_ms=now_ms, policy=self.DEBOUNCE):
                failure_count = cred.failures
                logger.info(
                    "skipping %s for one subject: credential state=%s failure_count=%d",
                    self.info.slug, cred.state, failure_count,
                )
                return True  # not an error: deliberately not attempted

            # 1. Pull data from vendor API.
            try:
                raw_data_list = await self.pull_from_vendor_api(username, password)
            except Exception as e:
                self._record_pull_outcome(user_id, ok=False, now_ms=now_ms)
                failure_count = self._debounce_state(user_id).failures
                logger.warning(
                    "pull failed for %s: error_type=%s failure_count=%d",
                    self.info.slug, type(e).__name__, failure_count,
                )
                return False
            self._record_pull_outcome(user_id, ok=True, now_ms=now_ms)

            if not raw_data_list:
                logger.info(f"No data pulled for user {user_id}")
                return True  # No data is not an error

            success_count = 0

            # 2. Process each data record
            for raw_data in raw_data_list:
                try:
                    # Inject system user ID with canonical key
                    raw_data["theta_user_id"] = user_id

                    # Check if already processed
                    if await self.is_data_already_processed(raw_data):
                        logger.info(f"Data already processed for user {user_id}")
                        continue

                    # Push data (function call)
                    push_success = await push_service.push_data(
                        platform="theta",
                        provider_slug=self.info.slug,
                        data=raw_data,
                        msg_id=str(uuid.uuid4()),
                    )

                    if push_success:
                        success_count += 1
                        logger.info(f"Successfully pushed data for user {user_id}")
                    else:
                        logger.error(f"Failed to push data for user {user_id}")

                except Exception as e:
                    logger.error(f"Error processing data for user {user_id}: {str(e)}")
                    continue

            logger.info(f"Processed {success_count} records for user {user_id}")
            return True

        except Exception as e:
            logger.error(f"Error in _pull_and_push_for_user: {str(e)}")
            return False

    async def pull_from_vendor_api(self, username: str, password: str) -> list[dict[str, Any]]:
        raise NotImplementedError("Subclasses must implement pull_from_vendor_api method")

    # ========== Raw Data Query Methods (for Management UI) ==========

    def get_table_name(self) -> str:
        """
        Get the database table name for this provider
        
        Default implementation: health_data_{provider_name}
        where provider_name is extracted from provider slug (e.g., theta_renpho -> renpho)
        
        Providers can override this method for custom table names.
        
        Returns:
            Full table name with schema (e.g., "health_data_renpho")
        """
        # Extract provider name from slug: theta_renpho -> renpho
        provider_name = self.info.slug.replace("theta_", "")
        return f"health_data_{provider_name}"

    def get_user_id_column(self) -> str:
        """
        Get the user ID column name for this provider
        
        Default: "theta_user_id"
        Override for providers using different column names (e.g., FrontierX uses "uid")
        
        Returns:
            Column name for system user ID
        """
        return "theta_user_id"

    def get_query_columns(self) -> list[str]:
        """
        Get the columns to select in raw data query
        
        Default: standard columns matching actual Garmin/Whoop table structure
        Override for providers with different column structure.
        
        Returns:
            List of column names to select
        """
        return [
            "id",
            self.get_user_id_column(),
            "external_user_id",
            "msg_id",
            "raw_data",
            "create_at",
            "update_at",
            "is_del"
        ]

    def get_order_by_clause(self) -> str:
        """
        Get the ORDER BY clause for raw data query
        
        Default: "ORDER BY create_at DESC, id DESC"
        Override for custom sorting.
        
        Returns:
            ORDER BY clause (without the "ORDER BY" prefix)
        """
        return "create_at DESC, id DESC"

    async def get_raw_data_records(
        self,
        page: int = 1,
        page_size: int = 20,
        user_id: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        """
        Query raw data records from provider's storage table
        
        This is a generic implementation that works for standard table structures.
        Providers with non-standard tables (like Resmed) should override this method.
        
        Standard table structure:
        - id: bigint (identity)
        - create_at, update_at: timestamp
        - user_id: varchar - system user ID
        - out_uid: bigint/varchar - external platform user ID
        - key: bigint - external data unique identifier
        - data: text - JSON format raw data
        
        Args:
            page: Page number (starting from 1)
            page_size: Number of records per page
            user_id: Optional filter for system user ID
            start_date: Optional start date (YYYY-MM-DD)
            end_date: Optional end date (YYYY-MM-DD)
            
        Returns:
            Dictionary containing paginated raw data with metadata
        """
        try:
            table_name = self.get_table_name()
            user_id_column = self.get_user_id_column()
            columns = self.get_query_columns()
            order_by = self.get_order_by_clause()
            
            # Build WHERE clause
            where_conditions = ["1=1"]
            params = {"limit": page_size, "offset": (page - 1) * page_size}
            
            # Add soft delete filter if table has is_del column
            if "is_del" in columns:
                where_conditions.append("is_del = false")
            
            if user_id:
                where_conditions.append(f"{user_id_column} = :user_id")
                params["user_id"] = user_id
                
            if start_date:
                where_conditions.append("create_at >= :start_date::timestamp")
                params["start_date"] = start_date
                
            if end_date:
                # End date should include the entire day
                where_conditions.append("create_at < (:end_date::timestamp + interval '1 day')")
                params["end_date"] = end_date
            
            where_clause = " AND ".join(where_conditions)
            
            # Count total records
            count_query = f"""
                SELECT COUNT(*) as total
                FROM {table_name}
                WHERE {where_clause}
            """
            count_result = await execute_query(query=count_query, params=params)
            total = count_result[0]["total"] if count_result else 0
            
            # Get paginated records
            columns_str = ", ".join(columns)
            query = f"""
                SELECT {columns_str}
                FROM {table_name}
                WHERE {where_clause}
                ORDER BY {order_by}
                LIMIT :limit OFFSET :offset
            """
            records = await execute_query(query=query, params=params)
            
            # Format records
            formatted_records = []
            for record in records:
                formatted_record = {}
                for col in columns:
                    value = record.get(col)
                    # Convert datetime to ISO format string
                    if hasattr(value, 'isoformat'):
                        formatted_record[col] = value.isoformat()
                    else:
                        formatted_record[col] = value
                formatted_records.append(formatted_record)
            
            total_pages = (total + page_size - 1) // page_size if total > 0 else 0
            
            return {
                "records": formatted_records,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": total_pages,
                "provider_slug": self.info.slug,
            }
            
        except Exception as e:
            logger.error(f"Error querying raw data for provider {self.info.slug}: {str(e)}")
            return {
                "records": [],
                "total": 0,
                "page": page,
                "page_size": page_size,
                "total_pages": 0,
                "provider_slug": self.info.slug,
                "error": str(e)
            }

    async def get_raw_data_by_id(self, record_id: int) -> dict[str, Any] | None:
        """
        Get a single raw data record by ID
        
        Args:
            record_id: Record ID from database
            
        Returns:
            Dictionary containing the record data, or None if not found
        """
        try:
            table_name = self.get_table_name()
            query_columns = self.get_query_columns()
            columns_str = ", ".join(query_columns)
            
            query = f"""
                SELECT {columns_str}
                FROM {table_name}
                WHERE id = :record_id
            """
            
            records = await execute_query(query=query, params={"record_id": record_id})
            
            if not records or len(records) == 0:
                logger.warning(f"Record with ID {record_id} not found in {table_name}")
                return None
            
            record = records[0]
            
            # Format the record (convert datetime to ISO format)
            formatted_record = {}
            for col in query_columns:
                value = record.get(col)
                if hasattr(value, 'isoformat'):
                    formatted_record[col] = value.isoformat()
                else:
                    formatted_record[col] = value
            
            return formatted_record
            
        except Exception as e:
            logger.error(f"Error getting record {record_id} for provider {self.info.slug}: {str(e)}")
            return None
