"""
Theta PostgreSQL Provider

PostgreSQL database connection configuration provider.
Validates and stores PostgreSQL connection credentials only.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

import psycopg

from mirobody.pulse.base import ProviderInfo
from mirobody.pulse.core import LinkType, ProviderStatus
from mirobody.pulse.core.models import ConnectInfoField
from mirobody.pulse.data_upload.models.requests import StandardPulseData
from mirobody.pulse.theta.platform.base import BaseThetaProvider
from mirobody.utils.config import safe_read_cfg


class ThetaPgsqlProvider(BaseThetaProvider):
    """Theta PostgreSQL Provider - Connection Configuration Only"""

    def __init__(self):
        super().__init__()

        # Load default configuration
        self.connection_timeout = 15

        logging.info("PostgreSQL provider initialized (connection validation only)")

    @classmethod
    def create_provider(cls, config: Dict[str, Any]) -> Optional['ThetaPgsqlProvider']:
        """
        Factory method to create PostgreSQL provider
        
        Returns:
            Provider instance
        """
        try:
            if not safe_read_cfg("ENABLE_PGSQL_DEVICE", ""):
                return None
            return cls()
        except Exception as e:
            logging.warning(f"Failed to create PostgreSQL provider: {e}")
            return None

    def register_pull_task(self) -> bool:
        """
        Do not register pull task - this provider only validates connection
        """
        return False

    @property
    def info(self) -> ProviderInfo:
        """Get Provider information"""
        return ProviderInfo(
            slug="theta_pgsql",
            name="PostgreSQL",
            description="PostgreSQL database connection configuration",
            logo="https://static.thetahealth.ai/res/elephant.png",
            supported=True,
            auth_type=LinkType.CUSTOMIZED,  # Customized auth type with all fields in connect_info
            status=ProviderStatus.AVAILABLE,
            connect_info_fields=[
                ConnectInfoField(
                    field_name="username",
                    field_type="string",
                    required=True,
                    label="Username",
                    placeholder="Enter your database username",
                    default_value=""
                ),
                ConnectInfoField(
                    field_name="password",
                    field_type="password",
                    required=True,
                    label="Password",
                    placeholder="Enter your database password",
                    default_value=""
                ),
                ConnectInfoField(
                    field_name="host",
                    field_type="string",
                    required=True,
                    label="Host",
                    placeholder="e.g., pg, localhost, or db.example.com",
                    default_value="pg"
                ),
                ConnectInfoField(
                    field_name="port",
                    field_type="number",
                    required=True,
                    label="Port",
                    placeholder="Default PostgreSQL port",
                    default_value="5432"
                ),
                ConnectInfoField(
                    field_name="database",
                    field_type="string",
                    required=True,
                    label="Database",
                    placeholder="Enter your database name",
                    default_value=""
                ),
            ]
        )

    async def _validate_credentials_v2(self, credentials: Dict[str, Any]) -> None:
        """
        Validate PostgreSQL connection credentials
        
        Args:
            credentials: Dict containing:
                - connect_info: Dict with all fields (username, password, host, port, database)
            
        Raises:
            ValueError: If credentials are invalid
            RuntimeError: If connection fails
        """
        logging.info(f"credentials: {credentials}")

        # Get all fields from connect_info
        connect_info = credentials.get("connect_info", {})
        if not connect_info:
            raise ValueError("connect_info is required for PostgreSQL connection")

        username = connect_info.get("username", "")
        password = connect_info.get("password", "")
        host = connect_info.get("host", "")
        port = int(connect_info.get("port", ""))
        database = connect_info.get("database", "")

        if not username or not password:
            raise ValueError("Username and password are required")

        try:
            # Test connection using psycopg (project's existing PostgreSQL driver)
            logging.info(f"Validating PostgreSQL connection to {host}:{port}/{database}")

            conn = await asyncio.wait_for(
                psycopg.AsyncConnection.connect(
                    host=host,
                    port=port,
                    dbname=database,
                    user=username,
                    password=password,
                ),
                timeout=self.connection_timeout
            )

            # Test query to verify connection
            async with conn.cursor() as cur:
                await cur.execute("SELECT version()")
                result = await cur.fetchone()
                version = result[0] if result else "Unknown"
                logging.info(f"PostgreSQL connection validated successfully: {version[:80]}...")

            await conn.close()

        except asyncio.TimeoutError:
            raise RuntimeError(f"Connection timeout after {self.connection_timeout} seconds")
        except psycopg.OperationalError as e:
            error_msg = str(e).lower()
            if "password" in error_msg or "authentication" in error_msg:
                raise ValueError("Invalid username or password")
            elif "database" in error_msg and "does not exist" in error_msg:
                raise ValueError(f"Database '{database}' does not exist")
            elif "connection" in error_msg or "host" in error_msg:
                raise RuntimeError(f"Cannot connect to {host}:{port} - check host/port and network")
            else:
                raise RuntimeError(f"Connection failed: {str(e)}")
        except Exception as e:
            logging.error(f"PostgreSQL connection validation failed: {str(e)}")
            raise RuntimeError(f"Connection failed: {str(e)}")

    # ===== Required abstract methods (no-op implementations) =====

    async def format_data(self, raw_data: Dict[str, Any]) -> StandardPulseData:
        """Not used - configuration only"""
        request_id = self.generate_request_id()
        user_id = raw_data.get("user_id", "")
        return self._create_empty_response(request_id, user_id)

    async def save_raw_data_to_db(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Not used - configuration only"""
        return []

    async def is_data_already_processed(self, raw_data: Dict[str, Any]) -> bool:
        """Not used - configuration only"""
        return False

    async def pull_from_vendor_api(self, username: str, password: str) -> List[Dict[str, Any]]:
        """Not used - configuration only"""
        return []
