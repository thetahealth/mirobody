"""
Core data models module

Defines data models and structures shared by all Platforms and Providers
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

from .constants import DataType, LinkType, ProviderStatus


class ConnectInfoField(BaseModel):
    """Connection info field definition for providers that need extra configuration"""

    field_name: str = Field(..., description="Field name (e.g., 'host', 'port', 'region')")
    field_type: str = Field(..., description="Field type: 'string', 'number', 'select', 'password'")
    required: bool = Field(default=True, description="Whether this field is required")
    label: str = Field(..., description="Display label for frontend")
    placeholder: Optional[str] = Field(default=None, description="Placeholder text")
    default_value: Optional[str] = Field(default=None, description="Default value")
    options: Optional[List[str]] = Field(default=None, description="Options for select type")


class ProviderInfo(BaseModel):
    """Provider information model"""

    slug: str = Field(..., description="Provider unique identifier")
    name: str = Field(..., description="Provider display name")
    description: str = Field(default="", description="Provider description")
    logo: Optional[str] = Field(default=None, description="Provider logo URL")
    supported: bool = Field(default=True, description="Whether supported")
    auth_type: LinkType = Field(default=LinkType.OAUTH, description="Authentication type")
    status: ProviderStatus = Field(default=ProviderStatus.AVAILABLE, description="Provider status")
    platform: Optional[str] = Field(default=None, description="Belonging platform identifier")
    connect_info_fields: Optional[List[ConnectInfoField]] = Field(
        default=None,
        description="Extra connection fields needed by this provider (e.g., host, port for databases)"
    )

class UserProvider(BaseModel):
    """User provider connection information"""

    slug: str = Field(..., description="Provider identifier")
    status: ProviderStatus = Field(..., description="Connection status")
    platform: str = Field(..., description="Belonging platform")
    connected_at: Optional[str] = Field(default=None, description="Connection time")
    last_sync_at: Optional[str] = Field(default=None, description="Last sync time")
    record_count: Optional[int] = Field(default=0, description="Data record count")
    llm_access: Optional[int] = Field(default=1, description="LLM access level (0: forbidden, 1: limited, 2: full)")


class LinkRequest(BaseModel):
    """Provider connection request"""

    user_id: str = Field(..., description="User ID")
    provider_slug: str = Field(..., description="Provider identifier")
    auth_type: LinkType = Field(default=LinkType.OAUTH, description="Authentication type")
    credentials: Dict[str, Any] = Field(default_factory=dict, description="Authentication credentials")
    options: Dict[str, Any] = Field(default_factory=dict, description="Connection options")
    platform: str = Field(..., description="Platform identifier")


class StandardHealthData(BaseModel):
    """Standardized health data model

    All Providers should convert raw data to this unified format
    """

    # Basic metadata
    user_id: str = Field(..., description="User ID")
    provider_slug: str = Field(..., description="Data source provider")
    data_type: DataType = Field(..., description="Data type")
    source_id: Optional[str] = Field(default=None, description="Data source unique ID")

    # Time information
    recorded_at: datetime = Field(..., description="Data recording time")
    received_at: datetime = Field(default_factory=datetime.utcnow, description="Data receiving time")
    start_time: Optional[datetime] = Field(default=None, description="Data start time (for time period data)")
    end_time: Optional[datetime] = Field(default=None, description="Data end time (for time period data)")

    # Data content
    value: Optional[float] = Field(default=None, description="Numeric data value")
    unit: Optional[str] = Field(default=None, description="Data unit")
    raw_data: Dict[str, Any] = Field(default_factory=dict, description="Raw data")
    processed_data: Dict[str, Any] = Field(default_factory=dict, description="Processed data")

    # Quality and status
    quality_score: Optional[float] = Field(default=None, description="Data quality score 0-1")
    is_validated: bool = Field(default=False, description="Whether validated")
    tags: List[str] = Field(default_factory=list, description="Data tags")

    # Metadata
    device_info: Optional[Dict[str, Any]] = Field(default=None, description="Device information")
    location_info: Optional[Dict[str, Any]] = Field(default=None, description="Location information")
    notes: Optional[str] = Field(default=None, description="Notes")


class WebhookEvent(BaseModel):
    """Generic webhook event model"""

    event_id: Optional[str] = Field(default=None, description="Event ID")
    event_type: str = Field(..., description="Event type")
    provider_slug: str = Field(..., description="Provider identifier")
    user_id: Optional[str] = Field(default=None, description="User ID")
    client_user_id: Optional[str] = Field(default=None, description="Client user ID")
    team_id: Optional[str] = Field(default=None, description="Team ID")
    data: Dict[str, Any] = Field(default_factory=dict, description="Event data")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Event timestamp")
    signature: Optional[str] = Field(default=None, description="Event signature")
    retry_count: int = Field(default=0, description="Retry count")


class ProcessingResult(BaseModel):
    """Data processing result"""

    success: bool = Field(..., description="Whether processing succeeded")
    processed_count: int = Field(default=0, description="Number of processed data")
    error_count: int = Field(default=0, description="Number of erroneous data")
    skipped_count: int = Field(default=0, description="Number of skipped data")
    message: str = Field(default="", description="Processing message")
    errors: List[str] = Field(default_factory=list, description="Error list")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Processing metadata")


class HealthDataBatch(BaseModel):
    """Health data batch"""

    batch_id: str = Field(..., description="Batch ID")
    user_id: str = Field(..., description="User ID")
    provider_slug: str = Field(..., description="Provider identifier")
    data_type: DataType = Field(..., description="Data type")
    data_list: List[StandardHealthData] = Field(..., description="Data list")
    created_at: datetime = Field(default_factory=datetime.utcnow, description="Creation time")
    processed_at: Optional[datetime] = Field(default=None, description="Processing time")
    result: Optional[ProcessingResult] = Field(default=None, description="Processing result")


class ProviderMetrics(BaseModel):
    """Provider performance metrics"""

    provider_slug: str = Field(..., description="Provider identifier")
    total_requests: int = Field(default=0, description="Total requests")
    successful_requests: int = Field(default=0, description="Successful requests")
    failed_requests: int = Field(default=0, description="Failed requests")
    average_response_time: float = Field(default=0.0, description="Average response time (seconds)")
    last_request_at: Optional[datetime] = Field(default=None, description="Last request time")
    error_rate: float = Field(default=0.0, description="Error rate")
    uptime_percentage: float = Field(default=100.0, description="Uptime percentage")
