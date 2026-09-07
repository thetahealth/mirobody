"""
Core data models module

Defines data models and structures shared by all Platforms and Providers
"""

from typing import Any

from pydantic import BaseModel, Field

from .constants import LinkType, ProviderStatus


class ConnectInfoField(BaseModel):
    """Connection info field definition for providers that need extra configuration"""

    field_name: str = Field(..., description="Field name (e.g., 'host', 'port', 'region')")
    field_type: str = Field(..., description="Field type: 'string', 'number', 'select', 'password'")
    required: bool = Field(default=True, description="Whether this field is required")
    label: str = Field(..., description="Display label for frontend")
    placeholder: str | None = Field(default=None, description="Placeholder text")
    default_value: str | None = Field(default=None, description="Default value")
    options: list[str] | None = Field(default=None, description="Options for select type")


class ProviderInfo(BaseModel):
    """Provider information model"""

    slug: str = Field(..., description="Provider unique identifier")
    name: str = Field(..., description="Provider display name")
    description: str = Field(default="", description="Provider description")
    logo: str | None = Field(default=None, description="Provider logo URL")
    supported: bool = Field(default=True, description="Whether supported")
    auth_type: LinkType = Field(default=LinkType.OAUTH, description="Authentication type")
    status: ProviderStatus = Field(default=ProviderStatus.AVAILABLE, description="Provider status")
    platform: str | None = Field(default=None, description="Belonging platform identifier")
    connect_info_fields: list[ConnectInfoField] | None = Field(
        default=None,
        description="Extra connection fields needed by this provider (e.g., host, port for databases)"
    )

class UserProvider(BaseModel):
    """User provider connection information"""

    slug: str = Field(..., description="Provider identifier")
    status: ProviderStatus = Field(..., description="Connection status")
    platform: str = Field(..., description="Belonging platform")
    connected_at: str | None = Field(default=None, description="Connection time")
    last_sync_at: str | None = Field(default=None, description="Last sync time")
    record_count: int | None = Field(default=0, description="Data record count")
    llm_access: int | None = Field(default=1, description="LLM access level (0: forbidden, 1: limited, 2: full)")


class LinkRequest(BaseModel):
    """Provider connection request"""

    user_id: str = Field(..., description="User ID")
    provider_slug: str = Field(..., description="Provider identifier")
    auth_type: LinkType = Field(default=LinkType.OAUTH, description="Authentication type")
    credentials: dict[str, Any] = Field(default_factory=dict, description="Authentication credentials")
    options: dict[str, Any] = Field(default_factory=dict, description="Connection options")
    platform: str = Field(..., description="Platform identifier")






