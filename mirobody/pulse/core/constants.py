"""
Core constants and enumerations module

Defines constants, enumerations and configurations shared by all Platforms and Providers
"""

from enum import Enum


class LinkType(str, Enum):
    """Connection type enumeration"""

    OAUTH1 = "oauth1"  # OAuth 1.0a (like Garmin)
    OAUTH2 = "oauth2"  # OAuth 2.0 (like Whoop)
    OAUTH = "oauth"   # Keep for backward compatibility, defaults to OAuth2
    PASSWORD = "password"
    TOKEN = "token"
    API_KEY = "api_key"
    LINK_TOKEN = "link_token"
    EMAIL = "email"
    SERVICE = "service"
    PLATFORM = "platform"  # Platform-level virtual provider
    CUSTOMIZED = "customized"  # Customized connection with dynamic fields (e.g., database connections)
    NONE = "none"


class ProviderStatus(str, Enum):
    """Provider status enumeration"""

    AVAILABLE = "available"
    CONNECTED = "connected"
    DISCONNECTED = "disconnected"
    RECONNECT = "reconnect"  # Need to reconnect (reconnect=1 in database)
    ERROR = "error"
    MAINTENANCE = "maintenance"









class CacheConfig:
    """Cache configuration"""

    DEFAULT_TTL = 24 * 60 * 60  # 24 hours
    PROVIDER_CACHE_TTL = 24 * 60 * 60  # Provider cache 24 hours
    USER_CACHE_TTL = 5 * 60  # User info cache 5 minutes


