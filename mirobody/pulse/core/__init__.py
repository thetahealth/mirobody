"""
Core module

Provides common functionalities for all Platforms and Providers, including:
- Unified logging
- Common data models and enumerations
- Standard data processing flow
- Common database operations
- Webhook processing framework
- Unified background task scheduler
- Encapsulated push service
"""

from .constants import (
    CacheConfig,
    CommonConfig,
    DataType,
    DocStatus,
    DocType,
    LinkType,
    ProcessAction,
    ProviderStatus,
    ResourceType,
)
from .database import BaseDatabaseService, CacheableDatabaseService

# Add new indicator and unit management
from .indicators_info import (
    StandardIndicator,
    get_all_indicators_info,
    get_standard_unit,
    is_valid_indicator,
)
from .models import (
    HealthDataBatch,
    LinkRequest,
    ProcessingResult,
    ProviderInfo,
    ProviderMetrics,
    StandardHealthData,
    UserProvider,
    WebhookEvent,
)
from .push_service import PushService, push_service
from .scheduler import PullTask, Scheduler, scheduler
from .units import convert_to_standard, get_all_units_info

__all__ = [
    # Constants and enumerations
    "LinkType",
    "ProviderStatus",
    "DataType",
    "DocType",
    "DocStatus",
    "ResourceType",
    "ProcessAction",
    "CacheConfig",
    "CommonConfig",
    # Data models
    "LinkRequest",
    "ProviderInfo",
    "UserProvider",
    "StandardHealthData",
    "WebhookEvent",
    "ProcessingResult",
    "HealthDataBatch",
    "ProviderMetrics",
    # Base services
    "BaseDatabaseService",
    "CacheableDatabaseService",
    # Scheduler and push service
    "PullTask",
    "Scheduler",
    "scheduler",
    "PushService",
    "push_service",
    # Indicator and unit management
    "StandardIndicator",
    "is_valid_indicator",
    "get_standard_unit",
    "get_all_indicators_info",
    # Unit conversion - Core API
    "convert_to_standard",  # Main API for unit conversion (includes indicator-specific logic)
    "get_all_units_info",  # Get all units info for frontend
]
