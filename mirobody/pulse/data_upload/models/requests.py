"""
Health API request models
"""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field


class VitalHealthRecord(BaseModel):
    """Vital health record"""

    source: str = Field(..., description="Data source")
    type: str = Field(..., description="Data type")
    timestamp: int = Field(..., description="Timestamp (milliseconds)")
    unit: Optional[str] = Field(None, description="Unit")
    value: float = Field(..., description="Value")
    timezone: Optional[str] = Field("UTC", description="Timezone info, e.g. America/Los_Angeles")  # Add timezone field


class VitalHealthRequest(BaseModel):
    """Vital health data request"""

    request_id: Optional[str] = Field(None, description="Request ID")
    healthData: List[VitalHealthRecord] = Field(..., description="Vital signs data")


# ==================== StandardPulseData Series Models ====================
# Migrated from pulse/core/models.py to solve circular dependency issues
# Based on .cursorrules specifications (lines 289-312)


class StandardPulseMetaInfo(BaseModel):
    """Pulse standard metadata format"""

    userId: str = Field(..., description="User ID")
    requestId: Optional[str] = Field(None, description="Request ID")
    timestamp: Optional[str] = Field(None, description="Request timestamp")
    source: Optional[str] = Field(None, description="Data source")
    timezone: str = Field(default="UTC", description="Timezone")
    taskId: Optional[str] = Field(None, description="Task ID, used to identify data from the same batch")


class StandardPulseRecord(BaseModel):
    """Pulse standard data record format

    Compatible with VitalHealthRecord to avoid unnecessary format conversion
    """

    source: str = Field(..., description="Data source, e.g. vital.garmin")
    type: str = Field(..., description="Data type, e.g. heartrate")
    timestamp: int = Field(..., description="Timestamp (milliseconds)")
    unit: Optional[str] = Field(None, description="Unit")
    value: Union[float, str] = Field(..., description="Value")  # Required field, consistent with VitalHealthRecord
    timezone: Optional[str] = Field(default="UTC", description="Timezone info, e.g. America/Los_Angeles")

    # Extended fields for complex data (VitalHealthRecord compatible, will be ignored)
    startTime: Optional[int] = Field(None, description="Start timestamp (milliseconds)")
    endTime: Optional[int] = Field(None, description="End timestamp (milliseconds)")

    # Extended fields for apple health
    source_id: Optional[str] = Field(None, description="Data source ID")
    task_id: Optional[str] = Field(None, description="Task ID")
    
    # Extended field for custom comment (e.g., meal details, food items)
    comment: Optional[str] = Field(None, description="Custom comment to be merged with system-generated comment")


class StandardPulseData(BaseModel):
    """Pulse standard data format

    This is the unified format that all Platform internal Event Providers should return,
    replacing the original Dict[str, Any] to provide type safety and standardization
    """

    metaInfo: StandardPulseMetaInfo = Field(..., description="Meta information")
    healthData: List[StandardPulseRecord] = Field(..., description="Health data record list")

    # Optional batch information
    batchInfo: Optional[Dict[str, Any]] = Field(None, description="Batch processing information")
    processingInfo: Optional[Dict[str, Any]] = Field(None, description="Processing status information")
